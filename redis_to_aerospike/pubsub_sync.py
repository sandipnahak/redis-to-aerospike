"""Follow Redis/MemoryDB keyspace notifications into Aerospike.

A mutation is applied by re-reading the key and writing a full snapshot, so
the Aerospike record matches Redis without reimplementing every command.
``del``, ``expired``, ``evicted``, ``rename_from``, and ``move_from`` delete
the Aerospike record. Cluster notifications are node-local, so each primary
gets its own ``PSUBSCRIBE``.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
import uuid
import zlib
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .config import MigrationConfig, RedisConfig, RunMode
from .event_log import EventLog, delete_entry, record_from_entry, upsert_entry
from .converters.registry import ConverterRegistry
from .keyspace import (
    KeyChange,
    KeyspaceNotifyConfig,
    key_matches,
    notification_db,
    parse_notification,
    select_channel_family,
    standalone_endpoint,
    subscription_pattern,
)
from .migrator import Migrator
from .progress import ProgressReporter
from .ratelimit import TokenBucket
from .redis_source import RedisSource, open_node_client
from .stats import MigrationStats

logger = logging.getLogger(__name__)

_STOP = object()


@dataclass(frozen=True)
class _Work:
    """One notification plus the receive-order sequence reserved for the log."""

    seq: int
    change: KeyChange

ConnectFn = Callable[[str, int], Any]
DiscoverFn = Callable[[], list[tuple[str, int]]]


class CatchupBuffer:
    """Latest notification per key while a scan is still writing.

    The listener drains Redis immediately, so the server-side pub/sub buffer
    stays small, but nothing is applied until the scan has finished. Applying
    during the scan would let a slow SCAN write overwrite a newer value.
    After :meth:`release`, further events sit in an unbounded side list until
    :meth:`seal` hands them to the live tail, in order.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._pending: dict[str | bytes, KeyChange] = {}
        self._released = False
        self._ahead: Optional[deque[KeyChange]] = None

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def offer(self, change: KeyChange) -> bool:
        """Keep ``change`` here. Return False when the live queue must take it."""
        with self._lock:
            if not self._released:
                # keymiss is not a write. Replacing a real change with it would
                # skip the replay of a key that was updated earlier in the scan.
                if change.action != "ignore":
                    self._pending[change.key] = change
                return True
            if self._ahead is not None:
                self._ahead.append(change)
                return True
            return False

    def release(self) -> list[KeyChange]:
        """Close the scan window. Return one change per key, oldest key first."""
        with self._lock:
            batch = list(self._pending.values())
            self._pending.clear()
            self._released = True
            if self._ahead is None:
                self._ahead = deque()
            return batch

    def seal(self) -> list[KeyChange]:
        """Stop the side list. Return events that arrived after :meth:`release`."""
        with self._lock:
            items = list(self._ahead or [])
            self._ahead = None
            return items


def primary_endpoints(client: Any, config: RedisConfig) -> list[tuple[str, int]]:
    """Primaries to subscribe to.

    Standalone is a single endpoint. Cluster mode asks the client for
    primaries only: replicas emit the same expiry events for keys they hold,
    and subscribing to them would delete or rewrite the same key twice.
    """
    if not config.cluster:
        return [standalone_endpoint(config)]

    getter = getattr(client, "get_primaries", None)
    nodes = list(getter()) if callable(getter) else []
    endpoints: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for node in nodes:
        host = getattr(node, "host", None)
        port = getattr(node, "port", None)
        if not host or not port:
            continue
        endpoint = (str(host), int(port))
        if endpoint not in seen:
            seen.add(endpoint)
            endpoints.append(endpoint)
    if not endpoints:
        raise RuntimeError(
            "Redis cluster returned no primary nodes to subscribe to for keyspace notifications"
        )
    return endpoints


def probe_keyspace_notifications(
    config: MigrationConfig,
    client: Any,
    *,
    connect: Optional[ConnectFn] = None,
    discover: Optional[DiscoverFn] = None,
    write_timeout_s: float = 3.0,
    expire_timeout_s: float = 3.0,
    expire_wait_s: float = 0.3,
) -> KeyspaceNotifyConfig:
    """Confirm notifications by writing a temporary key.

    MemoryDB does not implement ``CONFIG GET``. A rejected read is not evidence
    that ``notify-keyspace-events`` is off. This subscribes on every primary,
    writes ``redis2aerospike:notify-probe:<id>``, and waits for that key's
    event. A write event means the parameter publishes changes (``KEA``).
    No write event, then an ``expired`` event, means ``Ex``. No event means
    notifications are not active. The key is deleted before this returns.
    """
    key = f"redis2aerospike:notify-probe:{uuid.uuid4().hex}"
    db = notification_db(config.redis)
    patterns = (
        subscription_pattern("keyspace", db),
        subscription_pattern("keyevent", db),
    )
    connect_fn = connect or (lambda host, port: open_node_client(config.redis, host, port))
    discover_fn = discover or (lambda: primary_endpoints(client, config.redis))
    try:
        endpoints = discover_fn()
    except Exception as exc:
        return KeyspaceNotifyConfig(detail=f"probe failed: {exc}")
    if not endpoints:
        return KeyspaceNotifyConfig(detail="probe failed: no primary nodes")

    found: queue.Queue[KeyChange] = queue.Queue()
    stop = threading.Event()
    ready: list[threading.Event] = []
    threads: list[threading.Thread] = []
    for host, port in endpoints:
        gate = threading.Event()
        ready.append(gate)
        thread = threading.Thread(
            target=_probe_subscribe,
            args=(connect_fn, host, port, patterns, db, key, found, stop, gate),
            name=f"notify-probe-{host}:{port}",
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not all(gate.is_set() for gate in ready):
        time.sleep(0.01)
    if not any(gate.is_set() for gate in ready):
        stop.set()
        for thread in threads:
            thread.join(timeout=2)
        return KeyspaceNotifyConfig(detail="probe failed: no node accepted the subscription")

    try:
        try:
            client.set(key, b"1", px=15_000)
        except Exception as exc:
            return KeyspaceNotifyConfig(detail=f"probe failed: {exc}")
        change = _wait_probe(found, write_timeout_s)
        if change is not None and change.event != "expired":
            logger.info(
                "notify-keyspace-events is active (test write produced %s)",
                change.event,
            )
            return KeyspaceNotifyConfig(
                flags="KEA",
                readable=True,
                present=True,
                detail="confirmed by a test write",
            )
        if change is not None and change.event == "expired":
            logger.info("notify-keyspace-events is active for expired keys only")
            return KeyspaceNotifyConfig(
                flags="Ex",
                readable=True,
                present=True,
                detail="confirmed by an expiring test key",
            )
        try:
            client.delete(key)
        except Exception:
            logger.debug("could not delete notify probe key before the expiry check", exc_info=True)
        try:
            client.set(key, b"1", px=200)
        except Exception as exc:
            return KeyspaceNotifyConfig(detail=f"probe failed: {exc}")
        time.sleep(expire_wait_s)
        try:
            client.get(key)
        except Exception:
            logger.debug("expiry probe read failed", exc_info=True)
        change = _wait_probe(found, expire_timeout_s)
        if change is not None and change.event == "expired":
            logger.info("notify-keyspace-events is active for expired keys only")
            return KeyspaceNotifyConfig(
                flags="Ex",
                readable=True,
                present=True,
                detail="confirmed by an expiring test key",
            )
        if change is not None:
            logger.info(
                "notify-keyspace-events is active (test write produced %s)",
                change.event,
            )
            return KeyspaceNotifyConfig(
                flags="KEA",
                readable=True,
                present=True,
                detail="confirmed by a test write",
            )
        logger.error(
            "notify probe key %s produced no notification; "
            "notify-keyspace-events is not active",
            key,
        )
        return KeyspaceNotifyConfig(readable=True, present=False, detail="probe")
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=2)
        try:
            client.delete(key)
        except Exception:
            logger.debug("could not delete notify probe key %s", key, exc_info=True)


def _probe_subscribe(
    connect: ConnectFn,
    host: str,
    port: int,
    patterns: tuple[str, str],
    db: int,
    key: str,
    found: "queue.Queue[KeyChange]",
    stop: threading.Event,
    ready: threading.Event,
) -> None:
    client = None
    pubsub = None
    try:
        client = connect(host, port)
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        pubsub.psubscribe(*patterns)
        ready.set()
        while not stop.is_set():
            try:
                message = pubsub.get_message(timeout=0.5)
            except Exception as exc:
                logger.debug("notify probe read failed on %s:%s: %s", host, port, exc)
                break
            if not message or message.get("type") != "pmessage":
                continue
            change = parse_notification(message.get("channel"), message.get("data"), db)
            if change is None or change.key != key or change.action == "ignore":
                continue
            found.put(change)
    except Exception as exc:
        logger.debug("notify probe subscribe failed on %s:%s: %s", host, port, exc)
    finally:
        ready.set()
        _close_quietly(pubsub)
        _close_quietly(client)


def _wait_probe(found: "queue.Queue[KeyChange]", timeout_s: float) -> KeyChange | None:
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        try:
            return found.get(timeout=min(0.2, remaining))
        except queue.Empty:
            continue


def _close_quietly(obj: Any) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:
        pass


@dataclass
class _Subscription:
    endpoint: tuple[str, int]
    stop: threading.Event
    thread: Optional[threading.Thread] = None
    ready: threading.Event = field(default_factory=threading.Event)
    failed: bool = False


class KeyspaceListener:
    """Subscribe to keyspace notifications and queue :class:`KeyChange` values.

    ``connect`` and ``discover`` are seams for tests. By default, discovery
    uses the data client and each subscription is a new connection so the
    SCAN/read client is never blocked inside ``PSUBSCRIBE``.
    """

    def __init__(
        self,
        config: MigrationConfig,
        client: Any,
        *,
        pattern: str,
        db: int,
        connect: Optional[ConnectFn] = None,
        discover: Optional[DiscoverFn] = None,
        catchup: Optional[CatchupBuffer] = None,
    ):
        self._config = config
        self._client = client
        self._pattern = pattern
        self._db = db
        self._connect = connect or (lambda host, port: open_node_client(config.redis, host, port))
        self._discover = discover or (lambda: primary_endpoints(client, config.redis))
        self._catchup = catchup
        self._queue: queue.Queue[KeyChange] = queue.Queue(maxsize=max(1, config.queue_size))
        self._stop = threading.Event()
        self._first_pass = threading.Event()
        self._lock = threading.Lock()
        self._subs: dict[tuple[str, int], _Subscription] = {}
        self._manager: Optional[threading.Thread] = None
        self._closed = False
        self._last_restart = 0.0

    @property
    def queue(self) -> "queue.Queue[KeyChange]":
        return self._queue

    def start(self) -> int:
        """Start subscriptions and return how many nodes are listening."""
        self._manager = threading.Thread(
            target=self._manage, name="keyspace-listener", daemon=True
        )
        self._manager.start()
        if not self._first_pass.wait(timeout=30):
            logger.error("timed out discovering nodes for keyspace pub/sub")
            return 0
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            with self._lock:
                subs = list(self._subs.values())
            if not subs or all(sub.ready.is_set() for sub in subs):
                break
            time.sleep(0.01)
        live = self.live_count()
        if live:
            targets = ", ".join(f"{host}:{port}" for host, port in self._endpoints())
            logger.info(
                "listening for keyspace notifications on %d node(s) pattern=%s [%s]",
                live,
                self._pattern,
                targets,
            )
        return live

    def live_count(self) -> int:
        with self._lock:
            return sum(
                1
                for sub in self._subs.values()
                if sub.ready.is_set()
                and not sub.failed
                and sub.thread is not None
                and sub.thread.is_alive()
            )

    def stop(self) -> None:
        """Stop discovery and every subscription. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        manager = self._manager
        if manager is not None and manager.is_alive() and manager is not threading.current_thread():
            manager.join(timeout=5)
        with self._lock:
            subs = list(self._subs.values())
        for sub in subs:
            sub.stop.set()
        for sub in subs:
            if sub.thread is not None and sub.thread is not threading.current_thread():
                sub.thread.join(timeout=5)

    def _endpoints(self) -> list[tuple[str, int]]:
        with self._lock:
            return list(self._subs)

    def _manage(self) -> None:
        refresh = self._config.pubsub_refresh_s
        next_discover = 0.0
        try:
            while not self._stop.is_set():
                now = time.monotonic()
                if now >= next_discover:
                    try:
                        desired = self._discover()
                    except Exception as exc:
                        logger.error("could not list Redis nodes for pub/sub: %s", exc)
                        desired = self._endpoints()
                    self._reconcile(desired)
                    if not self._first_pass.is_set():
                        self._first_pass.set()
                    if refresh > 0:
                        next_discover = now + refresh
                    else:
                        next_discover = float("inf")
                if now - self._last_restart >= 5:
                    self._restart_dead()
                    self._last_restart = now
                if self._stop.wait(0.5):
                    break
        finally:
            self._first_pass.set()
            with self._lock:
                subs = list(self._subs.values())
            for sub in subs:
                sub.stop.set()
            for sub in subs:
                if sub.thread is not None and sub.thread is not threading.current_thread():
                    sub.thread.join(timeout=5)

    def _restart_dead(self) -> None:
        with self._lock:
            dead = [
                endpoint
                for endpoint, sub in self._subs.items()
                if sub.thread is not None and not sub.thread.is_alive()
            ]
        for endpoint in dead:
            logger.warning("restarting keyspace subscription to %s:%s", *endpoint)
            self._drop(endpoint)
            self._start_one(endpoint)

    def _reconcile(self, desired: list[tuple[str, int]]) -> None:
        with self._lock:
            dead = [
                endpoint
                for endpoint, sub in self._subs.items()
                if sub.thread is not None and not sub.thread.is_alive()
            ]
        for endpoint in dead:
            self._drop(endpoint)
        wanted = set(desired)
        with self._lock:
            current = set(self._subs)
        for endpoint in current - wanted:
            logger.info("stopping keyspace subscription to %s:%s", *endpoint)
            self._drop(endpoint)
        for endpoint in desired:
            with self._lock:
                present = endpoint in self._subs
            if not present:
                self._start_one(endpoint)

    def _start_one(self, endpoint: tuple[str, int]) -> None:
        sub = _Subscription(endpoint=endpoint, stop=threading.Event())

        def run() -> None:
            self._serve(sub)

        sub.thread = threading.Thread(
            target=run, name=f"keyspace-{endpoint[0]}:{endpoint[1]}", daemon=True
        )
        with self._lock:
            self._subs[endpoint] = sub
        sub.thread.start()

    def _drop(self, endpoint: tuple[str, int]) -> None:
        with self._lock:
            sub = self._subs.pop(endpoint, None)
        if sub is None:
            return
        sub.stop.set()
        if sub.thread is not None and sub.thread is not threading.current_thread():
            sub.thread.join(timeout=5)

    def _serve(self, sub: _Subscription) -> None:
        client = None
        pubsub = None
        host, port = sub.endpoint
        try:
            client = self._connect(host, port)
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            pubsub.psubscribe(self._pattern)
            sub.ready.set()
            while not sub.stop.is_set() and not self._stop.is_set():
                try:
                    message = pubsub.get_message(timeout=1.0)
                except Exception as exc:
                    logger.warning(
                        "keyspace pub/sub read failed on %s:%s: %s", host, port, exc
                    )
                    break
                if not message or message.get("type") != "pmessage":
                    continue
                change = parse_notification(
                    message.get("channel"), message.get("data"), self._db
                )
                if change is None:
                    continue
                self._put(change, sub)
        except Exception as exc:
            sub.failed = True
            logger.error(
                "keyspace pub/sub subscribe failed on %s:%s: %s", host, port, exc
            )
        finally:
            sub.ready.set()
            self._close_quietly(pubsub)
            self._close_quietly(client)

    def _put(self, change: KeyChange, sub: _Subscription) -> None:
        if self._catchup is not None and self._catchup.offer(change):
            return
        while not sub.stop.is_set() and not self._stop.is_set():
            try:
                self._queue.put(change, timeout=0.5)
                return
            except queue.Full:
                continue

    @staticmethod
    def _close_quietly(obj: Any) -> None:
        if obj is None:
            return
        try:
            obj.close()
        except Exception:
            pass


class PubSubRunner:
    """Apply keyspace notifications with the same converters as a scan."""

    def __init__(
        self,
        config: MigrationConfig,
        source: RedisSource,
        registry: ConverterRegistry,
        sink: Any,
        *,
        stats: Optional[MigrationStats] = None,
        notify_flags: Optional[str] = None,
        listener: Optional[KeyspaceListener] = None,
        event_log: Optional[EventLog] = None,
    ):
        self._config = config
        self._source = source
        self._registry = registry
        self.stats = stats or MigrationStats()
        self._notify_flags = notify_flags
        self._migrator = Migrator(config, source, registry, sink, stats=self.stats)
        self._event_limiter = TokenBucket(config.scan_rate_limit)
        family = select_channel_family(notify_flags, config.pubsub_channel)
        db = notification_db(config.redis)
        self._family = family
        self._pattern = subscription_pattern(family, db)
        self._catchup = (
            CatchupBuffer()
            if listener is None and config.sync_catchup and config.mode is RunMode.SYNC
            else None
        )
        self._event_log = event_log
        self._listener = listener or KeyspaceListener(
            config,
            source.client,
            pattern=self._pattern,
            db=db,
            catchup=self._catchup,
        )

    def run(
        self,
        stop: threading.Event,
        before_follow: Optional[Callable[[], None]] = None,
    ) -> MigrationStats:
        """Follow notifications until ``stop`` is set.

        Without catch-up, ``before_follow`` runs first and the subscription
        starts afterward. Events published during that scan are not buffered.
        With ``sync_catchup``, the subscription starts first, ``before_follow``
        is the scan, and keys that changed during the scan are replayed before
        the live tail.
        """
        self.stats.resume()
        try:
            if self._event_log is None and self._config.event_log:
                try:
                    self._event_log = EventLog(self._config.event_log)
                except OSError as exc:
                    logger.error(
                        "cannot open event log %s: %s", self._config.event_log, exc
                    )
                    self.stats.record_error("event_log")
                    return self.stats
                logger.info(
                    "recording pub/sub changes in order to %s", self._event_log.path
                )
            logger.info(
                "pubsub channel %s pattern %s (notify-keyspace-events=%s)",
                self._family,
                self._pattern,
                self._notify_flags if self._notify_flags is not None else "unreadable",
            )
            early = self._catchup is not None
            if early:
                if not self._subscriptions_up():
                    return self.stats
                logger.info(
                    "subscribed before the scan; keys that change during the copy are replayed"
                )
                flags = self._notify_flags or ""
                if "A" not in flags.upper():
                    logger.warning(
                        "sync_catchup only replays events that notify-keyspace-events "
                        "publishes (%s). KEA covers writes, deletes, and expiries; "
                        "Ex covers expiries only.",
                        flags or "unreadable",
                    )
            if before_follow is not None:
                before_follow()
                self.stats.resume()
            replay: list[KeyChange] = []
            if early and self._catchup is not None:
                replay = self._catchup.release()
                logger.info(
                    "scan finished; replaying %d key(s) that changed during the copy",
                    len(replay),
                )
            else:
                if not self._subscriptions_up():
                    return self.stats
            self._follow(stop, prelude=replay)
            return self.stats
        finally:
            self._listener.stop()
            if self._event_log is not None:
                self._event_log.close()
                self._event_log = None
            self.stats.finish()

    def _subscriptions_up(self) -> bool:
        live = self._listener.start()
        if live == 0:
            self.stats.record_error("pubsub:connect")
            logger.error("no Redis nodes accepted a keyspace subscription")
            return False
        return True

    def apply(self, change: KeyChange, seq: Optional[int] = None) -> None:
        """Apply one notification. Safe to call from worker threads.

        When ``seq`` is set, the outcome is appended to the event log in
        receive order, including when the Aerospike write fails.
        """
        entry = None
        try:
            entry = self._capture(change)
            if entry is None:
                return
            if entry["action"] == "delete":
                self._migrator.delete_key(change.key)
                return
            self._migrator.write_redis_record(
                record_from_entry(entry), full_replace=True
            )
        except Exception as exc:
            self.stats.record_error(f"pubsub:{type(exc).__name__}")
            logger.warning(
                "notification failed for key '%s' event '%s': %s",
                change.key,
                change.event,
                exc,
            )
        finally:
            if self._event_log is not None and seq is not None:
                self._event_log.complete(seq, entry)

    def _capture(self, change: KeyChange) -> Optional[dict[str, Any]]:
        """Decide the log line for one notification. Does not write Aerospike."""
        self.stats.record_event()
        if not key_matches(change.key, self._config.redis.scan_match):
            self.stats.record_skipped("match")
            return None
        if change.action == "ignore":
            self.stats.record_skipped(change.event)
            return None
        self._event_limiter.acquire()
        if change.action == "delete":
            return delete_entry(change.event, change.key)

        record = self._source.read_record(change.key)
        if record is None:
            return delete_entry(change.event, change.key)
        if record.value is None or not self._registry.supports(record.type):
            self.stats.record_skipped(record.type or "unsupported")
            return None
        return upsert_entry(change.event, record)

    def _follow(
        self, stop: threading.Event, prelude: Optional[list[KeyChange]] = None
    ) -> None:
        workers_n = max(1, self._config.workers)
        queues: list[queue.Queue] = [
            queue.Queue(maxsize=max(1, self._config.queue_size)) for _ in range(workers_n)
        ]
        workers: list[threading.Thread] = []
        for index, work in enumerate(queues):
            thread = threading.Thread(
                target=self._worker,
                args=(work,),
                name=f"pubsub-worker-{index}",
                daemon=True,
            )
            thread.start()
            workers.append(thread)

        def enqueue(change: KeyChange) -> None:
            if self._event_log is None:
                queues[_shard(change.key, workers_n)].put(change)
                return
            seq = self._event_log.assign()
            queues[_shard(change.key, workers_n)].put(_Work(seq, change))

        progress = ProgressReporter(self.stats, self._config.progress_interval).start()
        try:
            for change in prelude or []:
                enqueue(change)
            if self._catchup is not None:
                # Events that landed while the scan-window batch was queued.
                for change in self._catchup.seal():
                    enqueue(change)
            while not stop.is_set():
                try:
                    change = self._listener.queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                enqueue(change)
        finally:
            self._listener.stop()
            if self._catchup is not None:
                for change in self._catchup.seal():
                    enqueue(change)
            while True:
                try:
                    change = self._listener.queue.get_nowait()
                except queue.Empty:
                    break
                enqueue(change)
            for work in queues:
                work.put(_STOP)
            for thread in workers:
                thread.join()
            progress.stop()

    def _worker(self, work: queue.Queue) -> None:
        while True:
            item = work.get()
            try:
                if item is _STOP:
                    return
                if isinstance(item, _Work):
                    self.apply(item.change, item.seq)
                else:
                    self.apply(item)
            finally:
                work.task_done()


def _shard(key: str | bytes, n: int) -> int:
    raw = key if isinstance(key, bytes) else key.encode("utf-8")
    return zlib.crc32(raw) % n
