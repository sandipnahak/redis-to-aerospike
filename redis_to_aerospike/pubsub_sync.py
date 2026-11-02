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
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .config import MigrationConfig, RedisConfig
from .converters.registry import ConverterRegistry
from .keyspace import (
    KeyChange,
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

ConnectFn = Callable[[str, int], Any]
DiscoverFn = Callable[[], list[tuple[str, int]]]


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
    ):
        self._config = config
        self._client = client
        self._pattern = pattern
        self._db = db
        self._connect = connect or (lambda host, port: open_node_client(config.redis, host, port))
        self._discover = discover or (lambda: primary_endpoints(client, config.redis))
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
        self._listener = listener or KeyspaceListener(
            config,
            source.client,
            pattern=self._pattern,
            db=db,
        )

    def run(
        self,
        stop: threading.Event,
        before_follow: Optional[Callable[[], None]] = None,
    ) -> MigrationStats:
        """Follow notifications until ``stop`` is set.

        ``before_follow`` runs first when the mode is sync, so the keyspace
        copy finishes before the tail starts. Events published during that
        scan are not buffered.
        """
        self.stats.resume()
        try:
            if before_follow is not None:
                before_follow()
                self.stats.resume()
            logger.info(
                "pubsub channel %s pattern %s (notify-keyspace-events=%s)",
                self._family,
                self._pattern,
                self._notify_flags if self._notify_flags is not None else "unreadable",
            )
            live = self._listener.start()
            if live == 0:
                self.stats.record_error("pubsub:connect")
                logger.error("no Redis nodes accepted a keyspace subscription")
                return self.stats
            self._follow(stop)
            return self.stats
        finally:
            self._listener.stop()
            self.stats.finish()

    def apply(self, change: KeyChange) -> None:
        """Apply one notification. Safe to call from worker threads."""
        try:
            self._apply(change)
        except Exception as exc:
            self.stats.record_error(f"pubsub:{type(exc).__name__}")
            logger.warning(
                "notification failed for key '%s' event '%s': %s",
                change.key,
                change.event,
                exc,
            )

    def _apply(self, change: KeyChange) -> None:
        self.stats.record_event()
        if not key_matches(change.key, self._config.redis.scan_match):
            self.stats.record_skipped("match")
            return
        if change.action == "ignore":
            self.stats.record_skipped(change.event)
            return
        self._event_limiter.acquire()
        if change.action == "delete":
            self._migrator.delete_key(change.key)
            return

        record = self._source.read_record(change.key)
        if record is None:
            # The key disappeared between the notification and the read.
            self._migrator.delete_key(change.key)
            return
        if record.value is None or not self._registry.supports(record.type):
            self.stats.record_skipped(record.type or "unsupported")
            return
        self._migrator.write_redis_record(record, full_replace=True)

    def _follow(self, stop: threading.Event) -> None:
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
        progress = ProgressReporter(self.stats, self._config.progress_interval).start()
        try:
            while not stop.is_set():
                try:
                    change = self._listener.queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                queues[_shard(change.key, workers_n)].put(change)
        finally:
            self._listener.stop()
            while True:
                try:
                    change = self._listener.queue.get_nowait()
                except queue.Empty:
                    break
                queues[_shard(change.key, workers_n)].put(change)
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
                self.apply(item)
            finally:
                work.task_done()


def _shard(key: str | bytes, n: int) -> int:
    raw = key if isinstance(key, bytes) else key.encode("utf-8")
    return zlib.crc32(raw) % n
