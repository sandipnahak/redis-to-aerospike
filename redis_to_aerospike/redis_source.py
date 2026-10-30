"""Read side of the migration: stream records out of Redis.

``RedisSource`` walks the keyspace with ``SCAN`` (cursor-based, non-blocking) and
materializes each key's value and TTL. Values are returned as their raw Redis
shapes (bytes, dicts, lists, sets, ``(member, score)`` tuples); coercion into
native Aerospike types is the converter layer's job. Reads are pipelined so each
SCAN batch costs only a couple of round-trips.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, Iterable, Iterator, List, Literal, Tuple, cast

import redis
from redis.cluster import RedisCluster
from redis.exceptions import RedisError

from .config import RedisConfig, TtlCommand
from .dump_ttl import ttl_ms_from_dump
from .models import RedisRecord

logger = logging.getLogger(__name__)

# Standalone :class:`redis.Redis` or :class:`redis.cluster.RedisCluster`.
RedisConnection = redis.Redis | RedisCluster

# How remaining expiry is read. ``pttl`` is milliseconds (Redis). ``ttl`` is
# seconds, scaled to milliseconds so the rest of the pipeline stays uniform.
# ``expiretime`` / ``pexpiretime`` are absolute Unix timestamps. ``dump`` reads
# an absolute timestamp from a hash field in the DUMP payload. ``none`` means
# the source has no expiry commands, so every key is treated as persistent.
TtlReadMode = Literal["pttl", "ttl", "dump", "expiretime", "pexpiretime", "none"]

# Value read for each Redis type. Missing from the server COMMAND table means
# the source cannot return that type (MemoryDB Multi-Region has no lists).
_VALUE_COMMANDS = {
    "string": "get",
    "hash": "hgetall",
    "list": "lrange",
    "set": "smembers",
    "zset": "zrange",
}


def _command_names(client: Any) -> set[str] | None:
    """Lowercased command names from a cluster client's COMMAND cache.

    Standalone clients (and test doubles) have no cache and return ``None``,
    which means "try PTTL and learn from the error".
    """
    parser = getattr(client, "commands_parser", None)
    raw = getattr(parser, "commands", None)
    if not isinstance(raw, dict) or not raw:
        return None
    names: set[str] = set()
    for key in raw:
        if isinstance(key, bytes):
            names.add(key.decode("utf-8", "replace").lower())
        else:
            names.add(str(key).lower())
    return names


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _remaining_from_absolute_ms(expiry_ms: int, now_ms: int) -> int:
    remaining = expiry_ms - now_ms
    return remaining if remaining > 0 else 0


def _ensure_keyed_command(client: Any, command: str) -> None:
    """Teach a cluster client how to route *command* when COMMAND omitted it.

    redis-py refuses to send a command that is missing from the server's
    COMMAND table ("DUMP command doesn't exist in Redis commands") even when
    the server accepts it. Register the key position so an explicit
    ``ttl_command`` is actually sent.
    """
    parser = getattr(client, "commands_parser", None)
    commands = getattr(parser, "commands", None)
    if not isinstance(commands, dict):
        return
    key = command.lower()
    if key in commands or key.encode() in commands:
        return
    commands[key] = {
        "name": key,
        "flags": ["readonly", "fast"],
        "first_key_pos": 1,
        "last_key_pos": 1,
        "step_count": 1,
    }


def _is_unsupported_command(exc: BaseException, command: str) -> bool:
    """True when *exc* means the server or cluster client lacks *command*.

    Cluster clients raise ``RedisError`` ("PTTL command doesn't exist in Redis
    commands") without contacting the server. The server itself answers
    ``unknown command`` (``ERR unknown command 'TTL'``) or ``NOPERM``.

    ``ttl`` is matched as its own command so a ``PTTL`` error is not mistaken
    for a missing ``TTL``.
    """
    text = str(exc).lower()
    unsupported = (
        "doesn't exist" in text
        or "does not exist" in text
        or "unknown command" in text
        or "noperm" in text
    )
    if not unsupported:
        return False
    name = command.lower()
    return re.search(rf"(?<![a-z]){re.escape(name)}(?![a-z])", text) is not None


def redis_client_params(config: RedisConfig):
    """Translate a :class:`RedisConfig` into redis-py client arguments.

    Pure (no I/O) and the unit-tested seam for connection wiring. Returns
    ``(cluster, url, kwargs)`` where ``cluster`` selects the cluster client,
    ``url`` (when set) is passed to ``from_url`` and supersedes the discrete
    connection fields, and ``kwargs`` are the constructor keyword arguments.
    """
    # decode_responses stays False so binary-safe string values survive intact;
    # the converter decides how to decode/coerce them.
    kwargs: Dict[str, Any] = {"decode_responses": False}
    if config.socket_timeout is not None:
        kwargs["socket_timeout"] = config.socket_timeout
    if config.socket_connect_timeout is not None:
        kwargs["socket_connect_timeout"] = config.socket_connect_timeout

    if config.url:
        # The URL is the sole source of truth for the connection target;
        # rediss:// already encodes TLS, so the discrete fields are ignored.
        return config.cluster, config.url, kwargs

    kwargs["host"] = config.host
    kwargs["port"] = config.port
    if config.username:
        kwargs["username"] = config.username
    if config.password:
        kwargs["password"] = config.password
    # Redis Cluster forbids a non-zero db and RedisCluster() rejects the kwarg,
    # so only standalone connections carry it.
    if not config.cluster:
        kwargs["db"] = config.db

    if config.ssl:
        kwargs["ssl"] = True
        for key, value in (
            ("ssl_ca_certs", config.ssl_ca_certs),
            ("ssl_certfile", config.ssl_certfile),
            ("ssl_keyfile", config.ssl_keyfile),
            ("ssl_cert_reqs", config.ssl_cert_reqs),
        ):
            if value is not None:
                kwargs[key] = value

    return config.cluster, None, kwargs


class RedisSource:
    """Streams :class:`RedisRecord` objects from a Redis instance."""

    def __init__(self, config: RedisConfig, client: RedisConnection | None = None):
        self._config = config
        self._cluster = config.cluster
        # Chosen on the first read. ``None`` until then so a source that is
        # only pinged never probes TTL support.
        self._ttl_mode: TtlReadMode | None = None
        self._unsupported_read_types: set[str] = set()
        if client is not None:
            self._client = client
            return

        cluster, url, kwargs = redis_client_params(config)
        if cluster:
            self._client = (
                RedisCluster.from_url(url, **kwargs) if url else RedisCluster(**kwargs)
            )
        else:
            self._client = redis.Redis.from_url(url, **kwargs) if url else redis.Redis(**kwargs)

    @property
    def client(self) -> RedisConnection:
        return self._client

    def ping(self) -> bool:
        return bool(self._client.ping())

    def server_info(self) -> Dict[str, Any]:
        """Gather operationally useful facts about the Redis source.

        Returns a dict with the keyspace size of the configured db, memory
        usage, server version, and the number of keys carrying a TTL. Values
        that cannot be read are simply omitted; this is best-effort and never
        raises so it can run unconditionally before a migration.
        """
        info: Dict[str, Any] = {}
        try:
            dbsize = self._client.dbsize()
            # Cluster dbsize may come back as {node: int}; sum it.
            if isinstance(dbsize, dict):
                dbsize = sum(v for v in dbsize.values() if isinstance(v, int))
            info["keys"] = dbsize
        except Exception as exc:
            logger.debug("could not read Redis dbsize: %s", exc)

        try:
            nodes = self._info_nodes("memory")
            used: List[int] = []
            for n in nodes:
                v = n.get("used_memory")
                if isinstance(v, int):
                    used.append(v)
            if used:
                info["used_memory"] = sum(used)
                # In cluster mode the human-readable per-node value is no longer
                # meaningful; fall back to the single node's value otherwise.
                info["used_memory_human"] = (
                    nodes[0].get("used_memory_human") if len(nodes) == 1 else f"{sum(used)}B"
                )
        except Exception as exc:
            logger.debug("could not read Redis memory info: %s", exc)

        try:
            nodes = self._info_nodes("server")
            for node in nodes:
                if node.get("redis_version"):
                    info["redis_version"] = node["redis_version"]
                    break
        except Exception as exc:
            logger.debug("could not read Redis server info: %s", exc)

        try:
            nodes = self._info_nodes("keyspace")
            expires = 0
            found = False
            for node in nodes:
                db_stats = node.get(f"db{self._config.db}")
                if isinstance(db_stats, dict) and db_stats.get("expires") is not None:
                    expires += db_stats["expires"]
                    found = True
            if found:
                info["expires"] = expires
        except Exception as exc:
            logger.debug("could not read Redis keyspace info: %s", exc)

        return info

    def _info_nodes(self, section: str) -> List[Dict[str, Any]]:
        """Return INFO as a list of per-node dicts.

        Standalone returns a single flat dict; the cluster client returns
        ``{node: info_dict}``. Normalizing to a list lets ``server_info``
        aggregate uniformly.
        """
        raw = self._client.info(section)
        # Only the cluster client returns {node: info_dict}. Guarding on the
        # cluster flag avoids misreading a standalone keyspace section (whose
        # db0 value is itself a dict) as a per-node response.
        if (
            self._cluster
            and isinstance(raw, dict)
            and raw
            and all(isinstance(v, dict) for v in raw.values())
        ):
            return list(raw.values())
        return [raw] if isinstance(raw, dict) else []

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def iter_records(self, batch_size: int = 500) -> Iterator[RedisRecord]:
        """Yield every key in the configured keyspace as a :class:`RedisRecord`."""
        for keys in self._iter_key_batches(batch_size):
            if not keys:
                continue
            types, ttls = self._fetch_types_and_ttls(keys)
            values = self._fetch_values(keys, types)
            for key in keys:
                key_type = types[key]
                # The key may have been deleted between SCAN and read.
                if key_type == "none" or key not in values:
                    continue
                value = values[key]
                # Expiry race: the key vanished between the TYPE and value reads,
                # so the value read came back empty/None. Skip it rather than
                # writing a None or empty bin (which Aerospike rejects/deletes).
                if self._is_vanished(key_type, value):
                    continue
                yield RedisRecord(
                    key=self._decode_key(key),
                    type=key_type,
                    value=value,
                    ttl_ms=ttls[key],
                )

    @staticmethod
    def _is_vanished(key_type: str, value) -> bool:
        # Redis never stores empty collections, so an empty read means the key
        # was deleted/expired mid-flight. An empty string (b"") is still a valid
        # string value and must be kept.
        if key_type == "string":
            return value is None
        if key_type in ("hash", "list", "set", "zset"):
            return not value
        return False

    def _iter_key_batches(self, batch_size: int) -> Iterator[List[bytes]]:
        if self._cluster:
            yield from self._iter_cluster_key_batches(batch_size)
            return
        # Non-cluster path uses synchronous ``Redis.scan``; redis-py 7 stubs
        # type ``scan`` as possibly async, so we assert the real sync return shape.
        sync_client = cast(redis.Redis, self._client)
        cursor = 0
        while True:
            page = cast(
                Tuple[int, List[bytes]],
                sync_client.scan(
                    cursor=cursor, match=self._config.scan_match, count=batch_size
                ),
            )
            cursor, keys = page
            if keys:
                yield list(keys)
            if cursor == 0:
                break

    def _iter_cluster_key_batches(self, batch_size: int) -> Iterator[List[bytes]]:
        # A single-node cursor SCAN only walks one shard's slots; scan_iter is
        # routed by redis-py across every primary node. Chunk the flat stream
        # back into batches so the pipelined reads behave exactly as before.
        batch: List[bytes] = []
        for key in self._client.scan_iter(match=self._config.scan_match, count=batch_size):
            batch.append(key)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _fetch_types_and_ttls(self, keys: Iterable[bytes]):
        key_list = list(keys)
        mode = self._ttl_mode
        if mode is None:
            mode = self._initial_ttl_mode()
            self._ttl_mode = mode
            if mode != "pttl":
                self._log_ttl_mode(mode)
        try:
            return self._read_types_and_ttls(key_list, mode)
        except RedisError as exc:
            # Adaptive fallback applies only to the default PTTL command.
            # An explicit ttl_command (ttl, dump, expiretime, ...) is used as
            # given; a missing command is a configuration error.
            if self._config.ttl_command is not TtlCommand.PTTL:
                raise
            # redis-py's cluster client raises this *before* the command is
            # sent when the name is missing from the server COMMAND table:
            # "PTTL command doesn't exist in Redis commands".
            fallback = self._degrade_ttl_mode(mode, exc)
            if fallback is None:
                raise
            self._ttl_mode = fallback
            self._log_ttl_mode(fallback)
            return self._fetch_types_and_ttls(key_list)

    def _initial_ttl_mode(self) -> TtlReadMode:
        configured = self._config.ttl_command
        if configured is TtlCommand.PTTL:
            return self._choose_ttl_mode()
        _ensure_keyed_command(self._client, configured.value)
        return cast(TtlReadMode, configured.value)

    def _choose_ttl_mode(self) -> TtlReadMode:
        """Pick a TTL command from the server's COMMAND table, if it has one.

        Cluster clients cache that table and refuse to route anything absent
        from it. Standalone clients have no table; they try ``PTTL`` and degrade
        on the error instead.
        """
        names = _command_names(self._client)
        if names is None or "pttl" in names:
            return "pttl"
        if "ttl" in names:
            return "ttl"
        return "none"

    def _degrade_ttl_mode(self, mode: TtlReadMode, exc: BaseException) -> TtlReadMode | None:
        # Only fall back to TTL when the server's COMMAND table actually lists
        # it. MemoryDB Multi-Region answers both PTTL and TTL with
        # "ERR unknown command", and sending TTL after PTTL fails aborts the read.
        if mode == "pttl" and _is_unsupported_command(exc, "pttl"):
            names = _command_names(self._client)
            if names is not None and "ttl" in names:
                return "ttl"
            return "none"
        if mode == "ttl" and _is_unsupported_command(exc, "ttl"):
            return "none"
        return None

    def _log_ttl_mode(self, mode: TtlReadMode) -> None:
        if mode == "ttl" and self._config.ttl_command is TtlCommand.PTTL:
            logger.warning(
                "Source does not implement PTTL; reading expiry with TTL "
                "(second precision)."
            )
        elif mode == "none":
            logger.warning(
                "Source implements neither PTTL nor TTL, so keys are migrated "
                "without an expiry. Set redis.ttl_command to dump when the "
                "value stores an absolute expiry (MemoryDB Multi-Region)."
            )
        elif mode == "dump":
            logger.info(
                "Reading key expiry from DUMP hash field '%s' "
                "(absolute timestamp)",
                self._config.ttl_dump_field,
            )
        elif mode == "expiretime":
            logger.info("Reading key expiry with EXPIRETIME (absolute Unix seconds)")
        elif mode == "pexpiretime":
            logger.info(
                "Reading key expiry with PEXPIRETIME (absolute Unix milliseconds)"
            )
        elif mode == "ttl":
            logger.info("Reading key expiry with TTL (seconds)")

    def _read_types_and_ttls(self, keys: List[bytes], mode: TtlReadMode):
        pipe = self._client.pipeline(transaction=False)
        for key in keys:
            rk = cast(Any, key)
            pipe.type(rk)
            if mode == "pttl":
                pipe.pttl(rk)
            elif mode == "ttl":
                pipe.ttl(rk)
            elif mode == "dump":
                pipe.dump(rk)
            elif mode == "expiretime":
                pipe.expiretime(rk)
            elif mode == "pexpiretime":
                pipe.pexpiretime(rk)
        results = pipe.execute()

        types = {}
        ttls = {}
        stride = 1 if mode == "none" else 2
        now_ms = _now_ms()
        for i, key in enumerate(keys):
            raw_type = results[i * stride]
            types[key] = self._decode(raw_type)
            if mode == "none":
                ttls[key] = None
                continue
            ttls[key] = self._ttl_ms_from_reply(mode, results[i * stride + 1], now_ms)
        return types, ttls

    def _ttl_ms_from_reply(self, mode: TtlReadMode, raw: Any, now_ms: int) -> int | None:
        if mode == "dump":
            if not isinstance(raw, (bytes, bytearray)):
                return None
            return ttl_ms_from_dump(bytes(raw), self._config.ttl_dump_field, now_ms)
        if not isinstance(raw, int) or raw < 0:
            # PTTL/TTL/EXPIRETIME: -1 no expiry, -2 missing key.
            return None
        if mode == "ttl":
            return raw * 1000
        if mode == "expiretime":
            return _remaining_from_absolute_ms(raw * 1000, now_ms)
        if mode == "pexpiretime":
            return _remaining_from_absolute_ms(raw, now_ms)
        return raw

    def _fetch_values(self, keys: Iterable[bytes], types):
        pipe = self._client.pipeline(transaction=False)
        ordered_keys: List[bytes] = []
        for key in keys:
            key_type = types[key]
            if key_type == "none" or not self._read_supported(key_type):
                continue
            ordered_keys.append(key)
            rk = cast(Any, key)
            if key_type == "string":
                pipe.get(rk)
            elif key_type == "hash":
                pipe.hgetall(rk)
            elif key_type == "list":
                pipe.lrange(rk, 0, -1)
            elif key_type == "set":
                pipe.smembers(rk)
            elif key_type == "zset":
                pipe.zrange(rk, 0, -1, withscores=True)
            else:
                # Unknown/unsupported type (e.g. stream): fetch nothing, drop a
                # placeholder so indices stay aligned.
                pipe.exists(rk)
        results = pipe.execute()
        return dict(zip(ordered_keys, results))

    def _read_supported(self, key_type: str) -> bool:
        """False when the source COMMAND table omits this type's read command."""
        command = _VALUE_COMMANDS.get(key_type)
        if command is None:
            return True
        names = _command_names(self._client)
        if names is None or command in names:
            return True
        if key_type not in self._unsupported_read_types:
            self._unsupported_read_types.add(key_type)
            logger.warning(
                "Source does not implement %s; skipping Redis type '%s'",
                command.upper(),
                key_type,
            )
        return False

    @staticmethod
    def _decode(value):
        if isinstance(value, bytes):
            return value.decode("utf-8", "replace")
        return value

    @staticmethod
    def _decode_key(key: bytes):
        # Preserve fidelity: UTF-8 keys become str, binary keys stay bytes (a
        # valid Aerospike key type). Lossy "replace" decoding could collapse two
        # distinct binary keys into one and overwrite data.
        if not isinstance(key, bytes):
            return key
        try:
            return key.decode("utf-8")
        except UnicodeDecodeError:
            return key
