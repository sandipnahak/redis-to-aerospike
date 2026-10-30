"""Configuration objects for the migration.

Everything the app needs is captured in plain dataclasses so the wiring stays
explicit and easy to test. :meth:`MigrationConfig.from_env` provides a convenient
way to populate them from environment variables, and the CLI layers argument
parsing on top.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

# (hostname, port) for each Aerospike seed node.
AerospikeHost = Tuple[str, int]

# Aerospike set names are bounded; keep a conservative client-side limit.
_MAX_AEROSPIKE_SET_NAME_LEN = 128


def _validate_aerospike_set_label(name: str, *, field: str) -> str:
    """Return a stripped set name or raise ``ValueError``."""
    if not isinstance(name, str):
        raise TypeError(f"{field} must be a string")
    s = name.strip()
    if not s:
        raise ValueError(f"{field} must be non-empty")
    if "\x00" in s:
        raise ValueError(f"{field} must not contain NUL bytes")
    if len(s) > _MAX_AEROSPIKE_SET_NAME_LEN:
        raise ValueError(
            f"{field} exceeds {_MAX_AEROSPIKE_SET_NAME_LEN} characters "
            f"(got {len(s)})"
        )
    return s


def _as_bool(value: Any, default: bool = False) -> bool:
    """Coerce common truthy/falsey string (or native) values into a bool.

    Accepts ``1/true/yes/on`` (case-insensitive) as true and the corresponding
    ``0/false/no/off`` as false. ``None`` / empty falls back to ``default``.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return default
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def _normalize_hosts(hosts: Any, host: Any, port: Any) -> Optional[List[AerospikeHost]]:
    """Build a ``[(host, port), ...]`` list from YAML-style inputs.

    Supports either a ``hosts`` list (of ``[host, port]`` pairs or mappings with
    ``host``/``port`` keys) or a single ``host``/``port`` scalar pair. Returns
    ``None`` when nothing host-related was provided so callers keep their default.
    """
    if hosts:
        normalized: List[AerospikeHost] = []
        for entry in hosts:
            if isinstance(entry, dict):
                normalized.append((entry["host"], int(entry.get("port", 3000))))
            else:
                addr, p = entry
                normalized.append((addr, int(p)))
        return normalized
    if host is not None:
        return [(host, int(port) if port is not None else 3000)]
    return None


class HashStrategy(str, Enum):
    """How a Redis hash should be represented in Aerospike.

    * ``MAP_BIN``    -- store the whole hash as a single Aerospike map (1:1, simplest).
    * ``FIELD_BINS`` -- store each hash field as its own Aerospike bin (more native).
    """

    MAP_BIN = "map_bin"
    FIELD_BINS = "field_bins"


# Aerospike's default namespace max-ttl is 10 years (in seconds).
DEFAULT_MAX_TTL_S = 315_360_000


class TtlCommand(str, Enum):
    """Which Redis command supplies key expiry.

    * ``PTTL``        -- remaining time in milliseconds (default).
    * ``TTL``         -- remaining time in seconds.
    * ``DUMP``        -- absolute expiry stored in a hash field inside the DUMP payload.
    * ``EXPIRETIME``  -- absolute Unix expiry in seconds (Redis 7+).
    * ``PEXPIRETIME`` -- absolute Unix expiry in milliseconds (Redis 7+).
    """

    PTTL = "pttl"
    TTL = "ttl"
    DUMP = "dump"
    EXPIRETIME = "expiretime"
    PEXPIRETIME = "pexpiretime"


def _parse_ttl_command(value: Any) -> TtlCommand:
    if isinstance(value, TtlCommand):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            "ttl_command must be one of: pttl, ttl, dump, expiretime, pexpiretime"
        )
    try:
        return TtlCommand(value.strip().lower())
    except ValueError:
        allowed = ", ".join(item.value for item in TtlCommand)
        raise ValueError(f"ttl_command must be one of: {allowed} (got {value!r})") from None


class TtlOverflowPolicy(str, Enum):
    """What to do when a Redis TTL exceeds the Aerospike max-ttl.

    * ``REJECT``       -- reject the record before writing (the default).
    * ``CLAMP``        -- store it with exactly the max-ttl, warning once per run.
    * ``NEVER_EXPIRE`` -- store it as never-expire, warning once per run.
    """

    REJECT = "reject"
    CLAMP = "clamp"
    NEVER_EXPIRE = "never_expire"


class RecordExistsPolicy(str, Enum):
    """How writes behave when the Aerospike record already exists.

    * ``UPDATE``       -- create or update; new bins merge with existing (default).
    * ``REPLACE``      -- create or full replace so the record matches this write only.
    * ``CREATE_ONLY``  -- insert only; existing records are left unchanged (counted as skipped).
    """

    UPDATE = "update"
    REPLACE = "replace"
    CREATE_ONLY = "create_only"


@dataclass(frozen=True)
class AerospikeSetRoute:
    """Maps a Redis key glob (``fnmatch``) to an Aerospike set; first match wins.

    Optional ``hash_strategy`` and ``value_bin`` override the migration defaults
    for keys matching this route. ``value_bin`` applies only when the effective
    hash strategy for the record is ``map_bin`` (see docs).
    """

    pattern: str
    destination: str
    hash_strategy: Optional[HashStrategy] = None
    value_bin: Optional[str] = None


def _parse_set_routes(raw: Any) -> List[AerospikeSetRoute]:
    """Build a list of :class:`AerospikeSetRoute` from YAML-style mappings."""
    if raw is None:
        return []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("set_routes must be a list of mappings")
    routes: List[AerospikeSetRoute] = []
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"set_routes[{i}] must be a mapping")
        pattern = item.get("pattern")
        destination = item.get("destination")
        if pattern is None or destination is None:
            raise ValueError(
                f"set_routes[{i}] requires 'pattern' and 'destination' keys"
            )
        if not isinstance(pattern, str) or not isinstance(destination, str):
            raise TypeError(f"set_routes[{i}] pattern and destination must be strings")

        raw_hs = item.get("hash_strategy")
        route_hash: Optional[HashStrategy]
        if raw_hs is None or raw_hs == "":
            route_hash = None
        else:
            route_hash = HashStrategy(raw_hs)

        raw_vb = item.get("value_bin")
        route_vb: Optional[str]
        if raw_vb is None or raw_vb == "":
            route_vb = None
        elif not isinstance(raw_vb, str):
            raise TypeError(f"set_routes[{i}].value_bin must be a string")
        else:
            route_vb = raw_vb.strip() or None

        routes.append(
            AerospikeSetRoute(
                pattern=_validate_aerospike_set_label(
                    pattern, field=f"set_routes[{i}].pattern"
                ),
                destination=_validate_aerospike_set_label(
                    destination, field=f"set_routes[{i}].destination"
                ),
                hash_strategy=route_hash,
                value_bin=route_vb,
            )
        )
    return routes


@dataclass
class RedisConfig:
    """Connection settings for the Redis source."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0

    # --- Connection URL ----------------------------------------------------
    # When set, it is the sole source of truth for the connection target
    # (``redis://`` or ``rediss://`` for TLS); discrete host/port/db/auth/ssl
    # fields are ignored. Socket timeouts still apply.
    url: Optional[str] = None

    # --- Redis Cluster (sharded) -------------------------------------------
    # Use the cluster client + cluster-aware scanning. A single host/port (or
    # url) is enough to seed topology discovery; cluster mode always uses db 0.
    cluster: bool = False

    # --- Authentication (Redis 6+ ACLs) ------------------------------------
    username: Optional[str] = None
    password: Optional[str] = None

    # --- TLS ---------------------------------------------------------------
    ssl: bool = False
    ssl_ca_certs: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    # One of: required, optional, none.
    ssl_cert_reqs: Optional[str] = None

    # --- Timeouts (seconds; redis-py convention) ---------------------------
    socket_timeout: Optional[float] = None
    socket_connect_timeout: Optional[float] = None

    # Glob pattern passed to SCAN. ``*`` migrates every key.
    scan_match: str = "*"

    # How to read key expiry. ``pttl`` is remaining milliseconds. ``dump`` reads
    # an absolute timestamp from a hash field inside the DUMP payload (the
    # MemoryDB Multi-Region path, which has no TTL/PTTL/EXPIRETIME).
    ttl_command: TtlCommand = TtlCommand.PTTL
    # Hash field read when ``ttl_command`` is ``dump``.
    ttl_dump_field: str = "e"

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "RedisConfig":
        lookup: Mapping[str, str] = os.environ if env is None else env

        def as_float(name: str) -> Optional[float]:
            value = lookup.get(name)
            return float(value) if value not in (None, "") else None

        return cls(
            host=lookup.get("REDIS_HOST", cls.host),
            port=int(lookup.get("REDIS_PORT", cls.port)),
            db=int(lookup.get("REDIS_DB", cls.db)),
            url=lookup.get("REDIS_URL") or None,
            cluster=_as_bool(lookup.get("REDIS_CLUSTER"), cls.cluster),
            username=lookup.get("REDIS_USERNAME") or None,
            password=lookup.get("REDIS_PASSWORD") or None,
            ssl=_as_bool(lookup.get("REDIS_SSL"), cls.ssl),
            ssl_ca_certs=lookup.get("REDIS_SSL_CA_CERTS") or None,
            ssl_certfile=lookup.get("REDIS_SSL_CERTFILE") or None,
            ssl_keyfile=lookup.get("REDIS_SSL_KEYFILE") or None,
            ssl_cert_reqs=lookup.get("REDIS_SSL_CERT_REQS") or None,
            socket_timeout=as_float("REDIS_SOCKET_TIMEOUT"),
            socket_connect_timeout=as_float("REDIS_SOCKET_CONNECT_TIMEOUT"),
            scan_match=lookup.get("REDIS_SCAN_MATCH", cls.scan_match),
            ttl_command=_parse_ttl_command(
                lookup.get("REDIS_TTL_COMMAND", cls.ttl_command.value)
            ),
            ttl_dump_field=lookup.get("REDIS_TTL_DUMP_FIELD", cls.ttl_dump_field)
            or cls.ttl_dump_field,
        )

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "RedisConfig":
        """Overlay the keys present in ``data`` onto the dataclass defaults."""
        cfg = cls()
        if not data:
            return cfg
        data = dict(data)
        # Optional YAML alias: key_pattern sets scan_match when scan_match is absent.
        if "key_pattern" in data and "scan_match" not in data:
            data["scan_match"] = data.pop("key_pattern")
        elif "key_pattern" in data:
            data.pop("key_pattern")
        # "TTL check command" is the name used in the docs; ttl_command is canonical.
        if "ttl_check_command" in data and "ttl_command" not in data:
            data["ttl_command"] = data.pop("ttl_check_command")
        else:
            data.pop("ttl_check_command", None)
        if "ttl_command" in data and data["ttl_command"] is not None:
            data["ttl_command"] = _parse_ttl_command(data["ttl_command"])
        for key, value in data.items():
            if hasattr(cfg, key):
                setattr(cfg, key, value)
        return cfg


@dataclass
class AerospikeConfig:
    """Connection settings and key placement for the Aerospike sink."""

    # List of (host, port) tuples. Defaults to a single local node.
    hosts: List[AerospikeHost] = field(default_factory=lambda: [("localhost", 3000)])
    # Use the server's alternate-access address (NAT / cloud / Docker).
    use_services_alternate: bool = False

    # --- Authentication (Enterprise security-enabled clusters) -------------
    username: Optional[str] = None
    password: Optional[str] = None
    # One of: internal, external, external_insecure, pki. None uses the client
    # default (internal). Mapped to aerospike.AUTH_* constants at connect time.
    auth_mode: Optional[str] = None

    # --- TLS ----------------------------------------------------------------
    tls_enable: bool = False
    # The certificate subject name presented by the server; applied as the 3rd
    # element of each host tuple when set.
    tls_name: Optional[str] = None
    tls_cafile: Optional[str] = None
    # Mutual TLS: client certificate + key.
    tls_certfile: Optional[str] = None
    tls_keyfile: Optional[str] = None
    tls_keyfile_pw: Optional[str] = None

    # --- Timeouts (milliseconds; 0 = client default / no timeout) ----------
    socket_timeout_ms: int = 0
    total_timeout_ms: int = 0
    connect_timeout_ms: int = 1000
    login_timeout_ms: int = 5000

    namespace: str = "test"
    set_name: str = "redis"
    # Ordered glob routes: first pattern match sends the record to ``destination``.
    # Keys that match none use ``set_name``. Does not change Redis SCAN behavior.
    set_routes: List[AerospikeSetRoute] = field(default_factory=list)
    # Bin used for single-value records (strings, lists, sets, sorted sets, and
    # the map-bin hash strategy).
    value_bin: str = "value"

    # Store the primary key alongside the record (POLICY_KEY_SEND).
    send_key: bool = False
    # When a record already exists: merge (update), full replace, or create-only.
    record_exists_policy: RecordExistsPolicy = RecordExistsPolicy.UPDATE

    # Records whose estimated payload exceeds this many bytes are rejected
    # before being sent to the server. Aerospike's maximum object size is 8 MiB.
    max_record_size: int = 8 * 1024 * 1024
    # Maximum record TTL in seconds. Defaults to Aerospike's 10-year max-ttl.
    # Set to 0 to disable the boundary check entirely.
    max_ttl: int = DEFAULT_MAX_TTL_S

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "AerospikeConfig":
        lookup: Mapping[str, str] = os.environ if env is None else env
        host = lookup.get("AEROSPIKE_HOST", "localhost")
        port = int(lookup.get("AEROSPIKE_PORT", 3000))
        return cls(
            hosts=[(host, port)],
            use_services_alternate=_as_bool(
                lookup.get("AEROSPIKE_USE_SERVICES_ALTERNATE"), cls.use_services_alternate
            ),
            username=lookup.get("AEROSPIKE_USERNAME") or None,
            password=lookup.get("AEROSPIKE_PASSWORD") or None,
            auth_mode=lookup.get("AEROSPIKE_AUTH_MODE") or None,
            tls_enable=_as_bool(lookup.get("AEROSPIKE_TLS_ENABLE"), cls.tls_enable),
            tls_name=lookup.get("AEROSPIKE_TLS_NAME") or None,
            tls_cafile=lookup.get("AEROSPIKE_TLS_CAFILE") or None,
            tls_certfile=lookup.get("AEROSPIKE_TLS_CERTFILE") or None,
            tls_keyfile=lookup.get("AEROSPIKE_TLS_KEYFILE") or None,
            tls_keyfile_pw=lookup.get("AEROSPIKE_TLS_KEYFILE_PW") or None,
            socket_timeout_ms=int(lookup.get("AEROSPIKE_SOCKET_TIMEOUT_MS", cls.socket_timeout_ms)),
            total_timeout_ms=int(lookup.get("AEROSPIKE_TOTAL_TIMEOUT_MS", cls.total_timeout_ms)),
            connect_timeout_ms=int(lookup.get("AEROSPIKE_CONNECT_TIMEOUT_MS", cls.connect_timeout_ms)),
            login_timeout_ms=int(lookup.get("AEROSPIKE_LOGIN_TIMEOUT_MS", cls.login_timeout_ms)),
            namespace=lookup.get("AEROSPIKE_NAMESPACE", cls.namespace),
            set_name=lookup.get("AEROSPIKE_SET", cls.set_name),
            value_bin=lookup.get("AEROSPIKE_VALUE_BIN", cls.value_bin),
            send_key=_as_bool(lookup.get("AEROSPIKE_SEND_KEY"), cls.send_key),
            record_exists_policy=RecordExistsPolicy(
                lookup.get("AEROSPIKE_RECORD_EXISTS_POLICY", cls.record_exists_policy.value)
            ),
            max_record_size=int(lookup.get("AEROSPIKE_MAX_RECORD_SIZE", cls.max_record_size)),
            max_ttl=int(lookup.get("AEROSPIKE_MAX_TTL", cls.max_ttl)),
        )

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "AerospikeConfig":
        """Overlay the keys present in ``data`` onto the dataclass defaults.

        Accepts either a single ``host``/``port`` pair or a ``hosts`` list of
        ``[host, port]`` entries. Missing keys keep their default value, so a
        partial document is valid.
        """
        cfg = cls()
        if not data:
            return cfg
        data = dict(data)

        hosts = _normalize_hosts(data.pop("hosts", None), data.pop("host", None), data.pop("port", None))
        if hosts is not None:
            cfg.hosts = hosts

        if "set_routes" in data:
            cfg.set_routes = _parse_set_routes(data.pop("set_routes"))

        if "record_exists_policy" in data:
            cfg.record_exists_policy = RecordExistsPolicy(data.pop("record_exists_policy"))

        for key, value in data.items():
            if hasattr(cfg, key):
                setattr(cfg, key, value)
        return cfg


@dataclass
class MigrationConfig:
    """Tuning knobs for the migration pipeline itself."""

    redis: RedisConfig = field(default_factory=RedisConfig)
    aerospike: AerospikeConfig = field(default_factory=AerospikeConfig)
    # Number of worker threads writing to Aerospike.
    workers: int = 8
    # Number of keys fetched per SCAN round-trip.
    scan_batch: int = 500
    # Bounded work queue size; provides back-pressure on the producer.
    queue_size: int = 10_000
    # Optional rate limits in records/sec; 0 disables throttling.
    # scan_rate_limit caps how fast records are pulled from Redis (throttles
    # SCAN); write_rate_limit caps the aggregate insert rate into Aerospike
    # across all worker threads.
    scan_rate_limit: float = 0
    write_rate_limit: float = 0
    # Records per Aerospike batch_write. <= 1 keeps the single-write path
    # (one put/operate per record); > 1 flushes records in batches.
    write_batch_size: int = 1
    hash_strategy: HashStrategy = HashStrategy.MAP_BIN
    # How to handle records whose TTL exceeds aerospike.max_ttl.
    ttl_overflow_policy: TtlOverflowPolicy = TtlOverflowPolicy.REJECT
    # Seconds between progress heartbeat log lines. 0 disables the heartbeat.
    progress_interval: float = 10.0

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "MigrationConfig":
        lookup: Mapping[str, str] = os.environ if env is None else env
        return cls(
            redis=RedisConfig.from_env(lookup),
            aerospike=AerospikeConfig.from_env(lookup),
            workers=int(lookup.get("MIGRATION_WORKERS", cls.workers)),
            scan_batch=int(lookup.get("MIGRATION_SCAN_BATCH", cls.scan_batch)),
            queue_size=int(lookup.get("MIGRATION_QUEUE_SIZE", cls.queue_size)),
            scan_rate_limit=float(
                lookup.get("MIGRATION_SCAN_RATE_LIMIT", cls.scan_rate_limit)
            ),
            write_rate_limit=float(
                lookup.get("MIGRATION_WRITE_RATE_LIMIT", cls.write_rate_limit)
            ),
            write_batch_size=int(
                lookup.get("MIGRATION_WRITE_BATCH_SIZE", cls.write_batch_size)
            ),
            hash_strategy=HashStrategy(
                lookup.get("MIGRATION_HASH_STRATEGY", cls.hash_strategy.value)
            ),
            ttl_overflow_policy=TtlOverflowPolicy(
                lookup.get("MIGRATION_TTL_OVERFLOW_POLICY", cls.ttl_overflow_policy.value)
            ),
            progress_interval=float(
                lookup.get("MIGRATION_PROGRESS_INTERVAL", cls.progress_interval)
            ),
        )

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "MigrationConfig":
        """Build a config from a nested mapping (e.g. a parsed YAML document).

        Only the keys present in ``data`` override the dataclass defaults, so a
        partial document is valid. The ``redis`` and ``aerospike`` sub-sections
        are delegated to their own ``from_dict``; enum-valued pipeline knobs are
        coerced from their string values.
        """
        cfg = cls()
        if not data:
            return cfg
        data = dict(data)

        cfg.redis = RedisConfig.from_dict(data.pop("redis", None))
        cfg.aerospike = AerospikeConfig.from_dict(data.pop("aerospike", None))

        if "hash_strategy" in data:
            cfg.hash_strategy = HashStrategy(data.pop("hash_strategy"))
        if "ttl_overflow_policy" in data:
            cfg.ttl_overflow_policy = TtlOverflowPolicy(data.pop("ttl_overflow_policy"))

        for key, value in data.items():
            if hasattr(cfg, key):
                setattr(cfg, key, value)
        return cfg

    @classmethod
    def from_yaml(cls, path: str) -> "MigrationConfig":
        """Load configuration from a YAML file.

        PyYAML is imported lazily so the package remains importable without it;
        a clear error is raised if a config file is requested but PyYAML is not
        installed.
        """
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover - depends on environment
            raise RuntimeError(
                "PyYAML is required to load a --config file; install it with "
                "'pip install PyYAML'"
            ) from exc

        with open(path, "r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
        if data is not None and not isinstance(data, dict):
            raise ValueError(f"config file {path!r} must contain a YAML mapping at the top level")
        return cls.from_dict(data)
