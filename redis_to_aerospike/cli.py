"""Command-line entrypoint: wire config -> source/registry/sink -> migrator."""

from __future__ import annotations

import argparse
import logging
from typing import Any, Dict, List, Optional, Tuple

import shtab

from .aerospike_sink import AerospikeServerInfo, AerospikeSink
from .config import (
    HashStrategy,
    MigrationConfig,
    RecordExistsPolicy,
    TtlCommand,
    TtlOverflowPolicy,
    _parse_set_routes,
)
from .converters.registry import ConverterRegistry
from .log_banners import log_migration_banner
from .migrator import Migrator
from .redis_source import RedisSource
from .version import __version__

logger = logging.getLogger("redis_to_aerospike.cli")


# Maps an argparse dest -> (section, field, coerce). ``section`` is one of
# "redis"/"aerospike"/None (None means a top-level MigrationConfig field).
# ``coerce`` optionally transforms the raw CLI value before assignment.
_CLIArgBinding = Tuple[Optional[str], str, Any]
_ARG_MAP: Dict[str, _CLIArgBinding] = {
    "redis_host": ("redis", "host", None),
    "redis_port": ("redis", "port", None),
    "redis_db": ("redis", "db", None),
    "redis_url": ("redis", "url", None),
    "redis_cluster": ("redis", "cluster", None),
    "redis_username": ("redis", "username", None),
    "redis_password": ("redis", "password", None),
    "redis_ssl": ("redis", "ssl", None),
    "redis_ssl_ca_certs": ("redis", "ssl_ca_certs", None),
    "redis_ssl_certfile": ("redis", "ssl_certfile", None),
    "redis_ssl_keyfile": ("redis", "ssl_keyfile", None),
    "redis_ssl_cert_reqs": ("redis", "ssl_cert_reqs", None),
    "redis_socket_timeout": ("redis", "socket_timeout", None),
    "redis_socket_connect_timeout": ("redis", "socket_connect_timeout", None),
    "redis_match": ("redis", "scan_match", None),
    "aerospike_use_services_alternate": ("aerospike", "use_services_alternate", None),
    "aerospike_username": ("aerospike", "username", None),
    "aerospike_password": ("aerospike", "password", None),
    "aerospike_auth_mode": ("aerospike", "auth_mode", None),
    "aerospike_tls_enable": ("aerospike", "tls_enable", None),
    "aerospike_tls_name": ("aerospike", "tls_name", None),
    "aerospike_tls_cafile": ("aerospike", "tls_cafile", None),
    "aerospike_tls_certfile": ("aerospike", "tls_certfile", None),
    "aerospike_tls_keyfile": ("aerospike", "tls_keyfile", None),
    "aerospike_tls_keyfile_pw": ("aerospike", "tls_keyfile_pw", None),
    "aerospike_socket_timeout_ms": ("aerospike", "socket_timeout_ms", None),
    "aerospike_total_timeout_ms": ("aerospike", "total_timeout_ms", None),
    "aerospike_connect_timeout_ms": ("aerospike", "connect_timeout_ms", None),
    "aerospike_login_timeout_ms": ("aerospike", "login_timeout_ms", None),
    "aerospike_namespace": ("aerospike", "namespace", None),
    "aerospike_set": ("aerospike", "set_name", None),
    "value_bin": ("aerospike", "value_bin", None),
    "aerospike_send_key": ("aerospike", "send_key", None),
    "aerospike_record_exists_policy": ("aerospike", "record_exists_policy", RecordExistsPolicy),
    "max_ttl": ("aerospike", "max_ttl", None),
    "workers": (None, "workers", None),
    "scan_batch": (None, "scan_batch", None),
    "queue_size": (None, "queue_size", None),
    "scan_rate_limit": (None, "scan_rate_limit", None),
    "write_rate_limit": (None, "write_rate_limit", None),
    "write_batch_size": (None, "write_batch_size", None),
    "hash_strategy": (None, "hash_strategy", HashStrategy),
    "ttl_overflow_policy": (None, "ttl_overflow_policy", TtlOverflowPolicy),
    "progress_interval": (None, "progress_interval", None),
}


def build_config(args: argparse.Namespace) -> MigrationConfig:
    """Build the migration config.

    A YAML file (``--config``) provides the base; any explicitly-passed CLI
    flag then overrides the corresponding value. Flags the user did not pass are
    suppressed from the namespace (see :func:`parse_args`), so absence means
    "keep the YAML/default value".
    """
    provided = vars(args)

    config_path = provided.get("config")
    base = MigrationConfig.from_yaml(config_path) if config_path else MigrationConfig()

    # --aerospike-host / --aerospike-port jointly rebuild the single-node hosts
    # list, falling back to whatever the base already has for the missing half.
    if "aerospike_host" in provided or "aerospike_port" in provided:
        cur_host, cur_port = (
            base.aerospike.hosts[0] if base.aerospike.hosts else ("localhost", 3000)
        )
        host = provided.get("aerospike_host", cur_host)
        port = provided.get("aerospike_port", cur_port)
        base.aerospike.hosts = [(host, port)]

    for dest, (section, field_name, coerce) in _ARG_MAP.items():
        if dest not in provided:
            continue
        value = provided[dest]
        if coerce is not None:
            value = coerce(value)
        target = base if section is None else getattr(base, section)
        setattr(target, field_name, value)

    if "set_route" in provided:
        base.aerospike.set_routes = list(base.aerospike.set_routes) + list(provided["set_route"])

    if "redis_key_pattern" in provided and "redis_match" not in provided:
        base.redis.scan_match = provided["redis_key_pattern"]

    return base


def _parse_set_route_cli(token: str):
    """``argparse`` type for ``--set-route`` (see flag help for token forms)."""
    if "=" not in token:
        raise argparse.ArgumentTypeError(
            "expected PATTERN=SET_NAME, optionally PATTERN=SET=hash_strategy or "
            "PATTERN=SET=map_bin=CUSTOM_BIN"
        )
    parts = token.split("=", 3)
    if len(parts) < 2 or not parts[0].strip() or not parts[1].strip():
        raise argparse.ArgumentTypeError("PATTERN and SET_NAME must be non-empty")
    mapping: Dict[str, str] = {"pattern": parts[0], "destination": parts[1]}
    if len(parts) >= 3:
        hs = parts[2].strip()
        if hs:
            mapping["hash_strategy"] = hs
    if len(parts) == 4:
        if mapping.get("hash_strategy") != HashStrategy.MAP_BIN.value:
            raise argparse.ArgumentTypeError(
                "four-token --set-route requires the third segment to be map_bin "
                "before the map bin name"
            )
        vb = parts[3].strip()
        if not vb:
            raise argparse.ArgumentTypeError(
                "four-token --set-route requires a non-empty map bin name as the fourth segment"
            )
        mapping["value_bin"] = vb
    return _parse_set_routes([mapping])[0]


def build_parser() -> argparse.ArgumentParser:
    # All config-bearing flags default to SUPPRESS so the parsed namespace
    # contains *only* what the user explicitly passed. That lets build_config()
    # treat a YAML file (or the dataclass defaults) as the base and override it
    # with just the flags actually provided. Defaults live in config.py and are
    # documented in each flag's help text.
    parser = argparse.ArgumentParser(
        prog="redis2aerospike",
        description="Migrate Redis data into Aerospike using native Aerospike types.",
    )
    parser.add_argument(
        "--print-completion",
        choices=["bash", "zsh", "fish"],
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    config_arg = parser.add_argument(
        "--config",
        default=None,
        help="path to a YAML config file (optional). Provides the base config; "
        "explicitly-passed CLI flags override individual values.",
    )
    config_arg.complete = shtab.FILE  # type: ignore[attr-defined]

    sup = argparse.SUPPRESS

    redis = parser.add_argument_group("Redis source")
    redis.add_argument("--redis-host", default=sup, help="default: localhost")
    redis.add_argument("--redis-port", type=int, default=sup, help="default: 6379")
    redis.add_argument("--redis-db", type=int, default=sup, help="default: 0 (ignored for cluster)")
    redis.add_argument(
        "--redis-url",
        default=sup,
        help="connection URL (redis:// or rediss:// for TLS); overrides the discrete "
        "host/port/auth/ssl fields",
    )
    redis.add_argument(
        "--redis-cluster",
        action="store_true",
        default=sup,
        help="connect to a Redis Cluster (sharded) and scan every shard; uses db 0",
    )
    redis.add_argument("--redis-username", default=sup, help="ACL username (Redis 6+)")
    redis.add_argument("--redis-password", default=sup)
    redis.add_argument("--redis-ssl", action="store_true", default=sup, help="enable TLS")
    redis_ssl_ca = redis.add_argument(
        "--redis-ssl-ca-certs", default=sup, help="TLS CA certificate file"
    )
    redis_ssl_ca.complete = shtab.FILE  # type: ignore[attr-defined]
    redis_ssl_cert = redis.add_argument(
        "--redis-ssl-certfile", default=sup, help="mutual TLS: client certificate file"
    )
    redis_ssl_cert.complete = shtab.FILE  # type: ignore[attr-defined]
    redis_ssl_key = redis.add_argument(
        "--redis-ssl-keyfile", default=sup, help="mutual TLS: client key file"
    )
    redis_ssl_key.complete = shtab.FILE  # type: ignore[attr-defined]
    redis.add_argument(
        "--redis-ssl-cert-reqs",
        choices=["required", "optional", "none"],
        default=sup,
        help="server certificate verification (default: required)",
    )
    redis.add_argument(
        "--redis-socket-timeout", type=float, default=sup, help="socket timeout in seconds"
    )
    redis.add_argument(
        "--redis-socket-connect-timeout",
        type=float,
        default=sup,
        help="socket connect timeout in seconds",
    )
    redis.add_argument("--redis-match", default=sup, help="SCAN match pattern (default: *)")
    redis.add_argument(
        "--redis-key-pattern",
        default=sup,
        dest="redis_key_pattern",
        help="alias for --redis-match: Redis SCAN source filter (same as redis.scan_match in YAML)",
    )

    aero = parser.add_argument_group("Aerospike sink")
    aero.add_argument("--aerospike-host", default=sup, help="default: localhost")
    aero.add_argument("--aerospike-port", type=int, default=sup, help="default: 3000")
    aero.add_argument(
        "--aerospike-use-services-alternate",
        action="store_true",
        default=sup,
        help="use the server's alternate-access address (NAT / cloud / Docker)",
    )
    aero.add_argument("--aerospike-username", default=sup, help="security: username")
    aero.add_argument("--aerospike-password", default=sup, help="security: password")
    aero.add_argument(
        "--aerospike-auth-mode",
        choices=["internal", "external", "external_insecure", "pki"],
        default=sup,
        help="authentication mode (default: internal)",
    )
    aero.add_argument(
        "--aerospike-tls-enable", action="store_true", default=sup, help="enable TLS"
    )
    aero.add_argument(
        "--aerospike-tls-name",
        default=sup,
        help="server certificate subject name (applied to every host)",
    )
    aero_tls_ca = aero.add_argument(
        "--aerospike-tls-cafile", default=sup, help="TLS CA certificate file"
    )
    aero_tls_ca.complete = shtab.FILE  # type: ignore[attr-defined]
    aero_tls_cert = aero.add_argument(
        "--aerospike-tls-certfile", default=sup, help="mutual TLS: client certificate file"
    )
    aero_tls_cert.complete = shtab.FILE  # type: ignore[attr-defined]
    aero_tls_key = aero.add_argument(
        "--aerospike-tls-keyfile", default=sup, help="mutual TLS: client key file"
    )
    aero_tls_key.complete = shtab.FILE  # type: ignore[attr-defined]
    aero.add_argument(
        "--aerospike-tls-keyfile-pw", default=sup, help="mutual TLS: client key password"
    )
    aero.add_argument(
        "--aerospike-socket-timeout-ms", type=int, default=sup, help="default: 0 (no timeout)"
    )
    aero.add_argument(
        "--aerospike-total-timeout-ms", type=int, default=sup, help="default: 0 (no timeout)"
    )
    aero.add_argument(
        "--aerospike-connect-timeout-ms", type=int, default=sup, help="default: 1000"
    )
    aero.add_argument(
        "--aerospike-login-timeout-ms", type=int, default=sup, help="default: 5000"
    )
    aero.add_argument("--aerospike-namespace", default=sup, help="default: test")
    aero.add_argument("--aerospike-set", default=sup, help="default: redis")
    aero.add_argument(
        "--set-route",
        action="append",
        type=_parse_set_route_cli,
        default=sup,
        metavar="PATTERN=SET",
        help="glob on Redis key -> Aerospike set (repeatable; first match wins). "
        "Forms: PATTERN=SET; PATTERN=SET=hash_strategy (map_bin|field_bins); "
        "PATTERN=SET=map_bin=BIN_NAME for a custom map bin name.",
    )
    aero.add_argument(
        "--value-bin", default=sup, help="bin name for single-value records (default: value)"
    )
    aero.add_argument(
        "--aerospike-send-key",
        action="store_true",
        default=sup,
        help="store the primary key alongside the record (POLICY_KEY_SEND)",
    )
    aero.add_argument(
        "--aerospike-record-exists-policy",
        choices=[p.value for p in RecordExistsPolicy],
        default=sup,
        type=RecordExistsPolicy,
        help="when the Aerospike key exists: update (merge, default), replace (full record), "
        "or create_only (skip existing)",
    )
    aero.add_argument(
        "--max-ttl",
        type=int,
        default=sup,
        help="max record TTL in seconds (default: 10 years); 0 disables the check",
    )

    pipeline = parser.add_argument_group("Pipeline")
    pipeline.add_argument("--workers", type=int, default=sup, help="default: 8")
    pipeline.add_argument("--scan-batch", type=int, default=sup, help="default: 500")
    pipeline.add_argument("--queue-size", type=int, default=sup, help="default: 10000")
    pipeline.add_argument(
        "--scan-rate-limit",
        type=float,
        default=sup,
        help="max records/sec pulled from Redis (throttles SCAN); 0 = unlimited (default)",
    )
    pipeline.add_argument(
        "--write-rate-limit",
        type=float,
        default=sup,
        help="max records/sec written to Aerospike across all workers; 0 = unlimited (default)",
    )
    pipeline.add_argument(
        "--write-batch-size",
        type=int,
        default=sup,
        help="records per Aerospike batch_write; 1 = single writes (default)",
    )
    pipeline.add_argument(
        "--hash-strategy",
        choices=[s.value for s in HashStrategy],
        default=sup,
        help="default: map_bin",
    )
    pipeline.add_argument(
        "--ttl-overflow-policy",
        choices=[p.value for p in TtlOverflowPolicy],
        default=sup,
        help="how to handle TTLs above --max-ttl: reject (default), clamp, or never_expire",
    )

    runlog = parser.add_argument_group("Run and logging")
    runlog.add_argument(
        "--progress-interval",
        type=float,
        default=sup,
        help="seconds between progress log lines; 0 disables the heartbeat (default: 10)",
    )
    runlog.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="connect, gather server info, and print the run preview, then exit without writing",
    )
    runlog.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser


def _fish_completion(parser: argparse.ArgumentParser) -> str:
    """Generate a fish completion script from an ``argparse`` parser."""
    prog = parser.prog or "redis2aerospike"
    lines = [f"# fish completion for {prog}", ""]
    for action in parser._actions:
        if not action.option_strings or action.dest in {"help", "print_completion"}:
            continue
        if isinstance(action, argparse._VersionAction):
            continue

        opt_parts: List[str] = []
        for opt in action.option_strings:
            if opt.startswith("--"):
                opt_parts.append(f"-l {opt[2:]}")
            elif opt.startswith("-") and len(opt) == 2:
                opt_parts.append(f"-s {opt[1]}")

        desc = (action.help or "").replace("'", "\\'")
        parts = [
            f"complete -c {prog}",
            "-n '__fish_use_subcommand'",
            *opt_parts,
        ]
        if desc:
            parts.append(f"-d '{desc}'")

        if getattr(action, "complete", None) == shtab.FILE:
            parts.append("-r -F")
        elif action.choices is not None:
            choices = " ".join(str(c) for c in action.choices)
            parts.append(f"-xa '{choices}'")
        elif not isinstance(action, argparse._StoreTrueAction) and not isinstance(
            action, argparse._StoreFalseAction
        ):
            parts.append("-r")

        lines.append(" ".join(parts))

    return "\n".join(lines) + "\n"


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def _auth_summary(aero) -> str:
    """One-line auth description; the password is never printed."""
    if not aero.username:
        return "none"
    mode = aero.auth_mode or "internal"
    secret = "***" if aero.password else "(empty)"
    return f"username={aero.username} password={secret} mode={mode}"


def _tls_summary(aero) -> str:
    if not aero.tls_enable:
        return "disabled"
    mutual = "yes" if aero.tls_certfile else "no"
    name = aero.tls_name or "(none)"
    return f"enabled (name={name}, mutual={mutual})"


def _mask_url(url: str) -> str:
    """Hide the password in a redis:// URL of the form scheme://user:pass@host."""
    if "@" not in url or "://" not in url:
        return url
    scheme, _, rest = url.partition("://")
    creds, _, host = rest.partition("@")
    if ":" in creds:
        user, _, _ = creds.partition(":")
        creds = f"{user}:***"
    return f"{scheme}://{creds}@{host}"


def _redis_auth_summary(redis) -> str:
    """One-line Redis auth description; the password is never printed."""
    if not redis.username and not redis.password:
        return "none"
    user = redis.username or "(default)"
    secret = "***" if redis.password else "(none)"
    return f"username={user} password={secret}"


def _redis_tls_summary(redis) -> str:
    if not redis.ssl:
        return "disabled"
    mutual = "yes" if redis.ssl_certfile else "no"
    return f"enabled (mutual={mutual})"


def _rate_summary(rate: float) -> str:
    """Human-readable rate limit; 0 (or less) means no throttling."""
    return f"{rate:g}/s" if rate and rate > 0 else "unlimited"


def _batch_summary(batch_size: int) -> str:
    """Human-readable write batching; <= 1 means one write per record."""
    return f"{batch_size} records" if batch_size and batch_size > 1 else "single"


def render_preview(
    config: MigrationConfig,
    redis_info: Dict[str, Any],
    aerospike_info: Optional[AerospikeServerInfo],
) -> str:
    """Build the human-readable pre-run preview block.

    Pure (no logging / I/O) so it is trivially unit-testable. The caller logs
    the returned string at INFO before the migration starts.
    """
    redis = config.redis
    aero = config.aerospike

    def fmt(value: Any, fallback: str = "unknown") -> str:
        return fallback if value is None else str(value)

    estimated = redis_info.get("keys")
    if estimated is None:
        estimate_line = "  estimated keys : unknown"
    elif redis.scan_match == "*":
        estimate_line = f"  estimated keys : {estimated}"
    else:
        estimate_line = (
            f"  estimated keys : <= {estimated} "
            f"(whole-db count; match '{redis.scan_match}' filters further)"
        )

    if redis.url:
        endpoint_line = f"    endpoint    : {_mask_url(redis.url)}"
    else:
        db_part = "" if redis.cluster else f" db={redis.db}"
        endpoint_line = f"    endpoint    : {redis.host}:{redis.port}{db_part}"

    lines = [
        "migration preview",
        "  source (Redis):",
        f"    mode        : {'cluster' if redis.cluster else 'standalone'}",
        endpoint_line,
        f"    auth        : {_redis_auth_summary(redis)}",
        f"    tls         : {_redis_tls_summary(redis)}",
        f"    SCAN filter : {redis.scan_match}",
        f"    TTL command : {redis.ttl_command.value}",
        f"    keys        : {fmt(redis_info.get('keys'))}",
        f"    with TTL     : {fmt(redis_info.get('expires'))}",
        f"    used memory : {fmt(redis_info.get('used_memory_human'))}",
        f"    version     : {fmt(redis_info.get('redis_version'))}",
        "  target (Aerospike):",
        f"    hosts       : {aero.hosts}",
        f"    namespace   : {aero.namespace}",
    ]
    if aero.set_routes:
        lines.append(f"    set (default) : {aero.set_name}")
        for i, route in enumerate(aero.set_routes, start=1):
            bits: List[str] = []
            if route.hash_strategy is not None:
                bits.append(f"hash_strategy={route.hash_strategy.value}")
            if route.value_bin is not None:
                bits.append(f"value_bin={route.value_bin!r}")
            suffix = f" ({', '.join(bits)})" if bits else ""
            lines.append(f"    set route {i}   : {route.pattern} -> {route.destination}{suffix}")
    else:
        lines.append(f"    set         : {aero.set_name}")
    lines.extend(
        [
            f"    value bin   : {aero.value_bin}",
            f"    auth        : {_auth_summary(aero)}",
            f"    tls         : {_tls_summary(aero)}",
            f"    timeouts(ms): socket={aero.socket_timeout_ms} total={aero.total_timeout_ms} "
            f"connect={aero.connect_timeout_ms} login={aero.login_timeout_ms}",
            f"    send key    : {aero.send_key}",
            f"    record exists : {aero.record_exists_policy.value}",
            f"    services-alt: {aero.use_services_alternate}",
        ]
    )
    if aerospike_info is not None:
        lines.extend(
            [
                f"    nsup-period : {fmt(aerospike_info.nsup_period)}",
                f"    max-record-size : {fmt(aerospike_info.max_record_size)}",
                f"    stop-writes-pct : {fmt(aerospike_info.stop_writes_pct)}",
            ]
        )
    lines.extend(
        [
            "  pipeline:",
            f"    workers     : {config.workers}",
            f"    scan batch  : {config.scan_batch}",
            f"    queue size  : {config.queue_size}",
            f"    scan rate   : {_rate_summary(config.scan_rate_limit)}",
            f"    write rate  : {_rate_summary(config.write_rate_limit)}",
            f"    write batch : {_batch_summary(config.write_batch_size)}",
            f"    hash strategy : {config.hash_strategy.value}",
            f"    ttl overflow  : {config.ttl_overflow_policy.value}",
            f"    max ttl     : {aero.max_ttl}",
            f"    max record size : {aero.max_record_size}",
            f"    progress interval : {config.progress_interval}s",
        ]
    )
    lines.append(estimate_line)
    return "\n".join(lines)


def _redis_reports_expiring_keys(redis_info: Optional[Dict[str, Any]]) -> bool:
    expires = (redis_info or {}).get("expires")
    try:
        return expires is not None and int(expires) > 0
    except (TypeError, ValueError):
        return False


def _run_supplies_expiries(redis_info: Optional[Dict[str, Any]], ttl_command: Optional[TtlCommand]) -> bool:
    """True when this run is expected to write records that carry a TTL.

    Redis ``INFO keyspace`` misses MemoryDB expiries stored inside the value.
    ``dump``, ``expiretime``, and ``pexpiretime`` read those absolute timestamps.
    """
    if ttl_command in (TtlCommand.DUMP, TtlCommand.EXPIRETIME, TtlCommand.PEXPIRETIME):
        return True
    return _redis_reports_expiring_keys(redis_info)


def ttl_migration_blocked_reason(
    redis_info: Dict[str, Any],
    aerospike_info: Optional[AerospikeServerInfo],
    ttl_command: Optional[TtlCommand] = None,
) -> Optional[str]:
    """Return a fatal error message if this run would write TTLs the server rejects."""
    if aerospike_info is None or aerospike_info.nsup_period != 0:
        return None
    if not _run_supplies_expiries(redis_info, ttl_command):
        return None
    ns = aerospike_info.namespace
    command = f" ttl_command is {ttl_command.value}." if ttl_command is not None else ""
    expires = (redis_info or {}).get("expires")
    redis_part = ""
    if _redis_reports_expiring_keys(redis_info):
        redis_part = f" Redis reports {int(expires)} key(s) with an expiry."
    return (
        f"Records with TTL cannot be inserted into Aerospike namespace '{ns}': "
        "TTL expiration is disabled (nsup-period is 0). The server rejects those "
        "writes with error 22 (AS_ERR_FORBIDDEN)."
        f"{command}{redis_part} "
        "Set nsup-period to a non-zero value (for example 120) on the namespace, "
        "then re-run."
    )


def render_preflight(
    config: MigrationConfig,
    redis_info: Dict[str, Any],
    aerospike_info: Optional[AerospikeServerInfo],
) -> str:
    """Checklist of the checks that run before any key is scanned or written."""

    def shown(value: Any) -> str:
        return "unavailable" if value is None else str(value)

    lines = [
        "preflight checks",
        "  redis ping              : pass",
        "  aerospike connect       : pass",
        f"  redis key count         : {shown(redis_info.get('keys'))}",
        f"  redis INFO expires      : {shown(redis_info.get('expires'))}",
        f"  ttl command             : {config.redis.ttl_command.value}",
    ]
    if aerospike_info is None:
        lines.append("  aerospike namespace     : unavailable")
    else:
        lines.append(f"  namespace               : {aerospike_info.namespace}")
        lines.append(f"  nsup-period             : {shown(aerospike_info.nsup_period)}")
        lines.append(f"  max-record-size         : {shown(aerospike_info.max_record_size)}")
        lines.append(f"  stop-writes-pct         : {shown(aerospike_info.stop_writes_pct)}")
    blocked = ttl_migration_blocked_reason(
        redis_info, aerospike_info, config.redis.ttl_command
    )
    if blocked:
        lines.append("  ttl writes allowed      : fail")
    elif aerospike_info is not None and aerospike_info.nsup_period == 0:
        lines.append("  ttl writes allowed      : warn (nsup-period is 0)")
    else:
        lines.append("  ttl writes allowed      : pass")
    return "\n".join(lines)


def apply_server_info(
    config: MigrationConfig,
    aerospike_info: Optional[AerospikeServerInfo],
    redis_info: Optional[Dict[str, Any]] = None,
) -> None:
    """Use the Aerospike server settings to alert the user and tune the run.

    * Warns when the namespace has TTL eviction disabled (``nsup-period == 0``),
      since any TTLs written by this run will not be enforced — unless
      :func:`ttl_migration_blocked_reason` will abort the run (Redis has keys
      with TTL), in which case the warning is skipped to avoid duplicate noise.
    * Aligns the sink's max record size with the server's advertised limit so
      oversized records are rejected against the real boundary, not a guess.
    """
    if aerospike_info is None:
        return

    if aerospike_info.nsup_period == 0 and not _run_supplies_expiries(
        redis_info, config.redis.ttl_command
    ):
        logger.warning(
            "Aerospike namespace '%s' has nsup-period=0 (TTL eviction disabled); "
            "a write that sets a TTL is rejected with error 22 (AS_ERR_FORBIDDEN)",
            aerospike_info.namespace,
        )

    server_max = aerospike_info.max_record_size
    if server_max and server_max > 0:
        logger.info(
            "aligning max record size with server: %d bytes (was %d)",
            server_max,
            config.aerospike.max_record_size,
        )
        config.aerospike.max_record_size = server_max


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "print_completion", None):
        if args.print_completion == "fish":
            print(_fish_completion(parser), end="")
        else:
            print(shtab.complete(parser, shell=args.print_completion), end="")
        return 0

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    config = build_config(args)

    source = RedisSource(config.redis)
    registry = ConverterRegistry.from_config(config)
    sink = AerospikeSink(config.aerospike)

    try:
        source.ping()
    except Exception as exc:
        logger.error("cannot reach Redis: %s", exc)
        return 2

    try:
        sink.connect()
    except Exception as exc:
        logger.error("cannot reach Aerospike: %s", exc)
        source.close()
        return 2

    try:
        redis_info = source.server_info()
        aerospike_info = sink.server_info()

        apply_server_info(config, aerospike_info, redis_info)
        logger.info("\n%s", render_preflight(config, redis_info, aerospike_info))
        blocked = ttl_migration_blocked_reason(
            redis_info, aerospike_info, config.redis.ttl_command
        )
        if blocked:
            logger.error("%s", blocked)
            return 2

        logger.info("\n%s", render_preview(config, redis_info, aerospike_info))

        if args.dry_run:
            logger.info("dry run: no records were written")
            return 0

        log_migration_banner(logger)
        migrator = Migrator(config, source, registry, sink)
        stats = migrator.run()
        log_migration_banner(logger)
    finally:
        source.close()
        sink.close()

    report = stats.format_report()
    if stats.errors:
        logger.warning("\n%s", report)
        return 1
    logger.info("\n%s", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
