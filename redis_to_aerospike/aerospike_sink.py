"""Write side of the migration: persist records into Aerospike.

The Aerospike Python client is thread-safe and maintains its own connection pool,
so a single ``AerospikeSink`` (and its client) is shared by all worker threads.

``aerospike`` is imported lazily inside the methods that need it. That keeps the
rest of the package importable -- and unit-testable -- on machines without the
Aerospike C client installed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

# Aerospike composite key: (namespace, set_name, user_key).
AerospikeKeyTuple = Tuple[str, str, Union[str, bytes]]

from .config import AerospikeConfig, RecordExistsPolicy
from .models import AerospikeRecord, BinWritePolicy

logger = logging.getLogger(__name__)

# Batch ``write_many`` outcome string when CREATE_ONLY hits an existing record.
BATCH_RECORD_EXISTS_OUTCOME = "RecordExists"


class RecordAlreadyExists(Exception):
    """Raised on single-record CREATE_ONLY writes when the key already exists."""


class RecordTooLargeError(Exception):
    """Raised when a record's estimated size exceeds the configured limit.

    Rejecting oversized records up front gives a clear, actionable message
    instead of a generic server-side failure mid-write.
    """


@dataclass
class AerospikeServerInfo:
    """Operationally useful namespace settings read from the Aerospike server.

    The fields we act on now are surfaced as typed attributes; the full parsed
    namespace info is kept in :attr:`raw` so future checks (capacity alerts,
    stop-writes, etc.) need no further plumbing.
    """

    namespace: str
    # TTL-eviction scan period in seconds. 0 means expiration is DISABLED, so
    # records written with a TTL will not be reaped.
    nsup_period: Optional[int] = None
    # Maximum record size the server will accept, in bytes. 0/None means the
    # server does not advertise an explicit cap.
    max_record_size: Optional[int] = None
    stop_writes_pct: Optional[int] = None
    memory_size: Optional[int] = None
    device_total_bytes: Optional[int] = None
    raw: Dict[str, str] = field(default_factory=dict)

    @staticmethod
    def _extract_response(info_result: Any) -> Optional[str]:
        """Pull one node's response string out of an ``info_all`` result.

        ``info_all`` returns ``{node_id: (error, response)}`` (and some client
        versions return a bare string). This tolerates both, returning the
        first non-empty response string it finds.
        """
        if info_result is None:
            return None
        if isinstance(info_result, str):
            return info_result
        if isinstance(info_result, dict):
            values = info_result.values()
        else:
            values = [info_result]
        for value in values:
            response = value
            if isinstance(value, (tuple, list)):
                response = next(
                    (item for item in value if isinstance(item, str)), None
                )
            if isinstance(response, str) and response:
                return response
        return None

    @classmethod
    def parse(cls, info_result: Any, namespace: str) -> "AerospikeServerInfo":
        """Parse a ``namespace/<ns>`` info response into typed settings."""
        text = cls._extract_response(info_result) or ""
        raw: Dict[str, str] = {}
        for pair in text.split(";"):
            if "=" in pair:
                name, _, value = pair.partition("=")
                raw[name.strip()] = value.strip()

        def as_int(*names: str) -> Optional[int]:
            for name in names:
                if name in raw:
                    try:
                        return int(raw[name])
                    except ValueError:
                        return None
            return None

        return cls(
            namespace=namespace,
            nsup_period=as_int("nsup-period"),
            # Newer servers expose "max-record-size"; older ones cap by
            # "write-block-size". Either way it bounds a single record.
            max_record_size=as_int("max-record-size", "write-block-size"),
            stop_writes_pct=as_int("stop-writes-pct"),
            memory_size=as_int("memory-size"),
            device_total_bytes=as_int("device_total_bytes"),
            raw=raw,
        )


class AerospikeSink:
    """Writes :class:`AerospikeRecord` objects into Aerospike."""

    def __init__(self, config: AerospikeConfig, client: Optional[Any] = None):
        self._config = config
        self._client = client
        self._aerospike = None  # the imported module, cached after connect()

    # Map the config's auth_mode string to the aerospike.AUTH_* attribute name.
    _AUTH_MODE_ATTRS = {
        "internal": "AUTH_INTERNAL",
        "external": "AUTH_EXTERNAL",
        "external_insecure": "AUTH_EXTERNAL_INSECURE",
        "pki": "AUTH_PKI",
    }

    def connect(self) -> "AerospikeSink":
        if self._client is not None:
            return self
        import aerospike  # local import: optional dependency

        self._aerospike = aerospike
        client_config = self._build_client_config(aerospike)
        self._client = aerospike.client(client_config).connect()
        return self

    def _build_client_config(self, aerospike) -> Dict[str, Any]:
        """Assemble the Aerospike client config dict from :attr:`_config`.

        Pure (no I/O); the ``aerospike`` module is passed in only to resolve the
        ``AUTH_*`` / ``POLICY_KEY_SEND`` constants, so this is unit-testable with
        a fake module and without a live server.
        """
        cfg = self._config

        if cfg.tls_name:
            hosts = [(h[0], h[1], cfg.tls_name) for h in cfg.hosts]
        else:
            hosts = [tuple(h) for h in cfg.hosts]
        client_config: Dict[str, Any] = {"hosts": hosts}

        if cfg.username:
            client_config["user"] = cfg.username
            client_config["password"] = cfg.password or ""

        policies: Dict[str, Any] = {}
        if cfg.auth_mode:
            attr = self._AUTH_MODE_ATTRS.get(cfg.auth_mode.lower())
            if attr is not None and hasattr(aerospike, attr):
                policies["auth_mode"] = getattr(aerospike, attr)
        if cfg.login_timeout_ms:
            policies["login_timeout"] = cfg.login_timeout_ms

        # Default per-operation policy carrying timeouts and the key policy.
        op_policy: Dict[str, Any] = {}
        if cfg.socket_timeout_ms:
            op_policy["socket_timeout"] = cfg.socket_timeout_ms
        if cfg.total_timeout_ms:
            op_policy["total_timeout"] = cfg.total_timeout_ms
        if cfg.send_key and hasattr(aerospike, "POLICY_KEY_SEND"):
            op_policy["key"] = aerospike.POLICY_KEY_SEND
        if op_policy:
            for op in ("read", "write", "operate"):
                policies[op] = dict(op_policy)
        if policies:
            client_config["policies"] = policies

        if cfg.connect_timeout_ms:
            client_config["connect_timeout"] = cfg.connect_timeout_ms
        if cfg.use_services_alternate:
            client_config["use_services_alternate"] = True

        if cfg.tls_enable:
            tls: Dict[str, Any] = {"enable": True}
            for key, value in (
                ("cafile", cfg.tls_cafile),
                ("certfile", cfg.tls_certfile),
                ("keyfile", cfg.tls_keyfile),
                ("keyfile_pw", cfg.tls_keyfile_pw),
            ):
                if value is not None:
                    tls[key] = value
            client_config["tls"] = tls

        return client_config

    def server_info(self) -> Optional[AerospikeServerInfo]:
        """Read the target namespace's settings from the server.

        Best-effort: returns ``None`` (and logs a warning) if the info command
        fails, so a migration is never blocked by an inability to introspect.
        """
        if self._client is None:
            raise RuntimeError("AerospikeSink is not connected; call connect() first")
        namespace = self._config.namespace
        try:
            result = self._client.info_all(f"namespace/{namespace}")
        except Exception as exc:
            logger.warning("could not read Aerospike namespace info: %s", exc)
            return None
        return AerospikeServerInfo.parse(result, namespace)

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass

    def _aero(self):
        if self._aerospike is None:
            import aerospike

            self._aerospike = aerospike
        return self._aerospike

    def _key(self, record: AerospikeRecord) -> AerospikeKeyTuple:
        set_name = record.set_name or self._config.set_name
        return (self._config.namespace, set_name, record.key)

    def _encode_bin_value_for_write(self, value: Any) -> Any:
        """Serialize plain Python dicts as Aerospike key-ordered maps (CDT).

        Unordered maps are the default when a dict is written blindly; the
        client encodes :class:`aerospike.KeyOrderedDict` as ``MAP_KEY_ORDERED``,
        which matches how Redis hashes and sorted sets are represented here and
        avoids unordered-map pitfalls on newer servers.
        """
        if not isinstance(value, dict):
            return value
        aerospike = self._aero()
        kod = getattr(aerospike, "KeyOrderedDict", None)
        if kod is not None and not isinstance(value, kod):
            return kod(dict(value))
        return value

    def _encode_bins_for_write(self, bins: Dict[str, Any]) -> Dict[str, Any]:
        return {name: self._encode_bin_value_for_write(v) for name, v in bins.items()}

    def _write_policy(self, record: AerospikeRecord) -> Dict[str, Any]:
        # TTL is set via the write/operate policy. The legacy meta["ttl"] was
        # deprecated in the Aerospike client 19.1. A positive number is seconds,
        # 0 uses the namespace default, and 0xFFFFFFFF never expires.
        aerospike = self._aero()
        policy: Dict[str, Any] = {"ttl": record.ttl_s if record.ttl_s is not None else 0}
        mode = self._config.record_exists_policy
        if mode is RecordExistsPolicy.UPDATE:
            policy["exists"] = aerospike.POLICY_EXISTS_IGNORE
        elif mode is RecordExistsPolicy.REPLACE:
            policy["exists"] = aerospike.POLICY_EXISTS_CREATE_OR_REPLACE
        else:
            policy["exists"] = aerospike.POLICY_EXISTS_CREATE
        return policy

    def _record_exists_error_codes(self) -> frozenset[int]:
        """Integer result/exception codes meaning the record already exists."""
        aerospike = self._aero()
        codes: set[int] = set()
        for name in ("AEROSPIKE_ERR_RECORD_EXISTS", "AS_PROTO_RESULT_FAIL_RECORD_EXISTS"):
            v = getattr(aerospike, name, None)
            if isinstance(v, int):
                codes.add(v)
        try:
            import importlib

            ae = importlib.import_module("aerospike.exception")
            c = getattr(ae.RecordExistsError, "code", None)
            if isinstance(c, int):
                codes.add(c)
        except Exception:
            pass
        return frozenset(codes)

    def _is_record_exists_error(self, exc: BaseException) -> bool:
        """True when the server rejected the write because the record already exists."""
        cls = type(exc)
        if cls.__name__ == "RecordExistsError" and getattr(cls, "__module__", "") == "aerospike.exception":
            return True
        codes = self._record_exists_error_codes()
        if not codes:
            return False
        ec = getattr(exc, "code", None)
        try:
            if ec is not None and int(ec) in codes:
                return True
        except (TypeError, ValueError):
            pass
        return False

    @staticmethod
    def _estimate_size(value) -> int:
        """Approximate the payload size of a bin value in bytes.

        This is a deliberate over-simplification (it ignores per-element CDT
        overhead) used only to reject clearly-oversized records before sending
        them; the real Aerospike limit may be slightly higher.
        """
        if value is None or isinstance(value, bool):
            return 1
        if isinstance(value, (int, float)):
            return 8
        if isinstance(value, (bytes, bytearray)):
            return len(value)
        if isinstance(value, str):
            return len(value.encode("utf-8"))
        if isinstance(value, (list, tuple)):
            return sum(AerospikeSink._estimate_size(item) for item in value)
        if isinstance(value, dict):
            return sum(
                AerospikeSink._estimate_size(k) + AerospikeSink._estimate_size(v)
                for k, v in value.items()
            )
        return 0

    def _record_size(self, record: AerospikeRecord) -> int:
        return sum(
            self._estimate_size(name) + self._estimate_size(value)
            for name, value in record.bins.items()
        )

    def _too_large_reason(self, record: AerospikeRecord) -> Optional[str]:
        """Return a RecordTooLargeError message if the record exceeds the limit."""
        limit = self._config.max_record_size
        size = self._record_size(record)
        if size > limit:
            return (
                f"record '{record.key}' is ~{size} bytes, exceeding the "
                f"{limit} byte ({limit // (1024 * 1024)} MiB) Aerospike limit; skipping"
            )
        return None

    def write(self, record: AerospikeRecord) -> None:
        """Persist a single record, honoring per-bin write policies and TTL."""
        if self._client is None:
            raise RuntimeError("AerospikeSink is not connected; call connect() first")

        reason = self._too_large_reason(record)
        if reason is not None:
            raise RecordTooLargeError(reason)

        unique_bins = {
            name: value
            for name, value in record.bins.items()
            if record.policy_for(name) is BinWritePolicy.UNIQUE_LIST
        }

        try:
            if not unique_bins:
                self._client.put(
                    self._key(record),
                    self._encode_bins_for_write(record.bins),
                    policy=self._write_policy(record),
                )
                return

            self._client.operate(
                self._key(record),
                self._build_ops(record, unique_bins),
                policy=self._write_policy(record),
            )
        except Exception as exc:
            if (
                self._config.record_exists_policy is RecordExistsPolicy.CREATE_ONLY
                and self._is_record_exists_error(exc)
            ):
                raise RecordAlreadyExists from exc
            raise

    def write_many(self, records: List[AerospikeRecord]) -> List[Optional[str]]:
        """Persist a batch of records via a single ``batch_write`` round-trip.

        Returns a per-record outcome list aligned 1:1 with ``records``: ``None``
        for a successful insert, otherwise a short failure reason. Each record
        carries its own TTL and is checked individually against its batch reply,
        so one failing record never fails its batch mates.

        SCAN can legitimately yield a key more than once. Sending the same key
        twice in one ``batch_write`` is not rejected and won't crash, but the
        sub-commands contend on the same record and can trigger a "key busy"
        (hot-key) situation, which we want to avoid. Duplicate keys are therefore
        collapsed to a single write -- the last occurrence wins, which matches the
        overwrite semantics of sequential single writes -- and that write's reply
        is reported for every input position sharing the key.
        """
        if self._client is None:
            raise RuntimeError("AerospikeSink is not connected; call connect() first")

        from aerospike_helpers.batch.records import BatchRecords, Write

        results: List[Optional[str]] = [None] * len(records)
        # Aerospike key per non-oversized record, plus the last input slot that
        # carries each key (the one we actually send: last-write-wins).
        key_of: List[Optional[AerospikeKeyTuple]] = [None] * len(records)
        last_slot_for_key: Dict[AerospikeKeyTuple, int] = {}
        for i, record in enumerate(records):
            reason = self._too_large_reason(record)
            if reason is not None:
                results[i] = "RecordTooLargeError"
                continue
            key = self._key(record)
            key_of[i] = key
            last_slot_for_key[key] = i

        if not last_slot_for_key:
            return results

        # One Write per unique key, built from its winning (last) occurrence.
        writes = []
        sent_keys: List[AerospikeKeyTuple] = []
        for key, slot in last_slot_for_key.items():
            record = records[slot]
            writes.append(
                Write(key, self._ops_for(record), policy=self._batch_policy(record))
            )
            sent_keys.append(key)

        batch_records = BatchRecords(writes)
        self._client.batch_write(batch_records)

        reason_for_key = {
            key: self._batch_reason(reply)
            for key, reply in zip(sent_keys, batch_records.batch_records)
        }
        # Propagate each key's outcome to every position that shares it.
        for i, key in enumerate(key_of):
            if key is not None:
                results[i] = reason_for_key.get(key)
        return results

    def _batch_reason(self, reply: Any) -> Optional[str]:
        """Map a single batch reply to ``None`` (ok) or a failure reason.

        ``result == 0`` is ``AS_PROTO_RESULT_OK``. A non-zero code fails only
        that record; ``in_doubt`` is surfaced because the write may or may not
        have been applied.
        """
        result = getattr(reply, "result", 0)
        if result == 0:
            return None
        if self._config.record_exists_policy is RecordExistsPolicy.CREATE_ONLY:
            exists_codes = self._record_exists_error_codes()
            if exists_codes and result in exists_codes:
                return BATCH_RECORD_EXISTS_OUTCOME
        if result == 22:
            # AS_ERR_FORBIDDEN. The usual cause in this tool is a positive TTL
            # written while the namespace has nsup-period 0.
            return (
                "BatchError:22:forbidden (TTL write rejected; "
                "namespace nsup-period is 0)"
            )
        reason = f"BatchError:{result}"
        if getattr(reply, "in_doubt", False):
            reason += ":in_doubt"
        return reason

    def _batch_policy(self, record: AerospikeRecord) -> Dict[str, Any]:
        """Per-record batch write policy: this record's TTL plus key-send.

        The client's default op policies do not apply to batch sub-commands, so
        ``POLICY_KEY_SEND`` is set here when configured.
        """
        policy = dict(self._write_policy(record))
        if self._config.send_key:
            aerospike = self._aero()
            if hasattr(aerospike, "POLICY_KEY_SEND"):
                policy["key"] = aerospike.POLICY_KEY_SEND
        return policy

    def _ops_for(self, record: AerospikeRecord) -> List[dict]:
        """Build the operation list for a record (shared by single and batch).

        Records with no unique-list bins reduce to a plain ``write`` per bin;
        otherwise the unique-list handling in :meth:`_build_ops` is reused.
        """
        unique_bins = {
            name: value
            for name, value in record.bins.items()
            if record.policy_for(name) is BinWritePolicy.UNIQUE_LIST
        }
        if not unique_bins:
            from aerospike_helpers.operations import operations

            return [
                operations.write(name, self._encode_bin_value_for_write(value))
                for name, value in record.bins.items()
            ]
        return self._build_ops(record, unique_bins)

    def _build_ops(self, record: AerospikeRecord, unique_bins: Dict[str, Any]) -> List[dict]:
        aerospike = self._aero()
        from aerospike_helpers.operations import list_operations, operations

        # Ordered list + ADD_UNIQUE enforces set semantics server-side. NO_FAIL +
        # PARTIAL make re-runs idempotent: duplicates are silently skipped rather
        # than aborting the write.
        list_policy = {
            "list_order": aerospike.LIST_ORDERED,
            "write_flags": (
                aerospike.LIST_WRITE_ADD_UNIQUE
                | aerospike.LIST_WRITE_NO_FAIL
                | aerospike.LIST_WRITE_PARTIAL
            ),
        }

        ops: List[dict] = []
        for name, value in record.bins.items():
            if name in unique_bins:
                # Reset then uniquely append so the bin reflects exactly this set.
                ops.append(operations.write(name, []))
                ops.append(list_operations.list_append_items(name, list(value), list_policy))
            else:
                ops.append(operations.write(name, self._encode_bin_value_for_write(value)))
        return ops
