"""Shadow-read MemoryDB keys from Aerospike and gate on unexplained mismatches.

MemoryDB stays the source of the keys. Aerospike is read in the background for
a sample of those keys. A difference is explainable when it is only TTL skew
within ``parity_ttl_skew_ms``, or the MemoryDB key changed between the scan
read and a second read (an in-flight write or delete). Any other mismatch,
including a key still present in MemoryDB and missing in Aerospike, means the
run must not proceed.
"""

from __future__ import annotations

import logging
import math
import queue
import threading
import time
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from .migrator import Migrator
from .models import TTL_NEVER_EXPIRE, RedisRecord

logger = logging.getLogger(__name__)

_STOP = object()


class Verdict(str, Enum):
    MATCH = "match"
    MISSING = "missing"
    MISMATCH = "mismatch"
    TTL_SKEW = "ttl_skew"
    INFLIGHT = "inflight"


def key_in_sample(key: str | bytes, rate: float) -> bool:
    """True when this key falls in the parity sample.

    ``rate`` is a fraction from 0 to 1. ``1`` selects every key. The bucket is
    a CRC32 of the key, so the same sample is chosen on a later run.
    """
    if rate >= 1:
        return True
    if rate <= 0:
        return False
    raw = key if isinstance(key, bytes) else str(key).encode("utf-8", "surrogateescape")
    return (zlib.crc32(raw) % 10_000) < int(rate * 10_000)


def percentile(values: list[float], pct: float) -> float:
    """Linear percentile of ``values``. ``pct`` is 0-100. Empty input is 0."""
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (pct / 100.0)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return ordered[lo]
    weight = rank - lo
    return ordered[lo] * (1 - weight) + ordered[hi] * weight


def _canon(value: Any) -> Any:
    """Normalize Aerospike and converter values so a round-trip compares equal."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value
    if isinstance(value, tuple):
        return [_canon(item) for item in value]
    if isinstance(value, list):
        return [_canon(item) for item in value]
    if isinstance(value, dict):
        return {_canon(key): _canon(item) for key, item in value.items()}
    return value


def _never(ttl_s: Optional[int]) -> bool:
    return ttl_s is None or ttl_s < 0 or ttl_s >= TTL_NEVER_EXPIRE


def ttl_delta_ms(expected_s: Optional[int], actual_s: Optional[int]) -> Optional[int]:
    """Absolute TTL difference in milliseconds.

    ``None`` means one side expires and the other does not. That is not skew.
    """
    if _never(expected_s) and _never(actual_s):
        return 0
    if _never(expected_s) or _never(actual_s):
        return None
    return abs(int(expected_s) - int(actual_s)) * 1000


def classify(
    expected_bins: dict[str, Any],
    expected_ttl_s: Optional[int],
    actual_bins: Optional[dict[str, Any]],
    actual_ttl_s: Optional[int],
    skew_ms: int,
) -> Verdict:
    """Compare one expected record with the Aerospike read.

    Callers upgrade ``MISSING`` or ``MISMATCH`` to ``INFLIGHT`` when MemoryDB
    changed on a second read.
    """
    if actual_bins is None:
        return Verdict.MISSING
    values_equal = _canon(expected_bins) == _canon(actual_bins)
    delta = ttl_delta_ms(expected_ttl_s, actual_ttl_s)
    if values_equal and delta == 0:
        return Verdict.MATCH
    if values_equal and delta is not None and delta <= skew_ms:
        return Verdict.TTL_SKEW
    return Verdict.MISMATCH


def redis_changed(first: RedisRecord, second: Optional[RedisRecord]) -> bool:
    """True when the key disappeared or its value changed. TTL-only drift is not a change."""
    if second is None:
        return True
    return first.type != second.type or first.value != second.value


@dataclass
class ParityReport:
    """Counts for one shadow-read pass."""

    seen: int = 0
    checked: int = 0
    matched: int = 0
    missing: int = 0
    mismatch: int = 0
    ttl_skew: int = 0
    inflight: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)

    @property
    def unexplained(self) -> int:
        """Missing keys and value mismatches that a second MemoryDB read did not explain."""
        return self.missing + self.mismatch + self.errors

    @property
    def mismatch_rate(self) -> float:
        """Share of checked keys that were not an exact match, including explainable ones."""
        if self.checked == 0:
            return 0.0
        return (self.checked - self.matched) / self.checked

    def record(self, verdict: Verdict, key: str | bytes, latency_ms: float) -> None:
        with self._lock:
            self.checked += 1
            self.latencies_ms.append(latency_ms)
            if verdict is Verdict.MATCH:
                self.matched += 1
            elif verdict is Verdict.MISSING:
                self.missing += 1
            elif verdict is Verdict.MISMATCH:
                self.mismatch += 1
            elif verdict is Verdict.TTL_SKEW:
                self.ttl_skew += 1
            else:
                self.inflight += 1
            if verdict in (Verdict.MISSING, Verdict.MISMATCH) and len(self.examples) < 20:
                shown = key.decode("utf-8", "replace") if isinstance(key, bytes) else str(key)
                self.examples.append(f"{verdict.value} {shown}")

    def record_error(self, key: str | bytes) -> None:
        with self._lock:
            self.checked += 1
            self.errors += 1
            if len(self.examples) < 20:
                shown = key.decode("utf-8", "replace") if isinstance(key, bytes) else str(key)
                self.examples.append(f"error {shown}")

    def format(self) -> str:
        p50 = percentile(self.latencies_ms, 50)
        p95 = percentile(self.latencies_ms, 95)
        p99 = percentile(self.latencies_ms, 99)
        lines = [
            "parity",
            f"  seen          : {self.seen}",
            f"  checked       : {self.checked}",
            f"  matched       : {self.matched}",
            f"  missing       : {self.missing}",
            f"  mismatch      : {self.mismatch}",
            f"  ttl skew      : {self.ttl_skew}",
            f"  in-flight     : {self.inflight}",
            f"  errors        : {self.errors}",
            f"  mismatch rate : {self.mismatch_rate:.2%}",
            f"  unexplained   : {self.unexplained}",
            f"  aerospike p50 : {p50:.2f} ms",
            f"  aerospike p95 : {p95:.2f} ms",
            f"  aerospike p99 : {p99:.2f} ms",
        ]
        if self.examples:
            lines.append("  examples      :")
            lines.extend(f"    {item}" for item in self.examples)
        return "\n".join(lines)


class ParityRunner:
    """Scan MemoryDB and compare a sample with Aerospike. Does not write."""

    def __init__(self, config, source, registry, sink):
        self._config = config
        self._source = source
        self._sink = sink
        self._migrator = Migrator(config, source, registry, sink)
        self.report = ParityReport()
        self._queue: queue.Queue = queue.Queue(maxsize=max(1, config.queue_size))

    def run(self) -> ParityReport:
        workers = max(1, self._config.workers)
        threads = [
            threading.Thread(target=self._worker, name=f"parity-{i}", daemon=True)
            for i in range(workers)
        ]
        for thread in threads:
            thread.start()
        try:
            for record in self._source.iter_records(self._config.scan_batch):
                self.report.seen += 1
                if not key_in_sample(record.key, self._config.parity_sample):
                    continue
                self._queue.put(record)
        finally:
            for _ in threads:
                self._queue.put(_STOP)
            for thread in threads:
                thread.join()
        return self.report

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is _STOP:
                    return
                self._check(item)
            finally:
                self._queue.task_done()

    def _check(self, record: RedisRecord) -> None:
        prepared = self._migrator._prepare(record)
        if prepared is None:
            return
        started = time.perf_counter()
        try:
            got = self._sink.read(prepared.key, prepared.set_name)
        except Exception as exc:
            logger.warning("parity read failed for key '%s': %s", record.key, exc)
            self.report.record_error(record.key)
            return
        latency_ms = (time.perf_counter() - started) * 1000
        if got is None:
            actual_bins, actual_ttl = None, None
        else:
            actual_bins, actual_ttl = got
        verdict = classify(
            prepared.bins,
            prepared.ttl_s,
            actual_bins,
            actual_ttl,
            self._config.parity_ttl_skew_ms,
        )
        if verdict in (Verdict.MISSING, Verdict.MISMATCH):
            try:
                fresh = self._source.read_record(record.key)
            except Exception as exc:
                logger.warning("parity re-read failed for key '%s': %s", record.key, exc)
                fresh = record
            if redis_changed(record, fresh):
                verdict = Verdict.INFLIGHT
        self.report.record(verdict, record.key, latency_ms)
