import threading
import time
from typing import List, Optional

from redis_to_aerospike.config import (
    AerospikeSetRoute,
    HashStrategy,
    MigrationConfig,
    RecordExistsPolicy,
)
from redis_to_aerospike.converters.base import Converter
from redis_to_aerospike.converters.registry import ConverterRegistry
from redis_to_aerospike.aerospike_sink import RecordAlreadyExists
from redis_to_aerospike.migrator import Migrator
from redis_to_aerospike.models import AerospikeRecord, RedisRecord
from redis_to_aerospike.transforms import Transform


class FakeSource:
    def __init__(self, records):
        self._records = records

    def iter_records(self, batch_size):
        for r in self._records:
            yield r


class FakeSink:
    def __init__(self, fail_keys=None):
        self.written = {}
        self._fail_keys = set(fail_keys or [])
        self._lock = threading.Lock()

    def write(self, record: AerospikeRecord):
        if record.key in self._fail_keys:
            raise RuntimeError("boom")
        with self._lock:
            self.written[record.key] = record

    def write_many(self, records: List[AerospikeRecord]) -> List[Optional[str]]:
        """Batch path: mirror ``write`` outcomes (``_Sink`` protocol)."""
        results: List[Optional[str]] = []
        for record in records:
            if record.key in self._fail_keys:
                results.append("RuntimeError")
            else:
                with self._lock:
                    self.written[record.key] = record
                results.append(None)
        return results


def make_config(**kwargs):
    config = MigrationConfig(workers=4, scan_batch=10, queue_size=16)
    for k, v in kwargs.items():
        setattr(config, k, v)
    return config


class BatchSink:
    """A batch-capable sink double.

    ``fail_keys`` maps a key to a failure reason returned (per record) from
    write_many, exercising the per-reply error path.
    """

    def __init__(self, fail_keys=None):
        self.written = {}
        self.batches = []  # sizes of each flushed batch
        self._fail = dict(fail_keys or {})
        self._lock = threading.Lock()

    def write(self, record: AerospikeRecord):  # pragma: no cover - batch mode
        raise AssertionError("write() should not be called in batch mode")

    def write_many(self, records):
        with self._lock:
            self.batches.append(len(records))
        results = []
        for record in records:
            reason = self._fail.get(record.key)
            if reason is None:
                with self._lock:
                    self.written[record.key] = record
            results.append(reason)
        return results


class CountingBucket:
    """A TokenBucket stand-in that records the total tokens acquired."""

    def __init__(self):
        self.total = 0
        self._lock = threading.Lock()

    def acquire(self, tokens: float = 1.0) -> None:
        with self._lock:
            self.total += int(tokens)


def test_all_records_flow_through():
    records = [
        RedisRecord(key=f"k{i}", type="string", value=str(i).encode())
        for i in range(50)
    ]
    sink = FakeSink()
    migrator = Migrator(make_config(), FakeSource(records), ConverterRegistry(), sink)

    stats = migrator.run()

    assert stats.scanned == 50
    assert stats.migrated == 50
    assert stats.errors == 0
    assert len(sink.written) == 50
    assert sink.written["k7"].bins == {"value": 7}


def test_set_routes_set_destination_set_on_record():
    config = make_config()
    config.aerospike.set_routes = [AerospikeSetRoute("user:*", "users")]
    records = [
        RedisRecord(key="user:1", type="string", value=b"a"),
        RedisRecord(key="other", type="string", value=b"b"),
    ]
    sink = FakeSink()
    Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert sink.written["1"].set_name == "users"
    assert sink.written["other"].set_name is None


def test_route_hash_strategy_overrides_global_for_hash():
    config = make_config()
    config.hash_strategy = HashStrategy.MAP_BIN
    config.aerospike.set_routes = [
        AerospikeSetRoute("h:*", "hashes", hash_strategy=HashStrategy.FIELD_BINS)
    ]
    records = [
        RedisRecord(key="h:1", type="hash", value={b"a": b"1", b"b": b"2"}),
    ]
    sink = FakeSink()
    Migrator(
        config, FakeSource(records), ConverterRegistry.from_config(config), sink
    ).run()
    assert set(sink.written["1"].bins) == {"a", "b"}


def test_route_map_bin_custom_value_bin():
    config = make_config()
    config.aerospike.set_routes = [
        AerospikeSetRoute(
            "h:*", "hashes", hash_strategy=HashStrategy.MAP_BIN, value_bin="profile"
        )
    ]
    records = [RedisRecord(key="h:1", type="hash", value={b"x": b"y"})]
    sink = FakeSink()
    Migrator(
        config, FakeSource(records), ConverterRegistry.from_config(config), sink
    ).run()
    assert "profile" in sink.written["1"].bins
    assert sink.written["1"].bins["profile"] == {"x": "y"}


def test_route_value_bin_ignored_when_field_bins_effective():
    config = make_config()
    config.hash_strategy = HashStrategy.FIELD_BINS
    config.aerospike.set_routes = [
        AerospikeSetRoute("h:*", "hashes", value_bin="ignored")
    ]
    records = [RedisRecord(key="h:1", type="hash", value={b"f": b"v"})]
    sink = FakeSink()
    Migrator(
        config, FakeSource(records), ConverterRegistry.from_config(config), sink
    ).run()
    assert sink.written["1"].bins == {"f": "v"}


def test_unsupported_type_is_skipped_not_fatal():
    records = [
        RedisRecord(key="ok", type="string", value=b"1"),
        RedisRecord(key="weird", type="stream", value=None),
    ]
    sink = FakeSink()
    migrator = Migrator(make_config(workers=2), FakeSource(records), ConverterRegistry(), sink)

    stats = migrator.run()

    assert stats.migrated == 1
    assert stats.skipped == 1
    assert stats.skipped_by_type == {"stream": 1}
    assert "weird" not in sink.written


def test_write_errors_are_isolated():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(10)]
    sink = FakeSink(fail_keys={"k3", "k7"})
    migrator = Migrator(make_config(workers=4), FakeSource(records), ConverterRegistry(), sink)

    stats = migrator.run()

    assert stats.migrated == 8
    assert stats.errors == 2
    assert stats.errors_by_type == {"write:RuntimeError": 2}
    assert "k3" not in sink.written and "k7" not in sink.written


def test_transforms_are_applied():
    class TagTransform(Transform):
        def apply(self, record):
            record.bins["tag"] = "migrated"
            return record

    records = [RedisRecord(key="k", type="string", value=b"1")]
    sink = FakeSink()
    migrator = Migrator(
        make_config(workers=1),
        FakeSource(records),
        ConverterRegistry(),
        sink,
        transforms=[TagTransform()],
    )

    migrator.run()

    assert sink.written["k"].bins == {"value": 1, "tag": "migrated"}


def test_conversion_errors_are_isolated():
    class _BoomConverter(Converter):
        redis_type = "string"

        def convert(self, record):
            raise ValueError("cannot convert")

    registry = ConverterRegistry()
    registry.register(_BoomConverter())

    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(5)]
    sink = FakeSink()
    migrator = Migrator(make_config(workers=2), FakeSource(records), registry, sink)

    stats = migrator.run()

    assert stats.migrated == 0
    assert stats.errors == 5
    assert stats.errors_by_type == {"convert:ValueError": 5}
    assert sink.written == {}


def test_backpressure_with_tiny_queue_and_slow_sink_loses_nothing():
    class SlowSink(FakeSink):
        def write(self, record):
            time.sleep(0.001)
            super().write(record)

    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(40)]
    sink = SlowSink()
    config = make_config(workers=3, queue_size=1)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.scanned == 40
    assert stats.migrated == 40
    assert len(sink.written) == 40


def test_write_rate_limit_preserves_correctness_and_throttles():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(300)]
    sink = FakeSink()
    # capacity defaults to the rate (200), so the 100 records over the burst at
    # 200/s imply roughly >= 0.5s of throttling.
    config = make_config(workers=4, write_rate_limit=200)
    start = time.monotonic()
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()
    elapsed = time.monotonic() - start

    assert stats.migrated == 300
    assert stats.errors == 0
    assert len(sink.written) == 300
    assert elapsed >= 0.2


def test_scan_rate_limit_preserves_correctness():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(15)]
    sink = FakeSink()
    config = make_config(workers=2, scan_rate_limit=200)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.scanned == 15
    assert stats.migrated == 15


def test_default_config_is_not_throttled():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(100)]
    sink = FakeSink()
    config = make_config(workers=4)
    start = time.monotonic()
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()
    elapsed = time.monotonic() - start

    assert stats.migrated == 100
    # No limits configured, so the run is effectively instant.
    assert elapsed < 0.5


def test_duplicate_keys_from_scan_do_not_crash():
    # SCAN can legitimately return the same key more than once.
    records = [
        RedisRecord(key="dup", type="string", value=b"1"),
        RedisRecord(key="dup", type="string", value=b"1"),
    ]
    sink = FakeSink()
    stats = Migrator(make_config(workers=2), FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.migrated == 2
    assert "dup" in sink.written


def test_write_one_create_only_record_already_exists_counts_as_skip():
    class SkipSink(FakeSink):
        def write(self, record):
            if record.key == "exists":
                raise RecordAlreadyExists()
            super().write(record)

    records = [
        RedisRecord(key="exists", type="string", value=b"1"),
        RedisRecord(key="new", type="string", value=b"2"),
    ]
    config = make_config()
    config.aerospike.record_exists_policy = RecordExistsPolicy.CREATE_ONLY
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), SkipSink()).run()

    assert stats.migrated == 1
    assert stats.skipped == 1
    assert stats.skipped_by_type == {"exists": 1}
    assert stats.errors == 0


def test_batch_create_only_record_exists_outcome_counts_as_skip():
    records = [
        RedisRecord(key="hit", type="string", value=b"1"),
        RedisRecord(key="ok", type="string", value=b"2"),
    ]
    sink = BatchSink(
        fail_keys={
            "hit": "RecordExists",
        }
    )
    config = make_config(workers=1, write_batch_size=10)
    config.aerospike.record_exists_policy = RecordExistsPolicy.CREATE_ONLY
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.migrated == 1
    assert stats.skipped == 1
    assert stats.skipped_by_type == {"exists": 1}
    assert stats.errors == 0


def test_batch_record_exists_outcome_is_error_when_not_create_only():
    records = [RedisRecord(key="hit", type="string", value=b"1")]
    sink = BatchSink(fail_keys={"hit": "RecordExists"})
    config = make_config(workers=1, write_batch_size=10)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.migrated == 0
    assert stats.errors == 1
    assert "write:RecordExists" in stats.errors_by_type


# --- batch mode ------------------------------------------------------------

def test_batch_mode_migrates_all_records_via_write_many():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(50)]
    sink = BatchSink()
    config = make_config(workers=1, write_batch_size=8)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.migrated == 50
    assert stats.errors == 0
    assert len(sink.written) == 50
    # All went through write_many (not single write()); a final partial batch is
    # flushed at shutdown, so 50 records in batches of 8 means a trailing 2.
    assert sum(sink.batches) == 50
    assert 2 in sink.batches  # the leftover partial batch


def test_batch_mode_partial_final_batch_is_flushed():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(3)]
    sink = BatchSink()
    config = make_config(workers=1, write_batch_size=10)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    # Fewer records than one batch: still flushed on shutdown.
    assert stats.migrated == 3
    assert sink.batches == [3]


def test_batch_mode_per_record_failures_are_isolated():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(10)]
    sink = BatchSink(fail_keys={"k3": "BatchError:-3", "k7": "BatchError:-11:in_doubt"})
    config = make_config(workers=1, write_batch_size=10)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.migrated == 8
    assert stats.errors == 2
    assert stats.errors_by_type == {
        "write:BatchError:-3": 1,
        "write:BatchError:-11:in_doubt": 1,
    }
    assert "k3" not in sink.written and "k7" not in sink.written


def test_batch_whole_failure_counts_every_record_as_error():
    class BoomSink(BatchSink):
        def write_many(self, records):
            raise RuntimeError("cluster down")

    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(5)]
    sink = BoomSink()
    config = make_config(workers=1, write_batch_size=5)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.migrated == 0
    assert stats.errors == 5
    assert stats.errors_by_type == {"write:RuntimeError": 5}


def test_batch_mode_duplicate_keys_collapse_to_one_write():
    # SCAN can return the same key more than once; in one batch that must not
    # produce a duplicate key in the batch_write command.
    records = [
        RedisRecord(key="dup", type="string", value=b"1"),
        RedisRecord(key="dup", type="string", value=b"1"),
        RedisRecord(key="solo", type="string", value=b"1"),
    ]
    sink = BatchSink()
    config = make_config(workers=1, write_batch_size=10)
    stats = Migrator(config, FakeSource(records), ConverterRegistry(), sink).run()

    assert stats.errors == 0
    # Both duplicate positions count as migrated, but only the unique keys land.
    assert stats.migrated == 3
    assert set(sink.written) == {"dup", "solo"}


def test_replay_skips_a_record_when_aerospike_is_newer():
    sink = FakeSink()
    sink.last_update_ms = lambda key, set_name=None: 9_000_000_000_000
    migrator = Migrator(make_config(), FakeSource([]), ConverterRegistry(), sink)
    record = RedisRecord(key="user:1", type="string", value=b"v", ttl_ms=None)

    assert migrator.replay_upsert(record, "2026-11-03T16:42:00+05:30") is False
    migrator.replay_delete("user:1", "2026-11-03T16:42:00+05:30")

    assert sink.written == {}
    assert migrator.stats.skipped_by_type["latest"] == 2
    assert migrator.stats.migrated == 0
    assert migrator.stats.deleted == 0


def test_replay_writes_when_aerospike_is_older():
    sink = FakeSink()
    sink.last_update_ms = lambda key, set_name=None: 0
    migrator = Migrator(make_config(), FakeSource([]), ConverterRegistry(), sink)
    record = RedisRecord(key="user:1", type="string", value=b"v", ttl_ms=None)

    assert migrator.replay_upsert(record, "2026-11-03T16:42:00+05:30") is True
    assert "user:1" in sink.written
    assert migrator.stats.migrated == 1


def test_rate_limiter_counts_individual_records_even_when_batched():
    records = [RedisRecord(key=f"k{i}", type="string", value=b"1") for i in range(20)]
    sink = BatchSink()
    config = make_config(workers=1, write_batch_size=7)
    migrator = Migrator(config, FakeSource(records), ConverterRegistry(), sink)
    counter = CountingBucket()
    migrator._write_limiter = counter

    stats = migrator.run()

    assert stats.migrated == 20
    # One token per record, regardless of how they were batched.
    assert counter.total == 20
