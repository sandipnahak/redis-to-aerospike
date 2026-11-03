"""Parity classification and the shadow-read runner. No servers required."""

from redis_to_aerospike.config import MigrationConfig
from redis_to_aerospike.converters.registry import ConverterRegistry
from redis_to_aerospike.models import TTL_NEVER_EXPIRE, RedisRecord
from redis_to_aerospike.parity import (
    ParityRunner,
    Verdict,
    classify,
    key_in_sample,
    percentile,
    redis_changed,
)


def test_percentile_interpolates():
    assert percentile([], 95) == 0.0
    assert percentile([4.0], 99) == 4.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.5


def test_sample_is_stable_and_full_rate_selects_every_key():
    assert key_in_sample("user:1", 1) is True
    assert key_in_sample("user:1", 0) is False
    assert key_in_sample("user:1", 0.5) == key_in_sample("user:1", 0.5)


def test_classify_match_skew_and_mismatch():
    bins = {"value": "ada"}
    assert classify(bins, TTL_NEVER_EXPIRE, {"value": "ada"}, TTL_NEVER_EXPIRE, 5000) is Verdict.MATCH
    assert classify(bins, 10, {"value": "ada"}, 12, 5000) is Verdict.TTL_SKEW
    assert classify(bins, 10, {"value": "ada"}, 30, 5000) is Verdict.MISMATCH
    assert classify(bins, 10, None, None, 5000) is Verdict.MISSING
    assert classify(bins, 10, {"value": "other"}, 10, 5000) is Verdict.MISMATCH


def test_redis_changed_ignores_ttl_drift():
    first = RedisRecord(key="a", type="string", value=b"v", ttl_ms=1000)
    same = RedisRecord(key="a", type="string", value=b"v", ttl_ms=900)
    other = RedisRecord(key="a", type="string", value=b"w", ttl_ms=1000)
    assert redis_changed(first, same) is False
    assert redis_changed(first, other) is True
    assert redis_changed(first, None) is True


class _Source:
    def __init__(self, records, fresh):
        self._records = records
        self.fresh = fresh

    def iter_records(self, batch_size):
        yield from self._records

    def read_record(self, key):
        return self.fresh.get(key)


class _Sink:
    def __init__(self, rows):
        self.rows = rows

    def read(self, key, set_name=None):
        return self.rows.get(key)


def _record(key, value=b"v", ttl_ms=None):
    return RedisRecord(key=key, type="string", value=value, ttl_ms=ttl_ms)


def test_runner_reports_rates_and_blocks_on_unexplained_mismatches():
    records = [
        _record("match"),
        _record("skew", ttl_ms=10_000),
        _record("missing"),
        _record("inflight"),
        _record("bad"),
    ]
    fresh = {
        "missing": records[2],
        "bad": records[4],
    }
    sink = _Sink(
        {
            "match": ({"value": "v"}, TTL_NEVER_EXPIRE),
            "skew": ({"value": "v"}, 12),
            "bad": ({"value": "other"}, TTL_NEVER_EXPIRE),
        }
    )
    config = MigrationConfig(parity_sample=1, parity_ttl_skew_ms=5000, workers=2)
    report = ParityRunner(config, _Source(records, fresh), ConverterRegistry.from_config(config), sink).run()

    assert report.seen == 5
    assert report.checked == 5
    assert report.matched == 1
    assert report.ttl_skew == 1
    assert report.missing == 1
    assert report.inflight == 1
    assert report.mismatch == 1
    assert report.unexplained == 2
    assert report.mismatch_rate == 0.8
    assert len(report.latencies_ms) == 5
    text = report.format()
    assert "aerospike p95" in text
    assert "unexplained   : 2" in text
