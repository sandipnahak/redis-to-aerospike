"""Event log order and replay. No Redis or Aerospike server required."""

import json

from datetime import datetime, timedelta, timezone

from redis_to_aerospike.event_log import (
    EventLog,
    dump_obj,
    load_obj,
    record_from_entry,
    replay_log,
    timestamped_event_log_path,
    upsert_entry,
)
from redis_to_aerospike.models import RedisRecord


def test_dump_obj_roundtrips_binary_and_containers():
    value = {
        b"name": b"ada",
        "tags": {"a", "b"},
        "scores": [("x", 1.5)],
    }
    assert load_obj(dump_obj(value)) == value


def test_event_log_name_includes_a_local_timestamp():
    when = datetime(2026, 11, 3, 16, 3, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    assert (
        timestamped_event_log_path("migration/redis2aerospike-pubsub.jsonl", when)
        == "migration/redis2aerospike-pubsub-20261103T160300+0530.jsonl"
    )


def test_event_log_writes_lines_in_receive_order(tmp_path):
    path = tmp_path / "events.jsonl"
    log = EventLog(str(path), stamp=False)
    first = log.assign()
    second = log.assign()
    log.complete(second, {"action": "delete", "event": "del", "key": "b"})
    log.complete(first, {"action": "upsert", "event": "set", "key": "a"})
    log.complete(log.assign(), None)
    log.close()

    lines = path.read_text(encoding="utf-8").splitlines()
    parsed = [json.loads(line) for line in lines]
    assert [line["seq"] for line in parsed] == [1, 2]
    assert parsed[0]["key"] == "a"
    assert parsed[1]["action"] == "delete"
    assert all("T" in line["time"] for line in parsed)


def test_replay_reads_the_file_in_order(tmp_path):
    path = tmp_path / "events.jsonl"
    log = EventLog(str(path), stamp=False)
    record = RedisRecord(key="user:1", type="string", value=b"v", ttl_ms=1000)
    seq = log.assign()
    log.complete(seq, upsert_entry("set", record))
    seq = log.assign()
    log.complete(seq, {"action": "delete", "event": "del", "key": dump_obj("user:1")})
    log.close()

    seen = []

    def apply_entry(entry):
        seen.append(entry["action"])
        if entry["action"] == "upsert":
            restored = record_from_entry(entry)
            assert restored.key == "user:1"
            assert restored.value == b"v"
            assert restored.ttl_ms == 1000

    assert replay_log(str(path), apply_entry) == 2
    assert seen == ["upsert", "delete"]

    seen.clear()
    assert replay_log(str(path), apply_entry, from_seq=2) == 1
    assert seen == ["delete"]
