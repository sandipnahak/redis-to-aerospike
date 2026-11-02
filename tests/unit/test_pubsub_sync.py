"""Pub/sub apply and subscription behavior without a Redis server."""

import time

from redis_to_aerospike.config import (
    AerospikeConfig,
    AerospikeSetRoute,
    MigrationConfig,
    RedisConfig,
    RunMode,
)
from redis_to_aerospike.converters.registry import ConverterRegistry
from redis_to_aerospike.keyspace import KeyChange
from redis_to_aerospike.models import RedisRecord
from redis_to_aerospike.pubsub_sync import KeyspaceListener, PubSubRunner, primary_endpoints


class _Source:
    def __init__(self, record):
        self.record = record
        self.reads = []
        self.client = None

    def read_record(self, key):
        self.reads.append(key)
        if callable(self.record):
            return self.record(key)
        return self.record


class _Sink:
    def __init__(self):
        self.written = []
        self.removed = []

    def write(self, record):
        self.written.append(record)

    def write_many(self, records):
        raise AssertionError("pub/sub writes one record at a time")

    def remove(self, key, set_name=None):
        self.removed.append((set_name, key))


def _runner(source, sink, *, match="*", routes=()):
    config = MigrationConfig(
        mode=RunMode.PUBSUB,
        redis=RedisConfig(scan_match=match),
        aerospike=AerospikeConfig(set_name="redis", set_routes=list(routes)),
    )
    return PubSubRunner(
        config,
        source,
        ConverterRegistry.from_config(config),
        sink,
        notify_flags="KEA",
        listener=object(),
    )


def test_upsert_rereads_and_replaces_the_routed_record():
    source = _Source(RedisRecord(key="user:1", type="string", value=b"v", ttl_ms=None))
    sink = _Sink()
    runner = _runner(
        source,
        sink,
        routes=(AerospikeSetRoute(pattern="user:*", destination="users"),),
    )
    runner.apply(KeyChange(key="user:1", event="set", action="upsert"))

    assert source.reads == ["user:1"]
    assert len(sink.written) == 1
    record = sink.written[0]
    assert record.key == "1"
    assert record.set_name == "users"
    assert record.bins["value"] == "v"
    assert record.full_replace is True
    assert sink.removed == []
    assert runner.stats.events == 1
    assert runner.stats.migrated == 1


def test_expired_deletes_without_reading():
    source = _Source(lambda key: (_ for _ in ()).throw(AssertionError("read")))
    sink = _Sink()
    runner = _runner(source, sink)
    runner.apply(KeyChange(key="session:9", event="expired", action="delete"))

    assert source.reads == []
    assert sink.written == []
    assert sink.removed == [("redis", "session:9")]
    assert runner.stats.deleted == 1


def test_missing_key_on_upsert_deletes_the_aerospike_record():
    source = _Source(None)
    sink = _Sink()
    runner = _runner(source, sink)
    runner.apply(KeyChange(key="gone", event="set", action="upsert"))
    assert sink.removed == [("redis", "gone")]
    assert sink.written == []


def test_scan_match_skips_other_keys():
    source = _Source(RedisRecord(key="session:1", type="string", value=b"v", ttl_ms=None))
    sink = _Sink()
    runner = _runner(source, sink, match="user:*")
    runner.apply(KeyChange(key="session:1", event="set", action="upsert"))
    assert source.reads == []
    assert sink.written == []
    assert runner.stats.skipped_by_type["match"] == 1


def test_unsupported_type_is_skipped():
    source = _Source(RedisRecord(key="s", type="stream", value=b"1-0", ttl_ms=None))
    sink = _Sink()
    runner = _runner(source, sink)
    runner.apply(KeyChange(key="s", event="xadd", action="upsert"))
    assert sink.written == []
    assert sink.removed == []
    assert runner.stats.skipped_by_type["stream"] == 1


def test_unreadable_type_is_not_written_as_empty():
    source = _Source(RedisRecord(key="items", type="list", value=None, ttl_ms=None))
    sink = _Sink()
    runner = _runner(source, sink)
    runner.apply(KeyChange(key="items", event="lpush", action="upsert"))
    assert sink.written == []
    assert sink.removed == []
    assert runner.stats.skipped_by_type["list"] == 1


class _Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port


def test_primary_endpoints_uses_primaries_only():
    class Cluster:
        def get_primaries(self):
            return [_Node("10.0.0.1", 6379), _Node("10.0.0.2", 6379), _Node("10.0.0.1", 6379)]

    assert primary_endpoints(Cluster(), RedisConfig(cluster=True)) == [
        ("10.0.0.1", 6379),
        ("10.0.0.2", 6379),
    ]


def test_primary_endpoints_standalone_is_one_node():
    assert primary_endpoints(object(), RedisConfig(host="redis.local", port=6380)) == [
        ("redis.local", 6380)
    ]


class _ScriptedPubSub:
    def __init__(self, messages):
        self.messages = list(messages)
        self.patterns = []
        self.closed = False

    def psubscribe(self, pattern):
        self.patterns.append(pattern)

    def get_message(self, timeout=0.0):
        if self.messages:
            return self.messages.pop(0)
        time.sleep(0.01)
        return None

    def close(self):
        self.closed = True


class _ScriptedClient:
    def __init__(self, messages):
        self.pubsub_obj = _ScriptedPubSub(messages)
        self.closed = False

    def pubsub(self, ignore_subscribe_messages=True):
        return self.pubsub_obj

    def close(self):
        self.closed = True


def test_listener_delivers_one_notification_per_primary():
    client = _ScriptedClient(
        [
            {
                "type": "pmessage",
                "channel": b"__keyspace@0__:user:1",
                "data": b"set",
            }
        ]
    )
    config = MigrationConfig(mode=RunMode.PUBSUB, pubsub_refresh_s=0)
    listener = KeyspaceListener(
        config,
        client=None,
        pattern="__keyspace@0__:*",
        db=0,
        connect=lambda host, port: client,
        discover=lambda: [("10.0.0.1", 6379)],
    )
    try:
        assert listener.start() == 1
        change = listener.queue.get(timeout=2)
    finally:
        listener.stop()

    assert change.key == "user:1"
    assert change.event == "set"
    assert change.action == "upsert"
    assert client.pubsub_obj.patterns == ["__keyspace@0__:*"]
    assert client.closed is True
