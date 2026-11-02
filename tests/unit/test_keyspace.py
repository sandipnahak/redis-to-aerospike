"""Unit tests for keyspace-notification parsing. No server required."""

import pytest

from redis_to_aerospike.config import PubSubChannel, RedisConfig
from redis_to_aerospike.keyspace import (
    key_matches,
    normalize_notify_flags,
    notification_db,
    notification_warning,
    parse_notification,
    select_channel_family,
    standalone_endpoint,
    subscription_pattern,
)


def test_kea_selects_keyspace_and_ex_selects_keyevent():
    assert select_channel_family("KEA", PubSubChannel.AUTO) == "keyspace"
    assert select_channel_family("Ex", PubSubChannel.AUTO) == "keyevent"
    assert select_channel_family("", PubSubChannel.AUTO) == "keyspace"
    assert select_channel_family(None, PubSubChannel.AUTO) == "keyspace"


def test_explicit_channel_overrides_flags():
    assert select_channel_family("KEA", PubSubChannel.KEYEVENT) == "keyevent"
    assert select_channel_family("Ex", PubSubChannel.KEYSPACE) == "keyspace"


def test_subscription_pattern_includes_db():
    assert subscription_pattern("keyspace", 0) == "__keyspace@0__:*"
    assert subscription_pattern("keyevent", 2) == "__keyevent@2__:*"


def test_cluster_notifications_use_db_zero():
    assert notification_db(RedisConfig(cluster=True, db=3)) == 0
    assert notification_db(RedisConfig(db=2)) == 2


def test_parse_keyspace_message_keeps_colons_in_the_key():
    change = parse_notification(b"__keyspace@0__:user:1", b"hset", 0)
    assert change is not None
    assert change.key == "user:1"
    assert change.event == "hset"
    assert change.action == "upsert"


def test_parse_keyevent_expired_is_a_delete():
    change = parse_notification(b"__keyevent@0__:expired", b"session:9", 0)
    assert change is not None
    assert change.key == "session:9"
    assert change.event == "expired"
    assert change.action == "delete"


@pytest.mark.parametrize(
    "event",
    ["del", "expired", "evicted", "rename_from", "move_from"],
)
def test_delete_events(event):
    change = parse_notification(f"__keyspace@0__:k".encode(), event.encode(), 0)
    assert change is not None
    assert change.action == "delete"


def test_keymiss_is_ignored_and_other_db_is_dropped():
    miss = parse_notification(b"__keyevent@0__:keymiss", b"k", 0)
    assert miss is not None
    assert miss.action == "ignore"
    assert parse_notification(b"__keyspace@1__:k", b"set", 0) is None


def test_binary_key_is_preserved():
    raw = b"__keyspace@0__:" + bytes([0xFF, 0xFE])
    change = parse_notification(raw, b"set", 0)
    assert change is not None
    assert change.key == bytes([0xFF, 0xFE])


def test_key_matches_glob_and_binary_keys():
    assert key_matches("user:1", "user:*")
    assert not key_matches("session:1", "user:*")
    assert key_matches(bytes([0xFF]), "*")
    assert not key_matches(bytes([0xFF]), "user:*")


def test_normalize_notify_flags_reads_standalone_and_cluster_replies():
    assert normalize_notify_flags({b"notify-keyspace-events": b"KEA"}) == "KEA"
    assert normalize_notify_flags({b"notify-keyspace-events": b""}) == ""
    nested = {
        "node-a": {b"notify-keyspace-events": b"Ex"},
        "node-b": {"notify-keyspace-events": "Ex"},
    }
    assert normalize_notify_flags(nested) == "Ex"
    assert normalize_notify_flags({"timeout": "1"}) is None


def test_warning_when_flags_do_not_match_the_family():
    assert notification_warning("KEA", "keyspace") is None
    assert notification_warning("Ex", "keyevent") is None
    empty = notification_warning("", "keyspace")
    assert empty is not None
    assert "KEA" in empty
    assert "Ex" in empty
    unreadable = notification_warning(None, "keyspace")
    assert unreadable is not None
    assert "Could not read" in unreadable


def test_standalone_endpoint_prefers_the_url():
    assert standalone_endpoint(RedisConfig()) == ("localhost", 6379)
    assert standalone_endpoint(
        RedisConfig(url="rediss://example.memorydb.amazonaws.com:6379/0", port=1)
    ) == ("example.memorydb.amazonaws.com", 6379)
