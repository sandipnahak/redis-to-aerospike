"""Redis/MemoryDB keyspace-notification parsing.

Keyspace notifications are pub/sub messages, not stored keys. Redis publishes
them only when ``notify-keyspace-events`` contains ``K`` (keyspace channels)
and/or ``E`` (keyevent channels), plus a class of events such as ``A`` (most
commands) or ``x`` (expired).

MemoryDB sets that parameter on the cluster parameter group. Two useful values:

* ``KEA`` — every keyspace and keyevent notification (writes, deletes, expires).
* ``Ex`` — expired keys only, on the keyevent channel.

In cluster mode those messages stay on the node that owns the key. They are
not broadcast the way a normal ``PUBLISH`` is, so a listener must subscribe to
every primary.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlparse

from .config import PubSubChannel, RedisConfig

Action = Literal["upsert", "delete", "ignore"]

# The key no longer exists under this name. ``rename_from`` / ``move_from`` are
# the old name; the new name arrives as a separate upsert event.
_DELETE_EVENTS = frozenset(
    {"del", "expired", "evicted", "rename_from", "move_from"}
)
# A read of a missing key. Nothing changed, and flag ``m`` is not part of ``A``.
_IGNORE_EVENTS = frozenset({"keymiss"})


@dataclass(frozen=True)
class KeyChange:
    """One keyspace notification reduced to a sync action."""

    key: str | bytes
    event: str
    action: Action


def notification_db(config: RedisConfig) -> int:
    """Database embedded in ``__keyspace@N__`` / ``__keyevent@N__``.

    Cluster mode, including MemoryDB, only has database 0.
    """
    return 0 if config.cluster else config.db


def select_channel_family(flags: str | None, requested: PubSubChannel) -> str:
    """Choose ``keyspace`` or ``keyevent``.

    Subscribing to both families double-delivers every change when the
    parameter contains both ``K`` and ``E`` (as ``KEA`` does). ``auto`` follows
    ``K`` when it is set, otherwise ``E`` (the ``Ex`` expired-only setting).
    """
    if requested is PubSubChannel.KEYSPACE:
        return "keyspace"
    if requested is PubSubChannel.KEYEVENT:
        return "keyevent"
    letters = set(flags or "")
    if "K" in letters:
        return "keyspace"
    if "E" in letters:
        return "keyevent"
    return "keyspace"


def subscription_pattern(family: str, db: int) -> str:
    """Glob passed to ``PSUBSCRIBE`` for one notification family."""
    if family == "keyevent":
        return f"__keyevent@{db}__:*"
    return f"__keyspace@{db}__:*"


def notification_warning(flags: str | None, family: str) -> str | None:
    """Return a user-facing warning when this subscription will hear nothing.

    ``None`` flags means the server did not answer ``CONFIG GET``. The
    subscription still starts, because MemoryDB can deny the read while the
    parameter group is already set.
    """
    needed = "K" if family == "keyspace" else "E"
    if flags is None:
        return (
            "Could not read notify-keyspace-events. Subscribing anyway. "
            "On MemoryDB, open the cluster parameter group and set "
            "notify-keyspace-events to KEA (all keyspace and keyevent "
            "notifications) or Ex (expired keys only). Ex publishes keyevent "
            "channels; pass --pubsub-channel keyevent when auto-detect cannot "
            "read the parameter."
        )
    if needed in set(flags):
        return None
    shown = flags if flags else "(empty)"
    return (
        f"notify-keyspace-events is {shown!r}, which does not publish "
        f"{family} notifications (the value needs {needed!r}). "
        "In the MemoryDB console edit the cluster parameter group: KEA logs "
        "all keyspace and keyevent notifications, Ex logs only expired keys. "
        "Ex is a keyevent stream, so set pubsub_channel to keyevent or leave "
        "it on auto once the parameter can be read."
    )


def key_matches(key: str | bytes, pattern: str) -> bool:
    """Redis ``SCAN MATCH``-style filter for a notification key.

    ``*`` matches every key, including binary keys. Any other pattern is
    applied to the UTF-8 form; a key that is not valid UTF-8 does not match.
    """
    if pattern == "*":
        return True
    if isinstance(key, bytes):
        try:
            text = key.decode("utf-8")
        except UnicodeDecodeError:
            return False
    else:
        text = key
    return fnmatch.fnmatchcase(text, pattern)


def decode_key(raw: bytes) -> str | bytes:
    """UTF-8 keys become ``str``; other byte strings stay ``bytes``."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw


def parse_notification(channel: Any, data: Any, db: int) -> KeyChange | None:
    """Turn one ``pmessage`` channel/data pair into a :class:`KeyChange`.

    Keyspace: channel ``__keyspace@{db}__:<key>``, data is the event name.
    Keyevent: channel ``__keyevent@{db}__:<event>``, data is the key.
    """
    channel_b = _as_bytes(channel)
    data_b = _as_bytes(data)
    if channel_b is None or data_b is None:
        return None

    keyspace = f"__keyspace@{db}__:".encode("ascii")
    keyevent = f"__keyevent@{db}__:".encode("ascii")
    if channel_b.startswith(keyspace):
        key_raw = channel_b[len(keyspace) :]
        event = _event_name(data_b)
    elif channel_b.startswith(keyevent):
        event = _event_name(channel_b[len(keyevent) :])
        key_raw = data_b
    else:
        return None
    if event is None or not key_raw:
        return None

    if event in _IGNORE_EVENTS:
        action: Action = "ignore"
    elif event in _DELETE_EVENTS:
        action = "delete"
    else:
        action = "upsert"
    return KeyChange(key=decode_key(key_raw), event=event, action=action)


def normalize_notify_flags(raw: Any) -> str | None:
    """Pull ``notify-keyspace-events`` out of a ``CONFIG GET`` reply.

    Standalone redis-py returns ``{name: value}``. The cluster client may
    return ``{node: {name: value}}``. An empty string means the parameter was
    read and notifications are off. ``None`` means the reply did not contain
    the parameter.
    """
    found: list[str] = []

    def walk(node: Any) -> None:
        if not isinstance(node, dict):
            return
        for key, value in node.items():
            name = _text(key)
            if name == "notify-keyspace-events" and not isinstance(value, dict):
                found.append("" if value is None else _text(value))
            elif isinstance(value, dict):
                walk(value)

    walk(raw)
    if not found:
        return None
    unique = list(dict.fromkeys(found))
    nonempty = [item for item in unique if item]
    return nonempty[0] if nonempty else unique[0]


def standalone_endpoint(config: RedisConfig) -> tuple[str, int]:
    """Host and port of a non-cluster source, honoring a connection URL."""
    if config.url:
        parsed = urlparse(config.url)
        host = parsed.hostname or config.host
        port = parsed.port or config.port
        return host, int(port)
    return config.host, int(config.port)


def _event_name(raw: bytes) -> str | None:
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        return None
    if not text or any(ch.isspace() for ch in text):
        return None
    return text


def _as_bytes(value: Any) -> bytes | None:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    return None


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)
