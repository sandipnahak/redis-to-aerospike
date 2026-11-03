"""Ordered log of pub/sub changes, and replay of that log into Aerospike.

``pubsub`` and ``sync`` append one JSON line per applied change, in the order
the notifications were received. Workers may finish out of order; lines are
held until the next sequence number is ready so the file stays in order.
``mode: replay`` reads the file from top to bottom and writes Aerospike. Redis
is not contacted.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
from datetime import datetime
from typing import Any, Callable, Iterator, Optional

from .models import RedisRecord

logger = logging.getLogger(__name__)


def timestamped_event_log_path(path: str, when: datetime | None = None) -> str:
    """Insert a local timestamp before the file extension.

    ``migration/redis2aerospike-pubsub.jsonl`` becomes
    ``migration/redis2aerospike-pubsub-20261103T160300+0530.jsonl``.
    """
    moment = when or datetime.now().astimezone()
    stamp = moment.strftime("%Y%m%dT%H%M%S%z")
    root, ext = os.path.splitext(path)
    return f"{root}-{stamp}{ext}"


def dump_obj(value: Any) -> Any:
    """Encode a Redis value as JSON, including bytes, sets, and tuples."""
    if isinstance(value, bool) or value is None or isinstance(value, (int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"$": "b", "v": base64.b64encode(value).decode("ascii")}
    if isinstance(value, tuple):
        return {"$": "t", "v": [dump_obj(item) for item in value]}
    if isinstance(value, set):
        return {"$": "s", "v": [dump_obj(item) for item in value]}
    if isinstance(value, list):
        return {"$": "l", "v": [dump_obj(item) for item in value]}
    if isinstance(value, dict):
        return {
            "$": "m",
            "v": [[dump_obj(key), dump_obj(item)] for key, item in value.items()],
        }
    raise TypeError(f"cannot log a {type(value).__name__} value")


def load_obj(value: Any) -> Any:
    """Inverse of :func:`dump_obj`."""
    if not isinstance(value, dict) or "$" not in value:
        return value
    kind = value.get("$")
    inner = value.get("v")
    if kind == "b":
        return base64.b64decode(inner)
    if kind == "t":
        return tuple(load_obj(item) for item in inner)
    if kind == "s":
        return {load_obj(item) for item in inner}
    if kind == "l":
        return [load_obj(item) for item in inner]
    if kind == "m":
        return {load_obj(key): load_obj(item) for key, item in inner}
    return value


def upsert_entry(event: str, record: RedisRecord) -> dict[str, Any]:
    """One logged write. ``seq`` is filled in when the line is written."""
    return {
        "action": "upsert",
        "event": event,
        "key": dump_obj(record.key),
        "type": record.type,
        "value": dump_obj(record.value),
        "ttl_ms": record.ttl_ms,
    }


def delete_entry(event: str, key: str | bytes) -> dict[str, Any]:
    """One logged delete."""
    return {"action": "delete", "event": event, "key": dump_obj(key)}


def record_from_entry(entry: dict[str, Any]) -> RedisRecord:
    """Rebuild the Redis snapshot stored in an upsert line."""
    return RedisRecord(
        key=load_obj(entry["key"]),
        type=str(entry["type"]),
        value=load_obj(entry["value"]),
        ttl_ms=entry.get("ttl_ms"),
    )


class EventLog:
    """Append-only JSONL log that emits lines in receive order."""

    def __init__(self, path: str, *, stamp: bool = True, when: datetime | None = None):
        self.path = timestamped_event_log_path(path, when) if stamp else path
        if stamp and os.path.exists(self.path):
            root, ext = os.path.splitext(self.path)
            self.path = f"{root}-{os.getpid()}{ext}"
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._fh = open(self.path, "a", encoding="utf-8")
        self._lock = threading.Lock()
        self._next_seq = 1
        self._expect = 1
        self._pending: dict[int, Optional[dict[str, Any]]] = {}
        self._written = 0
        self._closed = False

    def assign(self) -> int:
        """Reserve the next receive-order sequence number."""
        with self._lock:
            seq = self._next_seq
            self._next_seq += 1
            return seq

    def complete(self, seq: int, entry: Optional[dict[str, Any]]) -> None:
        """Record the outcome of sequence ``seq``.

        ``None`` advances the sequence without a line (the event was ignored
        or skipped). A dict is written when every earlier sequence is done.
        """
        with self._lock:
            self._pending[seq] = entry
            while self._expect in self._pending:
                item = self._pending.pop(self._expect)
                if item is not None:
                    line = dict(item)
                    line["seq"] = self._expect
                    # Clock time is for tracking. Replay uses seq, not this field.
                    line["time"] = datetime.now().astimezone().isoformat(timespec="seconds")
                    self._fh.write(json.dumps(line, separators=(",", ":")) + "\n")
                    self._fh.flush()
                    self._written += 1
                self._expect += 1

    def close(self) -> None:
        """Flush and close. Safe to call more than once."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._pending:
                logger.warning(
                    "event log closed with %d change(s) still unordered",
                    len(self._pending),
                )
            logger.info("event log %s wrote %d line(s)", self.path, self._written)
            try:
                self._fh.flush()
                os.fsync(self._fh.fileno())
            except Exception:
                logger.debug("could not flush event log", exc_info=True)
            self._fh.close()


def iter_event_log(path: str) -> Iterator[dict[str, Any]]:
    """Yield each JSON object in file order."""
    with open(path, "r", encoding="utf-8") as handle:
        for lineno, raw in enumerate(handle, 1):
            text = raw.strip()
            if not text:
                continue
            try:
                entry = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{lineno}: invalid event log line") from exc
            if not isinstance(entry, dict):
                raise ValueError(f"{path}:{lineno}: event log line must be an object")
            yield entry


def seq_selected(entry: dict[str, Any], from_seq: Optional[int]) -> bool:
    """True when this line should be applied.

    ``from_seq`` is inclusive. ``None`` selects every line. ``time`` is not
    consulted; it is only a tracking stamp on the line.
    """
    if from_seq is None:
        return True
    seq = entry.get("seq")
    if isinstance(seq, bool) or not isinstance(seq, int):
        raise ValueError("event log line has no integer seq; replay cannot start at a sequence")
    return seq >= from_seq


def replay_log(
    path: str,
    apply_entry: Callable[[dict[str, Any]], None],
    from_seq: Optional[int] = None,
) -> int:
    """Apply lines in file order. Returns how many lines were applied.

    When ``from_seq`` is set, lines with a smaller ``seq`` are skipped.
    """
    count = 0
    for entry in iter_event_log(path):
        if not seq_selected(entry, from_seq):
            continue
        apply_entry(entry)
        count += 1
    return count
