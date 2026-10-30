"""Read an absolute expiry embedded in a Redis ``DUMP`` payload.

``DUMP`` does not carry the key's Redis TTL. Some values store that timestamp
themselves. The payload this tool understands is a hash whose field (default
``e``) is an absolute expiry: Unix milliseconds when the number is at least
``10**12``, Unix seconds when it is at least ``10**9``, otherwise a remaining
TTL in seconds.

The sample MemoryDB payload::

    \\x04\\x02\\x01v\\nTTO_FR_WEB\\x01e\\r1813638928335...

is a two-field hash: ``v`` = ``TTO_FR_WEB``, ``e`` = ``1813638928335``.
"""

from __future__ import annotations

from typing import Dict, Union

# RDB object types that encode a hash.
_RDB_TYPE_HASH = 4
_RDB_TYPE_HASH_ZIPLIST = 13
_RDB_TYPE_HASH_LISTPACK = 16

_FieldValue = Union[bytes, int]
_Fields = Dict[bytes, _FieldValue]

# Absolute timestamps below this are Unix seconds; at or above, milliseconds.
_MS_TIMESTAMP = 1_000_000_000_000
# At or above this, and below ``_MS_TIMESTAMP``, the number is Unix seconds.
_SEC_TIMESTAMP = 1_000_000_000


def ttl_ms_from_dump(payload: bytes, field: str, now_ms: int) -> int | None:
    """Remaining TTL in milliseconds, or ``None`` when the payload has no expiry.

    A timestamp that is already in the past yields ``0`` (due now), not
    "never expire".
    """
    if not payload or not field:
        return None
    try:
        raw = _hash_field(payload, field.encode("utf-8"))
    except ValueError:
        return None
    if raw is None:
        return None
    number = _as_int(raw)
    if number is None or number < 0:
        return None
    return _remaining_ms(number, now_ms)


def _remaining_ms(number: int, now_ms: int) -> int:
    if number >= _MS_TIMESTAMP:
        expiry_ms = number
    elif number >= _SEC_TIMESTAMP:
        expiry_ms = number * 1000
    else:
        # Small positive values are a duration in seconds, not a timestamp.
        return number * 1000
    remaining = expiry_ms - now_ms
    return remaining if remaining > 0 else 0


def _as_int(raw: _FieldValue) -> int | None:
    if isinstance(raw, int):
        return raw
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        return None
    digits = text[1:] if text.startswith("-") else text
    if not digits.isdigit():
        return None
    return int(text)


def _hash_field(payload: bytes, field: bytes) -> _FieldValue | None:
    if not payload:
        raise ValueError("empty dump")
    kind = payload[0]
    if kind == _RDB_TYPE_HASH:
        fields = _read_hash_table(payload, 1)
    elif kind == _RDB_TYPE_HASH_LISTPACK:
        blob, _ = _read_string(payload, 1)
        if not isinstance(blob, bytes):
            raise ValueError("listpack hash payload is not a string")
        fields = _read_listpack_hash(blob)
    elif kind == _RDB_TYPE_HASH_ZIPLIST:
        blob, _ = _read_string(payload, 1)
        if not isinstance(blob, bytes):
            raise ValueError("ziplist hash payload is not a string")
        fields = _read_ziplist_hash(blob)
    else:
        return None
    return fields.get(field)


def _read_length(buf: bytes, i: int) -> tuple[int, int, bool]:
    """RDB length. The third value is True when the length byte is a special encoding."""
    if i >= len(buf):
        raise ValueError("truncated length")
    b = buf[i]
    kind = b >> 6
    if kind == 0:
        return b & 0x3F, i + 1, False
    if kind == 1:
        if i + 1 >= len(buf):
            raise ValueError("truncated length")
        return ((b & 0x3F) << 8) | buf[i + 1], i + 2, False
    if kind == 2:
        if b == 0x80:
            if i + 5 > len(buf):
                raise ValueError("truncated length")
            return int.from_bytes(buf[i + 1 : i + 5], "big"), i + 5, False
        if b == 0x81:
            if i + 9 > len(buf):
                raise ValueError("truncated length")
            return int.from_bytes(buf[i + 1 : i + 9], "big"), i + 9, False
        raise ValueError(f"unsupported length byte {b:#x}")
    return b & 0x3F, i + 1, True


def _read_string(buf: bytes, i: int) -> tuple[_FieldValue, int]:
    length, i, special = _read_length(buf, i)
    if not special:
        end = i + length
        if end > len(buf):
            raise ValueError("truncated string")
        return buf[i:end], end
    if length == 0:
        if i >= len(buf):
            raise ValueError("truncated int8 string")
        return int.from_bytes(buf[i : i + 1], "little"), i + 1
    if length == 1:
        if i + 2 > len(buf):
            raise ValueError("truncated int16 string")
        return int.from_bytes(buf[i : i + 2], "little", signed=True), i + 2
    if length == 2:
        if i + 4 > len(buf):
            raise ValueError("truncated int32 string")
        return int.from_bytes(buf[i : i + 4], "little", signed=True), i + 4
    raise ValueError(f"unsupported string encoding {length}")


def _read_hash_table(buf: bytes, i: int) -> _Fields:
    count, i, special = _read_length(buf, i)
    if special:
        raise ValueError("hash length is a special encoding")
    fields: _Fields = {}
    for _ in range(count):
        name, i = _read_string(buf, i)
        value, i = _read_string(buf, i)
        if isinstance(name, bytes):
            fields[name] = value
    return fields


def _backlen_size(entry_len: int) -> int:
    if entry_len <= 127:
        return 1
    if entry_len < 16383:
        return 2
    if entry_len < 2_097_151:
        return 3
    if entry_len < 268_435_455:
        return 4
    return 5


def _read_listpack_entry(buf: bytes, i: int) -> tuple[_FieldValue, int]:
    if i >= len(buf):
        raise ValueError("truncated listpack")
    b = buf[i]
    if (b & 0x80) == 0:
        value: _FieldValue = b & 0x7F
        entry_len = 1
    elif (b & 0xC0) == 0x80:
        n = b & 0x3F
        end = i + 1 + n
        if end > len(buf):
            raise ValueError("truncated listpack string")
        value = buf[i + 1 : end]
        entry_len = 1 + n
    elif (b & 0xE0) == 0xC0:
        if i + 1 >= len(buf):
            raise ValueError("truncated listpack int")
        value = ((b & 0x1F) << 8) | buf[i + 1]
        if value >= 4096:
            value -= 8192
        entry_len = 2
    elif (b & 0xF0) == 0xE0:
        if i + 1 >= len(buf):
            raise ValueError("truncated listpack string")
        n = ((b & 0x0F) << 8) | buf[i + 1]
        end = i + 2 + n
        if end > len(buf):
            raise ValueError("truncated listpack string")
        value = buf[i + 2 : end]
        entry_len = 2 + n
    elif b == 0xF0:
        if i + 5 > len(buf):
            raise ValueError("truncated listpack string")
        n = int.from_bytes(buf[i + 1 : i + 5], "big")
        end = i + 5 + n
        if end > len(buf):
            raise ValueError("truncated listpack string")
        value = buf[i + 5 : end]
        entry_len = 5 + n
    elif b == 0xF1:
        if i + 3 > len(buf):
            raise ValueError("truncated listpack int16")
        value = int.from_bytes(buf[i + 1 : i + 3], "little", signed=True)
        entry_len = 3
    elif b == 0xF2:
        if i + 4 > len(buf):
            raise ValueError("truncated listpack int24")
        value = int.from_bytes(buf[i + 1 : i + 4], "little", signed=False)
        if value >= 1 << 23:
            value -= 1 << 24
        entry_len = 4
    elif b == 0xF3:
        if i + 5 > len(buf):
            raise ValueError("truncated listpack int32")
        value = int.from_bytes(buf[i + 1 : i + 5], "little", signed=True)
        entry_len = 5
    elif b == 0xF4:
        if i + 9 > len(buf):
            raise ValueError("truncated listpack int64")
        value = int.from_bytes(buf[i + 1 : i + 9], "little", signed=True)
        entry_len = 9
    else:
        raise ValueError(f"unsupported listpack encoding {b:#x}")
    nxt = i + entry_len + _backlen_size(entry_len)
    if nxt > len(buf):
        raise ValueError("truncated listpack backlen")
    return value, nxt


def _read_listpack_hash(blob: bytes) -> _Fields:
    # 4 byte total length + 2 byte element count.
    if len(blob) < 6 or blob[-1] != 0xFF:
        raise ValueError("truncated listpack")
    i = 6
    entries: list[_FieldValue] = []
    while i < len(blob) and blob[i] != 0xFF:
        value, i = _read_listpack_entry(blob, i)
        entries.append(value)
    if len(entries) % 2:
        raise ValueError("listpack hash has an odd number of entries")
    fields: _Fields = {}
    for n in range(0, len(entries), 2):
        name = entries[n]
        if isinstance(name, bytes):
            fields[name] = entries[n + 1]
    return fields


def _read_ziplist_entry(buf: bytes, i: int) -> tuple[_FieldValue, int]:
    if i >= len(buf):
        raise ValueError("truncated ziplist")
    if buf[i] < 254:
        i += 1
    else:
        i += 5
    if i >= len(buf):
        raise ValueError("truncated ziplist encoding")
    enc = buf[i]
    if enc < 0xC0:
        if enc & 0xC0 == 0x00:
            n = enc & 0x3F
            i += 1
        elif enc & 0xC0 == 0x40:
            if i + 1 >= len(buf):
                raise ValueError("truncated ziplist string")
            n = ((enc & 0x3F) << 8) | buf[i + 1]
            i += 2
        else:
            if i + 5 > len(buf):
                raise ValueError("truncated ziplist string")
            n = int.from_bytes(buf[i + 1 : i + 5], "big")
            i += 5
        end = i + n
        if end > len(buf):
            raise ValueError("truncated ziplist string")
        return buf[i:end], end
    i += 1
    if enc == 0xC0:
        width, signed = 2, True
    elif enc == 0xD0:
        width, signed = 4, True
    elif enc == 0xE0:
        width, signed = 8, True
    elif enc == 0xF0:
        width, signed = 3, False
    elif enc == 0xFE:
        width, signed = 1, True
    elif 0xF1 <= enc <= 0xFD:
        return enc - 0xF1, i
    else:
        raise ValueError(f"unsupported ziplist encoding {enc:#x}")
    end = i + width
    if end > len(buf):
        raise ValueError("truncated ziplist int")
    value = int.from_bytes(buf[i:end], "little", signed=signed)
    if enc == 0xF0 and value >= 1 << 23:
        value -= 1 << 24
    return value, end


def _read_ziplist_hash(blob: bytes) -> _Fields:
    if len(blob) < 11 or blob[-1] != 0xFF:
        raise ValueError("truncated ziplist")
    i = 10
    entries: list[_FieldValue] = []
    while i < len(blob) and blob[i] != 0xFF:
        value, i = _read_ziplist_entry(blob, i)
        entries.append(value)
    if len(entries) % 2:
        raise ValueError("ziplist hash has an odd number of entries")
    fields: _Fields = {}
    for n in range(0, len(entries), 2):
        name = entries[n]
        if isinstance(name, bytes):
            fields[name] = entries[n + 1]
    return fields
