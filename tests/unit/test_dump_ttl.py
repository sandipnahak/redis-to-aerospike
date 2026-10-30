"""DUMP payloads carry an application expiry, not the Redis key TTL."""

from redis_to_aerospike.dump_ttl import ttl_ms_from_dump

# redis-cli DUMP of a hash: v=TTO_FR_WEB, e=1813638928335, then RDB version + CRC.
MEMORYDB_DUMP = (
    b"\x04\x02\x01v\nTTO_FR_WEB\x01e\r1813638928335\x0b\x00'Lum4\xa68Y"
)
EXPIRY_MS = 1_813_638_928_335


def test_memorydb_dump_field_e_is_absolute_milliseconds():
    now = EXPIRY_MS - 90_000
    assert ttl_ms_from_dump(MEMORYDB_DUMP, "e", now) == 90_000


def test_past_timestamp_is_due_now():
    assert ttl_ms_from_dump(MEMORYDB_DUMP, "e", EXPIRY_MS + 1) == 0


def test_missing_field_has_no_expiry():
    assert ttl_ms_from_dump(MEMORYDB_DUMP, "missing", EXPIRY_MS) is None


def test_unix_seconds_timestamp():
    # type-4 hash, one field e = "1700000000" (10 digits, Unix seconds).
    payload = b"\x04\x01\x01e\n1700000000\x0b\x00" + b"\x00" * 8
    now_ms = 1_700_000_000_000 - 5_000
    assert ttl_ms_from_dump(payload, "e", now_ms) == 5_000


def test_small_number_is_a_duration_in_seconds():
    payload = b"\x04\x01\x01e\x043600\x0b\x00" + b"\x00" * 8
    assert ttl_ms_from_dump(payload, "e", 0) == 3_600_000


def test_non_hash_dump_has_no_embedded_expiry():
    # RDB string "hello" plus a dummy trailer the parser does not need.
    payload = b"\x00\x05hello"
    assert ttl_ms_from_dump(payload, "e", 0) is None


def test_listpack_hash_field():
    def lp_str(data: bytes) -> bytes:
        assert len(data) < 64
        encoded = bytes([0x80 | len(data)]) + data
        return encoded + bytes([len(encoded)])

    body = lp_str(b"v") + lp_str(b"TTO_FR_WEB") + lp_str(b"e") + lp_str(b"1813638928335")
    blob = (6 + len(body) + 1).to_bytes(4, "little") + (4).to_bytes(2, "little") + body + b"\xff"
    assert len(blob) < 64
    payload = bytes([16, len(blob)]) + blob
    assert ttl_ms_from_dump(payload, "e", EXPIRY_MS - 1_000) == 1_000
