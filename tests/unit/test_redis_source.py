"""Unit tests for RedisSource using an in-memory fakeredis backend."""

from typing import cast

import pytest
from redis.exceptions import RedisError

from redis_to_aerospike.config import RedisConfig, TtlCommand
from redis_to_aerospike.redis_source import RedisConnection, RedisSource, redis_client_params


@pytest.fixture
def source(fake_redis):
    return RedisSource(RedisConfig(), client=fake_redis)


def _by_key(records):
    return {r.key: r for r in records}


# --- redis_client_params ---------------------------------------------------

def test_client_params_discrete_defaults():
    cluster, url, kwargs = redis_client_params(RedisConfig())
    assert cluster is False
    assert url is None
    assert kwargs == {"decode_responses": False, "host": "localhost", "port": 6379, "db": 0}


def test_client_params_auth_and_ssl():
    cluster, url, kwargs = redis_client_params(
        RedisConfig(
            username="alice",
            password="secret",
            ssl=True,
            ssl_ca_certs="/certs/ca.pem",
            ssl_certfile="/certs/c.pem",
        )
    )
    assert (cluster, url) == (False, None)
    assert kwargs["username"] == "alice"
    assert kwargs["password"] == "secret"
    assert kwargs["ssl"] is True
    assert kwargs["ssl_ca_certs"] == "/certs/ca.pem"
    assert kwargs["ssl_certfile"] == "/certs/c.pem"


def test_client_params_socket_timeouts():
    _, _, kwargs = redis_client_params(
        RedisConfig(socket_timeout=2.5, socket_connect_timeout=1.0)
    )
    assert kwargs["socket_timeout"] == 2.5
    assert kwargs["socket_connect_timeout"] == 1.0


def test_client_params_url_ignores_discrete_fields():
    cluster, url, kwargs = redis_client_params(
        RedisConfig(url="rediss://h:6379/0", host="ignored", port=1234, socket_timeout=1.0)
    )
    assert cluster is False
    assert url == "rediss://h:6379/0"
    # URL is the source of truth: no host/port/db, but timeouts still apply.
    assert "host" not in kwargs and "port" not in kwargs and "db" not in kwargs
    assert kwargs["socket_timeout"] == 1.0


def test_client_params_cluster_omits_db():
    cluster, url, kwargs = redis_client_params(RedisConfig(cluster=True, db=0))
    assert cluster is True
    assert url is None
    assert "db" not in kwargs
    assert kwargs["host"] == "localhost"


# --- cluster scanning ------------------------------------------------------

class _FakeClusterClient:
    """Minimal cluster-like client exposing scan_iter across all shards."""

    def __init__(self, keys):
        self._keys = keys

    def scan_iter(self, match=None, count=None):
        yield from self._keys


def test_cluster_scan_chunks_into_batches():
    keys = [f"k{i}".encode() for i in range(25)]
    src = RedisSource(RedisConfig(cluster=True), client=cast(RedisConnection, _FakeClusterClient(keys)))
    batches = list(src._iter_key_batches(10))
    assert [len(b) for b in batches] == [10, 10, 5]
    assert [k for b in batches for k in b] == keys


# --- static decision helpers ----------------------------------------------

@pytest.mark.parametrize(
    "key_type,value,vanished",
    [
        ("string", None, True),
        ("string", b"", False),     # empty string is a real value
        ("string", b"x", False),
        ("hash", {}, True),
        ("list", [], True),
        ("set", set(), True),
        ("zset", [], True),
        ("hash", {b"f": b"v"}, False),
        ("stream", 1, False),       # unsupported types are not "vanished"
    ],
)
def test_is_vanished(key_type, value, vanished):
    assert RedisSource._is_vanished(key_type, value) is vanished


def test_decode_key_utf8_becomes_str():
    assert RedisSource._decode_key(b"user:1") == "user:1"


def test_decode_key_binary_stays_bytes():
    raw = b"\xff\x00\x01"
    assert RedisSource._decode_key(raw) == raw


# --- end-to-end iteration over a fake server -------------------------------

def test_materializes_each_type(source, fake_redis):
    fake_redis.set("s", "hello")
    fake_redis.hset("h", mapping={"a": "1"})
    fake_redis.rpush("l", "x", "y")
    fake_redis.sadd("st", "m1", "m2")
    fake_redis.zadd("z", {"m": 1.5})

    records = _by_key(source.iter_records(batch_size=10))

    assert records["s"].type == "string" and records["s"].value == b"hello"
    assert records["h"].type == "hash" and records["h"].value == {b"a": b"1"}
    assert records["l"].type == "list" and records["l"].value == [b"x", b"y"]
    assert records["st"].type == "set" and records["st"].value == {b"m1", b"m2"}
    assert records["z"].type == "zset" and records["z"].value == [(b"m", 1.5)]


def test_scan_paginates_across_batches(source, fake_redis):
    for i in range(250):
        fake_redis.set(f"k{i}", "v")
    records = list(source.iter_records(batch_size=10))
    assert len({r.key for r in records}) == 250


def test_ttl_is_captured_in_ms(source, fake_redis):
    fake_redis.set("with_ttl", "v", px=100_000)
    fake_redis.set("no_ttl", "v")
    records = _by_key(source.iter_records(batch_size=10))
    assert 0 < records["with_ttl"].ttl_ms <= 100_000
    assert records["no_ttl"].ttl_ms is None


def test_binary_key_is_preserved(source, fake_redis):
    raw = b"\xff\x01"
    fake_redis.set(raw, "v")
    records = list(source.iter_records(batch_size=10))
    keys = {r.key for r in records}
    assert raw in keys


def test_binary_string_value_is_preserved(source, fake_redis):
    blob = b"\x00\xff\x10"
    fake_redis.set("blob", blob)
    records = _by_key(source.iter_records(batch_size=10))
    assert records["blob"].value == blob


def test_expiry_race_none_value_is_skipped(source, fake_redis, monkeypatch):
    fake_redis.set("ghost", "v")

    # Simulate the key expiring between the TYPE read and the value read.
    def fake_fetch_values(keys, types):
        return {k: None for k in keys if types[k] != "none"}

    monkeypatch.setattr(source, "_fetch_values", fake_fetch_values)
    assert list(source.iter_records(batch_size=10)) == []


def test_server_info_reports_keys(source, fake_redis):
    fake_redis.set("a", "1")
    fake_redis.set("b", "2", px=100_000)

    info = source.server_info()

    # dbsize is always available; memory/version/keyspace are best-effort and
    # depend on the backend, so we only assert the reliable count here.
    assert info["keys"] == 2


class _CommandTable:
    def __init__(self, names):
        self.commands = {name: {"name": name} for name in names}


class _TtlPipe:
    """Pipeline double whose TTL commands can be rejected like MemoryDB."""

    def __init__(self, data, allowed, client):
        self._data = data
        self._allowed = allowed
        self._client = client
        self._ops = []

    def type(self, key):
        self._ops.append(("type", key))
        return self

    def pttl(self, key):
        self._ops.append(("pttl", key))
        return self

    def ttl(self, key):
        self._ops.append(("ttl", key))
        self._client.ttl_calls += 1
        return self

    def get(self, key):
        self._ops.append(("get", key))
        return self

    def execute(self):
        out = []
        for op, key in self._ops:
            if op in ("pttl", "ttl") and op not in self._allowed:
                raise RedisError(f"{op.upper()} command doesn't exist in Redis commands")
            type_name, value, ttl_s = self._data[key]
            if op == "type":
                out.append(type_name)
            elif op == "pttl":
                out.append(-1 if ttl_s is None else ttl_s * 1000)
            elif op == "ttl":
                out.append(-1 if ttl_s is None else ttl_s)
            elif op == "get":
                out.append(value)
        return out


class _TtlClient:
    def __init__(self, data, commands=None, allowed=None):
        # data: key bytes -> (type, value, ttl seconds or None)
        self._data = data
        self._allowed = set(allowed if allowed is not None else (commands or {"pttl", "ttl"}))
        self.ttl_calls = 0
        if commands is not None:
            self.commands_parser = _CommandTable(commands)

    def scan(self, cursor=0, match=None, count=None):
        return 0, list(self._data)

    def pipeline(self, transaction=False):
        return _TtlPipe(self._data, self._allowed, self)


def test_missing_pttl_and_ttl_migrates_without_expiry(caplog):
    # Amazon MemoryDB Multi-Region advertises neither TTL command.
    data = {b"user:1": (b"string", b"v", None)}
    client = _TtlClient(data, commands=["type", "get", "scan"])
    src = RedisSource(RedisConfig(), client=cast(RedisConnection, client))

    records = _by_key(src.iter_records(batch_size=10))

    assert records["user:1"].value == b"v"
    assert records["user:1"].ttl_ms is None
    assert "neither PTTL nor TTL" in caplog.text


def test_ttl_fallback_scales_seconds_to_milliseconds(caplog):
    data = {b"cache": (b"string", b"v", 30), b"forever": (b"string", b"v", None)}
    client = _TtlClient(data, commands=["type", "get", "ttl", "scan"])
    src = RedisSource(RedisConfig(), client=cast(RedisConnection, client))

    records = _by_key(src.iter_records(batch_size=10))

    assert records["cache"].ttl_ms == 30_000
    assert records["forever"].ttl_ms is None
    assert "does not implement PTTL" in caplog.text


def test_unadvertised_pttl_does_not_call_ttl():
    # No COMMAND cache and PTTL is rejected: do not send TTL. MemoryDB
    # Multi-Region answers that with "ERR unknown command 'TTL'".
    data = {b"cache": (b"string", b"v", 12)}
    client = _TtlClient(data, commands=None, allowed={"ttl"})
    src = RedisSource(RedisConfig(), client=cast(RedisConnection, client))

    records = _by_key(src.iter_records(batch_size=10))

    assert records["cache"].ttl_ms is None
    assert client.ttl_calls == 0


def test_server_unknown_command_ttl_migrates_without_expiry(caplog):
    # COMMAND listed TTL, but the server still rejects it.
    data = {b"sku": (b"string", b"v", 9)}

    class _RejectTtl(_TtlClient):
        def pipeline(self, transaction=False):
            pipe = super().pipeline(transaction=transaction)

            def execute():
                if any(op == "ttl" for op, _key in pipe._ops):
                    raise RedisError("ERR unknown command 'TTL'")
                return _TtlPipe.execute(pipe)

            pipe.execute = execute
            return pipe

    client = _RejectTtl(data, commands=["type", "get", "ttl", "scan"])
    src = RedisSource(RedisConfig(), client=cast(RedisConnection, client))

    records = _by_key(src.iter_records(batch_size=10))

    assert records["sku"].value == b"v"
    assert records["sku"].ttl_ms is None
    assert "neither PTTL nor TTL" in caplog.text


def test_unsupported_list_read_is_skipped(caplog):
    # MemoryDB Multi-Region has strings but no list commands.
    data = {
        b"s": (b"string", b"v", None),
        b"l": (b"list", [b"x"], None),
    }
    client = _TtlClient(data, commands=["type", "get", "scan"])
    src = RedisSource(RedisConfig(), client=cast(RedisConnection, client))

    records = _by_key(src.iter_records(batch_size=10))

    assert list(records) == ["s"]
    assert records["s"].value == b"v"
    assert "skipping Redis type 'list'" in caplog.text


# redis-cli DUMP: hash fields v=TTO_FR_WEB and e=1813638928335.
_MEMORYDB_DUMP = b"\x04\x02\x01v\nTTO_FR_WEB\x01e\r1813638928335\x0b\x00'Lum4\xa68Y"
_EXPIRY_MS = 1_813_638_928_335


class _DumpPipe:
    def __init__(self):
        self._ops = []

    def type(self, key):
        self._ops.append("type")
        return self

    def dump(self, key):
        self._ops.append("dump")
        return self

    def hgetall(self, key):
        self._ops.append("hgetall")
        return self

    def execute(self):
        out = []
        for op in self._ops:
            if op == "type":
                out.append(b"hash")
            elif op == "dump":
                out.append(_MEMORYDB_DUMP)
            elif op == "hgetall":
                out.append({b"v": b"TTO_FR_WEB", b"e": b"1813638928335"})
        return out


class _DumpClient:
    def scan(self, cursor=0, match=None, count=None):
        return 0, [b"M1TELnOQ9Q8:U=9dKVIhzfMV5:startSku"]

    def pipeline(self, transaction=False):
        return _DumpPipe()


def test_dump_command_reads_embedded_expiry(monkeypatch):
    monkeypatch.setattr("redis_to_aerospike.redis_source._now_ms", lambda: _EXPIRY_MS - 90_000)
    src = RedisSource(
        RedisConfig(ttl_command=TtlCommand.DUMP),
        client=cast(RedisConnection, _DumpClient()),
    )

    records = _by_key(src.iter_records(batch_size=10))

    record = records["M1TELnOQ9Q8:U=9dKVIhzfMV5:startSku"]
    assert record.type == "hash"
    assert record.value[b"v"] == b"TTO_FR_WEB"
    assert record.ttl_ms == 90_000


def test_expiretime_is_absolute_unix_seconds(monkeypatch):
    now_ms = 1_700_000_000_000

    class _Pipe:
        def __init__(self):
            self._ops = []

        def type(self, key):
            self._ops.append("type")
            return self

        def expiretime(self, key):
            self._ops.append("expiretime")
            return self

        def get(self, key):
            self._ops.append("get")
            return self

        def execute(self):
            out = []
            for op in self._ops:
                if op == "type":
                    out.append(b"string")
                elif op == "expiretime":
                    out.append(1_700_000_000 + 30)
                elif op == "get":
                    out.append(b"v")
            return out

    class _Client:
        def scan(self, cursor=0, match=None, count=None):
            return 0, [b"k"]

        def pipeline(self, transaction=False):
            return _Pipe()

    monkeypatch.setattr("redis_to_aerospike.redis_source._now_ms", lambda: now_ms)
    src = RedisSource(
        RedisConfig(ttl_command=TtlCommand.EXPIRETIME),
        client=cast(RedisConnection, _Client()),
    )
    records = _by_key(src.iter_records(batch_size=10))
    assert records["k"].ttl_ms == 30_000


def test_unrelated_redis_error_is_not_treated_as_missing_ttl():
    class _BrokenPipe(_TtlPipe):
        def execute(self):
            raise RedisError("connection reset by peer")

    class _Broken(_TtlClient):
        def pipeline(self, transaction=False):
            return _BrokenPipe(self._data, self._allowed, self)

    client = _Broken({b"a": (b"string", b"v", 1)}, commands=["pttl", "type", "get"])
    src = RedisSource(RedisConfig(), client=cast(RedisConnection, client))
    with pytest.raises(RedisError, match="connection reset"):
        list(src.iter_records(batch_size=10))


def test_server_info_is_best_effort(monkeypatch):
    class _BrokenClient:
        def dbsize(self):
            raise ConnectionError("down")

        def info(self, section):
            raise ConnectionError("down")

    src = RedisSource(RedisConfig(), client=cast(RedisConnection, _BrokenClient()))
    # Never raises; just returns whatever it could gather (here, nothing).
    assert src.server_info() == {}
