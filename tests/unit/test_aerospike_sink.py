"""Unit tests for AerospikeSink using a fake client and fake aerospike module.

These assert the exact client calls and operation structures the sink builds,
with no server (or native client) involved.
"""

import pytest

from redis_to_aerospike.aerospike_sink import (
    AerospikeServerInfo,
    AerospikeSink,
    BATCH_RECORD_EXISTS_OUTCOME,
    RecordAlreadyExists,
    RecordTooLargeError,
)
from redis_to_aerospike.config import AerospikeConfig, RecordExistsPolicy
from redis_to_aerospike.models import TTL_NEVER_EXPIRE, AerospikeRecord, BinWritePolicy


def make_sink(client, max_record_size=8 * 1024 * 1024, **aerospike_kwargs):
    return AerospikeSink(
        AerospikeConfig(namespace="test", set_name="redis", max_record_size=max_record_size, **aerospike_kwargs),
        client=client,
    )


def _expect_policy(fake_aerospike, ttl, *, exists: str = "ignore"):
    """Build expected write policy dict using fake aerospike constants."""
    key = {
        "ignore": "POLICY_EXISTS_IGNORE",
        "cor": "POLICY_EXISTS_CREATE_OR_REPLACE",
        "create": "POLICY_EXISTS_CREATE",
    }[exists]
    return {"ttl": ttl, "exists": getattr(fake_aerospike, key)}


def test_write_uses_record_level_set_name(fake_aero_client):
    sink = make_sink(fake_aero_client)
    sink.write(AerospikeRecord(key="k", bins={"value": 1}, set_name="custom"))
    key, bins, policy = fake_aero_client.puts[0]
    assert key == ("test", "custom", "k")
    assert bins == {"value": 1}


def test_plain_record_uses_put_with_key_bins_and_ttl(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)

    sink.write(AerospikeRecord(key="k", bins={"value": 42}, ttl_s=3))

    assert fake_aero_client.operates == []
    assert len(fake_aero_client.puts) == 1
    key, bins, policy = fake_aero_client.puts[0]
    assert key == ("test", "redis", "k")
    assert bins == {"value": 42}
    assert policy == _expect_policy(fake_aerospike, 3)


def test_dict_bin_put_wraps_key_ordered_dict(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    sink.write(AerospikeRecord(key="k", bins={"value": {"a": 1, "b": 2}}, ttl_s=3))

    _, bins, policy = fake_aero_client.puts[0]
    assert isinstance(bins["value"], fake_aerospike.KeyOrderedDict)
    assert dict(bins["value"]) == {"a": 1, "b": 2}
    assert policy == _expect_policy(fake_aerospike, 3)


def test_never_expire_ttl_passes_through(fake_aerospike, fake_aero_client):
    make_sink(fake_aero_client).write(
        AerospikeRecord(key="k", bins={"value": 1}, ttl_s=TTL_NEVER_EXPIRE)
    )
    _, _, policy = fake_aero_client.puts[0]
    assert policy == _expect_policy(fake_aerospike, 0xFFFFFFFF)


def test_none_ttl_becomes_namespace_default(fake_aerospike, fake_aero_client):
    make_sink(fake_aero_client).write(AerospikeRecord(key="k", bins={"value": 1}, ttl_s=None))
    _, _, policy = fake_aero_client.puts[0]
    assert policy == _expect_policy(fake_aerospike, 0)


def test_unique_list_record_uses_operate_with_reset_then_unique_append(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)

    record = AerospikeRecord(
        key="myset",
        bins={"value": ["a", "b", "c"]},
        ttl_s=10,
        bin_policies={"value": BinWritePolicy.UNIQUE_LIST},
    )
    sink.write(record)

    assert fake_aero_client.puts == []
    assert len(fake_aero_client.operates) == 1
    key, ops, policy = fake_aero_client.operates[0]
    assert key == ("test", "redis", "myset")
    assert policy == _expect_policy(fake_aerospike, 10)

    # First reset the bin to an empty list, then uniquely append the members.
    assert ops[0] == {"op": "write", "bin": "value", "val": []}
    assert ops[1]["op"] == "list_append_items"
    assert ops[1]["bin"] == "value"
    assert ops[1]["items"] == ["a", "b", "c"]

    policy = ops[1]["policy"]
    assert policy["list_order"] == fake_aerospike.LIST_ORDERED
    expected_flags = (
        fake_aerospike.LIST_WRITE_ADD_UNIQUE
        | fake_aerospike.LIST_WRITE_NO_FAIL
        | fake_aerospike.LIST_WRITE_PARTIAL
    )
    assert policy["write_flags"] == expected_flags


def test_mixed_bins_emit_write_and_unique_ops(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)

    record = AerospikeRecord(
        key="k",
        bins={"plain": 5, "value": ["a"]},
        bin_policies={"value": BinWritePolicy.UNIQUE_LIST},
    )
    sink.write(record)

    _, ops, _ = fake_aero_client.operates[0]
    op_kinds = [(o["op"], o["bin"]) for o in ops]
    assert ("write", "plain") in op_kinds
    assert ("write", "value") in op_kinds          # the reset
    assert ("list_append_items", "value") in op_kinds


def test_mixed_unique_list_and_map_bin_wraps_map_as_key_ordered(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    record = AerospikeRecord(
        key="k",
        bins={"tags": ["x"], "meta": {"role": "admin"}},
        bin_policies={"tags": BinWritePolicy.UNIQUE_LIST},
    )
    sink.write(record)

    _, ops, _ = fake_aero_client.operates[0]
    meta_writes = [o for o in ops if o["op"] == "write" and o["bin"] == "meta"]
    assert len(meta_writes) == 1
    assert isinstance(meta_writes[0]["val"], fake_aerospike.KeyOrderedDict)


def test_replace_policy_sets_create_or_replace_exists(fake_aerospike, fake_aero_client):
    sink = make_sink(
        fake_aero_client,
        record_exists_policy=RecordExistsPolicy.REPLACE,
    )
    sink.write(AerospikeRecord(key="k", bins={"value": 1}, ttl_s=2))
    assert fake_aero_client.puts[0][2] == _expect_policy(fake_aerospike, 2, exists="cor")


def test_create_only_put_raises_record_already_exists_by_error_code(fake_aerospike, fake_aero_client):
    """Some client paths surface AEROSPIKE_ERR_RECORD_EXISTS on a generic exception."""

    class ServerError(Exception):
        pass

    ServerError.code = fake_aerospike.AEROSPIKE_ERR_RECORD_EXISTS  # type: ignore[attr-defined]

    sink = make_sink(
        fake_aero_client,
        record_exists_policy=RecordExistsPolicy.CREATE_ONLY,
    )

    def _boom(*args, **kwargs):
        raise ServerError()

    fake_aero_client.put = _boom
    with pytest.raises(RecordAlreadyExists):
        sink.write(AerospikeRecord(key="k", bins={"value": 1}))


def test_create_only_put_raises_record_already_exists(fake_aerospike, fake_aero_client):
    class RecordExistsError(Exception):
        __module__ = "aerospike.exception"

    sink = make_sink(
        fake_aero_client,
        record_exists_policy=RecordExistsPolicy.CREATE_ONLY,
    )

    def _boom(*args, **kwargs):
        raise RecordExistsError()

    fake_aero_client.put = _boom
    with pytest.raises(RecordAlreadyExists):
        sink.write(AerospikeRecord(key="k", bins={"value": 1}))


def test_write_many_create_only_maps_record_exists_result(fake_aerospike, fake_aero_client):
    sink = make_sink(
        fake_aero_client,
        record_exists_policy=RecordExistsPolicy.CREATE_ONLY,
    )
    fake_aero_client.batch_results = {
        ("test", "redis", "x"): (fake_aerospike.AEROSPIKE_ERR_RECORD_EXISTS, False),
    }
    results = sink.write_many([AerospikeRecord(key="x", bins={"value": 1})])
    assert results == [BATCH_RECORD_EXISTS_OUTCOME]


def test_write_many_default_policy_record_exists_is_plain_batch_error(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    fake_aero_client.batch_results = {
        ("test", "redis", "x"): (fake_aerospike.AEROSPIKE_ERR_RECORD_EXISTS, False),
    }
    results = sink.write_many([AerospikeRecord(key="x", bins={"value": 1})])
    assert results == [f"BatchError:{fake_aerospike.AEROSPIKE_ERR_RECORD_EXISTS}"]


def test_write_many_ttl_forbidden_names_result_22(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    fake_aero_client.batch_results = {("test", "redis", "x"): (22, False)}
    results = sink.write_many([AerospikeRecord(key="x", bins={"value": 1}, ttl_s=60)])
    assert results == [
        "BatchError:22:forbidden (TTL write rejected; namespace nsup-period is 0)"
    ]


def test_write_before_connect_raises():
    sink = AerospikeSink(AerospikeConfig())  # no client injected
    with pytest.raises(RuntimeError):
        sink.write(AerospikeRecord(key="k", bins={"value": 1}))


def test_close_is_safe_and_propagates(fake_aero_client):
    make_sink(fake_aero_client).close()
    assert fake_aero_client.closed is True


def test_oversized_record_is_rejected(fake_aero_client):
    sink = make_sink(fake_aero_client, max_record_size=100)
    record = AerospikeRecord(key="big", bins={"value": "x" * 200})

    with pytest.raises(RecordTooLargeError) as exc:
        sink.write(record)

    assert "big" in str(exc.value)
    assert fake_aero_client.puts == []  # nothing sent to the server


def test_record_at_limit_is_written(fake_aero_client):
    sink = make_sink(fake_aero_client, max_record_size=1000)
    sink.write(AerospikeRecord(key="ok", bins={"value": "x" * 100}))
    assert len(fake_aero_client.puts) == 1


def test_default_limit_rejects_over_8mb(fake_aero_client):
    sink = make_sink(fake_aero_client)  # default 8 MiB limit
    record = AerospikeRecord(key="huge", bins={"value": b"x" * (8 * 1024 * 1024 + 1)})

    with pytest.raises(RecordTooLargeError):
        sink.write(record)


def test_size_estimate_covers_nested_collections():
    size = AerospikeSink._estimate_size({"a": [b"xx", "yy"], "b": 5})
    # 1 (key "a") + 2 + 2 (list items) + 1 (key "b") + 8 (int) = 14
    assert size == 14


# --- batch writes (write_many) ---------------------------------------------

def test_write_many_dict_bin_uses_key_ordered_dict(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    sink.write_many([AerospikeRecord(key="k", bins={"value": {"z": 0}})])

    ops = fake_aero_client.batches[0].batch_records[0].ops
    assert ops[0]["op"] == "write"
    assert isinstance(ops[0]["val"], fake_aerospike.KeyOrderedDict)


def test_write_many_builds_one_write_per_record_with_own_ttl(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)

    records = [
        AerospikeRecord(key="a", bins={"value": 1}, ttl_s=3),
        AerospikeRecord(key="b", bins={"value": 2}, ttl_s=TTL_NEVER_EXPIRE),
        AerospikeRecord(key="c", bins={"value": 3}, ttl_s=None),
    ]
    results = sink.write_many(records)

    assert results == [None, None, None]
    assert len(fake_aero_client.batches) == 1
    writes = fake_aero_client.batches[0].batch_records
    assert [w.key for w in writes] == [
        ("test", "redis", "a"),
        ("test", "redis", "b"),
        ("test", "redis", "c"),
    ]
    # Each Write carries that record's own TTL in its policy.
    assert writes[0].policy == _expect_policy(fake_aerospike, 3)
    assert writes[1].policy == _expect_policy(fake_aerospike, 0xFFFFFFFF)
    assert writes[2].policy == _expect_policy(fake_aerospike, 0)


def test_write_many_unique_list_emits_reset_then_unique_append(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)

    record = AerospikeRecord(
        key="myset",
        bins={"value": ["a", "b"]},
        bin_policies={"value": BinWritePolicy.UNIQUE_LIST},
    )
    sink.write_many([record])

    ops = fake_aero_client.batches[0].batch_records[0].ops
    assert ops[0] == {"op": "write", "bin": "value", "val": []}
    assert ops[1]["op"] == "list_append_items"
    assert ops[1]["items"] == ["a", "b"]


def test_write_many_send_key_sets_key_policy(fake_aerospike, fake_aero_client):
    sink = AerospikeSink(
        AerospikeConfig(namespace="test", set_name="redis", send_key=True),
        client=fake_aero_client,
    )
    sink.write_many([AerospikeRecord(key="k", bins={"value": 1}, ttl_s=5)])

    policy = fake_aero_client.batches[0].batch_records[0].policy
    assert policy == {**_expect_policy(fake_aerospike, 5), "key": fake_aerospike.POLICY_KEY_SEND}


def test_write_many_oversized_record_is_excluded_and_keeps_alignment(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client, max_record_size=100)

    records = [
        AerospikeRecord(key="ok1", bins={"value": "x"}),
        AerospikeRecord(key="big", bins={"value": "x" * 200}),
        AerospikeRecord(key="ok2", bins={"value": "y"}),
    ]
    results = sink.write_many(records)

    # The oversized record fails in place; the others stay aligned and are sent.
    assert results == [None, "RecordTooLargeError", None]
    sent_keys = [w.key for w in fake_aero_client.batches[0].batch_records]
    assert sent_keys == [("test", "redis", "ok1"), ("test", "redis", "ok2")]


def test_write_many_checks_each_reply_individually(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    # Mark "b" as a hard failure and "c" as an in-doubt failure; "a" succeeds.
    fake_aero_client.batch_results = {
        ("test", "redis", "b"): (-3, False),
        ("test", "redis", "c"): (-11, True),
    }

    records = [
        AerospikeRecord(key="a", bins={"value": 1}),
        AerospikeRecord(key="b", bins={"value": 2}),
        AerospikeRecord(key="c", bins={"value": 3}),
    ]
    results = sink.write_many(records)

    assert results == [None, "BatchError:-3", "BatchError:-11:in_doubt"]


def test_write_many_collapses_duplicate_keys_last_wins(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)

    records = [
        AerospikeRecord(key="dup", bins={"value": 1}, ttl_s=10),
        AerospikeRecord(key="other", bins={"value": 2}),
        AerospikeRecord(key="dup", bins={"value": 99}, ttl_s=20),
    ]
    results = sink.write_many(records)

    # Every input position gets an outcome; nothing is dropped from the result.
    assert results == [None, None, None]

    writes = fake_aero_client.batches[0].batch_records
    # "dup" is sent exactly once (no repeated key in one batch_write).
    sent_keys = [w.key for w in writes]
    assert sent_keys.count(("test", "redis", "dup")) == 1
    assert ("test", "redis", "other") in sent_keys
    assert len(writes) == 2

    # Last occurrence wins: the surviving "dup" write carries value 99 / ttl 20.
    dup_write = next(w for w in writes if w.key == ("test", "redis", "dup"))
    assert {"op": "write", "bin": "value", "val": 99} in dup_write.ops
    assert dup_write.policy == _expect_policy(fake_aerospike, 20)


def test_write_many_duplicate_key_failure_propagates_to_all_positions(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client)
    fake_aero_client.batch_results = {("test", "redis", "dup"): (-3, False)}

    records = [
        AerospikeRecord(key="dup", bins={"value": 1}),
        AerospikeRecord(key="dup", bins={"value": 2}),
    ]
    results = sink.write_many(records)

    # One write sent, its failure reported for both input positions.
    assert len(fake_aero_client.batches[0].batch_records) == 1
    assert results == ["BatchError:-3", "BatchError:-3"]


def test_write_many_all_oversized_sends_no_batch(fake_aerospike, fake_aero_client):
    sink = make_sink(fake_aero_client, max_record_size=10)
    results = sink.write_many([AerospikeRecord(key="big", bins={"value": "x" * 50})])

    assert results == ["RecordTooLargeError"]
    assert fake_aero_client.batches == []  # nothing sent to the server


def test_write_many_before_connect_raises():
    sink = AerospikeSink(AerospikeConfig())
    with pytest.raises(RuntimeError):
        sink.write_many([AerospikeRecord(key="k", bins={"value": 1})])


# --- client config building ------------------------------------------------

def _client_config(fake_aerospike, **kwargs):
    sink = AerospikeSink(AerospikeConfig(**kwargs))
    return sink._build_client_config(fake_aerospike)


def test_build_client_config_bare_is_hosts_only(fake_aerospike):
    cfg = _client_config(
        fake_aerospike,
        hosts=[("h", 3000)],
        connect_timeout_ms=0,
        login_timeout_ms=0,
    )
    assert cfg == {"hosts": [("h", 3000)]}


def test_build_client_config_default_includes_connect_timeout(fake_aerospike):
    cfg = _client_config(fake_aerospike, hosts=[("h", 3000)])
    # The dataclass default connect timeout is surfaced.
    assert cfg["hosts"] == [("h", 3000)]
    assert cfg["connect_timeout"] == 1000


def test_build_client_config_adds_user_and_password(fake_aerospike):
    cfg = _client_config(
        fake_aerospike, hosts=[("h", 3000)], username="admin", password="secret"
    )
    assert cfg["user"] == "admin"
    assert cfg["password"] == "secret"


def test_build_client_config_username_without_password_defaults_empty(fake_aerospike):
    cfg = _client_config(fake_aerospike, hosts=[("h", 3000)], username="admin")
    assert cfg["user"] == "admin"
    assert cfg["password"] == ""


def test_build_client_config_tls_name_rewrites_hosts(fake_aerospike):
    cfg = _client_config(
        fake_aerospike, hosts=[("h1", 4333), ("h2", 4333)], tls_name="mycluster"
    )
    assert cfg["hosts"] == [("h1", 4333, "mycluster"), ("h2", 4333, "mycluster")]


def test_build_client_config_tls_ca_only(fake_aerospike):
    cfg = _client_config(
        fake_aerospike, hosts=[("h", 4333)], tls_enable=True, tls_cafile="/certs/ca.pem"
    )
    assert cfg["tls"] == {"enable": True, "cafile": "/certs/ca.pem"}


def test_build_client_config_mutual_tls(fake_aerospike):
    cfg = _client_config(
        fake_aerospike,
        hosts=[("h", 4333)],
        tls_enable=True,
        tls_cafile="/certs/ca.pem",
        tls_certfile="/certs/client.pem",
        tls_keyfile="/certs/client.key",
        tls_keyfile_pw="pw",
    )
    assert cfg["tls"] == {
        "enable": True,
        "cafile": "/certs/ca.pem",
        "certfile": "/certs/client.pem",
        "keyfile": "/certs/client.key",
        "keyfile_pw": "pw",
    }


def test_build_client_config_disabled_tls_omits_tls(fake_aerospike):
    cfg = _client_config(
        fake_aerospike, hosts=[("h", 3000)], tls_enable=False, tls_cafile="/certs/ca.pem"
    )
    assert "tls" not in cfg


def test_build_client_config_timeouts_map_into_policies(fake_aerospike):
    cfg = _client_config(
        fake_aerospike,
        hosts=[("h", 3000)],
        socket_timeout_ms=1000,
        total_timeout_ms=2000,
        login_timeout_ms=7000,
        connect_timeout_ms=1500,
    )
    assert cfg["connect_timeout"] == 1500
    policies = cfg["policies"]
    assert policies["login_timeout"] == 7000
    for op in ("read", "write", "operate"):
        assert policies[op]["socket_timeout"] == 1000
        assert policies[op]["total_timeout"] == 2000


def test_build_client_config_auth_mode_maps_to_constant(fake_aerospike):
    cfg = _client_config(fake_aerospike, hosts=[("h", 3000)], auth_mode="external")
    assert cfg["policies"]["auth_mode"] == fake_aerospike.AUTH_EXTERNAL


def test_build_client_config_send_key_sets_key_policy(fake_aerospike):
    cfg = _client_config(fake_aerospike, hosts=[("h", 3000)], send_key=True)
    for op in ("read", "write", "operate"):
        assert cfg["policies"][op]["key"] == fake_aerospike.POLICY_KEY_SEND


def test_build_client_config_use_services_alternate(fake_aerospike):
    cfg = _client_config(
        fake_aerospike, hosts=[("h", 3000)], use_services_alternate=True
    )
    assert cfg["use_services_alternate"] is True


# --- server info parsing ---------------------------------------------------

def test_server_info_parse_extracts_typed_fields():
    text = "nsup-period=120;max-record-size=1048576;stop-writes-pct=90;memory-size=4294967296"
    info = AerospikeServerInfo.parse({"BB9": (None, text)}, "test")

    assert info.namespace == "test"
    assert info.nsup_period == 120
    assert info.max_record_size == 1048576
    assert info.stop_writes_pct == 90
    assert info.memory_size == 4294967296
    assert info.raw["nsup-period"] == "120"


def test_server_info_parse_falls_back_to_write_block_size():
    info = AerospikeServerInfo.parse("nsup-period=0;write-block-size=131072", "test")
    assert info.nsup_period == 0
    assert info.max_record_size == 131072


def test_server_info_parse_handles_bare_string_and_missing_fields():
    info = AerospikeServerInfo.parse("nsup-period=60", "test")
    assert info.nsup_period == 60
    assert info.max_record_size is None
    assert info.stop_writes_pct is None


def test_server_info_parse_empty_is_safe():
    info = AerospikeServerInfo.parse(None, "test")
    assert info.namespace == "test"
    assert info.raw == {}


class _InfoClient:
    def __init__(self, response):
        self._response = response

    def info_all(self, command):
        self.command = command
        return self._response


def test_sink_server_info_queries_namespace_and_parses(monkeypatch):
    client = _InfoClient({"node1": (None, "nsup-period=0;max-record-size=2048")})
    sink = AerospikeSink(AerospikeConfig(namespace="prod"), client=client)

    info = sink.server_info()

    assert client.command == "namespace/prod"
    assert info and info.nsup_period == 0
    assert info and info.max_record_size == 2048


def test_sink_server_info_returns_none_on_failure():
    class _Boom:
        def info_all(self, command):
            raise RuntimeError("no cluster")

    sink = AerospikeSink(AerospikeConfig(), client=_Boom())
    assert sink.server_info() is None
