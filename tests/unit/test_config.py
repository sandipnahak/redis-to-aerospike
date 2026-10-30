import pytest

from redis_to_aerospike.config import (
    DEFAULT_MAX_TTL_S,
    AerospikeConfig,
    HashStrategy,
    MigrationConfig,
    RecordExistsPolicy,
    RedisConfig,
    TtlOverflowPolicy,
)


def test_defaults():
    config = MigrationConfig()
    assert config.workers == 8
    assert config.scan_batch == 500
    assert config.scan_rate_limit == 0
    assert config.write_rate_limit == 0
    assert config.write_batch_size == 1
    assert config.hash_strategy is HashStrategy.MAP_BIN
    assert config.redis.host == "localhost"
    assert config.aerospike.namespace == "test"
    assert config.aerospike.value_bin == "value"
    assert config.aerospike.max_ttl == DEFAULT_MAX_TTL_S
    assert config.ttl_overflow_policy is TtlOverflowPolicy.REJECT
    assert config.aerospike.record_exists_policy is RecordExistsPolicy.UPDATE


def test_redis_from_env():
    env = {"REDIS_HOST": "r", "REDIS_PORT": "6380", "REDIS_DB": "2", "REDIS_PASSWORD": "secret"}
    cfg = RedisConfig.from_env(env)
    assert cfg.host == "r"
    assert cfg.port == 6380
    assert cfg.db == 2
    assert cfg.password == "secret"


def test_aerospike_from_env():
    env = {"AEROSPIKE_HOST": "a", "AEROSPIKE_PORT": "3100", "AEROSPIKE_NAMESPACE": "prod"}
    cfg = AerospikeConfig.from_env(env)
    assert cfg.hosts == [("a", 3100)]
    assert cfg.namespace == "prod"


def test_aerospike_max_ttl_from_env():
    cfg = AerospikeConfig.from_env({"AEROSPIKE_MAX_TTL": "12345"})
    assert cfg.max_ttl == 12345


def test_aerospike_record_exists_policy_from_env():
    cfg = AerospikeConfig.from_env({"AEROSPIKE_RECORD_EXISTS_POLICY": "create_only"})
    assert cfg.record_exists_policy is RecordExistsPolicy.CREATE_ONLY


def test_migration_from_env_parses_strategy_and_ints():
    env = {
        "MIGRATION_WORKERS": "16",
        "MIGRATION_SCAN_BATCH": "100",
        "MIGRATION_QUEUE_SIZE": "50",
        "MIGRATION_SCAN_RATE_LIMIT": "1000",
        "MIGRATION_WRITE_RATE_LIMIT": "250.5",
        "MIGRATION_WRITE_BATCH_SIZE": "100",
        "MIGRATION_HASH_STRATEGY": "field_bins",
        "MIGRATION_TTL_OVERFLOW_POLICY": "clamp",
    }
    cfg = MigrationConfig.from_env(env)
    assert cfg.workers == 16
    assert cfg.scan_batch == 100
    assert cfg.queue_size == 50
    assert cfg.scan_rate_limit == 1000
    assert cfg.write_rate_limit == 250.5
    assert cfg.write_batch_size == 100
    assert cfg.hash_strategy is HashStrategy.FIELD_BINS
    assert cfg.ttl_overflow_policy is TtlOverflowPolicy.CLAMP


def test_empty_password_env_becomes_none():
    assert RedisConfig.from_env({"REDIS_PASSWORD": ""}).password is None


# --- new Redis connection fields ------------------------------------------

def test_redis_connection_defaults():
    cfg = RedisConfig()
    assert cfg.username is None
    assert cfg.url is None
    assert cfg.cluster is False
    assert cfg.ssl is False
    assert cfg.ssl_ca_certs is None
    assert cfg.socket_timeout is None
    assert cfg.socket_connect_timeout is None


def test_redis_connection_from_env():
    env = {
        "REDIS_USERNAME": "alice",
        "REDIS_URL": "rediss://localhost:6379/0",
        "REDIS_CLUSTER": "true",
        "REDIS_SSL": "1",
        "REDIS_SSL_CA_CERTS": "/certs/ca.pem",
        "REDIS_SSL_CERTFILE": "/certs/client.pem",
        "REDIS_SSL_KEYFILE": "/certs/client.key",
        "REDIS_SSL_CERT_REQS": "required",
        "REDIS_SOCKET_TIMEOUT": "2.5",
        "REDIS_SOCKET_CONNECT_TIMEOUT": "1",
    }
    cfg = RedisConfig.from_env(env)
    assert cfg.username == "alice"
    assert cfg.url == "rediss://localhost:6379/0"
    assert cfg.cluster is True
    assert cfg.ssl is True
    assert cfg.ssl_ca_certs == "/certs/ca.pem"
    assert cfg.ssl_certfile == "/certs/client.pem"
    assert cfg.ssl_keyfile == "/certs/client.key"
    assert cfg.ssl_cert_reqs == "required"
    assert cfg.socket_timeout == 2.5
    assert cfg.socket_connect_timeout == 1.0


def test_redis_from_dict_overlays_new_fields():
    cfg = RedisConfig.from_dict(
        {"username": "u", "ssl": True, "cluster": True, "socket_timeout": 3.0}
    )
    assert cfg.username == "u"
    assert cfg.ssl is True
    assert cfg.cluster is True
    assert cfg.socket_timeout == 3.0
    # Untouched keys keep defaults.
    assert cfg.host == "localhost"


# --- new Aerospike connection fields --------------------------------------

def test_aerospike_connection_defaults():
    cfg = AerospikeConfig()
    assert cfg.username is None
    assert cfg.password is None
    assert cfg.auth_mode is None
    assert cfg.tls_enable is False
    assert cfg.tls_name is None
    assert cfg.socket_timeout_ms == 0
    assert cfg.total_timeout_ms == 0
    assert cfg.connect_timeout_ms == 1000
    assert cfg.login_timeout_ms == 5000
    assert cfg.use_services_alternate is False
    assert cfg.send_key is False


def test_aerospike_security_and_tls_from_env():
    env = {
        "AEROSPIKE_USERNAME": "admin",
        "AEROSPIKE_PASSWORD": "secret",
        "AEROSPIKE_AUTH_MODE": "external",
        "AEROSPIKE_TLS_ENABLE": "true",
        "AEROSPIKE_TLS_NAME": "mycluster",
        "AEROSPIKE_TLS_CAFILE": "/certs/ca.pem",
        "AEROSPIKE_SOCKET_TIMEOUT_MS": "1000",
        "AEROSPIKE_TOTAL_TIMEOUT_MS": "2000",
        "AEROSPIKE_CONNECT_TIMEOUT_MS": "1500",
        "AEROSPIKE_LOGIN_TIMEOUT_MS": "7000",
        "AEROSPIKE_USE_SERVICES_ALTERNATE": "yes",
        "AEROSPIKE_SEND_KEY": "1",
    }
    cfg = AerospikeConfig.from_env(env)
    assert cfg.username == "admin"
    assert cfg.password == "secret"
    assert cfg.auth_mode == "external"
    assert cfg.tls_enable is True
    assert cfg.tls_name == "mycluster"
    assert cfg.tls_cafile == "/certs/ca.pem"
    assert cfg.socket_timeout_ms == 1000
    assert cfg.total_timeout_ms == 2000
    assert cfg.connect_timeout_ms == 1500
    assert cfg.login_timeout_ms == 7000
    assert cfg.use_services_alternate is True
    assert cfg.send_key is True


def test_aerospike_empty_username_env_becomes_none():
    assert AerospikeConfig.from_env({"AEROSPIKE_USERNAME": ""}).username is None


# --- from_dict / from_yaml -------------------------------------------------

def test_aerospike_from_dict_single_host_and_fields():
    cfg = AerospikeConfig.from_dict(
        {"host": "node", "port": 4000, "namespace": "prod", "username": "u", "tls_enable": True}
    )
    assert cfg.hosts == [("node", 4000)]
    assert cfg.namespace == "prod"
    assert cfg.username == "u"
    assert cfg.tls_enable is True


def test_aerospike_from_dict_hosts_list():
    cfg = AerospikeConfig.from_dict({"hosts": [["n1", 3000], ["n2", 3000]]})
    assert cfg.hosts == [("n1", 3000), ("n2", 3000)]


def test_aerospike_from_dict_partial_keeps_defaults():
    cfg = AerospikeConfig.from_dict({"namespace": "only"})
    assert cfg.namespace == "only"
    assert cfg.hosts == [("localhost", 3000)]
    assert cfg.connect_timeout_ms == 1000


def test_aerospike_from_dict_record_exists_policy():
    cfg = AerospikeConfig.from_dict({"record_exists_policy": "replace"})
    assert cfg.record_exists_policy is RecordExistsPolicy.REPLACE


def test_migration_from_dict_nested_sections_and_enums():
    data = {
        "redis": {"host": "r", "port": 6380},
        "aerospike": {
            "host": "a",
            "port": 3100,
            "namespace": "ns",
            "username": "admin",
            "record_exists_policy": "create_only",
        },
        "workers": 12,
        "hash_strategy": "field_bins",
        "ttl_overflow_policy": "clamp",
    }
    cfg = MigrationConfig.from_dict(data)
    assert cfg.redis.host == "r"
    assert cfg.redis.port == 6380
    assert cfg.aerospike.hosts == [("a", 3100)]
    assert cfg.aerospike.namespace == "ns"
    assert cfg.aerospike.username == "admin"
    assert cfg.aerospike.record_exists_policy is RecordExistsPolicy.CREATE_ONLY
    assert cfg.workers == 12
    assert cfg.hash_strategy is HashStrategy.FIELD_BINS
    assert cfg.ttl_overflow_policy is TtlOverflowPolicy.CLAMP


def test_migration_from_dict_overlays_rate_limits_and_batch_size():
    cfg = MigrationConfig.from_dict(
        {"scan_rate_limit": 500, "write_rate_limit": 100, "write_batch_size": 50}
    )
    assert cfg.scan_rate_limit == 500
    assert cfg.write_rate_limit == 100
    assert cfg.write_batch_size == 50


def test_migration_from_dict_empty_is_defaults():
    cfg = MigrationConfig.from_dict(None)
    assert cfg.workers == 8
    assert cfg.aerospike.namespace == "test"


def test_migration_from_yaml_roundtrip(tmp_path):
    yaml = pytest.importorskip("yaml")
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "aerospike": {"namespace": "prod", "username": "admin", "tls_enable": True},
                "workers": 4,
            }
        )
    )
    cfg = MigrationConfig.from_yaml(str(path))
    assert cfg.aerospike.namespace == "prod"
    assert cfg.aerospike.username == "admin"
    assert cfg.aerospike.tls_enable is True
    assert cfg.workers == 4


def test_aerospike_from_dict_set_routes():
    cfg = AerospikeConfig.from_dict(
        {
            "set_name": "default",
            "set_routes": [
                {"pattern": "a:*", "destination": "A"},
                {"pattern": "b:*", "destination": "B"},
            ],
        }
    )
    assert cfg.set_name == "default"
    assert len(cfg.set_routes) == 2
    assert cfg.set_routes[0].pattern == "a:*"
    assert cfg.set_routes[0].destination == "A"


def test_aerospike_from_dict_set_routes_hash_and_value_bin():
    cfg = AerospikeConfig.from_dict(
        {
            "set_routes": [
                {"pattern": "x:*", "destination": "X", "hash_strategy": "field_bins"},
                {
                    "pattern": "y:*",
                    "destination": "Y",
                    "hash_strategy": "map_bin",
                    "value_bin": "data",
                },
            ],
        }
    )
    assert cfg.set_routes[0].hash_strategy is HashStrategy.FIELD_BINS
    assert cfg.set_routes[0].value_bin is None
    assert cfg.set_routes[1].hash_strategy is HashStrategy.MAP_BIN
    assert cfg.set_routes[1].value_bin == "data"


def test_redis_from_dict_key_pattern_alias():
    cfg = RedisConfig.from_dict({"key_pattern": "cache:*"})
    assert cfg.scan_match == "cache:*"


def test_redis_from_dict_scan_match_beats_key_pattern():
    cfg = RedisConfig.from_dict({"key_pattern": "a:*", "scan_match": "b:*"})
    assert cfg.scan_match == "b:*"


def test_redis_ttl_command_from_dict_and_alias():
    from redis_to_aerospike.config import TtlCommand

    cfg = RedisConfig.from_dict({"ttl_check_command": "DUMP", "ttl_dump_field": "exp"})
    assert cfg.ttl_command is TtlCommand.DUMP
    assert cfg.ttl_dump_field == "exp"


def test_redis_ttl_command_rejects_unknown():
    with pytest.raises(ValueError, match="ttl_command"):
        RedisConfig.from_dict({"ttl_command": "object"})
