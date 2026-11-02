"""Shared, server-free test doubles.

These fakes let the unit suite exercise the Aerospike sink and Redis source
without a real server (or even the real client libraries), keeping unit tests
fast and runnable everywhere.
"""

from __future__ import annotations

import sys
import types
from typing import Any

import pytest


class FakeAerospikeClient:
    """Records the calls the sink makes instead of talking to a server."""

    def __init__(self):
        self.puts = []      # list of (key, bins, policy)
        self.operates = []  # list of (key, ops, policy)
        self.batches = []   # list of submitted BatchRecords
        self.removes = []   # list of key tuples
        self.closed = False
        # Optional hook: maps a Write's key tuple -> (result_code, in_doubt).
        # Keys absent from this map default to success (result 0).
        self.batch_results = {}

    def put(self, key, bins, meta=None, policy=None):
        self.puts.append((key, bins, policy))

    def operate(self, key, ops, meta=None, policy=None):
        self.operates.append((key, ops, policy))

    def batch_write(self, batch_records, policy=None):
        # Mirror the real client: mutate the passed BatchRecords, setting a
        # result (and in_doubt) on every sub-record reply.
        self.batches.append(batch_records)
        for write in batch_records.batch_records:
            result, in_doubt = self.batch_results.get(write.key, (0, False))
            write.result = result
            write.in_doubt = in_doubt
        return batch_records

    def remove(self, key, policy=None):
        self.removes.append(key)

    def close(self):
        self.closed = True


@pytest.fixture
def fake_aerospike(monkeypatch):
    """Install a minimal fake ``aerospike`` + ``aerospike_helpers`` in sys.modules.

    The operation helpers return plain dicts so tests can assert on the exact
    operations the sink builds, with no native client required.
    """
    class KeyOrderedDict(dict):
        """Mirrors :class:`aerospike.KeyOrderedDict` for sink unit tests."""

    aero: Any = types.ModuleType("aerospike")
    aero.KeyOrderedDict = KeyOrderedDict
    aero.LIST_ORDERED = 1
    aero.LIST_WRITE_ADD_UNIQUE = 1
    aero.LIST_WRITE_NO_FAIL = 4
    aero.LIST_WRITE_PARTIAL = 8
    aero.TTL_NEVER_EXPIRE = 0xFFFFFFFF
    # Authentication modes and key policy used when building the client config.
    aero.AUTH_INTERNAL = 0
    aero.AUTH_EXTERNAL = 1
    aero.AUTH_EXTERNAL_INSECURE = 2
    aero.AUTH_PKI = 3
    aero.POLICY_KEY_SEND = 1
    aero.POLICY_EXISTS_IGNORE = 100
    aero.POLICY_EXISTS_CREATE_OR_REPLACE = 101
    aero.POLICY_EXISTS_CREATE = 102
    aero.AEROSPIKE_ERR_RECORD_EXISTS = 17

    helpers = types.ModuleType("aerospike_helpers")
    ops_pkg: Any = types.ModuleType("aerospike_helpers.operations")
    operations: Any = types.ModuleType("aerospike_helpers.operations.operations")
    list_operations: Any = types.ModuleType("aerospike_helpers.operations.list_operations")

    operations.write = lambda bin_name, value: {"op": "write", "bin": bin_name, "val": value}
    list_operations.list_append_items = lambda bin_name, items, policy: {
        "op": "list_append_items",
        "bin": bin_name,
        "items": items,
        "policy": policy,
    }
    ops_pkg.operations = operations
    ops_pkg.list_operations = list_operations

    # Batch helpers: plain objects capturing what the sink builds, with
    # settable result/in_doubt fields the fake client fills on batch_write.
    batch_pkg: Any = types.ModuleType("aerospike_helpers.batch")
    batch_records_mod: Any = types.ModuleType("aerospike_helpers.batch.records")

    class Write:
        def __init__(self, key, ops, meta=None, policy=None):
            self.key = key
            self.ops = ops
            self.meta = meta
            self.policy = policy
            self.result = 0
            self.in_doubt = False

    class BatchRecords:
        def __init__(self, batch_records=None):
            self.batch_records = list(batch_records or [])
            self.result = 0

    batch_records_mod.Write = Write
    batch_records_mod.BatchRecords = BatchRecords
    batch_pkg.records = batch_records_mod

    for name, module in {
        "aerospike": aero,
        "aerospike_helpers": helpers,
        "aerospike_helpers.operations": ops_pkg,
        "aerospike_helpers.operations.operations": operations,
        "aerospike_helpers.operations.list_operations": list_operations,
        "aerospike_helpers.batch": batch_pkg,
        "aerospike_helpers.batch.records": batch_records_mod,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    return aero


@pytest.fixture
def fake_aero_client():
    """A fresh recording Aerospike client double."""
    return FakeAerospikeClient()


@pytest.fixture
def fake_redis():
    """A fresh in-memory fakeredis client (binary-safe, like the real source)."""
    fakeredis = pytest.importorskip("fakeredis")
    return fakeredis.FakeStrictRedis()
