"""Data carried through the migration pipeline.

These models are deliberately free of any Redis or Aerospike client imports so
the converter layer stays pure and trivially unit-testable. The sink is the only
component that translates :class:`BinWritePolicy` values into concrete Aerospike
client operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

# "Never expire" TTL. Same value as aerospike.TTL_NEVER_EXPIRE (0xFFFFFFFF), so
# it can be written straight through without importing the client here.
TTL_NEVER_EXPIRE = 0xFFFFFFFF


class BinWritePolicy(Enum):
    """How a bin's value should be written to Aerospike.

    * ``PUT``         -- ordinary write via ``client.put`` (the default).
    * ``UNIQUE_LIST`` -- write as an ordered list that enforces unique members
      server-side (used for Redis sets, which have no native Aerospike type).
    """

    PUT = "put"
    UNIQUE_LIST = "unique_list"


@dataclass
class RedisRecord:
    """A single key read from Redis.

    ``value`` is the already-materialized Redis value, shaped by type:

    * string     -> ``bytes``/``str``
    * hash        -> ``dict``
    * list        -> ``list``
    * set         -> ``set``
    * zset        -> ``list`` of ``(member, score)`` tuples
    """

    # ``str`` for UTF-8 keys, ``bytes`` for binary keys (preserved losslessly).
    key: "str | bytes"
    type: str
    value: Any
    # Remaining time-to-live in milliseconds. ``None`` means the key has no TTL.
    ttl_ms: Optional[int] = None


@dataclass
class AerospikeRecord:
    """A record ready to be written to Aerospike."""

    key: "str | bytes"
    bins: Dict[str, Any]
    # Aerospike record TTL in seconds. ``None`` uses the namespace default;
    # ``TTL_NEVER_EXPIRE`` means the record should never expire.
    ttl_s: Optional[int] = None
    # Optional per-bin write strategy. Bins absent from this map use ``PUT``.
    bin_policies: Dict[str, BinWritePolicy] = field(default_factory=dict)
    # When set, overrides :attr:`AerospikeConfig.set_name` for this write only.
    set_name: Optional[str] = None
    # Replace the whole Aerospike record on write. Pub/sub snapshots set this so
    # a removed hash field does not leave a stale bin behind. ``create_only``
    # still refuses to overwrite an existing record.
    full_replace: bool = False

    def policy_for(self, bin_name: str) -> BinWritePolicy:
        return self.bin_policies.get(bin_name, BinWritePolicy.PUT)
