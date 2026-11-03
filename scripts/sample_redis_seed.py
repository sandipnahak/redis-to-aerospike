#!/usr/bin/env python3
"""Load Redis with sample keys for redis2aerospike migration testing.

Creates keys under ``sample:migrate:`` using every Redis type the migrator
supports: string, hash, list, set, and sorted set. Values are varied (ints,
floats, short strings, TTLs) so Aerospike coercion and TTL mapping can be
exercised.

Requires ``redis`` (``pip install redis`` or install this project).

This client is standalone Redis. For MemoryDB use
``scripts/sample_memorydb_seed.py`` (cluster, TLS, ACL username).

Example::

    docker compose up -d
    python scripts/sample_redis_seed.py
    redis2aerospike --redis-host localhost --redis-port 6379 \\
        --aerospike-host localhost --aerospike-port 3000 
        --aerospike-namespace test --aerospike-set redis --aerospike-send-key \\
        --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Iterable

import redis
from redis.exceptions import ConnectionError as RedisConnectionError


KEY_PREFIX = "sample:migrate"
PIPELINE_CHUNK = 500


def _split_counts(total: int, parts: int) -> list[int]:
    if total < parts:
        raise ValueError(f"count must be >= {parts} (one key per supported type)")
    base, rem = divmod(total, parts)
    return [base + (1 if i < rem else 0) for i in range(parts)]


def seed(client: redis.Redis, total: int) -> None:
    counts = _split_counts(total, 5)
    n_str, n_hash, n_list, n_set, n_zset = counts

    t0 = time.perf_counter()
    pending = 0
    pipe = client.pipeline(transaction=False)

    def flush_if_needed() -> None:
        nonlocal pending, pipe
        if pending >= PIPELINE_CHUNK:
            pipe.execute()
            pipe = client.pipeline(transaction=False)
            pending = 0

    # Strings: plain text, integer-like, float-like, bytes blob, some with TTL.
    for i in range(n_str):
        key = f"{KEY_PREFIX}:str:{i}"
        mod = i % 5
        if mod == 0:
            pipe.set(key, f"hello-{i}")
        elif mod == 1:
            pipe.set(key, str(10_000 + i))
        elif mod == 2:
            pipe.set(key, f"{i}.25")
        elif mod == 3:
            pipe.set(key, f"ttl-{i}", ex=60 + (i % 120))
        else:
            pipe.set(key, bytes([i % 256, (i * 7) % 256, (i * 13) % 256]))
        pending += 1
        flush_if_needed()

    # Hashes: string fields + one numeric field.
    for i in range(n_hash):
        key = f"{KEY_PREFIX}:hash:{i}"
        pipe.hset(
            key,
            mapping={
                "name": f"user-{i}",
                "region": ("us", "eu", "ap")[i % 3],
                "score": str(i % 1000),
                "ratio": f"{(i % 10) / 10:.1f}",
            },
        )
        pending += 1
        flush_if_needed()

    # Lists: ordered elements of mixed scalar types as strings.
    for i in range(n_list):
        key = f"{KEY_PREFIX}:list:{i}"
        pipe.delete(key)
        pipe.rpush(
            key,
            f"a-{i}",
            str(i),
            f"{i}.5",
            ("x", "y", "z")[i % 3],
        )
        pending += 2
        flush_if_needed()

    # Sets: unique members (strings + numeric strings).
    for i in range(n_set):
        key = f"{KEY_PREFIX}:set:{i}"
        pipe.delete(key)
        members = {f"m-{i}", f"m-{i}-b", str(i % 50), str((i + 1) % 50)}
        pipe.sadd(key, *members)
        pending += 2
        flush_if_needed()

    # Sorted sets: member -> score map (floats + ints).
    for i in range(n_zset):
        key = f"{KEY_PREFIX}:zset:{i}"
        pipe.delete(key)
        pipe.zadd(
            key,
            {
                f"p{i}a": float(i),
                f"p{i}b": float(i) + 0.5,
                f"p{i}c": (i % 7) - 3,
            },
        )
        pending += 2
        flush_if_needed()

    if pending:
        pipe.execute()

    elapsed = time.perf_counter() - t0
    print(f"Seeded {total} logical keys ({KEY_PREFIX}:*) in {elapsed:.2f}s.")


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    p.add_argument("--host", default="127.0.0.1", help="Redis host")
    p.add_argument("--port", type=int, default=6379, help="Redis port")
    p.add_argument("--db", type=int, default=0, help="Redis logical database")
    p.add_argument("--password", default=None, help="Redis password (if required)")
    p.add_argument(
        "--tls",
        action="store_true",
        help="Connect with TLS. Off by default for a local Redis.",
    )
    p.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Total keys to create (split evenly across 5 types)",
    )
    p.add_argument(
        "--flush",
        action="store_true",
        help="Run FLUSHDB on the selected database before seeding (destructive)",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        counts_preview = _split_counts(args.count, 5)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    client = redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
        ssl=args.tls,
        decode_responses=True,
    )
    try:
        client.ping()
    except RedisConnectionError as e:
        print(f"error: cannot connect to Redis at {args.host}:{args.port}: {e}", file=sys.stderr)
        return 1

    if args.flush:
        client.flushdb()
        print(f"Flushed Redis DB {args.db}.")

    print(
        f"Writing {args.count} keys as "
        f"strings={counts_preview[0]}, hashes={counts_preview[1]}, "
        f"lists={counts_preview[2]}, sets={counts_preview[3]}, zsets={counts_preview[4]} …",
    )
    seed(client, args.count)
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
