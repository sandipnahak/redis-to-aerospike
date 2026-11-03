#!/usr/bin/env python3
"""Load Redis with keys shaped for Aerospike *set routing* demos.

Creates keys under ``sample:route:`` using prefixes that pair naturally with
``aerospike.set_routes`` / ``--set-route`` (including optional per-route
``hash_strategy`` / ``value_bin`` for map hashes; see docs). By default at least **10,000** Redis
keys are created (``--count``, tunable); use ``--per-route N`` to set an exact
depth per domain instead.

* ``sample:route:user:*`` — profiles (strings + a few hashes)
* ``sample:route:session:*`` — session strings
* ``sample:route:cache:*`` — cache strings (many with TTL)
* ``sample:route:ledger:*`` — **not** covered by typical user/session/cache routes,
  so they land in the default Aerospike set

When those ``--set-route`` patterns end with a single ``*`` (e.g. ``sample:route:user:*``),
the Aerospike **user key** is only the trailing segment (Redis ``sample:route:user:7`` →
Aerospike key ``7`` in set ``users``). Ledger keys stay full Redis strings in the default set.

Requires ``redis`` (``pip install redis`` or install this project).

Example — seed then dry-run migration with routes (single SCAN, client-side routing)::

    docker compose up -d
    python scripts/sample_redis_seed_routing.py --flush
    redis2aerospike --redis-host localhost --redis-port 6379 \
        --aerospike-host localhost --aerospike-port 3000 \
        --aerospike-namespace test --aerospike-set redis \
        --aerospike-send-key \
        --set-route 'sample:route:user:*=users' \
        --set-route 'sample:route:session:*=sessions' \
        --set-route 'sample:route:cache:*=caches' \\
        --dry-run

YAML equivalent (``set_routes`` under ``aerospike:``)::

    aerospike:
      set_name: redis
      send_key: true
      set_routes:
        - pattern: "sample:route:user:*"
          destination: users
        - pattern: "sample:route:session:*"
          destination: sessions
        - pattern: "sample:route:cache:*"
          destination: caches

To exercise only Redis SCAN filtering (narrower I/O), combine with
``--redis-match 'sample:route:*'`` or ``redis.scan_match`` in config.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from typing import Iterable

import redis
from redis.exceptions import ConnectionError as RedisConnectionError

KEY_PREFIX = "sample:route"
PIPELINE_CHUNK = 500


def _total_keys_for_per_route(per_route: int) -> int:
    """Keys written for a given ``per_route`` (see :func:`seed`)."""
    n_user_profile = (per_route + 3) // 4  # i % 4 == 0 for i in 0 .. per_route - 1
    return per_route + n_user_profile + per_route + per_route + per_route


def _per_route_for_at_least_total(min_total: int) -> int:
    """Smallest ``per_route`` such that :func:`_total_keys_for_per_route` >= ``min_total``."""
    if min_total <= 0:
        return 1
    n = max(1, math.ceil(min_total / 4.25))
    while _total_keys_for_per_route(n) < min_total:
        n += 1
    return n


def seed(client: redis.Redis, per_route: int) -> int:
    """Write keys; return total logical key count."""
    t0 = time.perf_counter()
    pending = 0
    pipe = client.pipeline(transaction=False)
    total = 0
    n_user_profile = 0

    def flush_if_needed() -> None:
        nonlocal pending, pipe
        if pending >= PIPELINE_CHUNK:
            pipe.execute()
            pipe = client.pipeline(transaction=False)
            pending = 0

    # user:* — strings + occasional hash (still matches user:*)
    for i in range(per_route):
        sk = f"{KEY_PREFIX}:user:{i}"
        pipe.set(sk, f"display-name-{i}")
        total += 1
        pending += 1
        flush_if_needed()
        if i % 4 == 0:
            hk = f"{KEY_PREFIX}:user:{i}:profile"
            pipe.hset(
                hk,
                mapping={
                    "id": str(i),
                    "tier": ("free", "pro", "team")[i % 3],
                    "logins": str(10 + i % 100),
                },
            )
            total += 1
            n_user_profile += 1
            pending += 1
            flush_if_needed()

    # session:*
    for i in range(per_route):
        key = f"{KEY_PREFIX}:session:sid-{i}"
        pipe.set(key, f"payload-token-{i:x}")
        total += 1
        pending += 1
        flush_if_needed()

    # cache:* — short TTLs so TTL migration is visible
    for i in range(per_route):
        key = f"{KEY_PREFIX}:cache:key-{i}"
        pipe.set(key, f"cached-value-{i}", ex=120 + (i % 180))
        total += 1
        pending += 1
        flush_if_needed()

    # ledger:* — no route in the example above → default Aerospike set
    for i in range(per_route):
        key = f"{KEY_PREFIX}:ledger:entry-{i}"
        pipe.hset(
            key,
            mapping={"amount": str((i + 1) * 100), "currency": "USD", "seq": str(i)},
        )
        total += 1
        pending += 1
        flush_if_needed()

    if pending:
        pipe.execute()

    elapsed = time.perf_counter() - t0
    n_user_str = per_route
    print(
        f"Seeded {total} keys under {KEY_PREFIX}: "
        f"user strings={n_user_str}, user hashes={n_user_profile}, "
        f"session={per_route}, cache={per_route}, ledger={per_route} "
        f"in {elapsed:.2f}s."
    )
    return total


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(__doc__ or "").split("Example")[0].strip(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="See module docstring for full redis2aerospike examples.",
    )
    p.add_argument("--host", default="127.0.0.1", help="Redis host")
    p.add_argument("--port", type=int, default=6379, help="Redis port")
    p.add_argument("--db", type=int, default=0, help="Redis logical database")
    p.add_argument("--password", default=None, help="Redis password (if required)")
    p.add_argument(
        "--count",
        type=int,
        default=10_000,
        metavar="N",
        help="Minimum total keys to create (default: 10000); per-domain depth is derived automatically",
    )
    p.add_argument(
        "--per-route",
        type=int,
        default=None,
        metavar="N",
        help="Exact base count N per domain (user/session/cache/ledger strings each get N keys; "
        "user hashes add ceil(N/4) more). Overrides --count.",
    )
    p.add_argument(
        "--flush",
        action="store_true",
        help="Run FLUSHDB on the selected database before seeding (destructive)",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    if args.per_route is not None:
        if args.per_route < 1:
            print("error: --per-route must be >= 1", file=sys.stderr)
            return 2
        per_route = args.per_route
    else:
        if args.count < 1:
            print("error: --count must be >= 1", file=sys.stderr)
            return 2
        per_route = _per_route_for_at_least_total(args.count)

    client = redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        password=args.password,
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

    if args.per_route is None:
        print(
            f"Targeting >= {args.count} keys (per-route depth {per_route} → "
            f"{_total_keys_for_per_route(per_route)} keys) ..."
        )
    seed(client, per_route)
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
