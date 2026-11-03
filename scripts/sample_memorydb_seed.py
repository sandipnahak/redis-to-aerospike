#!/usr/bin/env python3
"""Load Amazon MemoryDB with the same sample keys as sample_redis_seed.py.

MemoryDB is always a cluster and uses TLS unless ``--no-tls`` is set.
Username and password are optional. Database 0 is the only database.
FLUSHDB is rejected, so this script does not delete existing keys.

The keys are ``sample:migrate:{str,hash,list,set,zset}:N``. They land on
different shards. The cluster client pipelines each command to the node that
owns that key.

Requires ``redis`` (``pip install redis`` or install this project).

Example::

    python scripts/sample_memorydb_seed.py \\
        --host clustercfg.example.memorydb.us-west-2.amazonaws.com \\
        --username app \\
        --password "$MEMORYDB_PASSWORD" \\
        --count 10000

Omit ``--username`` and ``--password`` when the cluster does not require ACL auth.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

from redis.cluster import RedisCluster
from redis.exceptions import RedisClusterException, RedisError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sample_redis_seed import KEY_PREFIX, _split_counts, seed  # noqa: E402


def parse_args(argv: Iterable[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--host", required=True, help="MemoryDB configuration endpoint")
    parser.add_argument("--port", type=int, default=6379, help="MemoryDB port")
    parser.add_argument("--username", default=None, help="MemoryDB ACL username, if required")
    parser.add_argument("--password", default=None, help="MemoryDB ACL password, if required")
    parser.add_argument(
        "--count",
        type=int,
        default=10_000,
        help="Total keys to create (split evenly across 5 types)",
    )
    parser.add_argument(
        "--no-tls",
        action="store_true",
        help="Disable TLS. MemoryDB in-transit encryption requires TLS; leave this off.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        counts = _split_counts(args.count, 5)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    kwargs = {
        "host": args.host,
        "port": args.port,
        "ssl": not args.no_tls,
        "decode_responses": True,
    }
    if args.username:
        kwargs["username"] = args.username
    if args.password:
        kwargs["password"] = args.password
    client = RedisCluster(**kwargs)
    try:
        client.ping()
    except (RedisError, RedisClusterException) as exc:
        print(
            f"error: cannot connect to MemoryDB at {args.host}:{args.port}: {exc}",
            file=sys.stderr,
        )
        return 1

    print(
        f"Writing {args.count} keys ({KEY_PREFIX}:*) on database 0 as "
        f"strings={counts[0]}, hashes={counts[1]}, lists={counts[2]}, "
        f"sets={counts[3]}, zsets={counts[4]} ...",
    )
    try:
        seed(client, args.count)
    except (RedisError, RedisClusterException) as exc:
        print(f"error: seed failed: {exc}", file=sys.stderr)
        return 1
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
