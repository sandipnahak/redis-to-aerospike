# redis2aerospike user guide

`redis2aerospike` is a command-line tool that copies data from **Redis** (or
**Valkey** and other **Redis-compatible** servers) into **Aerospike**, converting
each Redis value into a **native Aerospike type** as it goes (strings stay scalars,
hashes become maps, lists/sets become lists, sorted sets become maps). It streams
keys out of Redis with `SCAN` and writes them with a pool of worker threads, so it
stays fast and memory-bounded even on large keyspaces.

This guide is for **operators** -- people who install the built tool and run
migrations. You do not need to read or change any code to use it.

**Example software — not for production as-is.** This project is a **reference
example** only. **Do not use it in production** without your own review,
testing, and operational controls. It is provided **as-is**, with **no
warranty**, and **no commitment to support, maintenance, or updates.**

## The 30-second version

```bash
# 1. Install
pip install redis2aerospike

# 2. (optional) Preview what would happen, without writing anything
redis2aerospike \
  --redis-host localhost --redis-port 6379 \
  --aerospike-host localhost --aerospike-port 3000 \
  --aerospike-namespace test --aerospike-set redis \
  --dry-run

# 3. Run the real migration
redis2aerospike \
  --redis-host localhost --redis-port 6379 \
  --aerospike-host localhost --aerospike-port 3000 \
  --aerospike-namespace test --aerospike-set redis \
  --workers 8
```

Every flag has a sensible default, so the only things you usually need to supply
are where your Redis-compatible source is, where Aerospike is, and which namespace/set to write into.

To see the installed revision, run `redis2aerospike --version`. Notable changes by version are in the repo root [CHANGELOG.md](../CHANGELOG.md).

## Pick your path

- **Just trying it out?** Start with [Getting started](01-getting-started.md);
  it walks you through a throwaway local Redis + Aerospike using Docker.
- **Migrating a real deployment?** Read the two connection guides for your
  setup, then the running guide:
  - [Connecting to Redis](02-connecting-redis.md) (Redis, Valkey, and other RESP-compatible sources)
  - [Connecting to Aerospike](03-connecting-aerospike.md)
  - [Running and tuning a migration](05-running-and-tuning.md)

## Table of contents

| Guide | What it covers |
| --- | --- |
| [01 - Getting started](01-getting-started.md) | Prerequisites, install, verify, a first migration end to end. |
| [02 - Connecting to Redis](02-connecting-redis.md) | Source options for Redis, Valkey, and other wire-compatible servers: host/port/db, auth, URLs, TLS, Cluster. |
| [03 - Connecting to Aerospike](03-connecting-aerospike.md) | Hosts, namespace/set, authentication, TLS, timeouts, cloud/NAT. |
| [04 - Transferring data](04-transferring-data.md) | What gets copied, the type mapping, TTLs, subsets, re-runs. |
| [05 - Running and tuning](05-running-and-tuning.md) | Config methods, performance knobs, output, exit codes. |
| [06 - Troubleshooting](06-troubleshooting.md) | Symptom -> cause -> fix for common failures. |
| [07 - Configuration reference](07-configuration-reference.md) | Every flag, YAML key, env var, and default in one place. |
| [08 - nsup-period](08-nsup-period.md) | What `nsup-period` is, how preflight reads it on every node, and how to set it. |
| [Changelog](../CHANGELOG.md) | Version history and notable changes. |

## How you configure the tool

There are three ways to set options, and you can mix them:

1. **Command-line flags** -- e.g. `--redis-host`, `--workers`. Best for one-off runs.
2. **A YAML config file** -- pass it with `--config myconfig.yaml`. Best for
   repeatable runs and for the advanced Redis options that have no CLI flag.
3. **Environment variables** -- e.g. `REDIS_HOST`, `AEROSPIKE_NAMESPACE`. Best for
   secrets and containerized environments.

See [Running and tuning](05-running-and-tuning.md#how-configuration-is-resolved)
for exactly how these combine and which one wins when they overlap.
