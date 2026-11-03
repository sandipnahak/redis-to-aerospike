# Redis to Aerospike Migrator

`redis2aerospike` is a command-line tool that copies data from **Redis** (or
**Valkey** and other **Redis-compatible** servers using the same protocol) into
**Aerospike**, converting each Redis value into a **native Aerospike type** as it
goes. It streams keys out of Redis with `SCAN` and writes them with a pool of
worker threads, so it stays fast and memory-bounded even on large keyspaces.

The data model stays key-value to key-value: a Redis key becomes an Aerospike
record ``(namespace, set, primary_key)``, and its value becomes a native type. With
**set routes**, ``set`` and often ``primary_key`` are derived from the route pattern
(see [Transferring data](docs/04-transferring-data.md)); otherwise ``primary_key``
is the full Redis key and ``set`` comes from ``--aerospike-set``.

## Main features

- **Native Aerospike types** — Migrates Redis strings, hashes, lists, sets, and
  sorted sets (plus TTLs) into appropriate bins, lists, and maps; unsupported types
  (e.g. Streams) are skipped and counted in the summary.
- **Keyspace notifications** — `--mode pubsub` follows MemoryDB/Redis
  `notify-keyspace-events` (for example `KEA` or `Ex`) and applies each change.
  `--mode sync` scans first, then follows. `sync_catchup` subscribes before
  the scan and replays keys that change during the copy. Cluster mode subscribes to every
  primary, because those events stay on the node that owns the key. See
  [Keyspace notifications](docs/09-keyspace-notifications.md). `--mode replay`
  writes a recorded tail to Aerospike. See [Replay](docs/10-replay.md).
- **Multithreaded pipeline** — A producer streams `SCAN` results into a **bounded
  queue**; a pool of **worker threads** (default 8, `--workers`) converts and writes
  in parallel so the run stays fast without loading the whole keyspace into memory.
- **Key placement** — Narrow the source with Redis `SCAN` `MATCH`
  (`redis.scan_match`, `--redis-match`); optionally map key patterns to Aerospike
  sets (and shorter user keys) with `set_routes` / `--set-route`. See
  [Transferring data](docs/04-transferring-data.md).
- **Flexible configuration** — **CLI**, **YAML** (`--config`), and **environment
  variables**; any CLI flag you pass overrides the matching YAML value
  ([Configuration reference](docs/07-configuration-reference.md)).
- **Serious connectivity** — Redis or Valkey standalone or **Cluster**, **TLS** / mutual TLS,
  **ACL** auth, and connection **URLs**; Aerospike **multi-host**, Enterprise **auth**
  modes, **TLS** / mutual TLS, and client timeouts ([Redis](docs/02-connecting-redis.md),
  [Aerospike](docs/03-connecting-aerospike.md)).
- **Tuning and safety** — Scan and write **rate limits**, **batch** writes to
  Aerospike, **record-exists** policies (merge, full replace, or create-only),
  hash layout strategies, **TTL overflow** policies, dry-run **preview**,
  namespace checks, progress **heartbeat**, and a final **summary** (throughput,
  skips, errors) with meaningful **exit codes** ([Running and tuning](docs/05-running-and-tuning.md)).

> **New here?** The [user guide](docs/README.md) has step-by-step instructions
> for installing, connecting each database, transferring data, tuning, and
> troubleshooting.

> **Example software — not for production as-is.** This repository is provided as
> a **reference example** only. **Do not rely on it in production** without your
> own security review, testing, hardening, and operational ownership. Everything
> here is offered **as-is**, with **no warranty**, and
> **no promise of support, maintenance, or ongoing updates.**

## Type mapping

| Redis type | Aerospike representation |
| --- | --- |
| String | Single bin, coerced to `int` -> `float` -> `str` -> `bytes` (blob). |
| Hash | One **key-ordered** Map bin (default, `MAP_KEY_ORDERED`), or one bin per field (`--hash-strategy field_bins`). |
| List | Aerospike List (order preserved). |
| Set | Aerospike List written with the `ADD_UNIQUE` flag (ordered), enforcing set semantics. |
| Sorted Set | **Key-ordered** Aerospike Map (`MAP_KEY_ORDERED`) of `{member: score}`. |
| TTL | Redis ms TTL -> Aerospike second TTL; no expiry -> never-expire. |

Only these types are migrated; other types (e.g. Streams) are skipped and counted in
the run summary. See [Transferring data](docs/04-transferring-data.md) for details,
including TTL boundary handling, large-record limits, **Redis SCAN filtering**
(`scan_match`), and **per-pattern Aerospike set routing** (`set_routes`).

## Key placement

- **Narrow what Redis returns:** `redis.scan_match` in YAML, or `--redis-match` /
  `--redis-key-pattern` on the CLI (Redis `SCAN` `MATCH` only).
- **Choose the Aerospike set (and optional key suffix) per pattern:**
  `aerospike.set_routes` in YAML, or repeatable `--set-route` (forms: `PATTERN=SET`,
  `PATTERN=SET=hash_strategy`, or `PATTERN=SET=map_bin=CUSTOM_BIN` for a map bin name;
  first match wins; a single `*` in the pattern strips fixed literals from the Aerospike user
  key—see docs).

See [Transferring data](docs/04-transferring-data.md) for details and limitations.

## Install

Requires **Python 3.10+**.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install redis2aerospike
```

Verify it installed:

```bash
redis2aerospike --help
redis2aerospike --version
```

See [Versioning and changelog](#versioning-and-changelog) for the in-repo history.

### Shell completion

Generate and install tab completion for your shell:

```bash
# bash
redis2aerospike --print-completion bash > ~/.local/share/bash-completion/completions/redis2aerospike

# zsh (note leading underscore)
redis2aerospike --print-completion zsh > ~/.local/share/zsh/site-functions/_redis2aerospike

# fish (filename must match the command + .fish)
redis2aerospike --print-completion fish > ~/.config/fish/completions/redis2aerospike.fish
```

Restart your shell, or `source` the completion file, for changes to take effect.

### Develop from source

From a clone of this repository, install in editable mode with the dev extras
(tests and local tooling):

```bash
pip install -e ".[dev]"
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for running the test suite and opening pull requests.
The [getting started guide](docs/01-getting-started.md) also mentions this install path.

## Versioning and changelog

- **Installed build:** run `redis2aerospike --version` (or
  `python -c "import redis_to_aerospike; print(redis_to_aerospike.__version__)"`).
- **History:** [CHANGELOG.md](CHANGELOG.md) lists notable changes by version.

This project is a **reference example**; version numbers are for **traceability**
(installs, forks, issue reports), not a promise of PyPI cadence or formal releases.

### Versioning (for maintainers)

When you want a new version label (no GitHub Release or PyPI step required):

1. Set `__version__` in [`redis_to_aerospike/version.py`](redis_to_aerospike/version.py)
   (single source; `pyproject.toml` reads it at build time).
2. In [`CHANGELOG.md`](CHANGELOG.md), move `[Unreleased]` notes into a dated
   `## [x.y.z] - YYYY-MM-DD` section and open a fresh `[Unreleased]` at the top.
3. Commit (for example: `chore: bump version to x.y.z`).

Optional: `git tag vx.y.z` on that commit as a bookmark.

## Quick start

Start throwaway local Redis and Aerospike services (no auth/TLS):

```bash
docker compose up -d
```

Preview what would happen, without writing anything:

```bash
redis2aerospike \
  --redis-host localhost --redis-port 6379 \
  --aerospike-host localhost --aerospike-port 3000 \
  --aerospike-namespace test --aerospike-set redis \
  --dry-run
```

Run the migration:

```bash
redis2aerospike \
  --redis-host localhost --redis-port 6379 \
  --aerospike-host localhost --aerospike-port 3000 \
  --aerospike-namespace test --aerospike-set redis \
  --workers 8
```

Every flag has a sensible default; run `redis2aerospike --help` for the full
list.

### Sample data for migration tests

The script [`scripts/sample_redis_seed.py`](scripts/sample_redis_seed.py) loads
**10,000** keys (by default) across all supported Redis types—string, hash, list,
set, and sorted set—with mixed values and some expiring strings. Keys are prefixed
with `sample:migrate:`.

[`scripts/sample_redis_seed_routing.py`](scripts/sample_redis_seed_routing.py) seeds
**`sample:route:`** keys (`user:*`, `session:*`, `cache:*`, `ledger:*`) for
**`--set-route`** / `set_routes` demos. **Default is at least 10,000 keys**
(`--count`); use `--per-route N` for a smaller fixed layout. With the example routes,
Aerospike user keys are the short suffix after each prefix (e.g. ``7`` in set
``users``, not the full Redis key). See the script’s docstring for example CLI and
YAML.

```bash
python scripts/sample_redis_seed.py              # default localhost:6379, 10k keys
python scripts/sample_redis_seed.py --flush      # FLUSHDB first, then seed
python scripts/sample_redis_seed.py --count 5000 --host redis.example.com

python scripts/sample_redis_seed_routing.py --flush           # default: ≥10k keys
python scripts/sample_redis_seed_routing.py --count 50000 --flush
python scripts/sample_redis_seed_routing.py --per-route 50   # small fixed layout (262 keys)
```

Then run `redis2aerospike` with the same Redis host/port against your Aerospike
namespace.

## Configuring the tool

You can configure the tool three ways, and mix them:

- **CLI flags** -- e.g. `--redis-host`, `--workers`. Best for one-off runs.
- **A YAML file** -- pass it with `--config myconfig.yaml`. The YAML is the base;
  any CLI flag you also pass overrides the matching value. See
  [`config.example.yaml`](config.example.yaml) for an annotated template, and
  [`examples/`](examples) for ready-to-run `scan`, `pubsub`, and `sync` configs.
- **Environment variables** -- `REDIS_*`, `AEROSPIKE_*`, `MIGRATION_*`. Best for
  secrets and containers.

Some advanced Redis options (ACL username, connection URL, Cluster, TLS) are only
available via YAML or environment variables, not CLI flags. The
[configuration reference](docs/07-configuration-reference.md) lists every option
with its flag, YAML key, env var, and default.

### Connecting securely

The tool supports Redis ACL auth, TLS, connection URLs, and Redis Cluster (and
the same client options against Valkey where applicable), and
Aerospike Enterprise security with auth, TLS/mutual TLS, and tuned timeouts.
Passwords are never printed. See:

- [Connecting to Redis](docs/02-connecting-redis.md)
- [Connecting to Aerospike](docs/03-connecting-aerospike.md)

## Reading the output

A run prints, in order:

1. **Preview** -- a summary of both sides and the pipeline settings, before any write.
   `--dry-run` stops here.
2. **Checks** -- warnings based on the Aerospike namespace settings (e.g. TTL eviction
   disabled).
3. **Delimiter** -- the same three-line banner (`redis2aerospike: migration` between
   rule lines) is logged **twice**: once right before records are read and written,
   and once right after the write phase finishes (before the summary), so you can spot
   the migration window in a long log file.
4. **Progress** -- a compact heartbeat line every `--progress-interval` seconds.
5. **Summary** -- final counters (scanned, migrated, skipped, errors), timing, throughput,
   and skip/error breakdowns.

Exit codes: `0` success, `1` completed with errors, `2` could not connect. See
[Running and tuning](docs/05-running-and-tuning.md) and
[Troubleshooting](docs/06-troubleshooting.md) for more.

## Documentation

| Guide | What it covers |
| --- | --- |
| [Getting started](docs/01-getting-started.md) | Install, verify, first migration end to end. |
| [Connecting to Redis](docs/02-connecting-redis.md) | Host/port/db, auth, URLs, TLS, Cluster (Redis, Valkey, RESP-compatible). |
| [Connecting to Aerospike](docs/03-connecting-aerospike.md) | Hosts, namespace/set, auth, TLS, timeouts. |
| [Transferring data](docs/04-transferring-data.md) | Type mapping, TTLs, subsets, re-runs. |
| [Running and tuning](docs/05-running-and-tuning.md) | Config methods, performance, output, exit codes. |
| [Troubleshooting](docs/06-troubleshooting.md) | Common failures and fixes. |
| [Configuration reference](docs/07-configuration-reference.md) | Every flag, YAML key, env var, and default. |

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for how to set up a
development environment, run the tests, and open a pull request.

## Author

Personal project by **[Zohar Elkayam](https://github.com/realmgic)** (@realmgic). It is
not an official Aerospike, Inc. product.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for the full text.
