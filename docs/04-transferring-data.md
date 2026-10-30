# 04 - Transferring data

This guide explains exactly what the tool copies, how each Redis type becomes a
native Aerospike type, how TTLs and large records are handled, and what happens
when you run it more than once.

## How the transfer works

```
Redis ──SCAN+pipeline──> Producer ──> bounded queue ──> Worker 1..N ──> Aerospike
                                                            │
                                              convert -> (transform) -> write
```

1. A single producer streams keys out of Redis using `SCAN` (cursor-based, so it
   never blocks the server) and pipelines the value/TTL reads.
2. Records land on a bounded in-memory queue that provides back-pressure -- a
   fast Redis read can't outrun Aerospike writes and exhaust memory.
3. A pool of worker threads pulls records off the queue, converts each value to a
   native Aerospike type, and writes it.

Each record is processed independently. If one record fails to convert or write,
it is counted and the migration keeps going.

## Type mapping

| Redis type | Aerospike representation |
| --- | --- |
| **String** | A single bin, coerced in order: `int` -> `float` -> `str` -> `bytes` (blob). The first type that fits is used. |
| **Hash** | One **key-ordered** Map bin by default (`MAP_KEY_ORDERED`), or one bin per field with `--hash-strategy field_bins`. |
| **List** | An Aerospike List, with order preserved. |
| **Set** | An Aerospike List written with the `ADD_UNIQUE` flag (ordered), so set semantics are enforced server-side. |
| **Sorted set** | A **key-ordered** Aerospike Map of `{member: score}` (`MAP_KEY_ORDERED`). |

Map bins use the Aerospike Python client’s ``KeyOrderedDict`` so they serialize as
``MAP_KEY_ORDERED`` rather than the default unordered CDT map.

The Aerospike record identity is ``(namespace, set, primary_key)``. UTF-8 keys
are stored as strings; binary keys are preserved as-is. With **set routes**,
``set`` comes from the matched route and ``primary_key`` is usually the part of
the Redis key that matched the single ``*`` in the pattern (see below); keys
that do not match any route keep the full Redis key and the default set.

### Redis source filter vs Aerospike set routing

These are **orthogonal** controls:

| Control | Config / CLI | What it does |
| --- | --- | --- |
| **Source filter** (less data from Redis) | YAML `redis.scan_match` (alias `key_pattern` if `scan_match` is omitted), CLI `--redis-match` or `--redis-key-pattern` | Passed to Redis `SCAN` as `MATCH`. Only keys matching this glob enter the pipeline, which reduces SCAN results and follow-up reads. |
| **Set routes** (Aerospike placement + key) | YAML `aerospike.set_routes`, CLI `--set-route` (repeatable; see [Hash strategies](#hash-strategies) for optional per-route hash tokens) | After a key is read, the first matching route picks the **Aerospike set** and rewrites the **primary key** by dropping the fixed literal parts of the pattern around a single ``*`` (see below). Keys that match none use ``aerospike.set_name`` / ``--aerospike-set`` and the full Redis key. Does **not** run an extra Redis SCAN. Optional per-route ``hash_strategy`` / ``value_bin`` (map layout only) override the pipeline defaults for matching keys. |

`scan_match` defines the **superset** of keys for this run. Set routes only apply to keys already returned by SCAN.

**Set route semantics:** routes are ordered; **first match wins**. Patterns use Python [`fnmatch`](https://docs.python.org/3/library/fnmatch.html) (glob-style), evaluated on UTF-8 string keys. **Binary Redis keys** always use the default set (`set_name`) and the full Redis key, not route patterns.

**Primary key when a route matches:** if the route pattern contains **exactly one** ``*`` and no ``?`` or ``[`` characters, the substring that aligns with that ``*`` becomes the Aerospike user key—the literal segments before and after ``*`` are removed. Examples: pattern ``user:*`` with Redis key ``user:42`` → Aerospike key ``42`` in the routed set; ``app:*:item`` with ``app:7:item`` → ``7``. If stripping would yield an empty string, the full Redis key is kept. Patterns with **multiple** ``*``, ``?``, or ``[`` still match for routing and set choice, but the primary key is left **unchanged** (full Redis key).

**SCAN limitation:** Redis applies **one** `MATCH` pattern per scan. There is no built-in OR of unrelated globs (e.g. `user:*` and `cache:*`) in a single pass. Prefer one broader glob (e.g. `app:*`) or run separate migrations with different `scan_match` values if you need disjoint prefixes.

### String coercion, by example

| Redis string value | Stored in Aerospike as |
| --- | --- |
| `"42"` | integer `42` |
| `"3.14"` | float `3.14` |
| `"hello"` | string `"hello"` |
| arbitrary binary bytes | a blob |

### Hash strategies

`--hash-strategy` (YAML `hash_strategy`, env `MIGRATION_HASH_STRATEGY`) chooses
how a Redis hash is represented:

- **`map_bin`** (default): the entire hash is stored as a single Aerospike Map in
  the value bin. Simplest, 1:1 mapping.
- **`field_bins`**: each hash field becomes its own Aerospike bin. More "native"
  and queryable per field, but subject to Aerospike's bin-name length limits and
  per-record bin count.

```bash
redis2aerospike --redis-host localhost \
  --aerospike-host localhost --aerospike-namespace test --aerospike-set redis \
  --hash-strategy field_bins
```

### Per-route hash layout (set routes)

Each YAML mapping under ``aerospike.set_routes`` may include optional ``hash_strategy``
(``map_bin`` or ``field_bins``) and optional ``value_bin`` (the Aerospike bin name
for the single map when using ``map_bin``). Omitted keys fall back to the
pipeline-level ``hash_strategy`` and ``aerospike.value_bin``. If the effective
strategy for a key is ``field_bins``, any route ``value_bin`` is ignored.

CLI ``--set-route`` accepts ``PATTERN=SET``, ``PATTERN=SET=hash_strategy``, or
``PATTERN=SET=map_bin=CUSTOM_BIN`` for a custom map bin name (the third segment
must be ``map_bin`` when a fourth segment is present).

## TTL handling

Redis TTLs (in milliseconds) are converted to Aerospike TTLs (in seconds). Keys
with no expiry are written as **never-expire**.

Which Redis command supplies the expiry is `redis.ttl_command`:

| Command | Reply | Stored TTL |
| --- | --- | --- |
| `pttl` (default) | Remaining time in milliseconds. `-1` / `-2` mean no expiry. | That many milliseconds, rounded up to seconds. |
| `ttl` | Remaining time in seconds. | Seconds × 1000. |
| `pexpiretime` | Absolute Unix expiry in milliseconds. | `expiry - now`. |
| `expiretime` | Absolute Unix expiry in seconds. | `(expiry × 1000) - now`. |
| `dump` | RDB serialization of the value. | Absolute timestamp in hash field `ttl_dump_field` (default `e`). A number ≥ 10^12 is Unix milliseconds; ≥ 10^9 is Unix seconds; a smaller positive number is a remaining TTL in seconds. |

`dump` is for sources that have no TTL commands. Amazon MemoryDB Multi-Region
rejects `PTTL`, `TTL`, and `EXPIRETIME`, but `DUMP` returns the value. A hash
like `\x04\x02\x01v\nTTO_FR_WEB\x01e\r1813638928335...` is fields `v` =
`TTO_FR_WEB` and `e` = `1813638928335` (absolute milliseconds). The key's Redis
TTL is not part of `DUMP`; only this embedded field is.

```yaml
redis:
  cluster: true
  ttl_command: dump
  ttl_dump_field: e
```

### The max-TTL boundary

Aerospike caps a record's TTL at the namespace `max-ttl` (10 years by default).
The tool enforces a boundary before writing:

- `--max-ttl` (YAML `max_ttl`, env `AEROSPIKE_MAX_TTL`): the limit in **seconds**.
  Default is 10 years (`315360000`). Set to `0` to disable the check entirely.
- `--ttl-overflow-policy` (YAML `ttl_overflow_policy`, env
  `MIGRATION_TTL_OVERFLOW_POLICY`): what to do with a record whose TTL exceeds
  `--max-ttl`:

| Policy | Behavior |
| --- | --- |
| `reject` (default) | The record is rejected **before** any write and counted as a `convert:TtlTooLongError` error. |
| `clamp` | Stored with exactly `max-ttl`. A single warning is logged per run. |
| `never_expire` | Stored as never-expire. A single warning is logged per run. |

### Will the server actually expire TTLs?

If the target namespace has `nsup-period=0`, **TTL eviction is disabled** and any
TTLs you write will never be enforced by the server. The tool reads Redis
`INFO keyspace`: if any keys carry a TTL while `nsup-period` is `0`, the CLI
**exits with an error** before writing. Otherwise it logs a warning only. See
[Troubleshooting](06-troubleshooting.md#ttls-are-not-being-expired).

## Choosing which keys to migrate

By default every key is migrated. To migrate a subset, use the Redis match
pattern (`--redis-match`, see [Connecting to Redis](02-connecting-redis.md#selecting-which-keys-to-migrate)).

This also affects the **key estimate** shown in the pre-run preview:

- With `--redis-match *`, the estimate is the exact key count (`DBSIZE`).
- With any other pattern, the preview shows `<= <DBSIZE>` -- the whole-database
  count as an upper bound, since the matching subset isn't known until the scan
  runs.

## Large records

Aerospike rejects records above a maximum object size (8 MiB by default). To give
you a clear, actionable error instead of a mid-write server failure, the tool
estimates each record's size and rejects oversized ones up front with a
`write:RecordTooLargeError`.

On connect, the tool reads the server's advertised `max-record-size` and aligns
its own guard with that real limit (instead of the built-in 8 MiB default), so
the check matches your cluster's actual configuration.

## Running it more than once (idempotency)

Re-running a migration is usually safe:

- With the default **`update`** record-exists policy (`--aerospike-record-exists-policy update`,
  YAML `aerospike.record_exists_policy`, env `AEROSPIKE_RECORD_EXISTS_POLICY`), regular
  records use Aerospike create-or-update semantics: new bins from Redis are merged
  into any existing record, and bins present in the write are updated.
- **`replace`** replaces the entire Aerospike record when the key exists (and
  creates it when missing), so the stored record matches only the bins produced
  from Redis for that run—useful if older runs or manual edits left extra bins
  you want removed.
- **`create_only`** inserts only: if the record already exists, the write is
  skipped and counted under **skipped** (`exists`) in the summary—useful for
  incremental loads where Aerospike is already authoritative for some keys.
- **Set** bins use `ADD_UNIQUE` with the `NO_FAIL` and `PARTIAL` flags, so
  re-adding members that already exist is silently skipped rather than failing
  the write.

This means an interrupted migration can be re-run from the start without
producing duplicates or errors from already-migrated data (under the default
`update` policy).

## Types that are skipped

Only String, Hash, List, Set, and Sorted Set are supported. Other types (for
example, Redis **Streams**) are **skipped** and counted under "skipped by type"
in the summary, keyed by the Redis type name. The migration does not fail
because of them.

Next: [Running and tuning](05-running-and-tuning.md).
