# 06 - Troubleshooting

Most problems show up either as a connection failure (exit code `2`) or as
entries in the end-of-run summary's "skipped by type" / "errors by type"
breakdown. This guide maps the common symptoms to their cause and fix.

## First step: always dry-run

Before debugging a real migration, run with `--dry-run`. It connects, prints the
preview, and exits without writing. It isolates connection/config problems from
data problems in seconds.

```bash
redis2aerospike --config prod.yaml --dry-run
```

## Connection failures (exit code 2)

### `cannot reach Redis: ...`

The tool could not connect to Redis. Check, in order:

- **Host/port** are correct and reachable from this machine
  (`redis-cli -h <host> -p <port> ping`).
- **Authentication**: a password-protected Redis needs `--redis-password` (and
  `username` via YAML/env for ACLs). A wrong/missing password surfaces here.
- **TLS**: if Redis requires TLS, you must enable it (`ssl: true` or a
  `rediss://` URL). A plaintext connection to a TLS port fails here.
- **Cluster**: if Redis is a cluster, set `cluster: true`; a standalone client
  pointed at a cluster can fail or behave oddly.

See [Connecting to Redis](02-connecting-redis.md).

### `cannot reach Aerospike: ...`

The tool could not connect to Aerospike. Check:

- **Host/port** reachable; the default port is `3000`.
- **Authentication**: Enterprise security clusters need `--aerospike-username`
  and `--aerospike-password`, plus the correct `--aerospike-auth-mode`.
- **TLS**: enable `--aerospike-tls-enable` and set `--aerospike-tls-name` to the
  server certificate's subject name (it must match), with the right
  `--aerospike-tls-cafile`.
- **NAT / cloud / Docker**: if the cluster advertises addresses you can't reach,
  add `--aerospike-use-services-alternate`.

See [Connecting to Aerospike](03-connecting-aerospike.md).

## Records were skipped

Skips are expected and never fail the run. They appear under "skipped by type".

### Skip reason is a Redis type (e.g. `stream`)

That type isn't supported. Only String, Hash, List, Set, and Sorted Set are
migrated; everything else (such as Redis Streams) is skipped by design. If you
need that data, you'll have to handle it separately -- the tool will not migrate
it. See [Transferring data](04-transferring-data.md#types-that-are-skipped).

## Records errored

Errors appear under "errors by type" and cause a non-zero exit (`1`).

### `convert:TtlTooLongError`

A key's TTL exceeds `--max-ttl` and the TTL overflow policy is `reject` (the
default). Choose one:

- Raise the boundary: `--max-ttl <bigger-seconds>` (or `0` to disable the check).
- Clamp to the max: `--ttl-overflow-policy clamp`.
- Store as never-expire: `--ttl-overflow-policy never_expire`.

See [TTL handling](04-transferring-data.md#ttl-handling).

### `write:RecordTooLargeError`

The record's estimated size exceeds the limit (Aerospike's max object size, by
default 8 MiB, aligned to your server's advertised `max-record-size`). The value
in Redis is simply too big for a single Aerospike record. Options:

- Confirm the value really is that large (the error message includes the
  approximate byte size and the key).
- Reduce the source value, or restructure how that data is stored in Aerospike
  (this requires changes outside this tool's scope).

### Other `write:...` errors

Generic server-side write failures (timeouts, capacity, cluster issues). Check:

- The Aerospike cluster's health and that it isn't in **stop-writes** (the
  preview shows `stop-writes-pct`; if the namespace is over its capacity
  threshold, writes are rejected).
- Timeout settings -- raise `--aerospike-socket-timeout-ms` /
  `--aerospike-total-timeout-ms` if writes are timing out under load.
- Lowering `--workers` if you're overwhelming the cluster.

## `BatchError:22` on write

Aerospike result code **22** is `AS_ERR_FORBIDDEN`. For this tool it means the
write set a TTL and the namespace has `nsup-period=0` (expiration disabled,
the server default). The server rejects that write. It does not store the record.

This shows up after a MemoryDB `ttl_command: dump` run: Redis `INFO` reports no
expiring keys, but hash field `e` still becomes an Aerospike TTL. Every such key
fails with `BatchError:22`.

Set a non-zero `nsup-period` on the namespace (for example `120`), then re-run.
The failed keys were not written. Keys that had no expiry may already be in
Aerospike; a re-run updates them.

```
asinfo -v "set-config:context=namespace;id=<namespace>;nsup-period=120"
```

That change is dynamic, but it does not survive a restart until it is also in
the namespace configuration file.

## TTLs are not being expired

If you wrote records with TTLs but the server isn't expiring them, the namespace
likely has `nsup-period=0` (TTL eviction disabled).

Before migrating, the tool reads Redis `INFO keyspace` (keys with an expiry) and
Aerospike namespace info. If Redis reports one or more keys with a TTL **and** the
target namespace has `nsup-period=0`, the CLI **aborts** with exit code `2` and a
message such as:

```
Records with TTL cannot be inserted into Aerospike namespace 'X': TTL eviction is disabled (nsup-period is 0) ...
```

If Redis has **no** keys with TTL (or the keyspace stats could not be read) and
`ttl_command` is `pttl` or `ttl`, you only get a **warning** on connect:

```
Aerospike namespace 'X' has nsup-period=0 (TTL eviction disabled); a write that
sets a TTL is rejected with error 22 (AS_ERR_FORBIDDEN)
```

`ttl_command: dump` (and `expiretime` / `pexpiretime`) abort before writing in
this situation, because those commands attach an expiry that Redis `INFO` does
not count. See [`BatchError:22` on write](#batcherror22-on-write).

This is a **server configuration** issue -- an Aerospike administrator must set a
non-zero `nsup-period` on the namespace for TTL eviction to run.

## TLS / authentication pitfalls

- **`tls_name` mismatch**: `--aerospike-tls-name` must match the subject name in
  the server's certificate, or the handshake fails.
- **Mutual TLS incomplete**: mutual TLS needs *both* the client `certfile` and
  `keyfile` (plus `keyfile_pw` if the key is encrypted). Supplying only one
  fails.
- **Wrong auth mode**: external identity providers need
  `--aerospike-auth-mode external` (or `external_insecure` / `pki`), not the
  default `internal`.
- **Redis ACL username**: set it with `--redis-username`, YAML (`username:`), or
  `REDIS_USERNAME`.

## Reading the breakdown to find the dominant problem

The summary's "errors by type" / "skipped by type" sections are sorted and
counted, so the biggest contributor is obvious at a glance:

```
  errors by type:
    - write:RecordTooLargeError: 4120
    - write:AerospikeError: 3
```

Here, almost all failures are oversized records -- focus there rather than the
3 transient write errors. Re-run after addressing the dominant cause; re-runs are
idempotent (see [Running it more than once](04-transferring-data.md#running-it-more-than-once-idempotency)).

## Still stuck?

Run with `--log-level DEBUG` for per-record detail (including skipped keys and
the specific exceptions), and capture it to a file:

```bash
redis2aerospike --config prod.yaml --log-level DEBUG 2> debug.log
```

For a full list of options, see the
[Configuration reference](07-configuration-reference.md).
