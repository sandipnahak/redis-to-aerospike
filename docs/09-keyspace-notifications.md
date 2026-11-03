# 09 - Keyspace notifications (MemoryDB pub/sub)

`--mode scan` copies the keyspace once. `--mode pubsub` does not scan. It
subscribes to Redis keyspace notifications and applies each change to
Aerospike until you interrupt it. `--mode sync` scans first, then follows.

MemoryDB does not store pub/sub messages. Notifications are generated only
when the cluster parameter `notify-keyspace-events` is set. The tool reads
that value with `CONFIG GET` when it can, and subscribes to one channel
family so `KEA` (which emits both families) is not applied twice.

## Enable notifications on MemoryDB

1. Open the MemoryDB console and go to **Parameter groups**.
2. Select or create a custom parameter group and attach it to the cluster.
3. Edit `notify-keyspace-events`.
4. Set it to the events you want to follow:

| Value | What MemoryDB publishes |
| --- | --- |
| `KEA` | All keyspace and keyevent notifications: writes, deletes, expires, evictions. |
| `Ex` | Expired keys only. Common when Aerospike should drop records whose Redis TTL fired. |

`K` turns on `__keyspace@0__` channels and `E` turns on `__keyevent@0__`
channels. At least one of those letters is required; the rest of the string
chooses which commands emit an event. `A` means most commands (it does not
include key-miss events). `x` means expired keys.

With `pubsub_channel: auto` (the default):

- `KEA` subscribes to `__keyspace@0__:*` (the key is in the channel, the event name is the message).
- `Ex` subscribes to `__keyevent@0__:*` (the event name is in the channel, the key is the message).

MemoryDB does not implement `CONFIG GET`. When that command is rejected,
`pubsub` and `sync` subscribe on every primary and write a temporary key
named `redis2aerospike:notify-probe:<id>`. A notification for that key means
the parameter is active: a write event is treated as `KEA`, and only an
`expired` event is treated as `Ex`. No notification exits with code 2. The
key is deleted before the copy starts. `auto` then follows the probe result.

`pubsub` and `sync` also exit with code 2 when `CONFIG GET` succeeds and
`notify-keyspace-events` is missing, empty, or lacks `K` for a keyspace
subscription or `E` for a keyevent subscription. `scan` does not read the
parameter.

## Example configs

| Mode | File | What it does |
| --- | --- | --- |
| `scan` | [examples/config.scan.yaml](../examples/config.scan.yaml) | Copy the keyspace once and exit. |
| `pubsub` | [examples/config.pubsub.yaml](../examples/config.pubsub.yaml) | Follow `notify-keyspace-events` only. |
| `sync` | [examples/config.sync.yaml](../examples/config.sync.yaml) | Scan, then follow. |
| `replay` | [examples/config.replay.yaml](../examples/config.replay.yaml) | Write an event log to Aerospike. |

```bash
redis2aerospike --config examples/config.sync.yaml \
  --event-log redis2aerospike-sync.jsonl
```

`--event-log` records the tail when the YAML has no `event_log`. The file is
created next to the config, and the name gains a local timestamp before
`.jsonl`.

## Run it

MemoryDB is a cluster and speaks TLS. Subscribe on every primary: keyspace
notifications are generated on the node that owns the key and are **not**
broadcast the way a normal `PUBLISH` is.

```bash
redis2aerospike \
  --redis-host clustercfg.example.memorydb.amazonaws.com --redis-port 6379 \
  --redis-cluster --redis-ssl \
  --redis-username app --redis-password "$REDIS_PASSWORD" \
  --aerospike-host aerospike.example --aerospike-port 3000 \
  --aerospike-namespace test --aerospike-set redis \
  --mode sync
```

`--redis-match` / `redis.scan_match` filters both the scan and later
notifications. Set routes apply to writes and to deletes, so an expired
`user:42` removes the same Aerospike record a scan would have written.

Stop the listener with Ctrl+C. A second interrupt aborts immediately.
Leave it running in the background or under systemd, with the log on stderr,
as described in [Redirecting and capturing logs](05-running-and-tuning.md#redirecting-and-capturing-logs).
Events published while the process is down are gone; Redis pub/sub does not
keep them.

`--mode sync` with `sync_catchup: false` (the default) copies the keyspace,
then subscribes. Changes published **during** that scan are not replayed.

`event_log` appends each pub/sub change to a JSONL file in receive order.
`sync` logs the tail, not the initial scan. Recording, the line format, and
`--mode replay` / `--from-seq` are covered in [Replay](10-replay.md).

`sync_catchup: true` subscribes before the scan. Each key that changes during
the copy is replayed once the scan finishes, then the live tail continues.
That closes the scan window. It does not recover events from while the
process is stopped, or from a gap while a primary subscription is down.
Redis pub/sub does not store those. Use `KEA` so writes, deletes, and
expiries are all published. `Ex` only replays expiries.

## What each event does

| Redis event | Aerospike action |
| --- | --- |
| `set`, `hset`, `hdel`, `expire`, `lpush`, `sadd`, `zadd`, and other updates | Re-read the key and write a full snapshot. |
| `del`, `expired`, `evicted`, `rename_from`, `move_from` | Delete the Aerospike record. |
| `keymiss` | Ignored. |
| Key gone by the time it is read | Delete the Aerospike record. |
| Streams and other unsupported types | Skipped, same as a scan. |

The snapshot replaces the Aerospike record (unless
`record_exists_policy` is `create_only`) so a removed hash field does not
leave a stale bin. `create_only` still skips existing records, and a delete
notification still removes the record.

`Ex` only produces `expired`. Those keys are already gone in Redis, so the
tool deletes them in Aerospike and does not try to read them.

Writes use one worker per key (events for the same key stay in order).
`--workers` is how many keys are applied at once. `--write-batch-size`
applies to the scan only. `--scan-rate-limit` caps how fast notifications
are applied, and `--write-rate-limit` caps Aerospike writes and deletes.

Cluster primaries are rediscovered every `pubsub_refresh_s` seconds
(default 30) so a failover picks up the new primary. `0` subscribes once
and stays there. A dropped subscription is restarted. Events from the gap
are not recovered.

Replicas are not subscribed. They emit the same expiry for keys they hold,
and following them would apply the same event twice.
