# 10 - Replay mode

`--mode replay` reads a JSONL event log and writes those changes to Aerospike
in file order. Redis is not contacted. Use it to apply a tail that
`--mode pubsub` or `--mode sync` already recorded.

The log is a copy of the tail. A normal `pubsub` or `sync` run still writes
Aerospike as events arrive. Replay is a second pass over that file.

## Record the log

Set `event_log` in the config, or pass `--event-log`. Both do the same thing.
`scan` does not write this file.

```bash
redis2aerospike --config /etc/redis2aerospike/config.yaml \
  --event-log redis2aerospike-sync.jsonl
```

```yaml
mode: sync
event_log: redis2aerospike-sync.jsonl
```

The process creates the file as soon as `pubsub` or `sync` starts, before it
contacts Redis. The startup log prints the real path:

```text
recording pub/sub changes in order to /var/log/redis2aerospike/redis2aerospike-sync-20261103T160300+0530.jsonl
```

A relative path is placed next to the config file. With no `--config`, it is
placed in the current directory. The name you pass is the template. The run
inserts a local timestamp before `.jsonl` so each start has its own file.
That timestamp is for tracking which run produced the file. Replay does not
parse it. Pass the stamped path you see in the log.

`sync` logs the tail only. The initial `SCAN` is not in the file. Lines are
appended when a notification is applied. An empty file means the tail saw no
changes yet.

Example configs: [examples/config.sync.yaml](../examples/config.sync.yaml),
[examples/config.pubsub.yaml](../examples/config.pubsub.yaml).

## What a line contains

One JSON object per line, in receive order. `seq` starts at 1 for that file
and increases by one for each recorded change. `time` is the local clock time
when the line was written. Replay uses `seq`. It ignores `time` and the
timestamp in the filename.

A write stores the Redis snapshot taken when the event was applied:

```json
{"action":"upsert","event":"set","key":"user:1","type":"string","value":"ada","ttl_ms":60000,"seq":120,"time":"2026-11-03T16:42:00+05:30"}
```

A delete stores the key and the event name:

```json
{"action":"delete","event":"expired","key":"user:1","seq":121,"time":"2026-11-03T16:42:05+05:30"}
```

Binary values, sets, and maps use a small JSON encoding (`"$": "b"` and
similar) so the line stays text. Skipped events, including unsupported types
and keys outside `scan_match`, are not lines, so `seq` can jump.

## Replay the file

Point `--event-log` at the stamped file. `--mode replay` overrides a `sync`
or `pubsub` config, so the same Aerospike settings can be reused.

```bash
redis2aerospike --config /etc/redis2aerospike/config.yaml --mode replay \
  --event-log /var/log/redis2aerospike/redis2aerospike-sync-20261103T160300+0530.jsonl
```

Preview the count without writing:

```bash
redis2aerospike --config /etc/redis2aerospike/config.yaml --mode replay --dry-run \
  --event-log /var/log/redis2aerospike/redis2aerospike-sync-20261103T160300+0530.jsonl
```

`--from-seq` starts at that sequence and skips earlier lines. The number is
inclusive. Omit it to apply the whole file.

```bash
redis2aerospike --config /etc/redis2aerospike/config.yaml --mode replay \
  --event-log /var/log/redis2aerospike/redis2aerospike-sync-20261103T160300+0530.jsonl \
  --from-seq 120
```

The same start point in YAML is `from_seq: 120`. The environment variable is
`MIGRATION_FROM_SEQ`. A full example is
[examples/config.replay.yaml](../examples/config.replay.yaml).

## What replay writes

Lines are applied one at a time, in file order, so a later line for the same
key replaces an earlier one.

| Line | Aerospike action |
| --- | --- |
| `action: upsert` | Write the stored snapshot, unless the Aerospike record is already newer. The record replaces the existing one unless `record_exists_policy` is `create_only`. |
| `action: delete` | Delete the Aerospike record, unless that record is already newer than the line. |
| Any other `action` | Skip the line and count it. |

Before each write or delete, replay reads the destination record's last update
time. When that time is the same as or later than the line's `time`, the
destination is already the latest value and the line is skipped. The summary
counts those skips as `latest`. A missing record is written. A line with no
`time`, or a server that does not return a last update time, is applied.

Set routes, the TTL overflow policy, and the write rate limit apply the same
way they do on a live tail. Replay does not re-read Redis, so the snapshot in
the file is what gets written.

## What the log does not contain

- Keys copied by the initial `SCAN` of `--mode sync`.
- Changes that happened while the process was stopped. Redis pub/sub does not keep them.
- Changes missed while a primary subscription was down.
- `time` for ordering. Order is `seq`, then file order. `time` is compared only with the destination's last update time.

Catch-up of the scan window is `sync_catchup` on a live `sync` run. That is
separate from `--mode replay`. See
[Keyspace notifications](09-keyspace-notifications.md).

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Every selected line was applied. |
| `1` | The file was read, and one or more writes failed or a line was invalid. |
| `2` | No `--event-log`, `from_seq` is less than 1, or Aerospike could not be reached. Nothing was written. |
