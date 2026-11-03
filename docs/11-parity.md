# 11 - Parity mode

`--mode parity` checks that Aerospike matches the keys MemoryDB is serving.
It does not write to either database. Run it after the backfill, before
cutover.

MemoryDB remains the source of the keys. For each sampled key the tool reads
Aerospike on a background worker and compares the value and the TTL. The
comparison uses the same converters and set routes as a migration.

```bash
redis2aerospike --config /etc/redis2aerospike/config.yaml --mode parity \
  --parity-sample 0.01
```

`--parity-sample 1` checks every key. The default `0.01` checks 1 percent.
The sample is a hash of the key, so a later run selects the same keys.
Example: [examples/config.parity.yaml](../examples/config.parity.yaml).

## What is reported

| Count | Meaning |
| --- | --- |
| `seen` | Keys read from MemoryDB. |
| `checked` | Keys in the sample that were compared. |
| `matched` | Value and TTL agree. |
| `missing` | The key is still in MemoryDB and absent in Aerospike. |
| `mismatch` | The value or the TTL differs, and a second MemoryDB read shows the same value. |
| `ttl skew` | Values agree, and the remaining TTL differs by at most `--parity-ttl-skew` (default 5000 ms). |
| `in-flight` | The key changed or disappeared in MemoryDB between the scan and the second read. |
| `mismatch rate` | Checked keys that were not an exact match, including skew and in-flight writes. |
| `unexplained` | `missing` + `mismatch` + read errors. |
| `aerospike p50/p95/p99` | Latency of the shadow reads, in milliseconds. |

TTL skew and in-flight writes are expected while MemoryDB is still taking
traffic. A missing key or a value mismatch is not.

## When not to proceed

Exit `1` when `unexplained` is greater than zero, or when the scan saw keys
and the sample selected none. Do not cut over on that result. Fix the
backfill or widen the sample and run parity again.

Exit `0` when every difference is TTL skew or an in-flight write, or when
there is no difference.

Exit `2` when MemoryDB or Aerospike cannot be reached, or when
`parity_sample` is outside `(0, 1]`.

`--dry-run` connects and prints the sample it would check. It does not read
the keyspace.

## How a difference is explained

1. Read the key from MemoryDB.
2. Read the same key from Aerospike in the background and record the latency.
3. Compare the converted value and the remaining TTL.
4. When the value differs or Aerospike has no record, read MemoryDB once more.
   A changed or deleted key is in-flight. An unchanged key is a mismatch or
   a missing key.

A TTL that differs by more than `--parity-ttl-skew`, or a record that expires
on only one side, is a mismatch. It is not skew.
