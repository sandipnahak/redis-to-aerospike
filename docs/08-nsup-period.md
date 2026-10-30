# nsup-period

`nsup-period` is an Aerospike **namespace** setting. It controls **NSUP**, the
namespace supervisor: the background scan that deletes records whose TTL has
expired.

The value is the number of seconds between NSUP scans of that namespace.

| Value | What the server does |
| --- | --- |
| `0` | NSUP is off. Records are not expired. A write that sets a positive TTL is rejected with error 22 (`AS_ERR_FORBIDDEN`). This is the default since Aerospike 4.9. |
| `120` (example) | NSUP wakes every 120 seconds, walks the primary index, and removes records past their TTL. Positive-TTL writes are accepted. |

This tool turns a Redis expiry into an Aerospike record TTL. Those writes
succeed only when **every** node has a non-zero `nsup-period` on the target
namespace.

## Why one `asinfo` reading can disagree with the tool

`nsup-period` is stored **on each node**. `asinfo` with no `-h` talks to one
host (by default `localhost:3000`). A dynamic change applies only to that node.
Another node in the same cluster can still be `0`.

```bash
asinfo -v "namespace/pov" | tr -s ";" "\n" | grep nsup-period
# nsup-period=120     ← this host only
```

The migration sends `namespace/<namespace>` to **every** node (`info_all`). If
any node reports `0`, the run stops before it scans keys. The error names each
node:

```text
Records with TTL cannot be inserted into Aerospike namespace 'pov':
TTL expiration is disabled (nsup-period is 0). The server rejects those
writes with error 22 (AS_ERR_FORBIDDEN). ttl_command is dump.
Read nsup-period per node: BB9=0, BBA=120.
asinfo without -h reads one node, and set-config applies only to that node.
Set nsup-period to a non-zero value (for example 120) on every node, then re-run.
```

A node at `0` rejects the write. The record is not stored. Partitions mastered
on a node at `120` would accept the same write, so a mixed cluster fails part
of the keyspace with `BatchError:22`.

## Preflight

Before any `SCAN` or write, the CLI logs a `preflight checks` block. `nsup-period`
is one line in it. With one node the line is the number. With several nodes it
lists `node=value` pairs.

```text
preflight checks
  redis ping              : pass
  aerospike connect       : pass
  redis key count         : 1000
  redis INFO expires      : 0
  ttl command             : dump
  namespace               : pov
  nsup-period             : BB9=0, BBA=120
  max-record-size         : 1048576
  stop-writes-pct         : 90
  ttl writes allowed      : fail
```

| `ttl writes allowed` | Meaning |
| --- | --- |
| `pass` | Every node that answered has a non-zero `nsup-period`. |
| `warn (nsup-period is 0)` | At least one node is `0`, and this run is not expected to write expiries. The run continues. A later write that does set a TTL still fails with error 22. |
| `fail` | At least one node is `0`, and this run would write expiries. The process exits `2` and writes nothing. |

The run is expected to write expiries when either of these is true:

- `redis.ttl_command` is `dump`, `expiretime`, or `pexpiretime`. MemoryDB
  Multi-Region stores the expiry inside the value (hash field `e` by default).
  Redis `INFO keyspace` often reports `expires=0` for those keys, so the tool
  does not trust that counter for these commands.
- `redis.ttl_command` is `pttl` or `ttl` (the default is `pttl`) **and** Redis
  `INFO keyspace` reports `expires` greater than 0.

`pttl` or `ttl` with `expires` of 0, or with `expires` unavailable, logs the
warning and continues.

If namespace info cannot be read, `nsup-period` is left unknown and this check
does not stop the run. A node that is actually `0` still rejects TTL writes
later with error 22.

## Check every node

```bash
asadm -e "show config namespace for pov like nsup-period"
```

Or, one host at a time, the same info command the tool uses:

```bash
asinfo -h <host> -p 3000 -v "namespace/pov" | tr -s ";" "\n" | grep '^nsup-period='
```

Repeat `-h` for each cluster host. `120` on the host you tried first is not a
cluster-wide result.

## Set it on every node

Apply `120` (or another non-zero period, in seconds) to the whole cluster:

```bash
asadm -e "enable; manage config namespace pov param nsup-period to 120"
```

`asinfo` changes a single node. Use it only when you pass every host yourself:

```bash
asinfo -h <host> -p 3000 -v "set-config:context=namespace;id=pov;nsup-period=120"
```

Confirm the table shows a non-zero value on each node, then re-run the
migration. Keys that failed with error 22 were not written.

A dynamic change is dropped when that node restarts. Put the same value in the
namespace stanza so it survives:

```
namespace pov {
    nsup-period 120
}
```

## Read the TTL back with AQL

`{ttl}` and `{gen}` are record metadata. AQL hides them until metadata printing
is turned on for the session. `{ttl}` is the remaining life in seconds. `e` is
the absolute expiry copied from the Redis hash field, in Unix milliseconds.

```text
aql> SET RECORD_PRINT_METADATA true
RECORD_PRINT_METADATA = true

aql> SELECT * FROM pov WHERE PK = 'M1TELnOQ9Q8:U=7MvkjfzfMV5:startApp';
+--------------------------------------+--------------+---------------+----------+-------+
| PK                                   | v            | e             | {ttl}    | {gen} |
+--------------------------------------+--------------+---------------+----------+-------+
| "M1TELnOQ9Q8:U=7MvkjfzfMV5:startApp" | "Stationary" | 1806514849141 | 13164267 | 1     |
+--------------------------------------+--------------+---------------+----------+-------+
1 row in set (0.000 secs)
```

`13164267` seconds is the time left on that record. A `{ttl}` of `-1` means the
record was stored as never-expire.

## Error 22 during the write

`BatchError:22` and `AEROSPIKE_ERR_FAIL_FORBIDDEN` (`as_command.c`) are the same
server result. For a TTL written by this tool, the namespace node that owned
the record had `nsup-period` 0. Fix every node as above and re-run. The single
record path logs the raw server tuple; the batch path logs
`BatchError:22:forbidden (TTL write rejected; namespace nsup-period is 0)`.
