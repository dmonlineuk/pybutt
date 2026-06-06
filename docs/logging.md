# Logging and output

PyButt streams large tables across many concurrent workers — a thread per file
on import, a (spawned) process per partition on export. When something goes
wrong mid-run you need to know *which* unit of work failed and *why*, without
the lines from other workers drowning it out. This document explains the log
format, the structured identifiers, and how to interpret the output.

## Log format

Every line PyButt emits has the same shape:

```
<timestamp> <LEVEL> [<process>/<thread>] <logger>: <message>
```

For example:

```
2025-01-01 12:00:00 INFO [MainProcess/import_0] pybutt.importer: Importing file=dbo_Posts_part_00000.parquet table=dbo.Posts engine=mssql-python batch_size=1048576 transaction_mode=batch
```

- **timestamp** — lets you order lines from concurrent workers and measure how
  long a unit took.
- **`[process/thread]`** — the worker identity. Import worker threads are named
  `import_0`, `import_1`, …; export workers are separate processes (e.g.
  `SpawnPoolWorker-1`). This is what tells two otherwise-identical lines apart.
- **logger** — which subsystem emitted the line: `pybutt.importer`,
  `pybutt.exporter`, `pybutt.merger`, or `pybutt.base`.
- **message** — a short verb followed by structured `key=value` context (see
  below).

All PyButt output goes through the `pybutt` logger (not the root logger), so it
does not fight a host application's own logging configuration.

## Structured identifiers

Messages carry `key=value` context so concurrent units are distinguishable. The
keys you will see:

| Key | Meaning |
| --- | --- |
| `file` | The Parquet file (import) or output file (export) being processed. |
| `table` | The fully-qualified target/source table. |
| `engine` | `duckdb`, `pyodbc`, or `mssql-python`. |
| `rg` | Row group, as `current/total` (e.g. `3/40`). |
| `batch` | Zero-based batch index within the current row group. |
| `offset` | Rows already processed in this file when the unit started. |
| `rows` | Number of rows in the failing batch. |
| `partition` | Export partition number. |
| `rows_approx`, `size_mb`, `seconds`, `progress` | Per-file completion stats. |
| `rss` | Current process resident set size (memory in use), e.g. `1.8GB`. |
| `peak` | Highest RSS observed so far in this process. |

Before this, a retrying import printed lines like
`WARNING:root:batch in X.parquet retry 1/3 failed in X.parquet: Timeout` — with
no timestamp, worker id, or batch number, so N concurrent batches each failing
their first retry looked like duplicated output. The identifiers above remove
that ambiguity.

## Enabling verbose (DEBUG) output

INFO is the default and covers the useful per-file lifecycle (start, completion,
retries, errors). Pass `--verbose` / `-v` on any command for DEBUG output, which
adds per-row-group progress and the SQL queries used per partition:

```
pybutt import ... --verbose
pybutt export ... -v
```

From the Python API, call `configure_logging(verbose=True)` once at startup:

```python
from pybutt.core.logobs import configure_logging

configure_logging(verbose=True)
```

Library users who do not call `configure_logging` get standard `logging`
behaviour (PyButt adds no handler of its own until asked), so PyButt will not
emit formatted output unless you opt in.

## Reading INFO output (normal run)

```
2025-01-01 12:00:00 INFO [MainProcess/import_0] pybutt.importer: Importing file=dbo_Posts_part_00000.parquet table=dbo.Posts engine=mssql-python batch_size=1048576 transaction_mode=batch
2025-01-01 12:27:37 INFO [MainProcess/import_0] pybutt.importer: Completed file=dbo_Posts_part_00000.parquet rows=9968353 seconds=1657.05
```

One `Importing` line per file when a worker picks it up, one `Completed` line
with the row count and wall-clock seconds when it finishes.

## Reading DEBUG output

```
2025-01-01 12:00:01 DEBUG [MainProcess/import_0] pybutt.importer: Processed row group file=dbo_Posts_part_00000.parquet rg=1/40
2025-01-01 12:00:02 DEBUG [SpawnPoolWorker-1] pybutt.exporter: Partition query partition=0: SELECT ...
```

Per-row-group progress (import) and per-partition SQL (export) only appear at
DEBUG so INFO stays readable on large runs.

## Memory observability

Every boundary log line carries `rss` (current resident memory) and `peak` (the
highest RSS seen so far in that process). RSS is read cross-platform via
`psutil`, so the same fields appear on Windows, Linux, BSD and macOS. Peak is
tracked per process — import runs in one process (worker threads share it), while
each spawned export worker tracks and reports its own peak.

```
2025-01-01 12:00:00 INFO [MainProcess/import_0] pybutt.importer: Importing file=dbo_Posts_part_00000.parquet table=dbo.Posts engine=mssql-python batch_size=1048576 transaction_mode=batch rss=180.4MB peak=180.4MB
2025-01-01 12:27:37 INFO [MainProcess/import_0] pybutt.importer: Completed file=dbo_Posts_part_00000.parquet rows=9968353 seconds=1657.05 rss=2.1GB peak=2.1GB
```

The value of this is diagnosing the *silent* worker death described in
[concepts.md](concepts.md): when the Linux OOM-killer SIGKILLs a worker there is
no Python traceback, so the **last log line before the process vanishes** is the
evidence. With `rss`/`peak` on every boundary you can see memory climbing and
read off exactly which file/row-group/partition was in flight when it died.

### Memory heartbeat (`--mem-heartbeat`)

Boundary lines only print at file/row-group/partition transitions. A single very
large unit can run for many minutes between them, so an OOM-kill mid-unit leaves
a stale last line. The optional heartbeat logs RSS on a fixed interval regardless
of progress:

```bash
# Log memory every 30 seconds during the run (0 = off, the default)
pybutt import ... --mem-heartbeat 30
pybutt export ... --mem-heartbeat 30
```

```
2025-01-01 12:05:00 INFO [MainProcess/mem-heartbeat] pybutt.mem: Memory heartbeat unit=import rss=1.4GB peak=1.4GB
2025-01-01 12:05:30 INFO [MainProcess/mem-heartbeat] pybutt.mem: Memory heartbeat unit=import rss=1.9GB peak=1.9GB
2025-01-01 12:06:00 INFO [MainProcess/mem-heartbeat] pybutt.mem: Memory heartbeat unit=import rss=2.4GB peak=2.4GB
```

It is a low-overhead daemon thread, off by default so it adds no noise unless
you are hunting a leak or an OOM-kill. On export the heartbeat runs inside each
worker process (`unit=partition=N`), where the memory actually lives.

## Interpreting failures

PyButt distinguishes transient, fatal, and abnormal failures.

### Transient errors (retried)

```
2025-01-01 12:10:00 WARNING [MainProcess/import_0] pybutt.importer: bulkcopy(batch) attempt 1/3 failed file=dbo_Posts_part_00000.parquet rg=3/40 batch=7 rows=1048576 offset=7340032: Timeout Error: Timeout expired
```

A `WARNING` with `attempt N/M` is a retry. The operation is retried with
exponential backoff. If the run still completes, these were transient (e.g. a
busy server) and can be ignored. The `file`/`rg`/`batch`/`offset` identifiers
tell you exactly which unit retried.

### Out of memory (fatal, not retried)

```
2025-01-01 12:10:00 ERROR [MainProcess/import_0] pybutt.importer: Out of memory during bulkcopy(batch) - not retrying (fatal) file=dbo_Posts_part_00001.parquet rg=5/40 batch=12 offset=12582912
```

A `MemoryError` is **not** retried — retrying would re-allocate and make memory
pressure worse. This line is your signal that the worker is out of memory; see
[concepts.md](concepts.md) for the memory model and
[tuning.md](tuning.md)/[README "Memory Issues"](../README.md) for how to reduce
peak memory (lower `--worker-count`, re-export with a smaller `--rowgroup-size`).

On Windows/BSD, memory exhaustion typically surfaces as this `MemoryError`. On
Linux it may instead be the OS OOM-killer terminating the worker outright (see
below) — in that case there is no error line at all, so use the `rss`/`peak`
trend on the preceding boundary lines (or a `--mem-heartbeat`) to confirm memory
was the cause. See [Memory observability](#memory-observability) above.

### Worker failed (which unit died)

```
2025-01-01 12:10:00 ERROR [MainProcess/import_0] pybutt.importer: Worker failed file=dbo_Posts_part_00002.parquet: Timeout Error: Timeout expired
```

When an import worker raises, this line names the `file` (or `table`) it was
handling before the exception propagates, so a failure is never silent or
anonymous.

### Abnormal worker death on export

```
2025-01-01 12:10:00 ERROR [MainProcess/MainThread] pybutt.exporter: Export pool failed - a worker may have terminated abnormally (possible out-of-memory/SIGKILL); check earlier per-partition logs: ...
```

The export pool uses separate processes. If a worker is killed outright (for
example by the Linux OOM-killer, which leaves no Python traceback), the pool
fails with an opaque error and no partition context. PyButt catches this and
emits the line above; check the most recent per-partition `Exporting`/`Completed`
lines (by timestamp) to see which partition was in flight when the worker died.

## Notes

- Timestamps are local time, `YYYY-MM-DD HH:MM:SS`.
- Output goes to stderr.
- The format is identical on Windows, Linux, and BSD; export workers configure
  their own logging on startup (via the pool initialiser) so spawned processes
  format their lines the same way as the parent.
