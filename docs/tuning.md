# Tuning reference

Every tuning knob, what it controls, what it does **not** control, and how the
knobs interact. Read [concepts](concepts.md) first for the underlying model.

## Quick map

| Knob | Direction | Controls | Primary effect |
|---|---|---|---|
| `--fetch-size` | export | rows per DB cursor round-trip | network round-trips |
| `--rowgroup-size` | export | rows per Parquet rowgroup | file structure **and export memory** |
| `--batch-size` | import | rows per insert / `bulkcopy` call | insert granularity, commit size (batch mode), columnstore rowgroups (mssql-python) |
| `--transaction-mode` | import | commit boundary & retry scope | durability vs lock duration |
| `--worker-count` | both | parallel partitions (export) / files (import) | throughput, multiplies memory |
| `--file-count` | export | number of output Parquet files | parallelism granularity |
| `--retries` | both | transient-error retry attempts | resilience |

## Export knobs

### `--fetch-size` (`-F`)
- **What it is:** the number of rows pulled per `cursor.fetchmany()` call from SQL
  Server.
- **Default:** `min(max(1024, rowgroup_size), 8192)` — i.e. between 1024 and 8192,
  derived from the rowgroup size.
- **Used by:** the `pyodbc` and `mssql-python` export engines only. The `duckdb`
  export engine streams Arrow record batches itself and **ignores** this option.
- **What it does NOT do:** it does not bound export memory (the rowgroup buffer
  does). Raising it reduces round-trips at the cost of a larger transient read
  buffer; it does not change the Parquet file.

### `--rowgroup-size` (`-R`)
- **What it is:** the number of rows per rowgroup written into each Parquet file.
- **Default:** `1048576` (1,048,576), matching SQL Server's maximum columnstore
  rowgroup size.
- **Drives export memory:** the writer accumulates a full rowgroup in RAM before
  flushing. Peak per-worker memory scales with this value.
- **Downstream effect:** on import, `pyodbc`/`mssql-python` read one Parquet
  rowgroup at a time, so this value also becomes the import read-unit (and, for
  `mssql-python` in `rowgroup` mode, the resulting columnstore rowgroup size).

## Import knobs

### `--batch-size` (`-b`)
- **What it is:** the number of rows per `executemany` (pyodbc/duckdb) or
  `bulkcopy` (mssql-python) call.
- **Default:** `1000` for `pyodbc`/`duckdb`; `1048576` for `mssql-python`
  (engine-aware — see [defaults](defaults.md)). An explicit value always wins.
- **Also the commit unit** in `--transaction-mode batch`.
- **For mssql-python it sizes columnstore rowgroups** in `batch` mode: each batch
  closes a rowgroup, so small values create many tiny rowgroups — which is why
  its default is a full rowgroup. See [defaults](defaults.md).
- **What it does NOT do:** it does not bound import memory — a full Parquet
  rowgroup (or the whole file, for duckdb) is already in memory before batching.

### `--transaction-mode` (`-M`)
Controls the commit boundary and the retry scope. Values:

| Mode | Commit boundary | Retry scope | Notes |
|---|---|---|---|
| `row` | autocommit per insert | none (autocommit) | not recommended; partial loads on error |
| `batch` (default) | per `--batch-size` chunk | per batch | recommended general default |
| `rowgroup` | per Parquet rowgroup | per rowgroup | larger locks than batch; see engine note |
| `file` | per file | per file | all-or-nothing per file |

Engine interaction with `rowgroup` mode:
- `pyodbc`/`duckdb`: still issue `executemany` in `--batch-size` chunks within the
  rowgroup, but commit once per rowgroup.
- `mssql-python`: sends the **entire Parquet rowgroup as a single `bulkcopy`** —
  `--batch-size` is not used in this mode.

### `--cci` / `--no-cci`
Creates a clustered columnstore index on the per-worker temporary staging tables
used during multi-worker import. See the README "Columnstore on temporary tables"
section. Single-worker imports are unaffected.

## Shared knobs

### `--worker-count` (`-w`)
- **Export:** number of worker **processes**, each handling a partition.
- **Import:** number of parallel **threads**, each handling a file.
- **Memory:** multiplies per-worker memory (see [concepts](concepts.md)). A good
  starting point is your CPU core count, backed off if memory-bound.

### `--file-count` (`-f`) (export)
Number of Parquet output files to split the table into. More files = finer
parallelism granularity and smaller individual files.

### `--retries` (`-r`)
Number of attempts for transient SQL errors, applied at the transaction-mode
scope, with exponential backoff (`2**attempt` seconds between attempts).

## `--fetch-size` vs `--batch-size`: are they the same?

They are *analogous* (both are "rows per round-trip") but they are **not** the
same option on opposite ends:

| | `--fetch-size` (export) | `--batch-size` (import) |
|---|---|---|
| Side | read from SQL | write to SQL |
| Sizes | `cursor.fetchmany` | `executemany` / `bulkcopy` |
| Transaction role | none | **commit unit in `batch` mode** |
| Engine caveats | ignored by `duckdb` | sizes columnstore rowgroups for `mssql-python` |

Because `--batch-size` also doubles as a commit unit and (for mssql-python) a
columnstore-rowgroup sizer, it is not a pure mirror of `--fetch-size`. There is
also no read-side knob on import (read unit = Parquet rowgroup) and no write-side
batch knob on export (write unit = `--rowgroup-size`).

## Worked starting points

Large table export, memory-bounded box:
```bash
pybutt export ... --rowgroup-size 262144 --worker-count 4
```

High-throughput import to a heap/btree via pyodbc:
```bash
pybutt import ... --engine pyodbc --transaction-mode batch --batch-size 10000 --worker-count 4
```

Import to a columnstore target via mssql-python (large rowgroups):
```bash
pybutt import ... --engine mssql-python --transaction-mode rowgroup --worker-count 4
# or, in batch mode, a large batch size:
pybutt import ... --engine mssql-python --transaction-mode batch --batch-size 1048576
```
