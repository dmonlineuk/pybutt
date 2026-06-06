# Concepts: the data pipeline and where memory goes

This document builds the mental model you need before tuning anything. The single
most common source of confusion is *which* knob controls memory, so that is the
focus.

## The two pipelines

PyButt moves data in two directions, and the pipelines are not symmetric.

### Export: SQL Server -> Parquet

```
SQL Server  --fetch-size-->  cursor buffer  -->  rowgroup buffer  --rowgroup-size-->  Parquet file
            (rows per                          (accumulates a full
             round-trip)                        rowgroup in RAM)
```

1. A worker runs its partition query against SQL Server.
2. Rows are pulled from the server cursor in chunks of `--fetch-size`
   (`pyodbc`/`mssql-python` only — see [engines](engines.md)).
3. Rows accumulate in an in-memory buffer until they reach `--rowgroup-size`.
4. A full rowgroup is flushed to the Parquet file, and the buffer is released.

### Import: Parquet -> SQL Server

```
Parquet file  -->  rowgroup in RAM  --batch-size-->  INSERT / bulkcopy  -->  SQL Server
              (one Parquet rowgroup                  (rows per insert /
               read at a time*)                       commit unit)
```

1. A worker reads a Parquet file.
2. For `pyodbc`/`mssql-python`, it reads **one Parquet rowgroup at a time** into
   memory. For `duckdb` it reads the **whole file** into memory (except in
   `rowgroup` transaction mode). (*see [engines](engines.md))
3. The in-memory rows are sliced into `--batch-size` chunks and sent to SQL
   Server via `executemany` or `bulkcopy`.
4. Commits happen according to `--transaction-mode`.

## Where memory is actually consumed

This is the key correction to the most common assumption.

### Export memory is driven by `--rowgroup-size`, not `--fetch-size`

The Parquet writer buffers a **whole rowgroup** in RAM before flushing it. So:

- Peak RAM per worker ≈ one rowgroup's worth of rows (× column width).
- `--fetch-size` only changes how many rows are pulled per database round-trip.
  It trims network chattiness; it does **not** cap the writer's buffer.
- You therefore **cannot** have a large rowgroup *and* low memory on export. A
  1,048,576-row rowgroup means ~1M rows held in memory per worker, regardless of
  how small `--fetch-size` is.

To reduce export memory, lower `--rowgroup-size` (and/or `--worker-count`), not
`--fetch-size`.

### Import memory is driven by the Parquet rowgroup (or whole file)

By the time `--batch-size` is applied, a full unit is already resident in memory:

- `pyodbc` / `mssql-python`: one **Parquet rowgroup** is loaded, then sliced into
  `--batch-size` chunks. Peak RAM ≈ one Parquet rowgroup.
- `duckdb`: the **entire file** is loaded into an Arrow table first (except in
  `rowgroup` transaction mode). Peak RAM ≈ whole file.

So `--batch-size 10000` against a 1,048,576-row Parquet rowgroup does **not** keep
memory at ~10k rows — the rowgroup is already in memory. `--batch-size` controls
insert granularity and (in `batch` transaction mode) commit size; it is not your
primary memory lever on import.

To reduce import memory, export with a smaller `--rowgroup-size` (so each rowgroup
read is smaller), prefer `pyodbc`/`mssql-python` over `duckdb` for very large
files, and/or lower `--worker-count`.

### Workers multiply everything

`--worker-count` runs that many partitions/files concurrently, so multiply the
per-worker memory above by the worker count to estimate peak process memory.

## Three different "rowgroups"

The word "rowgroup" appears in three places. Keep them distinct:

1. **Parquet rowgroup** (`--rowgroup-size`): how rows are physically grouped
   inside a Parquet file. Set on export; read back on import.
2. **SQL Server columnstore rowgroup**: how SQL Server groups rows inside a
   clustered/columnstore index. Ideal size is up to 1,048,576 rows. How this is
   formed on import depends on the engine and transaction mode (see below).
3. **`--transaction-mode rowgroup`**: a *commit boundary* on import (commit once
   per Parquet rowgroup). It is about transaction scope, not physical storage.

## How columnstore rowgroups get formed on import

This matters when the target is a columnstore index and you want large, compressed
rowgroups rather than many tiny ones.

- **`executemany` (pyodbc, duckdb engines):** rows arrive as parameterised
  `INSERT ... VALUES`. They generally trickle through the **delta store** and are
  compressed into columnstore rowgroups later by SQL Server's tuple mover. The
  transaction mode changes the *commit* boundary, not how big the resulting
  columnstore rowgroups are.
- **`bulkcopy` (mssql-python engine):** each `bulkcopy` call is a bulk load. A
  bulk load of ≥102,400 rows goes **directly to a compressed columnstore
  rowgroup**, and each call closes a rowgroup. Therefore:
  - In `batch` mode, **each `--batch-size` chunk closes a columnstore rowgroup** —
    a small `--batch-size` produces many tiny rowgroups.
  - In `rowgroup` mode, the **whole Parquet rowgroup is one `bulkcopy`**, so the
    columnstore rowgroup mirrors the Parquet rowgroup.

The practical consequence: to get large columnstore rowgroups with the
`mssql-python` engine, either use `--transaction-mode rowgroup` with a large
Parquet rowgroup, or use a large `--batch-size`. See [defaults](defaults.md) for
the rationale behind engine-specific defaults that address this.

## Summary cheat-sheet

| You want to... | Change | Not |
|---|---|---|
| Lower export memory | `--rowgroup-size`, `--worker-count` | `--fetch-size` |
| Lower import memory | smaller Parquet rowgroup (set at export), `--worker-count`, avoid `duckdb` on huge files | `--batch-size` |
| Fewer DB round-trips on export | raise `--fetch-size` | — |
| Fewer inserts / larger commits on import | raise `--batch-size` | — |
| Larger columnstore rowgroups (mssql-python) | `rowgroup` mode or large `--batch-size` | small `--batch-size` |
