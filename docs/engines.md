# Engine behaviour

PyButt offers three engines for both export and import: `duckdb`, `pyodbc`, and
`mssql-python`. They are **not** interchangeable in behaviour — they differ in how
they stream data, how they use memory, and how they interact with `--batch-size`
and `--transaction-mode`. This document is the reference for those differences.

Defaults: export defaults to `duckdb`; import defaults to `pyodbc`.

## Export engines

| Engine | Reads from SQL via | Uses `--fetch-size`? | Buffers to `--rowgroup-size`? | Notes |
|---|---|---|---|---|
| `duckdb` (default) | DuckDB `odbc_query` Arrow stream | **No** | Yes | DuckDB manages its own streaming; `--fetch-size` has no effect. |
| `pyodbc` | `cursor.fetchmany(fetch_size)` | Yes | Yes | Broadest driver compatibility. |
| `mssql-python` | `cursor.fetchmany(fetch_size)` | Yes | Yes | Same streaming shape as pyodbc for export. |

All three accumulate a full rowgroup in memory before writing it (see
[concepts](concepts.md)).

## Import engines

| Engine | Reads Parquet via | Inserts via | Peak memory | `fast_executemany` |
|---|---|---|---|---|
| `pyodbc` (default) | pyarrow, one rowgroup at a time | `executemany` | one Parquet rowgroup | yes |
| `duckdb` | DuckDB, **whole file** (per-rowgroup only in `rowgroup` mode) | `executemany` over the pyodbc connection | whole file | yes |
| `mssql-python` | pyarrow, one rowgroup at a time | **`bulkcopy`** | one Parquet rowgroup | n/a |

Notes:
- The `duckdb` **import** engine only uses DuckDB to *read* the Parquet into an
  Arrow table; the actual INSERT still goes through the pyodbc connection. Its
  distinguishing trait is that it loads the entire file into memory first (except
  in `rowgroup` transaction mode).
- `mssql-python` uses native bulk load (`bulkcopy`), which is generally much
  faster than parameterised inserts and behaves differently with columnstore
  targets (see below).

## `--batch-size` × `--transaction-mode`, per engine

| Mode | `pyodbc` / `duckdb` | `mssql-python` |
|---|---|---|
| `row` | autocommit; `executemany` per `--batch-size` slice | autocommit; `bulkcopy` per `--batch-size` slice |
| `batch` | `executemany` + commit per `--batch-size` slice | `bulkcopy` + commit per `--batch-size` slice — **each slice closes a columnstore rowgroup** |
| `rowgroup` | `executemany` per `--batch-size` slice, **one commit per Parquet rowgroup** | **single `bulkcopy` of the whole Parquet rowgroup** (`--batch-size` ignored) |
| `file` | `executemany` per `--batch-size` slice, one commit per file | `bulkcopy` per `--batch-size` slice, one commit per file |

## Columnstore implications

If the import target is a columnstore index, the engine choice changes how
columnstore rowgroups form:

- **`pyodbc` / `duckdb`:** `executemany` inserts trickle through the delta store;
  SQL Server's tuple mover compresses them into rowgroups later. Transaction mode
  sets commit boundaries, not rowgroup sizes.
- **`mssql-python`:** `bulkcopy` of ≥102,400 rows lands directly as a compressed
  rowgroup, and every call closes a rowgroup. So:
  - `batch` mode + small `--batch-size` -> many tiny rowgroups (bad).
  - `batch` mode + large `--batch-size` -> large rowgroups.
  - `rowgroup` mode -> one rowgroup per Parquet rowgroup.

This is the central reason engine-specific defaults are worthwhile; see
[defaults](defaults.md).

## Choosing an engine

- **Export:** `duckdb` (default) is a good general choice. Use `pyodbc` for
  maximum driver compatibility, or `mssql-python` to match your import engine.
- **Import to a heap or rowstore index:** `pyodbc` (default) with `batch` mode.
- **Import to a columnstore index, large volumes:** `mssql-python` with either
  `rowgroup` mode or a large `--batch-size`.
- **Avoid `duckdb` import for very large single files** unless you have memory
  headroom (it loads the whole file).
