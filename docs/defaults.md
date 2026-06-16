# Defaults and their rationale

This is a living document. It records the **current** defaults shipped with
PyButt v2. Update it in the same change whenever a default is added or changed.

## Current defaults (as shipped)

### Export
| Option | Default | Notes |
|---|---|---|
| `--engine` | `pyodbc` | Broad compatibility. |
| `--rowgroup-size` | `1048576` | Matches SQL Server's max columnstore rowgroup. Also the dominant export-memory driver. |
| `--fetch-size` | `1000` | Cursor fetch size for pyodbc/mssql-python engines. |
| `--worker-count` | `1` | |
| `--file-count` | `1` | |
| `--retries` | `3` | |
| `--packet-size` | `4096` | TDS packet size in bytes (512–32767). |

### Import
| Option | Default | Notes |
|---|---|---|
| `--engine` | `mssql-python` | Native bulk insert (`bulkcopy`) for faster imports. |
| `--batch-size` | `1000` | Rows per insert / commit unit. |
| `--transaction-mode` | `rowgroup` | Row group boundary safety with independent retries. |
| `--worker-count` | `1` | |
| `--cci` | enabled | CCI on per-worker staging tables (multi-worker only). |
| `--retries` | `3` | |
| `--packet-size` | `4096` | Shared with export; see above. |

### Combine
| Option | Default | Notes |
|---|---|---|
| `--rowgroup-size` | `1048576` | For file combines. |
| `--retries` | `3` | |
| `--packet-size` | `4096` | For table combines. |

### Memory tuning (shared across export and import)
| Option | Default | Notes |
|---|---|---|
| `--mem-heartbeat` | `30.0` | Log RSS + system memory every N seconds. 0 to disable. |
| `--mem-threshold` | `85.0` | System memory % at which workers are throttled. 0 to disable. |
| `--mem-sleep` | `5.0` | Seconds to sleep per throttle check. |
| `--mem-max-wait` | `300.0` | Max seconds to wait during throttling. |
| `--mem-cooldown` | `30.0` | Seconds after a throttle event before re-checking. |

### Connection / security (shared across all commands)
| Option | Default | Notes |
|---|---|---|
| `--driver` | `ODBC Driver 18 for SQL Server` | |
| `--schema` | `dbo` | |
| `--trusted-connection` | `False` | |
| `--trust-cert` | `False` | |
| `--encrypt` | `True` | |

All defaults are defined as named constants in `pybutt/core/config.py`.

## Change log

- _(v2.0.0)_ Export `--engine` default changed from `duckdb` to `pyodbc`.
- _(v2.0.0)_ Import `--engine` default changed from `pyodbc` to `mssql-python`.
- _(v2.0.0)_ Import `--transaction-mode` default changed from `batch` to
  `rowgroup`.
- _(v2.0.0)_ `--packet-size` default changed from `16383` to `4096`.
- _(v2.0.0)_ `TransactionMode.ROW` removed; only `batch`, `rowgroup`, and
  `file` remain.
- _(v2.0.0)_ Engine-aware default mechanism (`ENGINE_DEFAULTS`,
  `resolve_engine_default`) removed. All defaults are now flat constants.
- _(v2.0.0)_ `--rowgroups-per-file` removed from export.
- _(v2.0.0)_ `--delete-files` removed from import and combine.
- _(v2.0.0)_ Import and combine now take `manifest_path` as a positional
  argument instead of `--input-path` + `--manifest-filename`.
- _(v2.0.0)_ Memory tuning options added (`--mem-heartbeat`, `--mem-threshold`,
  `--mem-sleep`, `--mem-max-wait`, `--mem-cooldown`).
- _(v2.0.0)_ `--verbose` short flag changed from `-v` to `-V`; `--version`
  uses `-v`.
- _(v2.0.0)_ Help flag `-?` added alongside `--help`.
- _(v2.0.0)_ `merge` command renamed to `combine`; `merger.py` → `combiner.py`,
  `TableMerger` → `TableCombine`.
- _(v2.0.0)_ `rewrite` command and related code removed.
- _(unreleased)_ Documentation introduced (the `docs/` folder).
