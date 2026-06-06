# Defaults and their rationale

This is a living document. It records the **current** defaults, and the rationale
behind any **engine-specific** defaults. Update it in the same change whenever a
default is added or changed.

## Current defaults (as shipped)

### Export
| Option | Default | Notes |
|---|---|---|
| `--engine` | `duckdb` | Fast Arrow-native streaming. |
| `--rowgroup-size` | `1048576` | Matches SQL Server's max columnstore rowgroup. Also the dominant export-memory driver. |
| `--fetch-size` | `min(max(1024, rowgroup_size), 8192)` | Derived; only used by `pyodbc`/`mssql-python` engines. |
| `--worker-count` | `1` | |
| `--file-count` | `1` | |
| `--retries` | `3` | |

### Import
| Option | Default | Notes |
|---|---|---|
| `--engine` | `pyodbc` | Broad compatibility; parameterised `executemany`. |
| `--batch-size` | `1000`, or `1048576` for `mssql-python` | Engine-aware (see below). Rows per insert / commit unit in `batch` mode. |
| `--transaction-mode` | `batch` | Balanced safety/perf; per-batch retries. |
| `--worker-count` | `1` | |
| `--cci` | enabled | CCI on per-worker staging tables (multi-worker only). |
| `--retries` | `3` | |

Most defaults are engine-independent; `--batch-size` is **engine-aware** (it
differs for `mssql-python`). An explicit value always overrides the default.

## Why engine-specific defaults are being considered

A single global default cannot be right for every engine, because the engines
have materially different mechanics (see [engines](engines.md)). The clearest
example:

- The `mssql-python` import engine uses `bulkcopy`. In `batch` transaction mode,
  **each `--batch-size` chunk closes a SQL Server columnstore rowgroup**.
- With the global default `--batch-size 1000`, importing into a columnstore index
  via `mssql-python` produces columnstore rowgroups of only 1000 rows — far below
  the ideal (up to 1,048,576), crippling compression and scan performance.
- The same `--batch-size 1000` is perfectly reasonable for `pyodbc`/`duckdb`,
  whose `executemany` inserts trickle through the delta store and are compacted by
  the tuple mover regardless.

So the *correct* default for `--batch-size` depends on the chosen engine. Rather
than forcing every user to know this, PyButt can apply an engine-aware default
when the user has not explicitly set the option.

## Mechanism (engine-aware defaults)

> Status: implemented. See `pybutt/core/config.py`.

- Options that benefit from engine-specific defaults use a sentinel (`None`)
  default, meaning "user did not specify" — the same pattern `--fetch-size`
  already uses.
- A central `ENGINE_DEFAULTS` registry maps `tunable -> {engine: value}`, holding
  only the values that *diverge* from the generic fallback.
- `resolve_engine_default(tunable, engine, value, fallback)` applies the rule:
  explicit `value` wins, else an engine-specific override, else the `fallback`
  (generic default supplied by the caller).
- Resolution happens in the `Importer`/`Exporter` constructors so the **Python
  API** gets the same behaviour as the CLI.
- Engines with no specific override fall back to the generic default.

The export side is wired through the same resolver (via `--fetch-size`) with no
overrides registered yet, so adding an export engine default later is a one-line
registry entry.

## Engine-specific default values

### Import `--batch-size`
| Engine | Default | Rationale |
|---|---|---|
| `pyodbc` | `1000` | `executemany` trickle insert; small batches are fine and keep locks short. |
| `duckdb` | `1000` | Same insert path as pyodbc. |
| `mssql-python` | `1048576` | Each `bulkcopy` batch closes a columnstore rowgroup, so the default produces one full, compressed rowgroup (SQL Server's max). |

### Export defaults
No engine-specific export defaults are registered yet. The framework is wired
through `--fetch-size`, so when a divergent value is identified it can be added
as a single `ENGINE_DEFAULTS["fetch_size"]` entry. Note `--fetch-size` is already
ignored by the `duckdb` engine, so any override would target `pyodbc`/
`mssql-python`.

## Change log

- _(unreleased)_ Engine-aware default mechanism implemented. Import
  `--batch-size` now defaults to `1048576` for the `mssql-python` engine
  (unchanged `1000` elsewhere). Export framework wired via `--fetch-size` with
  no overrides registered.
- _(unreleased)_ Documentation introduced (the `docs/` folder).
