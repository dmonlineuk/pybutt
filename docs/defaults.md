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
| `--batch-size` | `1000` | Rows per insert / commit unit in `batch` mode. |
| `--transaction-mode` | `batch` | Balanced safety/perf; per-batch retries. |
| `--worker-count` | `1` | |
| `--cci` | enabled | CCI on per-worker staging tables (multi-worker only). |
| `--retries` | `3` | |

These defaults are **engine-independent today**: the same value is used no matter
which `--engine` is selected.

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

## Proposed mechanism (engine-aware defaults)

> Status: design agreed in principle; concrete values still being finalised.

- Options that benefit from engine-specific defaults use a sentinel (`None`)
  default, meaning "user did not specify" — the same pattern `--fetch-size`
  already uses.
- A central registry maps `(option, engine) -> default value`.
- Resolution happens in the `Importer`/`Exporter` so the **Python API** gets the
  same behaviour as the CLI.
- An explicitly provided value always wins over any engine default.
- Engines with no specific override fall back to the existing global default.

## Engine-specific default values

> The values below are the home for finalised decisions. Entries marked **TBD**
> are not yet agreed and are not implemented.

### Import `--batch-size`
| Engine | Proposed default | Rationale |
|---|---|---|
| `pyodbc` | `1000` (unchanged) | `executemany` trickle insert; small batches are fine and keep locks short. |
| `duckdb` | `1000` (unchanged) | Same insert path as pyodbc. |
| `mssql-python` | **TBD** (candidate: `1048576`) | Each `bulkcopy` batch closes a columnstore rowgroup; the default should produce a full, compressed rowgroup. Candidate is SQL Server's max rowgroup size; an alternative (~900,000) leaves headroom under the max. Final value pending sign-off. |

### Export defaults
No engine-specific export defaults are agreed yet. **TBD** whether any are
warranted (e.g. `--fetch-size` is already ignored by the `duckdb` engine, so no
override is needed there).

## Change log

- _(unreleased)_ Documentation introduced; engine-aware default mechanism
  proposed. No default values changed yet.
