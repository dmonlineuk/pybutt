# PyButt v2 Refactoring Plan

> **Status: Completed.** All phases have been implemented. During
> implementation the following naming changes were made relative to the
> original plan below:
>
> - `merge` terminology → `combine` throughout (command, functions, classes)
> - `merger.py` → `combiner.py`, `TableMerger` → `TableCombine`
> - `merge_parquet_files()` → `combine_parquet_files()`
> - `files/merge.py` → `files/combine.py`
> - `default_temp_manifest_filename()` → `default_import_manifest_filename()`
> - CLI command files use `_command` suffix (e.g. `export_command.py`) instead
>   of the plan's `export.py` / `import_cmd.py`
> - Deprecated `rewrite` code has been fully removed
> - `SqlServerIOBase` and default constants excluded from public `__all__`

## Approach

After reviewing the full codebase and dependency graph, the recommended approach
is **targeted splits** rather than a full package rename. The existing package
layout (`core/`, `io/`, `files/`) is conventional and functional. Renaming every
package would touch 50+ import paths across source and tests for purely cosmetic
gain, and introduce awkward names like `import_/` and `inspect_/` to avoid
Python keyword clashes.

Instead, focus on the two areas that have genuinely outgrown their current
structure:

1. **Split `cli/cli.py`** — one file per command
2. **Split `files/files.py`** — separate manifest utilities, inspect logic, and
   merge logic into their own modules
3. **Add `__init__.py` public APIs** with `__all__` to all existing packages
4. **Clean up deprecated code** (rewrite command, delete_files plumbing)

---

## Current Structure

```
pybutt/
├── __init__.py                 (empty)
├── exceptions.py               (all exception classes)
├── cli/
│   ├── __init__.py             (empty)
│   └── cli.py                  (all commands in one 920-line file)
├── core/
│   ├── __init__.py             (empty)
│   ├── base.py                 (SqlServerIOBase, rows_from_arrow)
│   ├── config.py               (SqlConfig, TransactionMode, defaults, validators)
│   └── logobs.py               (logging, memory monitoring, MemoryGate)
├── files/
│   ├── __init__.py             (empty)
│   └── files.py                (manifest I/O, inspect, rewrite, merge_parquet_files)
└── io/
    ├── __init__.py             (empty)
    ├── exporter.py             (Exporter class)
    ├── importer.py             (Importer class)
    └── merger.py               (TableMerger class)
```

## Target Structure

```
pybutt/
├── __init__.py                 (top-level public API re-exports)
├── exceptions.py               (unchanged)
├── core/
│   ├── __init__.py             (public API: SqlServerIOBase, SqlConfig, etc.)
│   ├── base.py                 (unchanged)
│   ├── config.py               (unchanged)
│   └── logobs.py               (unchanged)
├── cli/
│   ├── __init__.py             (public API: app)
│   ├── app.py                  (Typer app, version callback, shared helpers)
│   ├── export.py               (export command)
│   ├── import_cmd.py           (import command)
│   ├── combine.py              (combine command)
│   ├── inspect_cmd.py          (inspect command)
│   └── purge.py                (purge command)
├── files/
│   ├── __init__.py             (public API: manifest + inspect + merge functions)
│   ├── manifest.py             (manifest I/O, validation, defaults)
│   ├── inspect.py              (inspect_parquet_file, inspect_manifest)
│   └── merge.py                (merge_parquet_files, _write_table_chunks)
└── io/
    ├── __init__.py             (public API: Exporter, Importer, TableMerger)
    ├── exporter.py             (unchanged)
    ├── importer.py             (unchanged)
    └── merger.py               (unchanged)
```

Changes are **only** inside `cli/` and `files/`, plus adding `__init__.py`
exports everywhere. No package renames, no import churn in `io/` or `core/`.

---

## Phase 1: Split `cli/cli.py`

### `cli/app.py` — shared Typer infrastructure

Extract from the current `cli.py`:

- `app = typer.Typer(...)` instance
- `_get_project_version()`
- `_version_callback()`
- `@app.callback()` with the `--version` flag
- `parse_columns()` helper
- `build_sql_config()` helper
- `configure_logging` import and `logger` instance

### `cli/export.py` — export command

```python
from pybutt.cli.app import app, build_sql_config, parse_columns
# ... other imports from core/config, core/logobs, io/exporter

@app.command("export", ...)
def export(...) -> None:
    ...
```

### `cli/import_cmd.py` — import command

Named `import_cmd.py` to avoid shadowing the `import` keyword.

```python
from pybutt.cli.app import app, build_sql_config

@app.command("import", ...)
def import_data(...) -> None:
    ...
```

### `cli/combine.py` — combine command

```python
from pybutt.cli.app import app, build_sql_config

@app.command("combine", ...)
def combine(...) -> None:
    ...
```

### `cli/inspect_cmd.py` — inspect command

Named `inspect_cmd.py` to avoid shadowing the `inspect` stdlib module.

```python
from pybutt.cli.app import app

@app.command("inspect", ...)
def inspect(...):
    ...
```

### `cli/purge.py` — purge command

```python
from pybutt.cli.app import app

@app.command("purge", ...)
def purge(...):
    ...
```

### `cli/__init__.py`

```python
from .app import app

# Import command modules so @app.command decorators register
from . import export      # noqa: F401
from . import import_cmd  # noqa: F401
from . import combine     # noqa: F401
from . import inspect_cmd # noqa: F401
from . import purge       # noqa: F401

__all__ = ["app"]
```

### Entry point update in `pyproject.toml`

```toml
[project.scripts]
pybutt = "pybutt.cli:app"
```

### Test impact

- `test_cli.py` currently imports `from pybutt.cli import cli` and patches
  `cli.Exporter`, `cli.Importer` etc. After the split, the monkeypatch targets
  change:
  - `cli.Exporter` → `pybutt.cli.export.Exporter`
  - `cli.Importer` → `pybutt.cli.import_cmd.Importer`
  - `cli.TableMerger` → `pybutt.cli.combine.TableMerger`
  - etc.
- `test_cli_help.py` — no changes needed (only invokes `cli.app`).

---

## Phase 2: Split `files/files.py`

### `files/manifest.py` — manifest I/O and validation

Extract:

| Function / Constant | Purpose |
|---|---|
| `MANIFEST_VERSION_1`, `MANIFEST_VERSION_2` | Version constants |
| `SUPPORTED_MANIFEST_TYPES` | Type validation set |
| `_parse_manifest_dict()` | Internal parser |
| `_validate_table_name()` | Internal validator |
| `default_manifest_filename()` | Default name generator |
| `default_temp_manifest_filename()` | Default temp name generator |
| `write_manifest()` | Write manifest JSON |
| `load_manifest()` | Load + parse manifest |
| `load_file_manifest()` | Load + validate file-type manifest |
| `validate_manifest_entries()` | Entry validation |

### `files/inspect.py` — parquet inspection

Extract:

| Function | Purpose |
|---|---|
| `inspect_parquet_file()` | Single file metadata |
| `inspect_manifest()` | Iterate manifest and inspect each file |

Imports `load_file_manifest` from `files.manifest`.

### `files/merge.py` — parquet file merging

Extract:

| Function | Purpose |
|---|---|
| `_write_table_chunks()` | Internal chunked writer |
| `merge_parquet_files()` | Merge all files in a manifest |

Imports `load_file_manifest`, `validate_manifest_entries`, `write_manifest` from
`files.manifest`.

### Deprecated rewrite code

`rewrite_single_file()`, `rewrite_parquet_files()`, and
`default_rewrite_manifest_filename()` should be deleted since the `rewrite`
command is deprecated. If there's a future need, it can be re-added. The CLI
still imports `rewrite_parquet_files` — that import should be removed when the
rewrite command is fully dropped.

### `files/__init__.py`

```python
from .manifest import (
    MANIFEST_VERSION_1,
    MANIFEST_VERSION_2,
    SUPPORTED_MANIFEST_TYPES,
    default_manifest_filename,
    default_temp_manifest_filename,
    load_file_manifest,
    load_manifest,
    validate_manifest_entries,
    write_manifest,
)
from .inspect import inspect_manifest, inspect_parquet_file
from .merge import merge_parquet_files

__all__ = [
    "MANIFEST_VERSION_1",
    "MANIFEST_VERSION_2",
    "SUPPORTED_MANIFEST_TYPES",
    "default_manifest_filename",
    "default_temp_manifest_filename",
    "load_file_manifest",
    "load_manifest",
    "validate_manifest_entries",
    "write_manifest",
    "inspect_manifest",
    "inspect_parquet_file",
    "merge_parquet_files",
]
```

### Import compatibility

Because everything is re-exported through `files/__init__.py`, existing imports
like `from pybutt.files.files import load_manifest` will break. Two options:

- **Option A (recommended)**: Update all imports to use the package path:
  `from pybutt.files import load_manifest`. This is cleaner and is the point of
  having `__init__.py` exports.
- **Option B**: Keep an empty `files/files.py` that re-imports from the new
  modules for backwards compatibility. Not recommended — this is internal code,
  not a public API with external consumers.

Affected import sites:
- `cli/export.py` (was `cli/cli.py`)
- `cli/import_cmd.py`
- `cli/combine.py`
- `io/exporter.py`
- `io/importer.py`
- `tests/test_files.py`

---

## Phase 3: Add `__init__.py` public APIs

### `core/__init__.py`

```python
from .base import SqlServerIOBase, rows_from_arrow
from .config import (
    ENGINE_CHOICES,
    SqlConfig,
    TransactionMode,
    coerce_transaction_mode,
    quote_identifier,
    sanitise_dsn_value,
    validate_engine,
    validate_identifier,
    validate_parameters,
    # defaults
    BATCH_SIZE_DEFAULT,
    CCI_DEFAULT,
    DRIVER_DEFAULT,
    ENCRYPT_DEFAULT,
    EXPORT_ENGINE_DEFAULT,
    FETCH_SIZE_DEFAULT,
    IMPORT_ENGINE_DEFAULT,
    MEM_COOLDOWN_DEFAULT,
    MEM_HEARTBEAT_DEFAULT,
    MEM_MAX_WAIT_DEFAULT,
    MEM_SLEEP_DEFAULT,
    MEM_THRESHOLD_DEFAULT,
    PACKET_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    ROWGROUP_SIZE_DEFAULT,
    SCHEMA_DEFAULT,
    TRANSACTION_MODE_DEFAULT,
    TRUST_CERT_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
)
from .logobs import (
    MemoryGate,
    MemoryHeartbeat,
    WorkerMonitor,
    configure_logging,
    context,
    get_logger,
    init_worker_logging,
    log_failure_summary,
    log_memory_budget,
    mem_fields,
    peak_rss_bytes,
    rss_bytes,
)

__all__ = [
    "SqlServerIOBase",
    "rows_from_arrow",
    "SqlConfig",
    "TransactionMode",
    "configure_logging",
    "get_logger",
    "MemoryGate",
    "MemoryHeartbeat",
    # ... (full list of public symbols)
]
```

### `io/__init__.py`

```python
from .exporter import Exporter
from .importer import Importer
from .merger import TableMerger

__all__ = ["Exporter", "Importer", "TableMerger"]
```

### `pybutt/__init__.py` — top-level convenience API

```python
from pybutt.core.config import SqlConfig, TransactionMode
from pybutt.core.base import SqlServerIOBase
from pybutt.io.exporter import Exporter
from pybutt.io.importer import Importer
from pybutt.io.merger import TableMerger
from pybutt.files import inspect_manifest, merge_parquet_files
from pybutt.exceptions import PyButtError

__all__ = [
    "SqlConfig",
    "TransactionMode",
    "SqlServerIOBase",
    "Exporter",
    "Importer",
    "TableMerger",
    "merge_parquet_files",
    "inspect_manifest",
    "PyButtError",
]
```

---

## Phase 4: Clean up deprecated code

1. **Delete rewrite functions** from `files/` — `rewrite_single_file()`,
   `rewrite_parquet_files()`, `default_rewrite_manifest_filename()`
2. **Remove `rewrite_parquet_files` import** from CLI
3. **Delete `delete_files` / `delete_originals` parameters** — already removed
   from CLI in PR #63, but still present in `merge_parquet_files()` signature.
   Clean up the function signature too.
4. **Delete commented-out `autocommit` lines** in `importer.py` (per PR #63
   review)
5. **Delete `rewrite` tests** — `test_cli_rewrite_*` already removed in PR #63

---

## Execution Order

1. **PR #63 first** — merge the CLI behaviour/defaults refactor
2. **Phase 1** — split `cli/cli.py` (biggest impact, cleanest win)
3. **Phase 2** — split `files/files.py`
4. **Phase 3** — add `__init__.py` public APIs
5. **Phase 4** — deprecated code cleanup

Phases 2–4 could be combined into a single PR if preferred, since they're all
non-behavioural. Phase 1 is best as its own PR because the test monkeypatch
target changes make the diff noisy.

---

## Dependency Graph (unchanged)

```
exceptions          (no internal deps)
    ↑
  core/config       (depends on: exceptions)
    ↑
  core/base         (depends on: core/config, core/logobs, exceptions)
  core/logobs       (no internal deps — stdlib + psutil only)
    ↑
  files/            (depends on: core/config, exceptions)
    ↑
  io/               (depends on: core/base, core/config, core/logobs, files/, exceptions)
    ↑
  cli/              (depends on: core/config, core/logobs, io/, files/, exceptions)
```

No circular dependencies. All arrows point upward.

---

## What We're NOT Doing (and Why)

| Considered | Decision | Reason |
|---|---|---|
| Rename `core/` → `base/` + `config/` | Skip | Pure aesthetics; forces every import to change |
| Rename `io/` → `export/` + `import_/` + `combine/` | Skip | `import_` and `inspect_` are ugly Python workarounds; `io/` is conventional |
| Move `logobs.py` out of `core/` | Skip | It's shared infrastructure; `core/` is the right home |
| Move `merger.py` to `combine/` | Skip | It's an IO class like Exporter/Importer; `io/` is consistent |
| Top-level `manifest.py` | Skip | `files/manifest.py` is more descriptive and groups related code |
