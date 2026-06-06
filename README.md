# PyButt

**Python Bulk Transfer Tool** - A tool for exporting SQL Server tables to Parquet files and importing Parquet data back into SQL Server.

## Features

- **SQL Server to Parquet Export**: Partition tables and export them as multiple Parquet files in parallel
- **Parquet to SQL Server Import**: Bulk import Parquet files into SQL Server with configurable batch sizing
- **Flexible Authentication**: Supports both SQL authentication and Windows integrated authentication
- **Command-Line Interface**: Full-featured CLI with Typer for easy command execution
- **Python API**: Use PyButt as a module in your Python projects for programmatic access
- **Manifest-Based Import**: Track exported files with automatic manifests
- **Performance Optimized**: Multi-process export and multi-threaded import for maximum throughput

## Prerequisites

Before installing PyButt, ensure your system has the required ODBC components:

### Linux

```bash
# Check for libodbc
ldconfig -p | grep libodbc

# Check for ODBC Driver 18 for SQL Server
odbcinst -q -d
```

**Required packages:**
- `libodbc.so.2` (usually from the `unixodbc` package)
- `msodbcsql` version 18
- `duckdb` (see https://duckdb.org/install/?platform=linux&environment=cli)

### Windows

Install these packages using winget, and set the PowerShell ExecutionPolicy so you can activate your virtual environment:

```pwsh
winget install -e --id Microsoft.msodbcsql.18
winget install -e --id DuckDB.cli
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser

# If you haven't already got `git` or `python`
winget install -e --id Git.Git
winget install -e --id Python.Python.3.14 --location C:\Python314
```

**Required packages:**
- `msodbcsql` version 18
- `duckdb` (see https://duckdb.org/install/?platform=windows&environment=cli)

## Installation

### Quick Start

PyButt uses `pyproject.toml` as the source of truth for runtime dependencies and optional development tooling.

```bash
git clone https://github.com/dmonlineuk/pybutt && cd pybutt
python -m venv .venv
source .venv/bin/activate  # On Windows: `.venv\Scripts\Activate.ps1`
python -m pip install --upgrade pip
pip install -e .
```

If you want the full developer environment with formatting, linting, and tests:

```bash
pip install -e .[dev]
```

### Install as a Package

For use in Python projects and enabling CLI executable:

```bash
pip install -e .
```

## Usage

### Command-Line Interface

PyButt provides two main commands: `export` and `import`.

#### Export Command

Export a SQL Server table to Parquet files:

```bash
pybutt export \
  --server YOUR_SERVER \
  --database YOUR_DB \
  --schema dbo \
  --table YOUR_TABLE \
  --username your_user \
  --output-path ./output
```

**Export Options:**

```
--server,               -s      SQL Server hostname or instance (required)
--database,             -d      Target database (required)
--schema,               -S      Table schema (default: dbo)
--table,                -t      Table name (required)
--output-path,          -o      Output directory for Parquet files (required)
--manifest-filename,    -m      Custom manifest filename to write (default: <schema>_<table>_manifest.json)
--username,             -u      SQL Server username
--password,             -p      SQL Server password (prompted if not provided)
--trusted-connection,   -T      Use Windows integrated authentication
--driver,               -D      ODBC driver name (default: ODBC Driver 18 for SQL Server)
--trust-cert,           -c      Trust the SQL Server TLS certificate
--encrypt/--no-encrypt, -e/-n   Enable/disable encrypted transport (default: enabled)
--retries,              -r      Number of retry attempts for transient errors (default: 3)
--pk-column,            -P      Primary key column for deterministic partitioning
--columns,              -C      Comma-separated list of columns to export (all by default)
--worker-count,         -w      Number of worker processes (default: 1)
--file-count,           -f      Number of output Parquet files (default: 1)
--rowgroup-size,        -R      Number of rows per rowgroup inside each Parquet file (default: 1048576)
--fetch-size,           -F      Cursor fetch size for pyodbc export (default: min(max(1024, rowgroup_size), 8192))
--engine,               -E      Export engine to use: duckdb, pyodbc, or mssql-python (default: duckdb)
--parameters                    Comma-separated list of parameter values to pass to a table-valued function (e.g. 12,'fred','1989')
--verbose,              -v      Show verbose logging output
```

**Examples:**

Export entire table with 4 parallel workers:
```bash
pybutt export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --output-path ./exports/customers \
  --username dbuser \
  --worker-count 4 \
  --file-count 4
```

Export using the pyodbc engine explicitly:
```bash
pybutt export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --output-path ./exports/customers \
  --username dbuser \
  --engine pyodbc
```

Export using the mssql-python engine:
```bash
pybutt export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --output-path ./exports/customers \
  --username dbuser \
  --engine mssql-python
```

Export specific columns using primary key partitioning:
```bash
pybutt export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Orders \
  --output-path ./exports/orders \
  --username dbuser \
  --pk-column OrderID \
  --columns "OrderID,OrderDate,Amount" \
  --file-count 8
```

Exporting database views is also supported. If partition statistics are unavailable for the target object, PyButt will fall back to `SELECT COUNT(*)` to determine the row count before partitioning.

Export from a TVF with parameters:
```bash
pybutt export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --schema export \
  --table tvf_users \
  --parameters "12,'fred','1989'" \
  --output-path ./exports/tvf_users \
  --username dbuser
```

Export using Windows authentication:
```bash
pybutt export \
  --server SQLSERVER01\INSTANCE \
  --database MyDatabase \
  --table LargeTable \
  --output-path ./exports \
  --trusted-connection
```

#### Import Command

Import Parquet files into a SQL Server table:

```bash
pybutt import \
  --server YOUR_SERVER \
  --database YOUR_DB \
  --schema dbo \
  --table YOUR_TABLE \
  --input-path ./export_data \
  --manifest-filename your_table_manifest.json \
  --username your_user
```

**Import Options:**

```
--server,                     -s      SQL Server hostname or instance (required)
--database,                   -d      Target database (required)
--schema,                     -S      Table schema (default: dbo)
--table,                      -t      Table name (required)
--input-path,                 -i      Directory containing Parquet files (required)
--manifest-filename,          -m      Manifest file name (required)
--output-manifest-filename,   -o      Override temporary worker manifest filename during multi-worker import
--username,                   -u      SQL Server username
--password,                   -p      SQL Server password (prompted if not provided)
--trusted-connection,         -T      Use Windows integrated authentication
--driver,                     -D      ODBC driver name (default: ODBC Driver 18 for SQL Server)
--trust-cert,                 -c      Trust the SQL Server TLS certificate
--encrypt/--no-encrypt,       -e/-n   Enable/disable encrypted transport (default: enabled)
--retries,                    -r      Number of retry attempts for transient errors (default: 3)
--worker-count,               -w      Number of parallel import threads (default: 1)
--batch-size,                 -b      Rows per batch insert (default: 1000)
--engine,                     -E      Import engine to use: duckdb, pyodbc, or mssql-python (default: pyodbc)
--transaction-mode,           -M      Transaction scope: row, batch (default), rowgroup, file
--cci/--no-cci                        Create a clustered columnstore index on per-worker temp tables (default: enabled)
--delete-files,               -x      Delete source parquet files and the manifest after import
--verbose,                    -v      Show verbose logging output
```

**Columnstore on temporary tables:**

When importing with `--worker-count` of 2 or more, PyButt creates one temporary
table per worker (`SELECT TOP 0 * INTO ... FROM <source>`) which can then be merged
into the target afterwards. By default a clustered columnstore index (CCI) is now
created on each temporary table to reduce the storage footprint of these staging
tables. Pass `--no-cci` to keep the previous heap behaviour.

Notes:
- The CCI is only created on the multi-worker path (single-worker imports are
  unaffected).
- Space savings come from columnstore compression. The SQL Server tuple mover
  compresses row groups as data is loaded once they are large enough, so the
  benefit applies when it matters most (large imports). Small loads may sit in
  the uncompressed delta store until a row group fills.
- Clustered columnstore indexes require SQL Server 2014+ (and are available in
  all editions from SQL Server 2016 SP1). On unsupported instances, or with
  source columns that columnstore does not support, use `--no-cci`.

**Examples:**

Basic import (uses BATCH transaction mode by default):
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --input-path ./exports/customers \
  --manifest-filename customers_manifest.json \
  --username dbuser
```

Import using the duckdb engine explicitly:
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --input-path ./exports/customers \
  --manifest-filename customers_manifest.json \
  --username dbuser \
  --engine duckdb
```

Import using the mssql-python engine (native bulk insert for faster imports):
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --input-path ./exports/customers \
  --manifest-filename customers_manifest.json \
  --username dbuser \
  --engine mssql-python
```

High-throughput import with larger batches (BATCH mode):
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Orders \
  --input-path ./imports/orders \
  --manifest-filename orders_manifest.json \
  --username dbuser \
  --worker-count 4 \
  --batch-size 5000 \
  --transaction-mode batch \
  --verbose
```

Import with row group transactions (for rowgroup granularity):
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table LargeTable \
  --input-path ./imports/data \
  --manifest-filename data_manifest.json \
  --username dbuser \
  --transaction-mode rowgroup
```

Import with file-level transactions (all-or-nothing for critical data):
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table FinancialData \
  --input-path ./imports/financials \
  --manifest-filename financials_manifest.json \
  --username dbuser \
  --transaction-mode file
```

Import with row-level transactions (minimum speed, no safety - for testing only):
```bash
pybutt import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table LargeTable \
  --input-path ./imports/data \
  --manifest-filename data_manifest.json \
  --username dbuser \
  --transaction-mode row
```

#### Merge Command

Merge objects listed in a manifest file. This command supports two types of merges depending on the manifest type:
- **Files manifest (`type: "files"`)**: Concatenates multiple Parquet files into a single output Parquet file.
- **Tables manifest (`type: "tables"`)**: Merges multiple temporary/worker SQL tables into a single target table on your SQL Server.

```bash
# File merge example:
pybutt merge \
  --manifest ./exports/customers/dbo_Customers_manifest.json \
  --output-file ./exports/customers/merged.parquet

# Table merge example:
pybutt merge \
  --manifest ./exports/customers/dbo_Customers_temp_manifest.json \
  --server YOUR_SERVER \
  --database YOUR_DB \
  --schema dbo \
  --table Customers \
  --username your_user
```

**Merge Options:**

```
--manifest,             -m    Path to the manifest file (required)
--output-file,          -o    Output Parquet file path (required for file merges)
--rowgroup-size,        -R    Rowgroup size for output Parquet file (default: 1048576)
--delete-files,         -x    Delete source files/manifests after successful merge
--server,               -s    SQL Server hostname or instance (required for table merges)
--database,             -d    Target database (required for table merges)
--schema,               -S    Target schema (required for table merges)
--table,                -t    Target table name (required for table merges)
--username,             -u    SQL Server username (for table merges)
--password,             -p    SQL Server password (for table merges)
--trusted-connection,   -T    Use Windows integrated authentication
--driver,               -D    ODBC driver name (default: ODBC Driver 18 for SQL Server)
--trust-cert,           -c    Trust the SQL Server TLS certificate
--encrypt/--no-encrypt, -e/-n Enable/disable encrypted transport (default: enabled)
--retries,              -r    Number of retry attempts for transient SQL errors (default: 3)
--verbose,              -v    Show verbose logging output
```

#### Inspect Command

Inspect details of the Parquet files listed in a manifest (including row counts, row group counts, size, and columns):

```bash
pybutt inspect ./exports/customers/dbo_Customers_manifest.json
```

**Inspect Options:**

```
manifest                      Path to manifest.json file (positional, required)
--verbose,            -v      Show full column definitions, schema, and detailed metadata
```

#### Rewrite Command

Rewrite existing Parquet files listed in a manifest using a new, updated row-group size:

```bash
pybutt rewrite \
  ./exports/customers/dbo_Customers_manifest.json \
  --output-path ./exports/customers_rewritten \
  --rowgroup-size 500000
```

**Rewrite Options:**

```
manifest                      Path to manifest.json file (positional, required)
--output-path,        -o      Output directory to write the rewritten Parquet files
--rowgroup-size,      -R      New row-group size to apply (required)
--new-manifest,       -m      Optional customized name for rewritten manifest JSON file
--delete-files,       -x      Delete original parquet files and manifest after rewriting
```

### Password Input

When you provide a username without a password, PyButt will prompt you interactively:

```bash
pybutt export \
  --server myserver \
  --database mydb \
  --table mytable \
  --output-path ./output \
  --username myuser
# You'll be prompted: Enter your password: [hidden input]
```

### Python API

Use PyButt as a module in your Python projects:

#### Configuration

First, create a `SqlConfig` object with your connection details:

```python
from pybutt.core.config import SqlConfig
from pybutt.io.exporter import Exporter
from pybutt.io.importer import Importer
from pathlib import Path

config = SqlConfig(
    server="sqlserver.example.com",
    database="MyDatabase",
    schema="dbo",
    table="Customers",
    username="dbuser",
    password="dbpassword",
    trusted_connection=False,
    trust_cert=False,
    encrypt=True,
    retries=3,
)
```

Or with Windows authentication:

```python
config = SqlConfig(
    server="SQLSERVER01\\INSTANCE",
    database="MyDatabase",
    schema="dbo",
    table="Customers",
    trusted_connection=True,
)
```

#### Exporting Data

```python
from pathlib import Path

exporter = Exporter(
    config=config,
    output_path=Path("./exports/customers"),
    pk_column=None,                          # None for CHECKSUM partitioning
    columns=None,                            # None for all columns
    worker_count=4,                          # Number of parallel processes
    file_count=4,                            # Number of output files
    fetch_size=None,                         # Cursor fetch size for pyodbc export (None = auto)
)

exporter.perform_work()
print("Export completed successfully!")
```

With primary key partitioning:

```python
exporter = Exporter(
    config=config,
    output_path=Path("./exports/orders"),
    pk_column="OrderID",                     # Use PK for deterministic partitioning
    columns=["OrderID", "OrderDate", "Amount"],
    worker_count=8,
    file_count=8,
    fetch_size=None,                         # Optional: tune pyodbc fetch size for streaming
)

exporter.perform_work()
```

#### Importing Data

**Default (batch-level transactions):**
```python
from pybutt.core.config import TransactionMode

importer = Importer(
    config=config,
    input_path=Path("./exports/customers"),
    manifest_filename="customers_manifest.json",
    worker_count=4,                          # Number of parallel threads
    batch_size=1000,                         # Rows per batch
    transaction_mode=TransactionMode.BATCH,  # Each batch in its own transaction (default, recommended)
)

importer.perform_work()
print("Import completed successfully!")
```

**With row group transactions (rowgroup granularity):**
```python
importer = Importer(
    config=config,
    input_path=Path("./exports/orders"),
    manifest_filename="orders_manifest.json",
    worker_count=4,
    batch_size=5000,
    transaction_mode=TransactionMode.ROWGROUP,  # Each row group in its own transaction
)

importer.perform_work()
```

**With file-level transactions (all-or-nothing safety):**
```python
importer = Importer(
    config=config,
    input_path=Path("./exports/data"),
    manifest_filename="data_manifest.json",
    worker_count=4,
    batch_size=1000,
    transaction_mode=TransactionMode.FILE,   # Entire file in one transaction
)

importer.perform_work()
```

**With row-level transactions (minimum speed, no safety - for testing only):**
```python
importer = Importer(
    config=config,
    input_path=Path("./exports/customers"),
    manifest_filename="customers_manifest.json",
    worker_count=4,
    batch_size=1000,
    transaction_mode=TransactionMode.ROW,    # Each row commits individually (no retries, autocommit)
)

importer.perform_work()
```

#### Complete Example

```python
from pathlib import Path
from pybutt.core.config import SqlConfig, TransactionMode
from pybutt.io.exporter import Exporter
from pybutt.io.importer import Importer

# Configure connection
config = SqlConfig(
    server="sqlserver.example.com",
    database="MyDatabase",
    schema="dbo",
    table="LargeTable",
    username="dbuser",
    password="dbpassword",
)

# Export
export_path = Path("./data_export")
exporter = Exporter(
    config=config,
    output_path=export_path,
    worker_count=4,
    file_count=4,
)
exporter.perform_work()
print("✓ Export complete")

# Import into another table
import_config = SqlConfig(
    server="sqlserver.example.com",
    database="MyDatabase",
    schema="dbo",
    table="LargeTableBackup",
    username="dbuser",
    password="dbpassword",
)

importer = Importer(
    config=import_config,
    input_path=export_path,
    manifest_filename="dbo_LargeTable_manifest.json",
    worker_count=4,
    batch_size=5000,
    transaction_mode=TransactionMode.BATCH,  # Batch-level transactions for balance
)
importer.perform_work()
print("✓ Import complete")
```

## Manifest Files

When exporting, PyButt automatically creates a manifest JSON file listing all generated Parquet files. This manifest is required for importing:

As of version 2, a manifest is a JSON object with a `version`, a `type`, and an
`entries` list. Two manifest types are supported:

- **`files`** — `entries` are Parquet file names (written by `export`, `rewrite`,
  and file `merge`).
- **`tables`** — `entries` are SQL Server table names (written during multi-worker
  `import` and table `merge`, for consumption by the `merge` command).

**Example file manifest** (`dbo_MyTable_manifest.json`):
```json
{
    "version": 2,
    "type": "files",
    "entries": [
        "dbo_MyTable_part_00000.parquet",
        "dbo_MyTable_part_00001.parquet",
        "dbo_MyTable_part_00002.parquet",
        "dbo_MyTable_part_00003.parquet"
    ]
}
```

**Example table manifest** (`dbo_MyTable_temp_manifest.json`):
```json
{
    "version": 2,
    "type": "tables",
    "entries": [
        "dbo.MyTable_01_a1b2c3d4",
        "dbo.MyTable_02_e5f6a7b8"
    ]
}
```

For backwards compatibility, legacy version 1 manifests — a plain JSON array of
Parquet file names — are still accepted when reading and are treated as a `files`
manifest:
```json
[
    "dbo_MyTable_part_00000.parquet",
    "dbo_MyTable_part_00001.parquet"
]
```

## Performance Tips

- **Export**: Increase `--worker-count` and `--file-count` for large tables (use values matching your CPU core count)
- **Import**: Use `--worker-count` up to your CPU core count and adjust `--batch-size` (higher values = fewer database round trips)
- **mssql-python engine**: Use `--engine mssql-python` for imports to leverage native bulk insert (`bulkcopy`) which is significantly faster than parameterized `INSERT` statements used by pyodbc
- **Primary Key Partitioning**: Use `--pk-column` for deterministic partitioning when re-importing the same data
- **Encryption**: Use `--no-encrypt` only in secure networks to reduce overhead

## Transaction Modes for Import

The `--transaction-mode` option controls how data is committed during import and how retries are handled. Choose based on your safety, performance, and recovery needs:

| Mode | Behavior | Retry Scope | Best For | Pros | Cons |
|------|----------|-------------|----------|------|------|
| **batch** | Each batch of `batch_size` rows commits together | Per-batch retry | **Recommended for most use cases** | Fast, limited lock duration, failed batches retry independently | Rare edge case: partial batch on non-retryable error |
| **rowgroup** | Each Parquet row group commits together | Per-rowgroup retry | Rowgroup granularity with safe retries | Row group boundary safety, independent rowgroup retries | Longer locks than batch mode, fewer retry opportunities |
| **file** | Entire file in one transaction | Entire file retry | Production, critical data | All-or-nothing atomicity, complete data integrity | Can hold locks longer on large files, if failure occurs entire file retries |
| **row** | Each row auto-commits immediately | No retries (unsafe with autocommit) | Non-critical data, testing - **Not recommended** | Zero lock contention | Partial loads on error, no rollback, autocommit prevents safe retries |

**Retry Behavior:**
- **batch/rowgroup modes**: When a batch or rowgroup fails, only that unit is rolled back and retried (up to `--retries` times). Already-committed units remain intact.
- **file mode**: If any part of the file fails, the entire file operation is retried. Previously committed batches are preserved by the transaction.
- **row mode**: No retries possible due to autocommit. Each row commits immediately, so failed rows cannot be retried without risking duplication.

**Recommended Configuration:**
```bash
pybutt import \
  --server YOUR_SERVER \
  --database YOUR_DB \
  --table YOUR_TABLE \
  --input-path ./data \
  --manifest-filename manifest.json \
  --username your_user \
  --transaction-mode batch \
  --batch-size 5000 \
  --worker-count 4
```

**Choosing a mode:**
- **Default**: Use `batch` (default) — minimal data safety and performance, but better chance of limited lock duration
- **Production/Critical Data with High Volume**: Use `rowgroup` for a balance between data safety, locking/blocking and speed
- **Safety-Critical (Small Files)**: Use `file` for complete all-or-nothing atomicity per file, but high chance of locking/blocking
- **Performance-Only, Non-Critical**: Use `row` for implicit transactions to prevent locking, but slow, and no data safety. **Not recommended**

**Retry Configuration:**
Use `--retries` (default: 3) to control retry attempts. This applies at the transaction scope level:
```bash
# Retry individual batches up to 5 times before failing
pybutt import \
  ... \
  --transaction-mode batch \
  --retries 5
```

## Troubleshooting

**Connection Issues:**
- Verify SQL Server hostname and port
- Check ODBC driver: `odbcinst -q -d`
- Test ODBC connection: `isql -v your_dsn username password`

**Empty Table Errors:**
- Ensure the table exists and contains data

**Memory Issues:**
- Reduce `--worker-count` or `--batch-size`
- Process smaller tables first to verify setup

**Frequent Batch/Rowgroup Failures:**
- Increase `--retries` and `--batch-size` for more resilient imports
- Check SQL Server logs for transient connection issues
- Verify network stability if errors are intermittent

## Contributions

When coding, please consider the following:

- Use the developer environment: `pip install -e .[dev]`
- Write tests for your changes and features that will pass when run: `pytest`
- Run isort: `isort .`
- Run black: `black .`
- Run ruff: `ruff check .`

## License

See LICENSE file for details.

