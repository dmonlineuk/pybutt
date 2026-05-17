# PyButt

**Python Bulk Transfer Tool** - A high-performance tool for exporting SQL Server tables to Parquet files and importing Parquet data back into SQL Server.

## Features

- **Efficient SQL Server to Parquet Export**: Partition tables and export them as multiple Parquet files in parallel
- **Parquet to SQL Server Import**: Bulk import Parquet files into SQL Server with configurable batch sizing
- **Flexible Authentication**: Supports both username/password and Windows integrated authentication
- **Command-Line Interface**: Full-featured CLI with Typer for easy command execution
- **Python API**: Use PyButt as a module in your Python projects for programmatic access
- **Manifest-Based Import**: Track and validate exported files with automatic manifests
- **Performance Optimized**: Multi-process export and multi-threaded import for maximum throughput

## Prerequisites

Before installing PyButt, ensure your system has the required ODBC components:

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

## Installation

### Quick Start

```bash
git clone https://github.com/dmonlineuk/pybutt && cd pybutt
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Install as a Package

For use in Python projects:

```bash
pip install -e .
```

## Usage

### Command-Line Interface

PyButt provides two main commands: `export` and `import`.

#### Export Command

Export a SQL Server table to Parquet files:

```bash
python -m pybutt.cli export \
  --server YOUR_SERVER \
  --database YOUR_DB \
  --schema dbo \
  --table YOUR_TABLE \
  --username your_user \
  --output-path ./output
```

**Export Options:**

```
--server, -s              SQL Server hostname or instance (required)
--database, -d            Target database (required)
--schema, -S              Table schema (default: dbo)
--table, -t               Table name (required)
--output-path, -o         Output directory for Parquet files (required)
--username, -u            SQL Server username
--password, -p            SQL Server password (prompted if not provided)
--trusted-connection      Use Windows integrated authentication
--driver                  ODBC driver name (default: ODBC Driver 18 for SQL Server)
--trust-cert              Trust the SQL Server TLS certificate
--encrypt/--no-encrypt    Enable/disable encrypted transport (default: enabled)
--retries                 Number of retry attempts for transient errors (default: 3)
--pk-column               Primary key column for deterministic partitioning
--columns                 Comma-separated list of columns to export (all by default)
--worker-count            Number of worker processes (default: 1)
--file-count              Number of output Parquet files (default: 1)
--verbose, -v             Show verbose logging output
```

**Examples:**

Export entire table with 4 parallel workers:
```bash
python -m pybutt.cli export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --output-path ./exports/customers \
  --username dbuser \
  --worker-count 4 \
  --file-count 4
```

Export specific columns using primary key partitioning:
```bash
python -m pybutt.cli export \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Orders \
  --output-path ./exports/orders \
  --username dbuser \
  --pk-column OrderID \
  --columns "OrderID,OrderDate,Amount" \
  --file-count 8
```

Export using Windows authentication:
```bash
python -m pybutt.cli export \
  --server SQLSERVER01\INSTANCE \
  --database MyDatabase \
  --table LargeTable \
  --output-path ./exports \
  --trusted-connection
```

#### Import Command

Import Parquet files into a SQL Server table:

```bash
python -m pybutt.cli import \
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
--server, -s              SQL Server hostname or instance (required)
--database, -d            Target database (required)
--schema, -S              Table schema (default: dbo)
--table, -t               Table name (required)
--input-path, -i          Directory containing Parquet files (required)
--manifest-filename, -m   Manifest file name (required)
--username, -u            SQL Server username
--password, -p            SQL Server password (prompted if not provided)
--trusted-connection      Use Windows integrated authentication
--driver                  ODBC driver name (default: ODBC Driver 18 for SQL Server)
--trust-cert              Trust the SQL Server TLS certificate
--encrypt/--no-encrypt    Enable/disable encrypted transport (default: enabled)
--retries                 Number of retry attempts for transient errors (default: 3)
--worker-count            Number of parallel import threads (default: 1)
--batch-size              Rows per batch insert (default: 1000)
--verbose, -v             Show verbose logging output
```

**Examples:**

Basic import:
```bash
python -m pybutt.cli import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Customers \
  --input-path ./exports/customers \
  --manifest-filename customers_manifest.json \
  --username dbuser
```

High-throughput import with larger batches:
```bash
python -m pybutt.cli import \
  --server sqlserver.example.com \
  --database MyDatabase \
  --table Orders \
  --input-path ./imports/orders \
  --manifest-filename orders_manifest.json \
  --username dbuser \
  --worker-count 4 \
  --batch-size 5000 \
  --verbose
```

### Password Input

When you provide a username without a password, PyButt will prompt you interactively:

```bash
python -m pybutt.cli export \
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
from pybutt.core import SqlConfig, Exporter, Importer
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
)

exporter.perform_work()
```

#### Importing Data

```python
importer = Importer(
    config=config,
    input_path=Path("./exports/customers"),
    manifest_filename="customers_manifest.json",
    worker_count=4,                          # Number of parallel threads
    batch_size=1000,                         # Rows per batch
)

importer.perform_work()
print("Import completed successfully!")
```

#### Complete Example

```python
from pathlib import Path
from pybutt.core import SqlConfig, Exporter, Importer

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
)
importer.perform_work()
print("✓ Import complete")
```

## Manifest Files

When exporting, PyButt automatically creates a manifest JSON file listing all generated Parquet files. This manifest is required for importing:

**Example manifest** (`dbo_MyTable_manifest.json`):
```json
[
    "dbo_MyTable_part_00000.parquet",
    "dbo_MyTable_part_00001.parquet",
    "dbo_MyTable_part_00002.parquet",
    "dbo_MyTable_part_00003.parquet"
]
```

## Performance Tips

- **Export**: Increase `--worker-count` and `--file-count` for large tables (use values matching your CPU core count)
- **Import**: Use `--worker-count` up to your CPU core count and adjust `--batch-size` (higher values = fewer database round trips)
- **Primary Key Partitioning**: Use `--pk-column` for deterministic partitioning when re-importing the same data
- **Encryption**: Use `--no-encrypt` only in secure networks to reduce overhead

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

## License

See LICENSE file for details.

