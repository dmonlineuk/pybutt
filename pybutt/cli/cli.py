from __future__ import annotations

import getpass
import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import typer
from typer.testing import CliRunner

from pybutt.core.config import (
    SqlConfig,
    TransactionMode,
)
from pybutt.files.files import (
    inspect_manifest,
    rewrite_parquet_files,
)
from pybutt.io.exporter import Exporter
from pybutt.io.importer import Importer

runner = CliRunner()


app = typer.Typer(
    context_settings={"help_option_names": ["-?", "--help"]},
    help="""
PyButt CLI for exporting SQL Server tables to Parquet files and importing
Parquet data back into SQL Server.

Commands:
  export   Export SQL Server data to one or more Parquet files.
  import   Import Parquet files into a SQL Server table using a manifest.
""",
)


def parse_columns(columns: str | None) -> list[str] | None:
    if columns is None:
        return None

    parsed = [column.strip() for column in columns.split(",") if column.strip()]
    if not parsed:
        raise typer.BadParameter("--columns cannot be empty")
    return parsed


def build_sql_config(
    server: str,
    database: str,
    schema: str,
    table: str,
    username: str | None,
    password: str | None,
    driver: str,
    trusted_connection: bool,
    trust_cert: bool,
    encrypt: bool,
    retries: int,
) -> SqlConfig:
    if not trusted_connection:
        if not username:
            raise typer.BadParameter(
                "username is required unless --trusted-connection is used"
            )

        # Prompt for password if not provided
        if not password:
            password = getpass.getpass("Enter your password: ")

    return SqlConfig(
        server=server,
        database=database,
        schema=schema,
        table=table,
        username=username,
        password=password,
        driver=driver,
        trusted_connection=trusted_connection,
        trust_cert=trust_cert,
        encrypt=encrypt,
        retries=retries,
    )


@app.command(
    "export",
    help=(
        "Export a SQL Server table to Parquet and write a manifest of output "
        "file names."
    ),
)
def export(
    server: str = typer.Option(  # noqa: B008
        ..., "--server", "-s", help="SQL Server hostname or instance."
    ),
    database: str = typer.Option(  # noqa: B008
        ..., "--database", "-d", help="Target SQL Server database."
    ),
    schema: str = typer.Option(  # noqa: B008
        "dbo", "--schema", "-S", help="Target table schema."
    ),
    table: str = typer.Option(  # noqa: B008
        ..., "--table", "-t", help="Target table name."
    ),
    output_path: Path = typer.Option(  # noqa: B008
        ...,
        "--output-path",
        "-o",
        help="Directory to write Parquet files and manifest.",
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
    manifest_filename: str | None = typer.Option(
        None,
        "--manifest-filename",
        "-m",
        help=(
            "Manifest filename to write for export. Defaults to "
            "<schema>_<table>_manifest.json."
        ),
    ),
    trusted_connection: bool = typer.Option(  # noqa: B008
        False,
        "--trusted-connection",
        "-T",
        help="Use integrated Windows authentication instead of username/password.",
    ),
    username: str | None = typer.Option(  # noqa: B008
        None,
        "--username",
        "-u",
        help="SQL Server username when not using trusted connection.",
    ),
    password: str | None = typer.Option(  # noqa: B008
        None,
        "--password",
        "-p",
        help="SQL Server password when not using trusted connection.",
    ),
    driver: str = typer.Option(  # noqa: B008
        "ODBC Driver 18 for SQL Server",
        "--driver",
        "-D",
        help="ODBC driver name.",
    ),
    trust_cert: bool = typer.Option(  # noqa: B008
        False,
        "--trust-cert",
        "-tc",
        help="Trust the SQL Server TLS certificate.",
    ),
    encrypt: bool = typer.Option(  # noqa: B008
        True,
        "--encrypt/--no-encrypt",
        "-e/-ne",
        help="Enable or disable SQL Server encrypted transport.",
    ),
    retries: int = typer.Option(  # noqa: B008
        3,
        "--retries",
        "-rc",
        help="Number of retry attempts for transient SQL errors.",
        min=1,
    ),
    pk_column: str | None = typer.Option(  # noqa: B008
        None,
        "--pk-column",
        "-P",
        help="Primary key column for deterministic partitioning.",
    ),
    columns: str | None = typer.Option(  # noqa: B008
        None,
        "--columns",
        "-c",
        help="Comma-separated list of columns to export. Defaults to all columns.",
    ),
    worker_count: int = typer.Option(  # noqa: B008
        1,
        "--worker-count",
        "-wc",
        help="Number of worker processes used for export.",
        min=1,
    ),
    file_count: int = typer.Option(  # noqa: B008
        1,
        "--file-count",
        "-fc",
        help="Number of Parquet output files.",
        min=1,
    ),
    rowgroup_size: int = typer.Option(  # noqa: B008
        1_048_576,
        "--rowgroup-size",
        "-rs",
        help="Number of rows per rowgroup in the Parquet files.",
        min=1,
    ),
    fetch_size: int | None = typer.Option(  # noqa: B008
        None,
        "--fetch-size",
        "-fs",
        help=(
            "Cursor fetch size for pyodbc export. "
            "Defaults to min(max(1024, rowgroup_size), 8192)."
        ),
        min=1,
    ),
    engine: str = typer.Option(  # noqa: B008
        "duckdb",
        "--engine",
        "-E",
        help="Export engine to use: duckdb or pyodbc.",
        case_sensitive=False,
    ),
    verbose: bool = typer.Option(  # noqa: B008
        False,
        "--verbose",
        "-v",
        help="Show verbose logging output.",
    ),
) -> None:
    """Export data from a SQL Server table to Parquet files.

    The command writes one or more Parquet files into OUTPUT_PATH and
    creates a manifest file listing the generated parquet file names.
    """

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = build_sql_config(
        server=server,
        database=database,
        schema=schema,
        table=table,
        username=username,
        password=password,
        driver=driver,
        trusted_connection=trusted_connection,
        trust_cert=trust_cert,
        encrypt=encrypt,
        retries=retries,
    )

    exporter = Exporter(
        config=config,
        output_path=output_path,
        pk_column=pk_column,
        columns=parse_columns(columns),
        worker_count=worker_count,
        file_count=file_count,
        rowgroup_size=rowgroup_size,
        fetch_size=fetch_size,
        engine=engine.lower(),
        manifest_filename=manifest_filename,
    )
    exporter.perform_work()
    typer.secho("Export completed successfully.", fg=typer.colors.GREEN)


@app.command(
    "import",
    help=("Import Parquet files into a SQL Server table using a manifest file."),
)
def import_data(
    server: str = typer.Option(  # noqa: B008
        ..., "--server", "-s", help="SQL Server hostname or instance."
    ),
    database: str = typer.Option(  # noqa: B008
        ..., "--database", "-d", help="Target SQL Server database."
    ),
    schema: str = typer.Option(  # noqa: B008
        "dbo", "--schema", "-S", help="Target table schema."
    ),
    table: str = typer.Option(  # noqa: B008
        ..., "--table", "-t", help="Target table name."
    ),
    input_path: Path = typer.Option(  # noqa: B008
        ...,
        "--input-path",
        "-i",
        help="Directory containing Parquet files and the manifest.",
        exists=True,
        file_okay=False,
        dir_okay=True,
    ),
    manifest_filename: str | None = typer.Option(  # noqa: B008
        None,
        "--manifest-filename",
        "-m",
        help=(
            "Manifest filename containing exported parquet file names. "
            "Defaults to <schema>_<table>_manifest.json."
        ),
    ),
    temp_manifest_filename: str | None = typer.Option(
        None,
        "--temp-manifest-filename",
        help=(
            "Override the temporary worker manifest filename written during "
            "multi-worker import. Defaults to <schema>_<table>_temp_manifest.json."
        ),
    ),
    trusted_connection: bool = typer.Option(  # noqa: B008
        False,
        "--trusted-connection",
        "-T",
        help="Use integrated Windows authentication instead of username/password.",
    ),
    username: str | None = typer.Option(  # noqa: B008
        None,
        "--username",
        "-u",
        help="SQL Server username when not using trusted connection.",
    ),
    password: str | None = typer.Option(  # noqa: B008
        None,
        "--password",
        "-p",
        help="SQL Server password when not using trusted connection.",
    ),
    driver: str = typer.Option(  # noqa: B008
        "ODBC Driver 18 for SQL Server",
        "--driver",
        "-D",
        help="ODBC driver name.",
    ),
    trust_cert: bool = typer.Option(  # noqa: B008
        False,
        "--trust-cert",
        "-tc",
        help="Trust the SQL Server TLS certificate.",
    ),
    encrypt: bool = typer.Option(  # noqa: B008
        True,
        "--encrypt/--no-encrypt",
        "-e/-ne",
        help="Enable or disable SQL Server encrypted transport.",
    ),
    retries: int = typer.Option(  # noqa: B008
        3,
        "--retries",
        "-rc",
        help="Number of retry attempts for transient SQL errors.",
        min=1,
    ),
    worker_count: int = typer.Option(  # noqa: B008
        1,
        "--worker-count",
        "-wc",
        help="Number of parallel import threads.",
        min=1,
    ),
    batch_size: int = typer.Option(  # noqa: B008
        1000,
        "--batch-size",
        "-b",
        help="Number of rows to insert per batch.",
        min=1,
    ),
    verbose: bool = typer.Option(  # noqa: B008
        False,
        "--verbose",
        "-v",
        help="Show verbose logging output.",
    ),
    engine: str = typer.Option(  # noqa: B008
        "pyodbc",
        "--engine",
        "-E",
        help="Import engine to use: duckdb or pyodbc.",
        case_sensitive=False,
    ),
    transaction_mode: TransactionMode = typer.Option(  # noqa: B008
        TransactionMode.BATCH,
        "--transaction-mode",
        "-tm",
        help=(
            "Transaction scope: row (no transaction, auto-commit), batch (per batch, "
            "recommended), rowgroup (per row group), file (entire file)."
        ),
    ),
    no_tempdb: bool = typer.Option(
        False,
        "--no-tempdb",
        help=(
            "Use local temp tables similar to the target table name instead of "
            "global temp tables in tempdb."
        ),
    ),
) -> None:
    """Import Parquet files into a SQL Server table.

    The command reads the manifest file and imports each Parquet file into the
    target table using parameterized batch inserts.
    """

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = build_sql_config(
        server=server,
        database=database,
        schema=schema,
        table=table,
        username=username,
        password=password,
        driver=driver,
        trusted_connection=trusted_connection,
        trust_cert=trust_cert,
        encrypt=encrypt,
        retries=retries,
    )

    importer = Importer(
        config=config,
        input_path=input_path,
        manifest_filename=manifest_filename,
        worker_count=worker_count,
        batch_size=batch_size,
        transaction_mode=transaction_mode,
        engine=engine.lower(),
        use_tempdb=not no_tempdb,
        temp_manifest_filename=temp_manifest_filename,
    )
    importer.perform_work()
    typer.secho("Import completed successfully.", fg=typer.colors.GREEN)


@app.command("inspect")
def inspect_command(
    manifest: Path = typer.Argument(..., help="Path to manifest.json"),  # noqa: B008
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show column details"
    ),  # noqa: B008
):
    """
    Inspect parquet files listed in a manifest.
    """
    inspect_manifest(manifest, verbose)


@app.command("rewrite")
def rewrite_command(
    manifest: Path = typer.Argument(...),  # noqa: B008
    outdir: Path = typer.Option(  # noqa: B008
        None, "--outdir", "-o", help="Output directory"
    ),
    rowgroup_size: int = typer.Option(..., "--rowgroup-size", "-r"),  # noqa: B008
    new_manifest: str | None = typer.Option(  # noqa: B008
        None,
        "--new-manifest",
        "-n",
        help=(
            "Optional filename for the rewritten manifest. Defaults to "
            "<manifest>_new.json based on the original manifest name."
        ),
    ),
    delete_originals: bool = typer.Option(
        False, "--delete-originals", "-d"
    ),  # noqa: B008
):
    """
    Rewrite parquet files with a new row-group size.
    """
    rewrite_parquet_files(
        manifest,
        outdir,
        rowgroup_size,
        new_manifest,
        delete_originals,
    )


def create_parquet(tmp_path: Path, name: str, rows=10, rowgroup_size=5):
    data = {
        "id": list(range(rows)),
        "value": [f"v{i}" for i in range(rows)],
    }
    table = pa.Table.from_pydict(data)
    file_path = tmp_path / name
    pq.write_table(table, file_path, row_group_size=rowgroup_size)
    return file_path


def test_cli_rewrite_default_outdir(tmp_path):
    create_parquet(tmp_path, "x.parquet", rows=8, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(
        app,
        [
            "files",
            "rewrite",
            str(manifest),
            "--rowgroup-size",
            "4",
            "--new-manifest",
            "manifest_new.json",
        ],
    )

    assert result.exit_code == 0

    # Check new file exists
    new_file = tmp_path / "x_new.parquet"
    assert new_file.exists()

    # Check manifest
    with open(tmp_path / "manifest_new.json") as f:
        files = json.load(f)
    assert files == ["x_new.parquet"]


if __name__ == "__main__":
    app()
