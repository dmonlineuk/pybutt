from __future__ import annotations

import getpass
import logging
from pathlib import Path

import typer

from pybutt.core import Exporter, Importer, SqlConfig, TransactionMode

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
    manifest_filename: str = typer.Option(  # noqa: B008
        ...,
        "--manifest-filename",
        "-m",
        help="Manifest filename containing exported parquet file names.",
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
    transaction_mode: TransactionMode = typer.Option(  # noqa: B008
        TransactionMode.BATCH,
        "--transaction-mode",
        "-tm",
        help=(
            "Transaction scope: row (no transaction, auto-commit), batch (per batch, "
            "recommended), rowgroup (per row group), file (entire file)."
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
    )
    importer.perform_work()
    typer.secho("Import completed successfully.", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
