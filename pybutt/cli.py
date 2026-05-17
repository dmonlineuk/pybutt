from __future__ import annotations

import getpass
import logging
from pathlib import Path
from typing import Optional

import typer

from pybutt.core import Exporter, Importer, SqlConfig

app = typer.Typer(
    help="""PyButt CLI for exporting SQL Server tables to Parquet files and importing Parquet data back into SQL Server.

Commands:
  export   Export SQL Server data to one or more Parquet files.
  import   Import Parquet files into a SQL Server table using a manifest.
""",
)


def parse_columns(columns: Optional[str]) -> Optional[list[str]]:
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
    username: Optional[str],
    password: Optional[str],
    driver: str,
    trusted_connection: bool,
    trust_cert: bool,
    encrypt: bool,
    retries: int,
) -> SqlConfig:
    if not trusted_connection:
        if not username:
            raise typer.BadParameter("username is required unless --trusted-connection is used")
        
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


@app.command("export", help="Export a SQL Server table to Parquet and write a manifest of output file names.")
def export(
    server: str = typer.Option(..., "--server", "-s", help="SQL Server hostname or instance."),
    database: str = typer.Option(..., "--database", "-d", help="Target SQL Server database."),
    schema: str = typer.Option("dbo", "--schema", "-S", help="Target table schema."),
    table: str = typer.Option(..., "--table", "-t", help="Target table name."),
    output_path: Path = typer.Option(
        ..., "--output-path", "-o",
        help="Directory to write Parquet files and manifest.",
        file_okay=False,
        dir_okay=True,
        writable=True,
    ),
    trusted_connection: bool = typer.Option(
        False,
        "--trusted-connection",
        help="Use integrated Windows authentication instead of username/password.",
    ),
    username: Optional[str] = typer.Option(
        None,
        "--username",
        "-u",
        help="SQL Server username when not using trusted connection.",
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        "-p",
        help="SQL Server password when not using trusted connection.",
    ),
    driver: str = typer.Option(
        "ODBC Driver 18 for SQL Server",
        "--driver",
        help="ODBC driver name.",
    ),
    trust_cert: bool = typer.Option(
        False,
        "--trust-cert",
        help="Trust the SQL Server TLS certificate.",
    ),
    encrypt: bool = typer.Option(
        True,
        "--encrypt/--no-encrypt",
        help="Enable or disable SQL Server encrypted transport.",
    ),
    retries: int = typer.Option(
        3,
        "--retries",
        help="Number of retry attempts for transient SQL errors.",
        min=1,
    ),
    pk_column: Optional[str] = typer.Option(
        None,
        "--pk-column",
        help="Primary key column for deterministic partitioning.",
    ),
    columns: Optional[str] = typer.Option(
        None,
        "--columns",
        help="Comma-separated list of columns to export. Defaults to all columns.",
    ),
    worker_count: int = typer.Option(
        1,
        "--worker-count",
        help="Number of worker processes used for export.",
        min=1,
    ),
    file_count: int = typer.Option(
        1,
        "--file-count",
        help="Number of Parquet output files.",
        min=1,
    ),
    verbose: bool = typer.Option(
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
    )
    exporter.perform_work()
    typer.secho("Export completed successfully.", fg=typer.colors.GREEN)


@app.command("import", help="Import Parquet files into a SQL Server table using a manifest file.")
def import_data(
    server: str = typer.Option(..., "--server", "-s", help="SQL Server hostname or instance."),
    database: str = typer.Option(..., "--database", "-d", help="Target SQL Server database."),
    schema: str = typer.Option("dbo", "--schema", "-S", help="Target table schema."),
    table: str = typer.Option(..., "--table", "-t", help="Target table name."),
    input_path: Path = typer.Option(
        ..., "--input-path", "-i",
        help="Directory containing Parquet files and the manifest.",
        exists=True,
        file_okay=False,
        dir_okay=True,
    ),
    manifest_filename: str = typer.Option(
        ..., "--manifest-filename", "-m",
        help="Manifest filename containing exported parquet file names.",
    ),
    trusted_connection: bool = typer.Option(
        False,
        "--trusted-connection",
        help="Use integrated Windows authentication instead of username/password.",
    ),
    username: Optional[str] = typer.Option(
        None,
        "--username",
        "-u",
        help="SQL Server username when not using trusted connection.",
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        "-p",
        help="SQL Server password when not using trusted connection.",
    ),
    driver: str = typer.Option(
        "ODBC Driver 18 for SQL Server",
        "--driver",
        help="ODBC driver name.",
    ),
    trust_cert: bool = typer.Option(
        False,
        "--trust-cert",
        help="Trust the SQL Server TLS certificate.",
    ),
    encrypt: bool = typer.Option(
        True,
        "--encrypt/--no-encrypt",
        help="Enable or disable SQL Server encrypted transport.",
    ),
    retries: int = typer.Option(
        3,
        "--retries",
        help="Number of retry attempts for transient SQL errors.",
        min=1,
    ),
    worker_count: int = typer.Option(
        1,
        "--worker-count",
        help="Number of parallel import threads.",
        min=1,
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        help="Number of rows to insert per batch.",
        min=1,
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Show verbose logging output.",
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
    )
    importer.perform_work()
    typer.secho("Import completed successfully.", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()
