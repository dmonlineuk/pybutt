from pathlib import Path

import typer

from pybutt.cli.app import (
    app,
    build_sql_config,
)
from pybutt.core.config import (
    DRIVER_DEFAULT,
    ENCRYPT_DEFAULT,
    PACKET_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    ROWGROUP_SIZE_DEFAULT,
    SCHEMA_DEFAULT,
    TRUST_CERT_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
)
from pybutt.core.logobs import configure_logging, get_logger
from pybutt.exceptions import PyButtError
from pybutt.files import (
    combine_parquet_files,
    load_manifest,
    write_manifest,
)
from pybutt.io.combiner import TableCombine

logger = get_logger("cli.combine")


@app.command(
    "combine",
    help=(
        "Combine objects listed in a manifest. "
        "For file manifests, concatenate Parquet files to a single output. "
        "For table manifests, insert from SQL tables into a single target table."
    ),
)
def combine(
    manifest_path: Path = typer.Argument(  # noqa: B008
        ..., help="Path to the input manifest file"
    ),  # noqa: B008
    verbose: bool = typer.Option(  # noqa: B008
        False,
        "--verbose",
        "-V",
        help="Show verbose logging output.",
    ),
    server: str | None = typer.Option(  # noqa: B008
        None,
        "--server",
        "-s",
        help="SQL Server host.",
        rich_help_panel="Server Connection Options",
    ),
    database: str | None = typer.Option(  # noqa: B008
        None,
        "--database",
        "-d",
        help="Target database.",
        rich_help_panel="Server Connection Options",
    ),
    driver: str = typer.Option(  # noqa: B008
        DRIVER_DEFAULT,
        "--driver",
        "-D",
        help="ODBC driver name.",
        rich_help_panel="Server Connection Options",
    ),
    schema: str = typer.Option(  # noqa: B008
        SCHEMA_DEFAULT,
        "--schema",
        "-S",
        help="Target schema.",
        rich_help_panel="SQL Data Object Options",
    ),
    table: str | None = typer.Option(  # noqa: B008
        None,
        "--table",
        "-t",
        help="Target table.",
        rich_help_panel="SQL Data Object Options",
    ),
    username: str | None = typer.Option(  # noqa: B008
        None,
        "--username",
        "-u",
        help="SQL Server username when not using trusted connection.",
        rich_help_panel="Server Security Options",
    ),
    password: str | None = typer.Option(  # noqa: B008
        None,
        "--password",
        "-p",
        help="SQL Server password when not using trusted connection.",
        rich_help_panel="Server Security Options",
    ),
    trusted_connection: bool = typer.Option(  # noqa: B008
        TRUSTED_CONNECTION_DEFAULT,
        "--trusted-connection",
        "-T",
        help="Use integrated Windows authentication instead of username/password.",
        rich_help_panel="Server Security Options",
    ),
    trust_cert: bool = typer.Option(  # noqa: B008
        TRUST_CERT_DEFAULT,
        "--trust-cert",
        "-c",
        help="Trust the SQL Server TLS certificate.",
        rich_help_panel="Server Security Options",
    ),
    encrypt: bool = typer.Option(  # noqa: B008
        ENCRYPT_DEFAULT,
        "--encrypt/--no-encrypt",
        help="Enable or disable SQL Server encrypted transport.",
        rich_help_panel="Server Security Options",
    ),
    output_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-file",
        "-o",
        help="Output Parquet file when combining files.",
        rich_help_panel="File Options",
        file_okay=True,
        dir_okay=False,
    ),
    rowgroup_size: int = typer.Option(  # noqa: B008
        ROWGROUP_SIZE_DEFAULT,
        "--rowgroup-size",
        "-R",
        help="Rowgroup size for output.",
        rich_help_panel="File Options",
    ),
    output_manifest_filename: str | None = typer.Option(  # noqa: B008
        None,
        "--combined-manifest-filename",
        "-m",
        help=(
            "Override the combined manifest filename for the written file. Defaults"
            " to <manifest-filename>-combined.json."
        ),
        rich_help_panel="File Options",
    ),
    retries: int = typer.Option(  # noqa: B008
        RETRIES_DEFAULT,
        "--retries",
        "-r",
        help="Number of retry attempts for transient SQL errors.",
        rich_help_panel="Transport Tuning Options",
        min=1,
    ),
    packet_size: int = typer.Option(  # noqa: B008
        PACKET_SIZE_DEFAULT,
        "--packet-size",
        help=(
            "TDS packet size in bytes (512-32767). "
            "Note: encrypted connections are capped at 16383."
        ),
        rich_help_panel="Transport Tuning Options",
        min=512,
        max=32767,
    ),
) -> None:
    """Combine objects listed in a manifest.

    For file manifests, this command concatenates Parquet files into a single output.
    For table manifests, it inserts from SQL tables into a single target table.
    """

    configure_logging(verbose)

    try:
        manifest = load_manifest(manifest_path)
    except PyButtError as exc:
        typer.secho(f"Combine failed: {exc}", fg=typer.colors.RED, err=True)
        raise SystemExit(1) from exc

    if manifest["type"] == "files":
        if output_file is None:
            raise typer.BadParameter("--output-file is required for file manifests")

        try:
            combine_parquet_files(
                manifest_path,
                output_file,
                rowgroup_size,
            )
        except PyButtError as exc:
            typer.secho(f"Combine failed: {exc}", fg=typer.colors.RED, err=True)
            raise SystemExit(1) from exc
        typer.secho("File combine completed successfully.", fg=typer.colors.GREEN)
        return

    # tables manifest
    if manifest["type"] == "tables":
        if not (server and database and schema and table):
            raise typer.BadParameter(
                "--server, --database, --schema and "
                "--table are required for table manifests"
            )

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
            packet_size=packet_size,
        )

        try:
            combiner = TableCombine(config=config, sources=manifest["entries"])
            combiner.combine(target_schema=schema, target_table=table)
        except PyButtError as exc:
            typer.secho(f"Combine failed: {exc}", fg=typer.colors.RED, err=True)
            raise SystemExit(1) from exc

        # ToDo: Review where this should be, and consider
        # adding user override for path and filename
        new_manifest_name = f"{manifest_path.stem}_combined{manifest_path.suffix}"
        write_manifest(
            manifest_path.parent / new_manifest_name,
            [f"{schema}.{table}"],
            manifest_type="tables",
        )

        typer.secho("Table combine completed successfully.", fg=typer.colors.GREEN)
        return

    raise typer.BadParameter(f"Unsupported manifest type: {manifest['type']}")
