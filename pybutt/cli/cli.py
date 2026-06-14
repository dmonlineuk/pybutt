from __future__ import annotations

import getpass
import tomllib
from pathlib import Path

import typer

from pybutt.core.config import (
    DRIVER_DEFAULT,
    SCHEMA_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
    TRUST_CERT_DEFAULT,
    ENCRYPT_DEFAULT,
    EXPORT_ENGINE_DEFAULT,
    FETCH_SIZE_DEFAULT,
    ROWGROUP_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    PACKET_SIZE_DEFAULT,
    MEM_HEARTBEAT_DEFAULT,
    MEM_THRESHOLD_DEFAULT,
    MEM_SLEEP_DEFAULT,
    MEM_MAX_WAIT_DEFAULT,
    MEM_COOLDOWN_DEFAULT,
    IMPORT_ENGINE_DEFAULT,
    BATCH_SIZE_DEFAULT,
    TRANSACTION_MODE_DEFAULT,
    CCI_DEFAULT,
    SqlConfig,
    TransactionMode,
)
from pybutt.core.logobs import configure_logging, get_logger
from pybutt.exceptions import PyButtError
from pybutt.files.files import (
    inspect_manifest,
    load_manifest,
    merge_parquet_files,
    rewrite_parquet_files,
    write_manifest,
)
from pybutt.io.exporter import Exporter
from pybutt.io.importer import Importer
from pybutt.io.merger import TableMerger

app = typer.Typer(
    context_settings={"help_option_names": ["-?", "--help"]},
    help="""
PyButt CLI for exporting and importing between MS SQL Server tables and Parquet
files. Can also be used for inspecting Parquet files and merging files or tables
based on manifest definitions.
""",
)

logger = get_logger("cli")


def _get_project_version() -> str:
    p = Path(__file__).resolve().parents[2] / "pyproject.toml"
    return tomllib.loads(p.read_text(encoding="utf-8"))["project"]["version"]


def _version_callback(ctx, param, value: bool):
    if not value or ctx.resilient_parsing:
        return
    typer.echo("PyButt version: ", nl=False)
    typer.echo(_get_project_version())
    raise typer.Exit()


@app.callback(invoke_without_command=True)
def _main_callback(
    ctx: typer.Context,
    version: bool = typer.Option(
        False, "--version", "-v", callback=_version_callback, is_eager=True
    ),
):
    """PyButt CLI root callback."""
    return


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
    table: str,
    username: str | None,
    password: str | None,
    schema: str = SCHEMA_DEFAULT,
    driver: str = DRIVER_DEFAULT,
    trusted_connection: bool = TRUSTED_CONNECTION_DEFAULT,
    trust_cert: bool = TRUST_CERT_DEFAULT,
    encrypt: bool = ENCRYPT_DEFAULT,
    retries: int = RETRIES_DEFAULT,
    packet_size: int = PACKET_SIZE_DEFAULT,
) -> SqlConfig:
    if not trusted_connection:
        if not username:
            raise typer.BadParameter(
                "--username is required unless --trusted-connection is used"
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
        packet_size=packet_size,
    )


@app.command(
    "export",
    help=(
        "Export a SQL Server table to Parquet and write a manifest of output "
        "file names."
    ),
)
def export(
    verbose: bool = typer.Option(  # noqa: B008
        False,
        "--verbose",
        "-V",
        help="Show verbose logging output.",
    ),
    server: str = typer.Option(  # noqa: B008
        ...,
        "--server",
        "-s",
        help="SQL Server hostname or instance.",
        rich_help_panel="Server Connection Options",
    ),
    database: str = typer.Option(  # noqa: B008
        ...,
        "--database",
        "-d",
        help="Target SQL Server database.",
        rich_help_panel="Server Connection Options",
    ),
    engine: str = typer.Option(  # noqa: B008
        EXPORT_ENGINE_DEFAULT,
        "--engine",
        "-E",
        help="Export engine to use: duckdb, pyodbc, or mssql-python.",
        rich_help_panel="Server Connection Options",
    ),
    driver: str = typer.Option(  # noqa: B008
        DRIVER_DEFAULT,
        "--driver",
        "-D",
        help="ODBC driver to use.",
        rich_help_panel="Server Connection Options",
    ),
    schema: str = typer.Option(  # noqa: B008
        SCHEMA_DEFAULT,
        "--schema",
        "-S",
        help="Target table schema.",
        rich_help_panel="SQL Data Object Options",
    ),
    table: str = typer.Option(  # noqa: B008
        ...,
        "--table",
        "-t",
        help="Target table name.",
        rich_help_panel="SQL Data Object Options",
    ),
    parameters: str | None = typer.Option(  # noqa: B008
        None,
        "--parameters",
        help=(
            "Comma-separated list of parameter values to pass to a table-valued "
            "function. Example: --parameters 12,'fred','1989'."
        ),
        rich_help_panel="SQL Data Object Options",
    ),
    columns: str | None = typer.Option(  # noqa: B008
        None,
        "--columns",
        "-C",
        help="Comma-separated list of columns to export. Defaults to all columns.",
        rich_help_panel="SQL Data Object Options",
    ),
    pk_column: str | None = typer.Option(  # noqa: B008
        None,
        "--pk-column",
        "-P",
        help="Primary key column for deterministic partitioning.",
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
    output_path: Path = typer.Option(  # noqa: B008
        ...,
        "--output-path",
        "-o",
        help="Directory to write Parquet files and manifest.",
        rich_help_panel="File Options",
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
        rich_help_panel="File Options",
    ),
    file_count: int = typer.Option(  # noqa: B008
        1,
        "--file-count",
        "-f",
        help=("Number of Parquet output files. "),
        rich_help_panel="File Options",
        min=1,
    ),
    fetch_size: int | None = typer.Option(  # noqa: B008
        FETCH_SIZE_DEFAULT,
        "--fetch-size",
        "-F",
        help=("Cursor fetch size for pyodbc export."),
        rich_help_panel="Transport Tuning Options",
        min=1,
    ),
    rowgroup_size: int = typer.Option(  # noqa: B008
        ROWGROUP_SIZE_DEFAULT,
        "--rowgroup-size",
        "-R",
        help="Number of rows per rowgroup in the Parquet files.",
        rich_help_panel="Transport Tuning Options",
        min=1,
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
    worker_count: int = typer.Option(  # noqa: B008
        1,
        "--worker-count",
        "-w",
        help="Number of worker processes used for export.",
        rich_help_panel="Transport Tuning Options",
        min=1,
    ),
    mem_heartbeat: float = typer.Option(  # noqa: B008
        MEM_HEARTBEAT_DEFAULT,
        "--mem-heartbeat",
        "-h",
        help=("Log process memory (RSS + system %) every N seconds."),
        rich_help_panel="Memory Tuning Options",
        min=0,
    ),
    mem_threshold: float = typer.Option(  # noqa: B008
        MEM_THRESHOLD_DEFAULT,
        "--mem-threshold",
        help=(
            "System memory % at which workers are throttled. "
            "Set to 0 to disable throttling."
        ),
        rich_help_panel="Memory Tuning Options",
        min=0,
        max=100,
    ),
    mem_sleep: float = typer.Option(  # noqa: B008
        MEM_SLEEP_DEFAULT,
        "--mem-sleep",
        help=("Seconds to sleep per throttle check when memory is high. "),
        rich_help_panel="Memory Tuning Options",
        min=0.1,
    ),
    mem_max_wait: float = typer.Option(  # noqa: B008
        MEM_MAX_WAIT_DEFAULT,
        "--mem-max-wait",
        help=("Max total seconds to wait during memory throttling before giving up."),
        rich_help_panel="Memory Tuning Options",
        min=0,
    ),
    mem_cooldown: float = typer.Option(  # noqa: B008
        MEM_COOLDOWN_DEFAULT,
        "--mem-cooldown",
        help=(
            "Seconds after a throttle event before re-checking. Prevents "
            "the gate from serialising workers"
        ),
        rich_help_panel="Memory Tuning Options",
        min=0,
    ),
) -> None:
    """Export data from a SQL Server table to Parquet files.

    The command writes one or more Parquet files into OUTPUT_PATH and
    creates a manifest file listing the generated parquet file names.
    """

    configure_logging(verbose)

    if mem_threshold > 0:
        logger.info(
            "Memory throttling enabled: workers will sleep when system "
            f"memory exceeds {mem_threshold:.0f}%% "
            f"(--mem-threshold 0 to disable)"
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

    effective_file_count = file_count if file_count is not None else 1

    try:
        exporter = Exporter(
            config=config,
            output_path=output_path,
            pk_column=pk_column,
            columns=parse_columns(columns),
            worker_count=worker_count,
            file_count=effective_file_count,
            rowgroup_size=rowgroup_size,
            fetch_size=fetch_size,
            engine=engine.lower(),
            manifest_filename=manifest_filename,
            parameters=parameters,
            mem_heartbeat=mem_heartbeat,
            mem_threshold=mem_threshold,
            mem_sleep=mem_sleep,
            mem_max_wait=mem_max_wait,
            mem_cooldown=mem_cooldown,
        )
        exporter.perform_work()
    except PyButtError as exc:
        typer.secho(f"Export failed: {exc}", fg=typer.colors.RED, err=True)
        raise SystemExit(1) from exc
    typer.secho("Export completed successfully.", fg=typer.colors.GREEN)


@app.command(
    "import",
    help=("Import Parquet files into a SQL Server table using a manifest file."),
)
def import_data(
    manifest_path: Path = typer.Argument(  # noqa: B008
        ..., help="Path to the input manifest file"
    ),
    verbose: bool = typer.Option(  # noqa: B008
        False,
        "--verbose",
        "-V",
        help="Show verbose logging output.",
    ),
    server: str = typer.Option(  # noqa: B008
        ...,
        "--server",
        "-s",
        help="SQL Server hostname or instance.",
        rich_help_panel="Server Connection Options",
    ),
    database: str = typer.Option(  # noqa: B008
        ...,
        "--database",
        "-d",
        help="Target SQL Server database.",
        rich_help_panel="Server Connection Options",
    ),
    driver: str = typer.Option(  # noqa: B008
        DRIVER_DEFAULT,
        "--driver",
        "-D",
        help="ODBC driver name.",
        rich_help_panel="Server Connection Options",
    ),
    engine: str = typer.Option(  # noqa: B008
        IMPORT_ENGINE_DEFAULT,
        "--engine",
        "-E",
        help="Import engine to use: duckdb, pyodbc, or mssql-python.",
        rich_help_panel="Server Connection Options",
        case_sensitive=False,
    ),
    transaction_mode: TransactionMode = typer.Option(  # noqa: B008
        TRANSACTION_MODE_DEFAULT,
        "--transaction-mode",
        "-M",
        help=(
            "Transaction scope: batch (per batch), rowgroup (per row group, "
            "recommended), file (entire file)."
        ),
        rich_help_panel="Server Connection Options",
    ),
    schema: str = typer.Option(  # noqa: B008
        "dbo",
        "--schema",
        "-S",
        help="Target table schema.",
        rich_help_panel="SQL Data Object Options",
    ),
    table: str = typer.Option(  # noqa: B008
        ...,
        "--table",
        "-t",
        help="Target table name.",
        rich_help_panel="SQL Data Object Options",
    ),
    cci: bool = typer.Option(  # noqa: B008
        CCI_DEFAULT,
        "--cci/--no-cci",
        help=(
            "Create a clustered columnstore index on the per-worker temp tables "
            "used during multi-worker import. Use --no-cci to keep the previous "
            "heap behaviour. Enabled by default."
        ),
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
        "-e/-n",
        help="Enable or disable SQL Server encrypted transport.",
        rich_help_panel="Server Security Options",
    ),
    temp_manifest_filename: str | None = typer.Option(  # noqa: B008
        None,
        "--imported-manifest-filename",
        "-o",
        help=(
            "Override the import worker manifest filename written during "
            "multi-worker import. Defaults to <schema>_<table>_import_manifest.json."
        ),
        rich_help_panel="File Options",
    ),
    batch_size: int | None = typer.Option(  # noqa: B008
        BATCH_SIZE_DEFAULT,
        "--batch-size",
        "-b",
        help="Rows per batch insert.",
        rich_help_panel="Transport Tuning Options",
        min=1,
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
    worker_count: int = typer.Option(  # noqa: B008
        1,
        "--worker-count",
        "-w",
        help="Number of parallel import threads.",
        rich_help_panel="Transport Tuning Options",
        min=1,
    ),
    mem_heartbeat: float = typer.Option(  # noqa: B008
        MEM_HEARTBEAT_DEFAULT,
        "--mem-heartbeat",
        "-h",
        help=("Log process memory (RSS + system %) every N seconds."),
        rich_help_panel="Memory Tuning Options",
        min=0,
    ),
    mem_threshold: float = typer.Option(  # noqa: B008
        MEM_THRESHOLD_DEFAULT,
        "--mem-threshold",
        help=(
            "System memory % at which workers are throttled. "
            "Set to 0 to disable throttling."
        ),
        rich_help_panel="Memory Tuning Options",
        min=0,
        max=100,
    ),
    mem_sleep: float = typer.Option(  # noqa: B008
        MEM_SLEEP_DEFAULT,
        "--mem-sleep",
        help=("Seconds to sleep per throttle check when memory is high. "),
        rich_help_panel="Memory Tuning Options",
        min=0.1,
    ),
    mem_max_wait: float = typer.Option(  # noqa: B008
        MEM_MAX_WAIT_DEFAULT,
        "--mem-max-wait",
        help=("Max total seconds to wait during memory throttling before giving up."),
        rich_help_panel="Memory Tuning Options",
        min=0,
    ),
    mem_cooldown: float = typer.Option(  # noqa: B008
        MEM_COOLDOWN_DEFAULT,
        "--mem-cooldown",
        help=(
            "Seconds after a throttle event before re-checking. Prevents "
            "the gate from serialising workers"
        ),
        rich_help_panel="Memory Tuning Options",
        min=0,
    ),
) -> None:
    """Import one or more Parquet files into SQL Server tables.

    The command reads the manifest file and imports each Parquet file into the
    target table. If the number of workers is greater than 1, the import will be
    done using multiple tables created to the same data schema as the target table.
    """

    configure_logging(verbose)

    if mem_threshold > 0:
        logger.info(
            "Memory throttling enabled: threads will sleep when system "
            f"memory exceeds {mem_threshold:.0f}%% "
            f"(--mem-threshold 0 to disable)"
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
        importer = Importer(
            config=config,
            input_path=manifest_path.parent,
            manifest_filename=manifest_path.name,
            worker_count=worker_count,
            batch_size=batch_size,
            transaction_mode=transaction_mode,
            engine=engine.lower(),
            temp_manifest_filename=temp_manifest_filename,
            create_cci=cci,
            mem_heartbeat=mem_heartbeat,
            mem_threshold=mem_threshold,
            mem_sleep=mem_sleep,
            mem_max_wait=mem_max_wait,
            mem_cooldown=mem_cooldown,
        )
        importer.perform_work()
    except PyButtError as exc:
        typer.secho(f"Import failed: {exc}", fg=typer.colors.RED, err=True)
        raise SystemExit(1) from exc
    typer.secho("Import completed successfully.", fg=typer.colors.GREEN)


@app.command(
    "combine",
    help=(
        "Merge objects listed in a manifest. "
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
        "-e/-n",
        help="Enable or disable SQL Server encrypted transport.",
        rich_help_panel="Server Security Options",
    ),
    output_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-file",
        "-o",
        help="Output Parquet file when merging files.",
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
            "Override the combined manifest filename written during. Defaults to <maneifest-filename>-merged.json."
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
        typer.secho(f"Merge failed: {exc}", fg=typer.colors.RED, err=True)
        raise SystemExit(1) from exc

    if manifest["type"] == "files":
        if output_file is None:
            raise typer.BadParameter("--output-file is required for file manifests")

        try:
            merge_parquet_files(
                manifest_path,
                output_file,
                rowgroup_size,
            )
        except PyButtError as exc:
            typer.secho(f"Merge failed: {exc}", fg=typer.colors.RED, err=True)
            raise SystemExit(1) from exc
        typer.secho("File merge completed successfully.", fg=typer.colors.GREEN)
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
            merger = TableMerger(config=config, sources=manifest["entries"])
            merger.merge(target_schema=schema, target_table=table)
        except PyButtError as exc:
            typer.secho(f"Merge failed: {exc}", fg=typer.colors.RED, err=True)
            raise SystemExit(1) from exc

        # ToDo: Review where this should be, and consider adding user override for path and filename
        new_manifest_name = f"{manifest_path.stem}_merged{manifest_path.suffix}"
        write_manifest(
            manifest_path.parent / new_manifest_name,
            [f"{schema}.{table}"],
            manifest_type="tables",
        )

        typer.secho("Table merge completed successfully.", fg=typer.colors.GREEN)
        return

    raise typer.BadParameter(f"Unsupported manifest type: {manifest['type']}")


@app.command(
    "inspect",
    help=(
        "Inspect Parquet files listed in a manifest. "
        "Shows file-level metadata and optionally column-level details."
    ),
)
def inspect(
    manifest: Path = typer.Argument(  # noqa: B008
        ..., help="Path to the input manifest file"
    ),
    verbose: bool = typer.Option(  # noqa: B008
        False, "--verbose", "-V", help="Show column details"
    ),
):
    """
    Inspect parquet files listed in a manifest.
    """
    inspect_manifest(manifest, verbose)


@app.command(
    "purge",
    help=(
        "Purge Parquet files or SQL tables listed in a manifest. "
        "Also deletes the input manifest file."
    ),
)
def purge(
    manifest: Path = typer.Argument(  # noqa: B008
        ..., help="Path to the input manifest file"
    ),
    verbose: bool = typer.Option(  # noqa: B008
        False, "--verbose", "-V", help="Show column details"
    ),
):
    """
    Inspect parquet files listed in a manifest.
    """


if __name__ == "__main__":
    app()
