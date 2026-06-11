from __future__ import annotations

import getpass
import tomllib
from pathlib import Path

import typer

from pybutt.core.config import (
    DEFAULT_MEM_COOLDOWN,
    DEFAULT_MEM_HEARTBEAT,
    DEFAULT_MEM_MAX_WAIT,
    DEFAULT_MEM_SLEEP,
    DEFAULT_MEM_THRESHOLD,
    DEFAULT_PACKET_SIZE,
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
    schema: str,
    table: str,
    username: str | None,
    password: str | None,
    driver: str,
    trusted_connection: bool,
    trust_cert: bool,
    encrypt: bool,
    retries: int,
    packet_size: int = DEFAULT_PACKET_SIZE,
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
        "-c",
        help="Trust the SQL Server TLS certificate.",
    ),
    encrypt: bool = typer.Option(  # noqa: B008
        True,
        "--encrypt/--no-encrypt",
        "-e/-n",
        help="Enable or disable SQL Server encrypted transport.",
    ),
    retries: int = typer.Option(  # noqa: B008
        3,
        "--retries",
        "-r",
        help="Number of retry attempts for transient SQL errors.",
        min=1,
    ),
    packet_size: int = typer.Option(  # noqa: B008
        DEFAULT_PACKET_SIZE,
        "--packet-size",
        help=(
            "TDS packet size in bytes (512\u201332767). "
            f"Default: {DEFAULT_PACKET_SIZE} (max for encrypted connections)."
        ),
        min=512,
        max=32767,
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
        "-C",
        help="Comma-separated list of columns to export. Defaults to all columns.",
    ),
    worker_count: int = typer.Option(  # noqa: B008
        1,
        "--worker-count",
        "-w",
        help="Number of worker processes used for export.",
        min=1,
    ),
    file_count: int | None = typer.Option(  # noqa: B008
        None,
        "--file-count",
        "-f",
        help=(
            "Number of Parquet output files. "
            "Mutually exclusive with --rowgroups-per-file."
        ),
        min=1,
    ),
    rowgroups_per_file: int | None = typer.Option(  # noqa: B008
        None,
        "--rowgroups-per-file",
        help=(
            "Number of rowgroups per output file. The total file count is "
            "derived from total_rows / (rowgroups_per_file × rowgroup_size). "
            "Mutually exclusive with --file-count."
        ),
        min=1,
    ),
    parameters: str | None = typer.Option(
        None,
        "--parameters",
        help=(
            "Comma-separated list of parameter values to pass to a table-valued "
            "function. Example: --parameters 12,'fred','1989'."
        ),
    ),
    rowgroup_size: int = typer.Option(  # noqa: B008
        1_048_576,
        "--rowgroup-size",
        "-R",
        help="Number of rows per rowgroup in the Parquet files.",
        min=1,
    ),
    fetch_size: int | None = typer.Option(  # noqa: B008
        None,
        "--fetch-size",
        "-F",
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
        help="Export engine to use: duckdb, pyodbc, or mssql-python.",
        case_sensitive=False,
    ),
    mem_heartbeat: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_HEARTBEAT,
        "--mem-heartbeat",
        help=(
            "Log process memory (RSS + system %%) every N seconds. "
            f"Default: {DEFAULT_MEM_HEARTBEAT}s. Set to 0 to disable."
        ),
        min=0,
    ),
    mem_threshold: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_THRESHOLD,
        "--mem-threshold",
        help=(
            "System memory %% at which workers are throttled. "
            f"Default: {DEFAULT_MEM_THRESHOLD}%%. "
            "Set to 0 to disable throttling."
        ),
        min=0,
        max=100,
    ),
    mem_sleep: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_SLEEP,
        "--mem-sleep",
        help=(
            "Seconds to sleep per throttle check when memory is high. "
            f"Default: {DEFAULT_MEM_SLEEP}s."
        ),
        min=0.1,
    ),
    mem_max_wait: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_MAX_WAIT,
        "--mem-max-wait",
        help=(
            "Max total seconds to wait during memory throttling before "
            f"giving up. Default: {DEFAULT_MEM_MAX_WAIT}s."
        ),
        min=0,
    ),
    mem_cooldown: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_COOLDOWN,
        "--mem-cooldown",
        help=(
            "Seconds after a throttle event before re-checking. Prevents "
            f"the gate from serialising workers. Default: {DEFAULT_MEM_COOLDOWN}s."
        ),
        min=0,
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

    if file_count is not None and rowgroups_per_file is not None:
        raise typer.BadParameter(
            "--file-count and --rowgroups-per-file are mutually exclusive"
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
            rowgroups_per_file=rowgroups_per_file,
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
        "--output-manifest-filename",
        "-o",
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
        "-c",
        help="Trust the SQL Server TLS certificate.",
    ),
    encrypt: bool = typer.Option(  # noqa: B008
        True,
        "--encrypt/--no-encrypt",
        "-e/-n",
        help="Enable or disable SQL Server encrypted transport.",
    ),
    retries: int = typer.Option(  # noqa: B008
        3,
        "--retries",
        "-r",
        help="Number of retry attempts for transient SQL errors.",
        min=1,
    ),
    packet_size: int = typer.Option(  # noqa: B008
        DEFAULT_PACKET_SIZE,
        "--packet-size",
        help=(
            "TDS packet size in bytes (512\u201332767). "
            f"Default: {DEFAULT_PACKET_SIZE} (max for encrypted connections)."
        ),
        min=512,
        max=32767,
    ),
    worker_count: int = typer.Option(  # noqa: B008
        1,
        "--worker-count",
        "-w",
        help="Number of parallel import threads.",
        min=1,
    ),
    batch_size: int | None = typer.Option(  # noqa: B008
        None,
        "--batch-size",
        "-b",
        help="Rows per batch insert. Default: 1000 (mssql-python: 1048576).",
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
        help="Import engine to use: duckdb, pyodbc, or mssql-python.",
        case_sensitive=False,
    ),
    transaction_mode: TransactionMode = typer.Option(  # noqa: B008
        TransactionMode.BATCH,
        "--transaction-mode",
        "-M",
        help=(
            "Transaction scope: row (no transaction, auto-commit), batch (per batch, "
            "recommended), rowgroup (per row group), file (entire file)."
        ),
    ),
    delete_files: bool = typer.Option(
        False,
        "--delete-files",
        "-x",
        help=("Delete source parquet files and the manifest after import."),
    ),
    cci: bool = typer.Option(
        True,
        "--cci/--no-cci",
        help=(
            "Create a clustered columnstore index on the per-worker temp tables "
            "used during multi-worker import. Use --no-cci to keep the previous "
            "heap behaviour. Enabled by default."
        ),
    ),
    mem_heartbeat: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_HEARTBEAT,
        "--mem-heartbeat",
        help=(
            "Log process memory (RSS + system %%) every N seconds. "
            f"Default: {DEFAULT_MEM_HEARTBEAT}s. Set to 0 to disable."
        ),
        min=0,
    ),
    mem_threshold: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_THRESHOLD,
        "--mem-threshold",
        help=(
            "System memory %% at which workers are throttled. "
            f"Default: {DEFAULT_MEM_THRESHOLD}%%. "
            "Set to 0 to disable throttling."
        ),
        min=0,
        max=100,
    ),
    mem_sleep: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_SLEEP,
        "--mem-sleep",
        help=(
            "Seconds to sleep per throttle check when memory is high. "
            f"Default: {DEFAULT_MEM_SLEEP}s."
        ),
        min=0.1,
    ),
    mem_max_wait: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_MAX_WAIT,
        "--mem-max-wait",
        help=(
            "Max total seconds to wait during memory throttling before "
            f"giving up. Default: {DEFAULT_MEM_MAX_WAIT}s."
        ),
        min=0,
    ),
    mem_cooldown: float = typer.Option(  # noqa: B008
        DEFAULT_MEM_COOLDOWN,
        "--mem-cooldown",
        help=(
            "Seconds after a throttle event before re-checking. Prevents "
            f"the gate from serialising workers. Default: {DEFAULT_MEM_COOLDOWN}s."
        ),
        min=0,
    ),
) -> None:
    """Import Parquet files into a SQL Server table.

    The command reads the manifest file and imports each Parquet file into the
    target table using parameterized batch inserts.
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
            input_path=input_path,
            manifest_filename=manifest_filename,
            worker_count=worker_count,
            batch_size=batch_size,
            transaction_mode=transaction_mode,
            engine=engine.lower(),
            temp_manifest_filename=temp_manifest_filename,
            delete_files=delete_files,
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
    "merge",
    help=(
        "Merge objects listed in a manifest. "
        "For file manifests, concatenate Parquet files to a single output. "
        "For table manifests, merge SQL tables into a single target table."
    ),
)
def merge(
    manifest_path: Path = typer.Option(  # noqa: B008
        ..., "--manifest", "-m", help="Path to manifest file.", exists=True
    ),
    # File merge options
    output_file: Path | None = typer.Option(  # noqa: B008
        None,
        "--output-file",
        "-o",
        help="Output Parquet file when merging files.",
        file_okay=True,
        dir_okay=False,
    ),
    delete_files: bool = typer.Option(
        False,
        "--delete-files",
        "-x",
        help=("Delete source parquet files and the manifest after merging."),
    ),
    rowgroup_size: int = typer.Option(  # noqa: B008
        1_048_576, "--rowgroup-size", "-R", help="Rowgroup size for output."
    ),
    # SQL merge options (target)
    server: str | None = typer.Option(
        None, "--server", "-s", help="SQL Server host."
    ),  # noqa: B008
    database: str | None = typer.Option(  # noqa: B008
        None, "--database", "-d", help="Target database."
    ),
    schema: str | None = typer.Option(
        None, "--schema", "-S", help="Target schema."
    ),  # noqa: B008
    table: str | None = typer.Option(
        None, "--table", "-t", help="Target table."
    ),  # noqa: B008
    trusted_connection: bool = typer.Option(
        False, "--trusted-connection", "-T"
    ),  # noqa: B008
    username: str | None = typer.Option(None, "--username", "-u"),  # noqa: B008
    password: str | None = typer.Option(None, "--password", "-p"),  # noqa: B008
    driver: str = typer.Option(  # noqa: B008
        "ODBC Driver 18 for SQL Server", "--driver", "-D", help="ODBC driver name."
    ),
    trust_cert: bool = typer.Option(False, "--trust-cert", "-c"),  # noqa: B008
    encrypt: bool = typer.Option(True, "--encrypt/--no-encrypt", "-e/-n"),  # noqa: B008
    retries: int = typer.Option(3, "--retries", "-r", min=1),  # noqa: B008
    packet_size: int = typer.Option(  # noqa: B008
        DEFAULT_PACKET_SIZE,
        "--packet-size",
        help=(
            "TDS packet size in bytes (512–32767). "
            f"Default: {DEFAULT_PACKET_SIZE} (max for encrypted connections)."
        ),
        min=512,
        max=32767,
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),  # noqa: B008
) -> None:
    """Merge manifest entries according to manifest type."""

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
                delete_originals=delete_files,
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

        new_manifest_name = f"{manifest_path.stem}_merged{manifest_path.suffix}"
        write_manifest(
            manifest_path.parent / new_manifest_name,
            [f"{schema}.{table}"],
            manifest_type="tables",
        )

        if delete_files and manifest_path.exists():
            manifest_path.unlink()
        typer.secho("Table merge completed successfully.", fg=typer.colors.GREEN)
        return

    raise typer.BadParameter(f"Unsupported manifest type: {manifest['type']}")


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
        None, "--output-path", "-o", help="Output directory"
    ),
    rowgroup_size: int = typer.Option(..., "--rowgroup-size", "-R"),  # noqa: B008
    new_manifest: str | None = typer.Option(  # noqa: B008
        None,
        "--new-manifest",
        "-m",
        help=(
            "Optional filename for the rewritten manifest. Defaults to "
            "<manifest>_new.json based on the original manifest name."
        ),
    ),
    delete_files: bool = typer.Option(
        False,
        "--delete-files",
        "-x",
        help=("Delete source parquet files and the manifest after merge."),
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
        delete_originals=delete_files,
    )


if __name__ == "__main__":
    app()
