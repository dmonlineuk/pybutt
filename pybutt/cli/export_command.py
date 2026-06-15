from pathlib import Path

import typer

from pybutt.cli.app import (
    app,
    build_sql_config,
    parse_columns,
)
from pybutt.core.config import (
    DRIVER_DEFAULT,
    ENCRYPT_DEFAULT,
    EXPORT_ENGINE_DEFAULT,
    FETCH_SIZE_DEFAULT,
    MEM_COOLDOWN_DEFAULT,
    MEM_HEARTBEAT_DEFAULT,
    MEM_MAX_WAIT_DEFAULT,
    MEM_SLEEP_DEFAULT,
    MEM_THRESHOLD_DEFAULT,
    PACKET_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    ROWGROUP_SIZE_DEFAULT,
    SCHEMA_DEFAULT,
    TRUST_CERT_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
)
from pybutt.core.logobs import configure_logging, get_logger
from pybutt.exceptions import PyButtError
from pybutt.io.exporter import Exporter

logger = get_logger("cli")


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
        "-e",
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
        "-a",
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
