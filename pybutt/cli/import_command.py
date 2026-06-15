from pathlib import Path

import typer

from pybutt.cli.app import (
    app,
    build_sql_config,
)
from pybutt.core.config import (
    BATCH_SIZE_DEFAULT,
    CCI_DEFAULT,
    DRIVER_DEFAULT,
    ENCRYPT_DEFAULT,
    IMPORT_ENGINE_DEFAULT,
    MEM_COOLDOWN_DEFAULT,
    MEM_HEARTBEAT_DEFAULT,
    MEM_MAX_WAIT_DEFAULT,
    MEM_SLEEP_DEFAULT,
    MEM_THRESHOLD_DEFAULT,
    PACKET_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    SCHEMA_DEFAULT,
    TRANSACTION_MODE_DEFAULT,
    TRUST_CERT_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
    TransactionMode,
)
from pybutt.core.logobs import configure_logging, get_logger
from pybutt.exceptions import PyButtError
from pybutt.io.importer import Importer

logger = get_logger("cli.import")


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
        "-e",
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
