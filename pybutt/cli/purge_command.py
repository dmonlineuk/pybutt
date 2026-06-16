from pathlib import Path

import typer

from pybutt.cli.app import app, build_sql_config
from pybutt.core.config import (
    DRIVER_DEFAULT,
    ENCRYPT_DEFAULT,
    PACKET_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    TRUST_CERT_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
)
from pybutt.core.logobs import configure_logging, get_logger
from pybutt.exceptions import PyButtError
from pybutt.files import load_manifest
from pybutt.io.purger import TablePurger

logger = get_logger("cli.purge")


@app.command(
    "purge",
    help=(
        "Purge objects listed in a manifest. "
        "For file manifests, deletes each Parquet file then removes the manifest. "
        "For table manifests, drops each SQL table then removes the manifest."
    ),
)
def purge(
    manifest_path: Path = typer.Argument(  # noqa: B008
        ..., help="Path to the input manifest file."
    ),
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
        help="SQL Server host (required for table manifests).",
        rich_help_panel="Server Connection Options",
    ),
    database: str | None = typer.Option(  # noqa: B008
        None,
        "--database",
        "-d",
        help="Target database (required for table manifests).",
        rich_help_panel="Server Connection Options",
    ),
    driver: str = typer.Option(  # noqa: B008
        DRIVER_DEFAULT,
        "--driver",
        "-D",
        help="ODBC driver name.",
        rich_help_panel="Server Connection Options",
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
    """Purge objects listed in a manifest and delete the manifest file."""

    configure_logging(verbose)

    try:
        manifest = load_manifest(manifest_path)
    except PyButtError as exc:
        typer.secho(f"Purge failed: {exc}", fg=typer.colors.RED, err=True)
        raise SystemExit(1) from exc

    if manifest["type"] == "files":
        _purge_files(manifest, manifest_path)
        return

    if manifest["type"] == "tables":
        _purge_tables(
            manifest,
            manifest_path,
            server=server,
            database=database,
            driver=driver,
            username=username,
            password=password,
            trusted_connection=trusted_connection,
            trust_cert=trust_cert,
            encrypt=encrypt,
            retries=retries,
            packet_size=packet_size,
        )
        return

    typer.secho(
        f"Purge failed: unsupported manifest type '{manifest['type']}'",
        fg=typer.colors.RED,
        err=True,
    )
    raise SystemExit(1)


def _purge_files(manifest: dict, manifest_path: Path) -> None:
    """Delete Parquet files listed in the manifest, then delete the manifest."""
    base_dir = manifest_path.parent
    entries = manifest["entries"]
    deleted = 0
    missing = 0

    for entry in entries:
        filepath = base_dir / entry
        if filepath.exists():
            filepath.unlink()
            logger.info(f"Deleted file: {filepath}")
            deleted += 1
        else:
            logger.warning(f"File not found (skipping): {filepath}")
            missing += 1

    manifest_path.unlink()
    logger.info(f"Deleted manifest: {manifest_path}")

    summary = f"Purge complete: {deleted} file(s) deleted"
    if missing:
        summary += f", {missing} file(s) not found (skipped)"
    summary += ", manifest removed."
    typer.secho(summary, fg=typer.colors.GREEN)


def _purge_tables(
    manifest: dict,
    manifest_path: Path,
    *,
    server: str | None,
    database: str | None,
    driver: str,
    username: str | None,
    password: str | None,
    trusted_connection: bool,
    trust_cert: bool,
    encrypt: bool,
    retries: int,
    packet_size: int,
) -> None:
    """Drop SQL tables listed in the manifest, then delete the manifest."""
    if not (server and database):
        raise typer.BadParameter(
            "--server and --database are required for table manifests"
        )

    entries = manifest["entries"]
    if not entries:
        typer.secho("No tables to purge.", fg=typer.colors.YELLOW)
        manifest_path.unlink()
        logger.info(f"Deleted manifest: {manifest_path}")
        return

    config = build_sql_config(
        server=server,
        database=database,
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
        purger = TablePurger(config=config, sources=entries)
        dropped = purger.purge()
    except PyButtError as exc:
        typer.secho(f"Purge failed: {exc}", fg=typer.colors.RED, err=True)
        raise SystemExit(1) from exc

    manifest_path.unlink()
    logger.info(f"Deleted manifest: {manifest_path}")

    typer.secho(
        f"Purge complete: {len(dropped)} table(s) dropped, manifest removed.",
        fg=typer.colors.GREEN,
    )
