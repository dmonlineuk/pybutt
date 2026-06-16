import getpass
import tomllib
from pathlib import Path

import typer

from pybutt.core.config import (
    DRIVER_DEFAULT,
    ENCRYPT_DEFAULT,
    PACKET_SIZE_DEFAULT,
    RETRIES_DEFAULT,
    TRUST_CERT_DEFAULT,
    TRUSTED_CONNECTION_DEFAULT,
    SqlConfig,
)

app = typer.Typer(
    context_settings={"help_option_names": ["-?", "--help"]},
    help="""
PyButt CLI for exporting and importing between MS SQL Server tables and Parquet
files. Can also be used for inspecting Parquet files and combining files or tables
based on manifest definitions.
""",
)


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
    username: str | None,
    password: str | None,
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
        username=username,
        password=password,
        driver=driver,
        trusted_connection=trusted_connection,
        trust_cert=trust_cert,
        encrypt=encrypt,
        retries=retries,
        packet_size=packet_size,
    )
