from pathlib import Path

import typer

from pybutt.cli.app import app
from pybutt.core.logobs import get_logger

logger = get_logger("cli.purge")


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
        False,
        "--verbose",
        "-V",
        help="Show verbose logging output.",
    ),
):
    """
    Purge parquet files or SQL tables listed in a manifest. Also
    deletes the input manifest file.
    """
