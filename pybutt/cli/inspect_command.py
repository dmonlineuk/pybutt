from pathlib import Path

import typer

from pybutt.cli.app import app
from pybutt.files.files import inspect_manifest


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
