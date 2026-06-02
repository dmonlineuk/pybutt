import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pybutt.exceptions import (
    DuplicateManifestEntryError,
    InvalidManifestEntryError,
    InvalidManifestError,
    ManifestNotFoundError,
    MissingManifestEntryError,
)


def load_manifest(manifest_path: str | Path) -> list[str]:
    manifest_path = Path(manifest_path)

    if not manifest_path.exists():
        raise ManifestNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise InvalidManifestError("Manifest must be a list of filenames")

    return data


def validate_manifest_entries(files: list[str], base_dir: Path) -> list[str]:
    seen = set()
    validated = []

    for item in files:
        if not isinstance(item, str):
            raise InvalidManifestEntryError(
                f"Invalid manifest entry (not string): {item}"
            )

        if item in seen:
            raise DuplicateManifestEntryError(f"Duplicate file in manifest: {item}")

        filepath = base_dir / item
        if not filepath.exists():
            raise MissingManifestEntryError(f"Missing file: {filepath}")

        seen.add(item)
        validated.append(item)

    return validated


def inspect_parquet_file(filepath: Path, verbose: bool = False) -> dict:
    pf = pq.ParquetFile(filepath)

    info = {
        "file": filepath.name,
        "rows": pf.metadata.num_rows,
        "row_groups": pf.metadata.num_row_groups,
        "row_group_sizes": {
            pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)
        },
    }

    if verbose:
        schema = pf.schema_arrow
        info["columns"] = {field.name: str(field.type) for field in schema}

    return info


def inspect_manifest(manifest_path: str | Path, verbose: bool = False):
    manifest_path = Path(manifest_path)
    base_dir = manifest_path.parent

    files = load_manifest(manifest_path)

    for filename in files:
        filepath = base_dir / filename
        if not filepath.exists():
            print(f"Missing file: {filepath}")
            continue

        info = inspect_parquet_file(filepath, verbose=verbose)

        print(info["file"])
        print(f"  rows: {info['rows']}")
        print(f"  row groups: {info['row_groups']}")
        print(f"  group sizes: {info['row_group_sizes']}")

        if verbose:
            print("  columns:")
            for col, typ in info["columns"].items():
                print(f"    {col}: {typ}")

        print()


def rewrite_single_file(
    src_path: Path,
    dst_path: Path,
    new_rowgroup_size: int,
):
    """
    Rewrite a single parquet file with a new row-group size.
    Streaming: does not load entire file into memory.
    """
    pf = pq.ParquetFile(src_path)

    # Create writer using the same schema
    with pq.ParquetWriter(dst_path, pf.schema_arrow, compression="snappy") as writer:

        for batch in pf.iter_batches(batch_size=new_rowgroup_size):
            table = pa.Table.from_batches([batch])
            writer.write_table(table)


def rewrite_parquet_files(
    manifest_path: Path,
    output_dir: Path | None,
    new_rowgroup_size: int,
    new_manifest_name: str,
    delete_originals: bool = False,
):
    """
    Rewrite all parquet files listed in a manifest with a new row-group size.
    """
    manifest_path = Path(manifest_path)
    base_dir = manifest_path.parent

    if output_dir is None:
        output_dir = base_dir

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = load_manifest(manifest_path)

    new_files = []

    for filename in files:
        src = base_dir / filename
        dst_name = f"{src.stem}_new{src.suffix}"
        dst = output_dir / dst_name

        rewrite_single_file(src, dst, new_rowgroup_size)
        new_files.append(dst.name)

        if delete_originals:
            src.unlink()

    # Write new manifest
    new_manifest_path = output_dir / new_manifest_name
    with open(new_manifest_path, "w") as f:
        json.dump(new_files, f, indent=4)

    return new_manifest_path


if __name__ == "__main__":
    pass
