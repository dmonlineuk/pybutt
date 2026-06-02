import json
from pathlib import Path

import pyarrow.parquet as pq


def load_manifest(manifest_path: str | Path) -> list[str]:
    manifest_path = Path(manifest_path)

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Manifest must be a list of filenames")

    return data


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
