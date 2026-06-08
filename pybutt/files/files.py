import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pybutt.core.config import validate_identifier
from pybutt.exceptions import (
    DuplicateManifestEntryError,
    InvalidManifestEntryError,
    InvalidManifestError,
    ManifestNotFoundError,
    MissingManifestEntryError,
    PathTraversalError,
    UnsupportedManifestTypeError,
    UnsupportedManifestVersionError,
)

MANIFEST_VERSION_1 = 1
MANIFEST_VERSION_2 = 2
SUPPORTED_MANIFEST_TYPES = frozenset({"files", "tables"})


def _parse_manifest_dict(data):
    if not isinstance(data, dict):
        raise InvalidManifestError(
            "Manifest must be a list or an object with version, type, and entries"
        )

    version = data.get("version")
    if version not in {MANIFEST_VERSION_1, MANIFEST_VERSION_2}:
        raise UnsupportedManifestVersionError(
            f"Unsupported manifest version: {version}"
        )

    manifest_type = data.get("type")
    if version == MANIFEST_VERSION_1:
        manifest_type = manifest_type or "files"
        if manifest_type != "files":
            raise UnsupportedManifestTypeError(
                "Version 1 manifests support only type 'files'"
            )
    else:
        if manifest_type not in SUPPORTED_MANIFEST_TYPES:
            raise InvalidManifestError(
                "Manifest type must be 'files' or 'tables' for version 2"
            )

    entries = data.get("entries")
    if not isinstance(entries, list):
        raise InvalidManifestError("Manifest entries must be a list")

    if manifest_type == "tables":
        return {
            "version": version,
            "type": manifest_type,
            "entries": [_validate_table_name(e) for e in entries],
        }

    return {"version": version, "type": manifest_type, "entries": entries}


def _validate_table_name(value):
    if not isinstance(value, str):
        raise InvalidManifestEntryError(
            f"Invalid manifest table entry (not string): {value}"
        )

    parts = value.split(".")
    if len(parts) != 2:
        raise InvalidManifestEntryError(
            f"Invalid table name format, expected schema.table: {value}"
        )

    schema, table = parts
    validate_identifier(schema)
    validate_identifier(table)
    return value


def default_manifest_filename(schema: str, table: str, suffix: str = "manifest") -> str:
    return f"{schema}_{table}_{suffix}.json"


def default_temp_manifest_filename(schema: str, table: str) -> str:
    return f"{schema}_{table}_temp_manifest.json"


def default_rewrite_manifest_filename(manifest_path: Path) -> str:
    manifest_path = Path(manifest_path)
    return f"{manifest_path.stem}_new{manifest_path.suffix}"


def load_manifest(manifest_path: str | Path) -> dict:
    manifest_path = Path(manifest_path)

    if not manifest_path.exists():
        raise ManifestNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path) as f:
        data = json.load(f)

    if isinstance(data, list):
        return {"version": MANIFEST_VERSION_1, "type": "files", "entries": data}

    return _parse_manifest_dict(data)


def validate_manifest_entries(manifest: dict, base_dir: Path) -> list[str]:
    seen = set()
    validated = []

    for item in manifest["entries"]:
        if not isinstance(item, str):
            raise InvalidManifestEntryError(
                f"Invalid manifest entry (not string): {item}"
            )

        if item in seen:
            raise DuplicateManifestEntryError(f"Duplicate file in manifest: {item}")

        if manifest["type"] == "files":
            filepath = (base_dir / item).resolve()
            if not filepath.is_relative_to(base_dir.resolve()):
                raise PathTraversalError(
                    f"Manifest entry escapes base directory: {item}"
                )
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

    manifest = load_manifest(manifest_path)
    if manifest["type"] != "files":
        raise UnsupportedManifestTypeError(
            f"Inspect only supports file manifests, got: {manifest['type']}"
        )

    for filename in manifest["entries"]:
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
    new_manifest_name: str | None = None,
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

    manifest = load_manifest(manifest_path)
    if manifest["type"] != "files":
        raise UnsupportedManifestTypeError(
            f"Rewrite only supports file manifests, got: {manifest['type']}"
        )

    new_manifest_name = new_manifest_name or default_rewrite_manifest_filename(
        manifest_path
    )

    new_files = []

    for filename in validate_manifest_entries(manifest, base_dir):
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
        json.dump(
            {
                "version": MANIFEST_VERSION_2,
                "type": "files",
                "entries": new_files,
            },
            f,
            indent=4,
        )

    if (
        delete_originals
        and manifest_path.exists()
        and manifest_path != new_manifest_path
    ):
        manifest_path.unlink()

    return new_manifest_path


def _write_table_chunks(writer, table, rowgroup_size: int):
    if table.num_rows < rowgroup_size:
        return table

    offset = 0
    while offset + rowgroup_size <= table.num_rows:
        chunk = table.slice(offset, rowgroup_size)
        writer.write_table(chunk, row_group_size=rowgroup_size)
        offset += rowgroup_size

    if offset < table.num_rows:
        return table.slice(offset)

    return None


def merge_parquet_files(
    manifest_path: Path,
    output_file: Path,
    rowgroup_size: int = 1_048_576,
    delete_originals: bool = False,
    new_manifest_name: str | None = None,
):
    """Merge all parquet files listed in a manifest into a single Parquet file.

    The resulting file will use the schema of the first file. All subsequent
    files must be schema-compatible (column names/types) or behavior is undefined.
    """
    manifest_path = Path(manifest_path)
    base_dir = manifest_path.parent

    manifest = load_manifest(manifest_path)
    if manifest["type"] != "files":
        raise UnsupportedManifestTypeError(
            f"Merge only supports file manifests, got: {manifest['type']}"
        )

    entries = validate_manifest_entries(manifest, base_dir)
    if not entries:
        raise InvalidManifestError("Manifest contains no entries to merge")

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Use schema from first file
    first_path = base_dir / entries[0]
    first_pf = pq.ParquetFile(first_path)
    schema = first_pf.schema_arrow

    with pq.ParquetWriter(output_file, schema, compression="snappy") as writer:
        buffered_table = None

        for entry in entries:
            src = base_dir / entry
            pf = pq.ParquetFile(src)
            # If schema differs, let pyarrow handle or raise downstream
            for batch in pf.iter_batches():
                table = pa.Table.from_batches([batch])
                if buffered_table is None:
                    buffered_table = table
                else:
                    buffered_table = pa.concat_tables([buffered_table, table])

                buffered_table = _write_table_chunks(
                    writer, buffered_table, rowgroup_size
                )

        if buffered_table is not None and buffered_table.num_rows > 0:
            writer.write_table(buffered_table, row_group_size=rowgroup_size)

    if delete_originals:
        for entry in entries:
            src = base_dir / entry
            if src.exists() and src != output_file:
                src.unlink()
        if manifest_path.exists():
            manifest_path.unlink()

    # Write a manifest for the merged output (single entry)
    new_manifest_name = (
        new_manifest_name or f"{manifest_path.stem}_merged{manifest_path.suffix}"
    )
    new_manifest_path = base_dir / new_manifest_name
    with open(new_manifest_path, "w") as f:
        json.dump(
            {
                "version": MANIFEST_VERSION_2,
                "type": "files",
                "entries": [output_file.name],
            },
            f,
            indent=4,
        )

    return output_file


if __name__ == "__main__":
    pass
