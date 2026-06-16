from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pybutt.exceptions import InvalidManifestError
from pybutt.files.manifest import (
    load_file_manifest,
    validate_manifest_entries,
    write_manifest,
)


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


def combine_parquet_files(
    manifest_path: Path,
    output_file: Path,
    rowgroup_size: int = 1_048_576,
    delete_originals: bool = False,
    new_manifest_name: str | None = None,
):
    """combine all parquet files listed in a manifest into a single Parquet file.

    The resulting file will use the schema of the first file. All subsequent
    files must be schema-compatible (column names/types) or behavior is undefined.
    """
    manifest_path = Path(manifest_path)
    base_dir = manifest_path.parent

    manifest = load_file_manifest(manifest_path, operation="combine")

    entries = validate_manifest_entries(manifest, base_dir)
    if not entries:
        raise InvalidManifestError("Manifest contains no entries to combine")

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

    new_manifest_name = (
        new_manifest_name or f"{manifest_path.stem}_combined{manifest_path.suffix}"
    )
    write_manifest(base_dir / new_manifest_name, [output_file.name])

    return output_file
