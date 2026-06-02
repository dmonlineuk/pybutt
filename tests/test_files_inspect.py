import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pybutt.files.files import (
    inspect_manifest,
    inspect_parquet_file,
)


def create_parquet(tmp_path: Path, name: str, rows: int = 9, rowgroup_size: int = 5):
    data = {
        "id": list(range(rows)),
        "value": [f"v{i}" for i in range(rows)],
    }
    table = pa.Table.from_pydict(data)

    file_path = tmp_path / name
    pq.write_table(table, file_path, row_group_size=rowgroup_size)
    return file_path


def test_inspect_parquet_file_basic(tmp_path):
    file_path = create_parquet(tmp_path, "test.parquet", rows=9, rowgroup_size=5)

    info = inspect_parquet_file(file_path)

    assert info["file"] == "test.parquet"
    assert info["rows"] == 9
    assert info["row_groups"] == 2
    assert info["row_group_sizes"] == {5, 4}


def test_inspect_parquet_file_verbose(tmp_path):
    file_path = create_parquet(tmp_path, "test.parquet")

    info = inspect_parquet_file(file_path, verbose=True)

    assert "columns" in info
    assert info["columns"]["id"] == "int64"
    assert info["columns"]["value"] == "string"


def test_inspect_manifest(tmp_path, capsys):
    # Create two parquet files
    create_parquet(tmp_path, "a.parquet", rows=6, rowgroup_size=3)
    create_parquet(tmp_path, "b.parquet", rows=4, rowgroup_size=2)

    # Create manifest
    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["a.parquet", "b.parquet"], f)

    inspect_manifest(manifest)

    out = capsys.readouterr().out
    assert "a.parquet" in out
    assert "rows: 6" in out
    assert "row groups: 2" in out
    assert "b.parquet" in out
