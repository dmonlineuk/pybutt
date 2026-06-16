import json

import pyarrow.parquet as pq
import pytest

from pybutt.exceptions import DuplicateManifestEntryError, MissingManifestEntryError
from pybutt.files import (
    combine_parquet_files,
    inspect_manifest,
    inspect_parquet_file,
    load_manifest,
    validate_manifest_entries,
)


def test_inspect_parquet_file_basic(tmp_path, create_parquet):
    file_path = create_parquet(tmp_path, "test.parquet", rows=9, rowgroup_size=5)

    info = inspect_parquet_file(file_path)

    assert info["file"] == "test.parquet"
    assert info["rows"] == 9
    assert info["row_groups"] == 2
    assert info["row_group_sizes"] == {5, 4}


def test_inspect_parquet_file_verbose(tmp_path, create_parquet):
    file_path = create_parquet(tmp_path, "test.parquet")

    info = inspect_parquet_file(file_path, verbose=True)

    assert "columns" in info
    assert info["columns"]["id"] == "int64"
    assert info["columns"]["value"] == "string"


def test_inspect_manifest(tmp_path, capsys, create_parquet):
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


def test_merge_parquet_files(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet", rows=3, rowgroup_size=2)
    create_parquet(tmp_path, "b.parquet", rows=2, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(["a.parquet", "b.parquet"]))

    output_file = tmp_path / "merged.parquet"
    combine_parquet_files(manifest, output_file, rowgroup_size=3)

    assert output_file.exists()
    merged = pq.read_table(output_file)
    assert merged.num_rows == 5
    assert list(merged.column_names) == ["id", "value"]


def test_merge_parquet_files_respects_rowgroup_size(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet", rows=3, rowgroup_size=2)
    create_parquet(tmp_path, "b.parquet", rows=2, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(["a.parquet", "b.parquet"]))

    output_file = tmp_path / "merged.parquet"
    combine_parquet_files(manifest, output_file, rowgroup_size=3)

    pf = pq.ParquetFile(output_file)
    assert pf.metadata.num_row_groups == 2
    assert [
        pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)
    ] == [3, 2]


def test_merge_parquet_files_writes_manifest(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet", rows=3, rowgroup_size=2)
    create_parquet(tmp_path, "b.parquet", rows=2, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(["a.parquet", "b.parquet"]))

    output_file = tmp_path / "merged.parquet"
    combine_parquet_files(manifest, output_file, rowgroup_size=3)

    expected_manifest = tmp_path / f"{manifest.stem}_combined{manifest.suffix}"
    assert expected_manifest.exists()
    with open(expected_manifest) as f:
        data = json.load(f)
    assert data == {
        "version": 2,
        "type": "files",
        "entries": [output_file.name],
    }


def test_merge_parquet_files_deletes_sources_and_manifest(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet", rows=3, rowgroup_size=2)
    create_parquet(tmp_path, "b.parquet", rows=2, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(["a.parquet", "b.parquet"]))

    output_file = tmp_path / "merged.parquet"
    combine_parquet_files(manifest, output_file, rowgroup_size=3, delete_originals=True)

    assert output_file.exists()
    assert not (tmp_path / "a.parquet").exists()
    assert not (tmp_path / "b.parquet").exists()
    assert not manifest.exists()


def test_validate_manifest_entries_rejects_duplicates(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet")
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet", "a.parquet"]')

    with pytest.raises(DuplicateManifestEntryError, match="Duplicate file in manifest"):
        validate_manifest_entries(load_manifest(manifest), tmp_path)


def test_load_manifest_legacy_list(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet")
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet"]')

    result = load_manifest(manifest)
    assert result == {
        "version": 1,
        "type": "files",
        "entries": ["a.parquet"],
    }


def test_load_manifest_version1_object(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet")
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"version": 1, "type": "files", "entries": ["a.parquet"]}')

    result = load_manifest(manifest)
    assert result == {
        "version": 1,
        "type": "files",
        "entries": ["a.parquet"],
    }


def test_load_manifest_version2_table_list(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"version": 2, "type": "tables", "entries": ["dbo.MyTable"]}')

    result = load_manifest(manifest)
    assert result == {
        "version": 2,
        "type": "tables",
        "entries": ["dbo.MyTable"],
    }


def test_validate_manifest_entries_rejects_missing_file(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["missing.parquet"]')

    with pytest.raises(MissingManifestEntryError, match="Missing file"):
        validate_manifest_entries(load_manifest(manifest), tmp_path)
