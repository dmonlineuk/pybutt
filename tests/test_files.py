import json

import pyarrow.parquet as pq
import pytest

from pybutt.exceptions import DuplicateManifestEntryError
from pybutt.files.files import (
    inspect_manifest,
    inspect_parquet_file,
    load_manifest,
    rewrite_parquet_files,
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


def test_rewrite_default_outdir(tmp_path, create_parquet):
    # Create original parquet
    create_parquet(tmp_path, "data.parquet", rows=12, rowgroup_size=3)

    # Create manifest
    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["data.parquet"], f)

    # Rewrite without specifying outdir
    new_manifest = rewrite_parquet_files(
        manifest_path=manifest,
        output_dir=None,
        new_rowgroup_size=6,
        new_manifest_name="manifest_new.json",
        delete_originals=False,
    )

    # Check new file exists
    new_file = tmp_path / "data_new.parquet"
    assert new_file.exists()

    # Check manifest content
    with open(new_manifest) as f:
        files = json.load(f)
    assert files == ["data_new.parquet"]

    # Validate rowgroup size
    pf = pq.ParquetFile(new_file)
    assert pf.metadata.num_row_groups == 2


def test_validate_manifest_entries_rejects_duplicates(tmp_path, create_parquet):
    create_parquet(tmp_path, "a.parquet")
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet", "a.parquet"]')

    with pytest.raises(DuplicateManifestEntryError, match="Duplicate file in manifest"):
        validate_manifest_entries(load_manifest(manifest), tmp_path)


def test_validate_manifest_entries_rejects_missing_file(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["missing.parquet"]')

    with pytest.raises(FileNotFoundError, match="Missing file"):
        validate_manifest_entries(load_manifest(manifest), tmp_path)
