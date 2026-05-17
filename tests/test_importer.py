import pytest
import json
from pathlib import Path
from pybutt.core import Importer, SqlConfig


def make_importer(tmp_path):
    cfg = SqlConfig(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        username="user",
        password="pass",
    )

    return Importer(
        config=cfg,
        input_path=tmp_path,
        manifest_filename="manifest.json"
    )


# ------------------------------------------------------------
# ✅ MANIFEST
# ------------------------------------------------------------

def test_manifest_valid(tmp_path):
    files = ["a.parquet", "b.parquet"]

    for f in files:
        (tmp_path / f).write_text("x")

    (tmp_path / "manifest.json").write_text(json.dumps(files))

    imp = make_importer(tmp_path)

    result = imp.load_manifest()
    assert result == files


def test_manifest_missing_file(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps(["missing.parquet"]))

    imp = make_importer(tmp_path)

    with pytest.raises(FileNotFoundError):
        imp.load_manifest()


def test_manifest_duplicate(tmp_path):
    files = ["a.parquet", "a.parquet"]

    (tmp_path / "a.parquet").write_text("x")
    (tmp_path / "manifest.json").write_text(json.dumps(files))

    imp = make_importer(tmp_path)

    with pytest.raises(ValueError):
        imp.load_manifest()


# ------------------------------------------------------------
# ✅ IMPORT FILE
# ------------------------------------------------------------

def test_import_file(monkeypatch, tmp_path):

    file = tmp_path / "file.parquet"
    file.write_text("fake")

    (tmp_path / "manifest.json").write_text('["file.parquet"]')

    class FakeBatch:
        columns = []
        def to_pylist(self): return []

    class FakeTable:
        def to_batches(self, max_chunksize):
            return []

    class FakeParquet:
        num_row_groups = 1
        schema = type("S", (), {"names": ["col1", "col2"]})

        def read_row_group(self, *_):
            return FakeTable()

    monkeypatch.setattr(
        "pybutt.core.pq.ParquetFile",
        lambda *_: FakeParquet()
    )

    imp = make_importer(tmp_path)
    assert imp.import_file("file.parquet") is True


# ------------------------------------------------------------
# ✅ PERFORM WORK
# ------------------------------------------------------------

def test_import_perform(monkeypatch, tmp_path):

    files = ["f1.parquet", "f2.parquet"]

    for f in files:
        (tmp_path / f).write_text("x")

    (tmp_path / "manifest.json").write_text(json.dumps(files))

    calls = {"count": 0}

    def fake_import(self, f):
        calls["count"] += 1
        return True

    monkeypatch.setattr(Importer, "import_file", fake_import)

    imp = make_importer(tmp_path)
    imp.perform_work()

    assert calls["count"] == 2