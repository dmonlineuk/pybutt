import json
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import (
    SqlConfig,
    quote_identifier,
    validate_identifier,
)
from pybutt.exceptions import InvalidIdentifierError, MissingManifestEntryError
from pybutt.io.exporter import Exporter
from pybutt.io.importer import Importer


def test_validate_identifier_valid():
    assert validate_identifier("valid_name_1") == "valid_name_1"


def test_validate_identifier_invalid():
    with pytest.raises(InvalidIdentifierError, match="Invalid identifier"):
        validate_identifier("123invalid")


def test_quote_identifier_escapes_brackets():
    assert quote_identifier("name]with]brackets") == "[name]]with]]brackets]"


def test_sqlserverio_base_builds_dsn_for_trusted_connection():
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
        trust_cert=True,
        encrypt=True,
        driver="ODBC Driver 18 for SQL Server",
    )
    base = SqlServerIOBase(config)
    assert "Driver={ODBC Driver 18 for SQL Server}" in base.dsn
    assert "Server=localhost" in base.dsn
    assert "Database=TestDb" in base.dsn
    assert "Trusted_Connection=Yes" in base.dsn
    assert "TrustServerCertificate=Yes" in base.dsn


def test_sqlserverio_base_builds_dsn_for_username_password():
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        username="user",
        password="secret",
        trusted_connection=False,
        trust_cert=False,
        encrypt=False,
        driver="ODBC Driver 18 for SQL Server",
    )
    base = SqlServerIOBase(config)
    assert "Uid=user" in base.dsn
    assert "Pwd=***" not in base.dsn
    assert "TrustServerCertificate=No" in base.dsn
    assert "Encrypt=Yes" not in base.dsn


def test_exporter_invalid_file_count(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)

    with pytest.raises(ValueError, match="file_count must be at least 1"):
        Exporter(config=config, output_path=Path("./out"), worker_count=1, file_count=0)


def test_exporter_invalid_engine(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)

    with pytest.raises(ValueError, match="engine must be one of"):
        Exporter(
            config=config,
            output_path=Path("./out"),
            worker_count=1,
            file_count=1,
            engine="invalid",
        )


def test_exporter_fetch_size_default(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
    )

    assert exporter.fetch_size == 8192


def test_exporter_fetch_size_override(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
        fetch_size=16_384,
    )

    assert exporter.fetch_size == 16_384


def test_pyodbc_export_buffers_rows_for_parquet_rowgroups(monkeypatch, tmp_path):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config,
        output_path=tmp_path,
        worker_count=1,
        file_count=1,
        rowgroup_size=2,
        fetch_size=1,
        engine="pyodbc",
    )

    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("col1",)]
    mock_cursor.execute.return_value = None
    rows = [(1,), (2,), (3,), (4,)]
    mock_cursor.fetchmany.side_effect = [[rows[0]], [rows[1]], [rows[2]], [rows[3]], []]
    mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    exporter.connection_p = lambda *args, **kwargs: mock_connection

    exporter._export_partition_with_pyodbc(
        "SELECT col1 FROM dbo.MyTable",
        tmp_path / "test.parquet",
        "test.parquet",
    )

    parquet_file = pq.ParquetFile(tmp_path / "test.parquet")
    assert parquet_file.num_row_groups == 2
    assert parquet_file.metadata.num_rows == 4


def test_importer_invalid_engine():
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    with pytest.raises(ValueError, match="engine must be one of"):
        Importer(
            config=config,
            input_path=Path("./data"),
            manifest_filename="manifest.json",
            engine="invalid",
        )


def test_exporter_build_partition_query_without_pk(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config, output_path=Path("./out"), worker_count=1, file_count=4
    )
    exporter.partition_count = 4
    exporter.chunk_size = 25
    query = exporter.build_partition_query(2)
    assert "WHERE ABS(CHECKSUM(*)) % 4 = 2" in query
    assert "SELECT *" in query


def test_exporter_build_partition_query_with_pk(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        pk_column="id",
        columns=["id", "name"],
        worker_count=1,
        file_count=4,
    )
    exporter.partition_count = 4
    exporter.chunk_size = 25
    exporter.get_table_columns = lambda: ["id", "name"]
    query = exporter.build_partition_query(1)
    assert "ROW_NUMBER() OVER (ORDER BY [id])" in query
    assert "WHERE rn > 25 AND rn <= 50" in query
    assert "SELECT [id], [name]" in query


def test_importer_load_manifest(tmp_path):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )
    input_path = tmp_path / "data"
    input_path.mkdir()
    manifest_path = input_path / "manifest.json"

    files = ["part_00000.parquet", "part_00001.parquet"]
    for name in files:
        (input_path / name).write_text("empty")
    manifest_path.write_text(str(files).replace("'", '"'))

    importer = Importer(
        config=config,
        input_path=input_path,
        manifest_filename="manifest.json",
    )
    result = importer.load_manifest()
    assert result == {
        "version": 1,
        "type": "files",
        "entries": files,
    }


def test_importer_load_manifest_missing_file(tmp_path):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )
    input_path = tmp_path / "data"
    input_path.mkdir()
    manifest_path = input_path / "manifest.json"
    manifest_path.write_text('["missing.parquet"]')

    importer = Importer(
        config=config,
        input_path=input_path,
        manifest_filename="manifest.json",
    )
    with pytest.raises(MissingManifestEntryError, match="Missing file"):
        importer.load_manifest_entries()


def test_importer_make_temp_table_name_local():
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="users",
        trusted_connection=True,
    )
    importer = Importer(
        config=config,
        input_path=Path("./data"),
        manifest_filename="manifest.json",
        use_tempdb=False,
    )
    name = importer._make_temp_table_name(0)
    assert name.startswith("dbo.users_01_")
    assert name.count(".") == 1


def test_importer_validate_schema_mismatch():
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )
    importer = Importer(
        config=config,
        input_path=Path("./data"),
        manifest_filename="manifest.json",
    )
    with pytest.raises(ValueError, match="Schema mismatch"):
        importer.validate_schema(["a", "b"], ["b", "c"], "part_00000.parquet")


def test_importer_write_temp_manifest(tmp_path):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )
    input_path = tmp_path / "data"
    input_path.mkdir()
    importer = Importer(
        config=config,
        input_path=input_path,
        manifest_filename="manifest.json",
    )

    temp_tables = ["##dbo_MyTable_01_abcd1234", "##dbo_MyTable_02_efgh5678"]
    manifest_file = importer._write_temp_manifest(temp_tables)

    assert manifest_file.exists()
    assert manifest_file.parent == input_path
    with open(manifest_file) as f:
        data = json.load(f)

    assert data == {
        "version": 2,
        "type": "tables",
        "entries": temp_tables,
    }


def test_importer_perform_work_with_multiple_workers(monkeypatch, tmp_path):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )
    input_path = tmp_path / "data"
    input_path.mkdir()
    filenames = ["a.parquet", "b.parquet", "c.parquet"]
    for name in filenames:
        (input_path / name).write_text("empty")
    manifest_path = input_path / "manifest.json"
    manifest_path.write_text(json.dumps(filenames))

    importer = Importer(
        config=config,
        input_path=input_path,
        manifest_filename="manifest.json",
        worker_count=2,
    )

    created = ["##dbo_MyTable_01_abcd1234", "##dbo_MyTable_02_efgh5678"]
    monkeypatch.setattr(importer, "_create_temp_tables", lambda count: created)

    calls = {}

    def fake_import_files_to_temp_table(target_table, assigned):
        calls[target_table] = assigned

    monkeypatch.setattr(
        importer, "_import_files_to_temp_table", fake_import_files_to_temp_table
    )

    manifest_written = {}

    def fake_write_temp_manifest(temp_tables):
        manifest_written["tables"] = temp_tables
        return input_path / "dummy.json"

    monkeypatch.setattr(importer, "_write_temp_manifest", fake_write_temp_manifest)

    importer.perform_work()

    assert calls == {
        "##dbo_MyTable_01_abcd1234": ["a.parquet", "c.parquet"],
        "##dbo_MyTable_02_efgh5678": ["b.parquet"],
    }
    assert manifest_written["tables"] == created
