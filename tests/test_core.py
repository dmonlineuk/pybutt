from pathlib import Path

import pytest

from pybutt.core import (
    Exporter,
    Importer,
    SqlConfig,
    SqlServerIOBase,
    quote_identifier,
    validate_identifier,
)


def test_validate_identifier_valid():
    assert validate_identifier("valid_name_1") == "valid_name_1"


def test_validate_identifier_invalid():
    with pytest.raises(ValueError, match="Invalid identifier"):
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
    assert result == files


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
    with pytest.raises(FileNotFoundError, match="Missing file"):
        importer.load_manifest()


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
