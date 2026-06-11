import json
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import (
    DEFAULT_IMPORT_BATCH_SIZE,
    SqlConfig,
    quote_identifier,
    resolve_engine_default,
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


def test_exporter_source_reference_with_parameters(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="export",
        table="tvf_users",
        trusted_connection=True,
    )

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
        parameters="12,'fred','1989'",
    )

    assert exporter._source_reference() == "[export].[tvf_users](12,'fred','1989')"
    exporter.partition_count = 3
    query = exporter.build_partition_query(1)
    assert "FROM [export].[tvf_users](12,'fred','1989')" in query


def test_exporter_partition_meta_falls_back_to_count(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyView",
        trusted_connection=True,
    )

    class DummyResult:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return (self._value,)

    class DummyConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query):
            if "sys.dm_db_partition_stats" in query:
                return DummyResult(0)
            if "SELECT COUNT(*)" in query:
                return DummyResult(42)
            raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(Exporter, "connection_d", lambda self: DummyConnection())
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
    )

    assert exporter.total_rows == 42


def test_exporter_partition_meta_uses_parameters_for_count(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="export",
        table="tvf_users",
        trusted_connection=True,
    )

    class DummyResult:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return (self._value,)

    class DummyConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query):
            if "sys.dm_db_partition_stats" in query:
                return DummyResult(0)
            if "SELECT COUNT(*)" in query:
                assert "FROM [export].[tvf_users](12,'fred','1989')" in query
                return DummyResult(42)
            raise AssertionError(f"Unexpected query: {query}")

    monkeypatch.setattr(Exporter, "connection_d", lambda self: DummyConnection())
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
        parameters="12,'fred','1989'",
    )

    assert exporter.total_rows == 42


def test_exporter_writes_manifest_version_2(monkeypatch, tmp_path):
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
        rowgroup_size=1_048_576,
    )
    exporter.partition_count = 1
    exporter.export_partition = lambda n: "dbo_MyTable_part_00000.parquet"

    class _AsyncResult:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

    class DummyPool:
        _pool = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map_async(self, func, args):
            return _AsyncResult([func(arg) for arg in args])

    class DummyContext:
        def Pool(self, count, **kwargs):
            return DummyPool()

    monkeypatch.setattr("pybutt.io.exporter.get_context", lambda _: DummyContext())

    exporter.perform_work()

    manifest_file = tmp_path / exporter.manifest_filename
    assert manifest_file.exists()
    with open(manifest_file) as f:
        manifest_data = json.load(f)

    assert manifest_data["version"] == 2
    assert manifest_data["type"] == "files"
    assert manifest_data["entries"] == ["dbo_MyTable_part_00000.parquet"]


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


def test_duckdb_export_uses_rowgroup_size(tmp_path, monkeypatch):
    import duckdb

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
        rowgroup_size=7,
        engine="duckdb",
    )

    conn = duckdb.connect(database=":memory:")
    conn.execute(
        "CREATE TABLE test AS "
        "SELECT range::INT64 AS id, CAST(range AS VARCHAR) AS value "
        "FROM range(18)"
    )
    reader = conn.execute("SELECT * FROM test").arrow()

    exporter._write_parquet_from_record_batches(
        reader, tmp_path / "test.parquet", "test.parquet"
    )

    parquet_file = pq.ParquetFile(tmp_path / "test.parquet")
    assert parquet_file.num_row_groups == 3
    assert [
        parquet_file.metadata.row_group(i).num_rows
        for i in range(parquet_file.num_row_groups)
    ] == [7, 7, 4]


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


def test_importer_default_manifest_filename():
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
        manifest_filename=None,
    )
    assert importer.manifest_filename == "dbo_MyTable_manifest.json"


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
    )
    name = importer._make_temp_table_name(0)
    assert name.startswith("dbo.users_01_")
    assert name.count(".") == 1


def test_importer_make_columnstore_index_name():
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
    )
    index_name = importer._make_columnstore_index_name("dbo.users_01_abcd1234")
    assert index_name == "[cci_users_01_abcd1234]"


def _make_fake_connection():
    fake_cursor = MagicMock()
    fake_connection = MagicMock()
    fake_connection.cursor.return_value.__enter__.return_value = fake_cursor
    fake_connection.__enter__.return_value = fake_connection
    return fake_connection, fake_cursor


def test_create_temp_tables_creates_columnstore_index_by_default(monkeypatch):
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

    fake_connection, fake_cursor = _make_fake_connection()
    monkeypatch.setattr(
        importer, "connection_p", lambda autocommit=False: fake_connection
    )

    temp_tables = importer._create_temp_tables(1)

    statements = [call.args[0] for call in fake_cursor.execute.call_args_list]
    assert any(stmt.startswith("SELECT TOP 0 *") for stmt in statements)
    cci_statements = [
        stmt
        for stmt in statements
        if stmt.startswith("CREATE CLUSTERED COLUMNSTORE INDEX")
    ]
    assert len(cci_statements) == 1
    assert temp_tables[0] in cci_statements[0]
    assert "[cci_" in cci_statements[0]


def test_create_temp_tables_skips_columnstore_index_when_disabled(monkeypatch):
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
        create_cci=False,
    )

    fake_connection, fake_cursor = _make_fake_connection()
    monkeypatch.setattr(
        importer, "connection_p", lambda autocommit=False: fake_connection
    )

    importer._create_temp_tables(2)

    statements = [call.args[0] for call in fake_cursor.execute.call_args_list]
    assert all(
        not stmt.startswith("CREATE CLUSTERED COLUMNSTORE INDEX") for stmt in statements
    )


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


def test_importer_perform_work_deletes_original_files_and_manifest(
    monkeypatch, tmp_path
):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )
    input_path = tmp_path / "data"
    input_path.mkdir()
    filenames = ["a.parquet", "b.parquet"]
    for name in filenames:
        (input_path / name).write_text("empty")
    manifest_path = input_path / "manifest.json"
    manifest_path.write_text(json.dumps(filenames))

    importer = Importer(
        config=config,
        input_path=input_path,
        manifest_filename="manifest.json",
        delete_files=True,
    )

    monkeypatch.setattr(
        importer, "import_file", lambda filename, target_table=None: None
    )

    importer.perform_work()

    assert not any((input_path / name).exists() for name in filenames)
    assert not manifest_path.exists()


def test_importer_multi_worker_defaults_to_local_temp_tables(monkeypatch, tmp_path):
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
        worker_count=2,
    )

    imported = []
    created_tables = []

    def fake_load_manifest_entries():
        return ["a.parquet", "b.parquet"]

    def fake_create_temp_tables(count):
        for i in range(count):
            created_tables.append(importer._make_temp_table_name(i))
        return created_tables

    monkeypatch.setattr(importer, "load_manifest_entries", fake_load_manifest_entries)
    monkeypatch.setattr(importer, "_create_temp_tables", fake_create_temp_tables)
    monkeypatch.setattr(
        importer, "_write_temp_manifest", lambda tables: tmp_path / "temp_manifest.json"
    )
    monkeypatch.setattr(
        importer,
        "_import_files_to_temp_table",
        lambda target_table, filenames: imported.append((target_table, filenames)),
    )

    importer.perform_work()

    assert created_tables
    assert all(not name.startswith("##") for name in created_tables)
    assert all("." in name for name in created_tables)


def test_exporter_accepts_mssql_python_engine(monkeypatch):
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
        engine="mssql-python",
    )

    assert exporter.engine == "mssql-python"


def test_importer_accepts_mssql_python_engine(tmp_path):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )

    importer = Importer(
        config=config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        engine="mssql-python",
    )

    assert importer.engine == "mssql-python"


def _import_config():
    return SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
    )


def test_resolve_engine_default_explicit_value_wins():
    assert resolve_engine_default("batch_size", "mssql-python", 42, 1000) == 42


def test_resolve_engine_default_uses_engine_override():
    assert resolve_engine_default("batch_size", "mssql-python", None, 1000) == 1_048_576


def test_resolve_engine_default_falls_back_for_other_engines():
    assert resolve_engine_default("batch_size", "pyodbc", None, 1000) == 1000


def test_resolve_engine_default_unknown_tunable_uses_fallback():
    assert resolve_engine_default("nope", "mssql-python", None, 7) == 7


def test_importer_default_batch_size_pyodbc(tmp_path):
    importer = Importer(
        config=_import_config(),
        input_path=tmp_path,
        manifest_filename="manifest.json",
        engine="pyodbc",
    )

    assert importer.batch_size == DEFAULT_IMPORT_BATCH_SIZE


def test_importer_default_batch_size_mssql_python(tmp_path):
    importer = Importer(
        config=_import_config(),
        input_path=tmp_path,
        manifest_filename="manifest.json",
        engine="mssql-python",
    )

    assert importer.batch_size == 1_048_576


def test_importer_explicit_batch_size_overrides_engine_default(tmp_path):
    importer = Importer(
        config=_import_config(),
        input_path=tmp_path,
        manifest_filename="manifest.json",
        engine="mssql-python",
        batch_size=500,
    )

    assert importer.batch_size == 500


def test_exporter_fetch_size_no_engine_override(monkeypatch):
    config = _import_config()
    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)
    exporter = Exporter(
        config=config,
        output_path=Path("./out"),
        rowgroup_size=1_048_576,
        engine="mssql-python",
    )

    assert exporter.fetch_size == 8192


def test_connection_m_builds_correct_connection_string(monkeypatch):
    config = SqlConfig(
        server="myserver.example.com",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        username="user",
        password="secret",
        trusted_connection=False,
        trust_cert=True,
        encrypt=True,
    )

    captured_conn_str = []

    def mock_connect(conn_str):
        captured_conn_str.append(conn_str)
        mock_conn = MagicMock()
        return mock_conn

    import mssql_python

    monkeypatch.setattr(mssql_python, "connect", mock_connect)

    base = SqlServerIOBase(config)
    base.connection_m(autocommit=False)

    assert len(captured_conn_str) == 1
    conn_str = captured_conn_str[0]
    assert "Server=myserver.example.com" in conn_str
    assert "Database=TestDb" in conn_str
    assert "UID=user" in conn_str
    assert "PWD=secret" in conn_str
    assert "TrustServerCertificate=Yes" in conn_str
    assert "Encrypt=Yes" in conn_str


def test_connection_m_trusted_connection(monkeypatch):
    config = SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
        trust_cert=False,
        encrypt=False,
    )

    captured_conn_str = []

    def mock_connect(conn_str):
        captured_conn_str.append(conn_str)
        mock_conn = MagicMock()
        return mock_conn

    import mssql_python

    monkeypatch.setattr(mssql_python, "connect", mock_connect)

    base = SqlServerIOBase(config)
    base.connection_m(autocommit=True)

    assert len(captured_conn_str) == 1
    conn_str = captured_conn_str[0]
    assert "Trusted_Connection=Yes" in conn_str
    assert "TrustServerCertificate=No" in conn_str
    assert "Encrypt=Yes" not in conn_str


def test_connection_dsn_includes_default_packet_size():
    config = SqlConfig(
        server="myserver",
        database="TestDb",
        schema="dbo",
        table="T",
        username="u",
        password="p",
    )
    base = SqlServerIOBase(config)
    assert "PacketSize=16383" in base.dsn


def test_connection_dsn_custom_packet_size():
    config = SqlConfig(
        server="myserver",
        database="TestDb",
        schema="dbo",
        table="T",
        username="u",
        password="p",
        packet_size=8192,
    )
    base = SqlServerIOBase(config)
    assert "PacketSize=8192" in base.dsn


def test_connection_m_includes_packet_size(monkeypatch):
    config = SqlConfig(
        server="myserver",
        database="TestDb",
        schema="dbo",
        table="T",
        username="u",
        password="p",
        packet_size=4096,
    )

    captured_conn_str = []

    def mock_connect(conn_str):
        captured_conn_str.append(conn_str)
        mock_conn = MagicMock()
        return mock_conn

    import mssql_python

    monkeypatch.setattr(mssql_python, "connect", mock_connect)

    base = SqlServerIOBase(config)
    base.connection_m(autocommit=False)

    assert len(captured_conn_str) == 1
    assert "PacketSize=4096" in captured_conn_str[0]
