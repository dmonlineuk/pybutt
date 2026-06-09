"""
Tests for pybutt.io.exporter module.

Covers:
- Exporter.__init__ validation (engine, file_count, fetch_size)
- _source_reference (with/without parameters)
- build_partition_query (pk_column and CHECKSUM strategies)
- _pyodbc_type_code_to_pyarrow type mapping
- _pyodbc_schema_from_description
- _write_parquet_from_record_batches
- _export_partition_with_pyodbc
- _export_partition_with_mssql
- export_partition dispatch and error handling
- perform_work manifest writing
"""

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc
import pytest

from pybutt.core.config import SqlConfig
from pybutt.exceptions import (
    ConfigurationError,
    DataExportError,
    EngineSelectionError,
)
from pybutt.io.exporter import Exporter


@pytest.fixture
def mock_config():
    return SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
        retries=1,
    )


def _make_exporter(mock_config, tmp_path, **kwargs):
    """Helper to create an Exporter with partition_meta mocked out."""
    defaults = dict(
        config=mock_config,
        output_path=tmp_path,
        worker_count=1,
        file_count=2,
        rowgroup_size=100,
        engine="pyodbc",
    )
    defaults.update(kwargs)
    with patch.object(Exporter, "partition_meta"):
        exp = Exporter(**defaults)
    # Set values that partition_meta would normally set
    exp.total_rows = 1000
    exp.partition_count = defaults.get("file_count", 2)
    exp.chunk_size = 1000 // exp.partition_count
    return exp


class TestExporterInit:
    """Test Exporter.__init__ validation."""

    def test_invalid_engine_raises(self, mock_config, tmp_path):
        with pytest.raises(EngineSelectionError):
            _make_exporter(mock_config, tmp_path, engine="sqlite")

    def test_file_count_zero_raises(self, mock_config, tmp_path):
        with pytest.raises(ConfigurationError, match="file_count must be at least 1"):
            _make_exporter(mock_config, tmp_path, file_count=0)

    def test_file_count_negative_raises(self, mock_config, tmp_path):
        with pytest.raises(ConfigurationError, match="file_count must be at least 1"):
            _make_exporter(mock_config, tmp_path, file_count=-1)

    def test_fetch_size_zero_raises(self, mock_config, tmp_path):
        with pytest.raises(ConfigurationError, match="fetch_size must be at least 1"):
            _make_exporter(mock_config, tmp_path, fetch_size=0)

    def test_fetch_size_negative_raises(self, mock_config, tmp_path):
        with pytest.raises(ConfigurationError, match="fetch_size must be at least 1"):
            _make_exporter(mock_config, tmp_path, fetch_size=-1)

    def test_valid_engines_accepted(self, mock_config, tmp_path):
        for engine in ("duckdb", "pyodbc", "mssql-python"):
            exp = _make_exporter(mock_config, tmp_path, engine=engine)
            assert exp.engine == engine

    def test_output_path_created(self, mock_config, tmp_path):
        output = tmp_path / "new_dir"
        _make_exporter(mock_config, tmp_path, output_path=output)
        assert output.exists()

    def test_custom_manifest_filename(self, mock_config, tmp_path):
        exp = _make_exporter(
            mock_config, tmp_path, manifest_filename="custom_manifest.json"
        )
        assert exp.manifest_filename == "custom_manifest.json"

    def test_default_manifest_filename(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert "dbo" in exp.manifest_filename
        assert "MyTable" in exp.manifest_filename

    def test_explicit_fetch_size_used(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, fetch_size=5000)
        assert exp.fetch_size == 5000

    def test_columns_validated(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, columns=["col_a", "col_b"])
        assert exp.columns == ["col_a", "col_b"]

    def test_pk_column_validated(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, pk_column="id")
        assert exp.pk_column == "id"


class TestSourceReference:
    """Test _source_reference method."""

    def test_without_parameters(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert exp._source_reference() == "[dbo].[MyTable]"

    def test_with_parameters(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, parameters="12,'fred','1989'")
        assert exp._source_reference() == "[dbo].[MyTable](12,'fred','1989')"


class TestBuildPartitionQuery:
    """Test build_partition_query with different strategies."""

    def test_checksum_partition_no_columns(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, file_count=4)
        query = exp.build_partition_query(2)
        assert "CHECKSUM(*)" in query
        assert "% 4 = 2" in query
        assert "*" in query

    def test_checksum_partition_with_columns(self, mock_config, tmp_path):
        exp = _make_exporter(
            mock_config, tmp_path, file_count=4, columns=["id", "name"]
        )
        query = exp.build_partition_query(1)
        assert "CHECKSUM(*)" in query
        assert "[id], [name]" in query

    @patch.object(Exporter, "get_table_columns", return_value=["id", "value"])
    def test_pk_partition_no_explicit_columns(self, mock_gtc, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, pk_column="id", file_count=2)
        query = exp.build_partition_query(0)
        assert "ROW_NUMBER()" in query
        assert "ORDER BY [id]" in query
        assert "rn > 0 AND rn <= 500" in query

    def test_pk_partition_with_explicit_columns(self, mock_config, tmp_path):
        exp = _make_exporter(
            mock_config, tmp_path, pk_column="id", file_count=2, columns=["id", "value"]
        )
        query = exp.build_partition_query(1)
        assert "ROW_NUMBER()" in query
        assert "[id], [value]" in query
        assert "rn > 500 AND rn <= 1000" in query

    def test_checksum_with_parameters(self, mock_config, tmp_path):
        exp = _make_exporter(
            mock_config, tmp_path, file_count=3, parameters="42,'test'"
        )
        query = exp.build_partition_query(0)
        assert "[dbo].[MyTable](42,'test')" in query


class TestPyodbcTypeMapping:
    """Test _pyodbc_type_code_to_pyarrow type conversion."""

    def test_integer_types(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_TINYINT, None, None, None)
            == pa.int32()
        )
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_SMALLINT, None, None, None)
            == pa.int32()
        )
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_INTEGER, None, None, None)
            == pa.int32()
        )
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_BIGINT, None, None, None)
            == pa.int64()
        )

    def test_float_types(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_REAL, None, None, None)
            == pa.float32()
        )
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_FLOAT, None, None, None)
            == pa.float32()
        )
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_DOUBLE, None, None, None)
            == pa.float64()
        )

    def test_decimal_type(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        result = exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_DECIMAL, 18, 4, None)
        assert result == pa.decimal128(18, 4)

    def test_decimal_type_defaults(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        result = exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_NUMERIC, None, None, None)
        assert result == pa.decimal128(38, 0)

    def test_string_types(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        for tc in (
            pyodbc.SQL_CHAR,
            pyodbc.SQL_VARCHAR,
            pyodbc.SQL_LONGVARCHAR,
            pyodbc.SQL_WCHAR,
            pyodbc.SQL_WVARCHAR,
            pyodbc.SQL_WLONGVARCHAR,
        ):
            assert exp._pyodbc_type_code_to_pyarrow(tc, None, None, None) == pa.string()

    def test_binary_types(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        for tc in (pyodbc.SQL_BINARY, pyodbc.SQL_VARBINARY, pyodbc.SQL_LONGVARBINARY):
            assert exp._pyodbc_type_code_to_pyarrow(tc, None, None, None) == pa.binary()

    def test_bit_type(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_BIT, None, None, None)
            == pa.bool_()
        )

    def test_date_type(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert (
            exp._pyodbc_type_code_to_pyarrow(pyodbc.SQL_TYPE_DATE, None, None, None)
            == pa.date32()
        )

    def test_time_type(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert exp._pyodbc_type_code_to_pyarrow(
            pyodbc.SQL_TYPE_TIME, None, None, None
        ) == pa.time64("us")

    def test_timestamp_type(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert exp._pyodbc_type_code_to_pyarrow(
            pyodbc.SQL_TYPE_TIMESTAMP, None, None, None
        ) == pa.timestamp("us")

    def test_unknown_type_defaults_to_string(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        assert exp._pyodbc_type_code_to_pyarrow(99999, None, None, None) == pa.string()


class TestPyodbcSchemaFromDescription:
    """Test _pyodbc_schema_from_description."""

    def test_builds_schema_from_description(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        # Code reads: column[5] = precision, column[6] = scale
        # Tuple: (name, type_code, display_size, internal_size, ?, precision, scale)
        description = [
            ("id", pyodbc.SQL_INTEGER, None, 4, None, None, False),
            ("name", pyodbc.SQL_VARCHAR, None, 100, None, None, True),
            ("amount", pyodbc.SQL_DECIMAL, None, None, None, 10, 2),
        ]
        schema = exp._pyodbc_schema_from_description(description)
        assert len(schema) == 3
        assert schema.field("id").type == pa.int32()
        assert schema.field("name").type == pa.string()
        assert schema.field("amount").type == pa.decimal128(10, 2)

    def test_short_description_tuple(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path)
        # Tuple with minimal fields (only name, type_code)
        description = [
            ("col1", pyodbc.SQL_BIGINT),
        ]
        schema = exp._pyodbc_schema_from_description(description)
        assert schema.field("col1").type == pa.int64()


class TestWriteParquetFromRecordBatches:
    """Test _write_parquet_from_record_batches."""

    def test_writes_single_batch(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, rowgroup_size=5)
        schema = pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.string())])
        batch = pa.RecordBatch.from_pydict(
            {"id": [1, 2, 3], "val": ["a", "b", "c"]}, schema=schema
        )
        reader = pa.RecordBatchReader.from_batches(schema, [batch])

        filepath = tmp_path / "test_single.parquet"
        exp._write_parquet_from_record_batches(reader, filepath, "test_single.parquet")

        assert filepath.exists()
        table = pq.read_table(filepath)
        assert table.num_rows == 3

    def test_writes_multiple_batches_with_rowgroup_splitting(
        self, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, rowgroup_size=3)
        schema = pa.schema([pa.field("id", pa.int32())])
        batches = [
            pa.RecordBatch.from_pydict({"id": [1, 2, 3]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [4, 5, 6]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [7, 8]}, schema=schema),
        ]
        reader = pa.RecordBatchReader.from_batches(schema, batches)

        filepath = tmp_path / "test_multi.parquet"
        exp._write_parquet_from_record_batches(reader, filepath, "test_multi.parquet")

        assert filepath.exists()
        table = pq.read_table(filepath)
        assert table.num_rows == 8

    def test_writes_empty_batch(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, rowgroup_size=10)
        schema = pa.schema([pa.field("id", pa.int32())])
        reader = pa.RecordBatchReader.from_batches(schema, [])

        filepath = tmp_path / "test_empty.parquet"
        exp._write_parquet_from_record_batches(reader, filepath, "test_empty.parquet")

        assert filepath.exists()
        table = pq.read_table(filepath)
        assert table.num_rows == 0

    def test_raises_data_export_error_on_failure(self, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, rowgroup_size=5)
        schema = pa.schema([pa.field("id", pa.int32())])

        # Create a mock reader that raises an exception
        class FailingReader:
            @property
            def schema(self):
                return schema

            def __iter__(self):
                raise RuntimeError("disk full")

        with pytest.raises(DataExportError, match="disk full"):
            exp._write_parquet_from_record_batches(
                FailingReader(), tmp_path / "fail.parquet", "fail.parquet"
            )


class TestExportPartition:
    """Test export_partition dispatch and error handling."""

    @patch.object(Exporter, "_export_partition_with_pyodbc")
    @patch.object(Exporter, "retry")
    def test_dispatch_pyodbc(
        self, mock_retry, mock_pyodbc_export, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc")
        # Make retry just call the function
        mock_retry.side_effect = lambda fn, context: fn()

        exp.export_partition(0)
        mock_pyodbc_export.assert_called_once()

    @patch.object(Exporter, "_export_partition_with_duckdb")
    @patch.object(Exporter, "retry")
    def test_dispatch_duckdb(
        self, mock_retry, mock_duckdb_export, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="duckdb")
        mock_retry.side_effect = lambda fn, context: fn()

        exp.export_partition(0)
        mock_duckdb_export.assert_called_once()

    @patch.object(Exporter, "_export_partition_with_mssql")
    @patch.object(Exporter, "retry")
    def test_dispatch_mssql(self, mock_retry, mock_mssql_export, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="mssql-python")
        mock_retry.side_effect = lambda fn, context: fn()

        exp.export_partition(0)
        mock_mssql_export.assert_called_once()

    @patch.object(Exporter, "retry")
    def test_memory_error_propagated(self, mock_retry, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc")
        mock_retry.side_effect = MemoryError("out of memory")

        with pytest.raises(MemoryError):
            exp.export_partition(0)

    @patch.object(Exporter, "retry")
    def test_generic_error_propagated(self, mock_retry, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc")
        mock_retry.side_effect = DataExportError("fail")

        with pytest.raises(DataExportError):
            exp.export_partition(0)

    @patch.object(Exporter, "_export_partition_with_pyodbc")
    @patch.object(Exporter, "retry")
    def test_returns_filename(self, mock_retry, mock_export, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc")
        mock_retry.side_effect = lambda fn, context: fn()

        # Create a dummy file to simulate export
        expected = "dbo_MyTable_part_00001.parquet"
        (tmp_path / expected).write_bytes(b"dummy")

        result = exp.export_partition(1)
        assert result == expected


class TestPerformWork:
    """Test perform_work orchestration and manifest writing."""

    @patch.object(Exporter, "export_partition")
    def test_writes_manifest(self, mock_export, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc", file_count=3)
        exp.partition_count = 3

        mock_export.side_effect = lambda n: f"dbo_MyTable_part_{n:05d}.parquet"

        # Mock multiprocessing Pool to run in-process
        with patch("pybutt.io.exporter.get_context") as mock_ctx:
            mock_pool = MagicMock()
            mock_pool.__enter__ = MagicMock(return_value=mock_pool)
            mock_pool.__exit__ = MagicMock(return_value=False)
            mock_pool._pool = []  # no real workers

            def _fake_map_async(fn, iterable):
                result = MagicMock()
                result.get.return_value = [fn(n) for n in iterable]
                return result

            mock_pool.map_async.side_effect = _fake_map_async
            mock_ctx.return_value.Pool.return_value = mock_pool

            exp.perform_work()

        manifest_path = tmp_path / exp.manifest_filename
        assert manifest_path.exists()
        with open(manifest_path) as f:
            data = json.load(f)
        assert data["version"] == 2
        assert data["type"] == "files"
        assert len(data["entries"]) == 3
        assert data["entries"][0] == "dbo_MyTable_part_00000.parquet"

    @patch.object(Exporter, "export_partition")
    def test_pool_error_propagated(self, mock_export, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc", file_count=2)
        exp.partition_count = 2

        with patch("pybutt.io.exporter.get_context") as mock_ctx:
            mock_pool = MagicMock()
            mock_pool.__enter__ = MagicMock(return_value=mock_pool)
            mock_pool.__exit__ = MagicMock(return_value=False)
            mock_pool._pool = []

            mock_result = MagicMock()
            mock_result.get.side_effect = RuntimeError("worker died")
            mock_pool.map_async.return_value = mock_result
            mock_ctx.return_value.Pool.return_value = mock_pool

            with pytest.raises(RuntimeError, match="worker died"):
                exp.perform_work()


class TestExportPartitionWithPyodbc:
    """Test _export_partition_with_pyodbc edge cases."""

    @patch.object(Exporter, "connection_p")
    def test_no_description_raises(self, mock_conn_p, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_p.return_value = mock_conn
        mock_cursor.description = None

        with pytest.raises(DataExportError, match="no column metadata"):
            exp._export_partition_with_pyodbc(
                "SELECT 1", tmp_path / "out.parquet", "out.parquet"
            )

    @patch.object(Exporter, "connection_p")
    def test_empty_result_writes_empty_parquet(
        self, mock_conn_p, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="pyodbc")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_p.return_value = mock_conn
        mock_cursor.description = [("col1", pyodbc.SQL_INTEGER, None, 4, None, None)]
        mock_cursor.fetchmany.return_value = []

        filepath = tmp_path / "empty.parquet"
        exp._export_partition_with_pyodbc("SELECT 1", filepath, "empty.parquet")

        assert filepath.exists()
        table = pq.read_table(filepath)
        assert table.num_rows == 0


class TestExportPartitionWithMssql:
    """Test _export_partition_with_mssql edge cases."""

    @patch.object(Exporter, "connection_m")
    def test_no_description_raises(self, mock_conn_m, mock_config, tmp_path):
        exp = _make_exporter(mock_config, tmp_path, engine="mssql-python")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_m.return_value = mock_conn
        mock_cursor.description = None

        with pytest.raises(DataExportError, match="no column metadata"):
            exp._export_partition_with_mssql(
                "SELECT 1", tmp_path / "out.parquet", "out.parquet"
            )

    @patch.object(Exporter, "connection_m")
    def test_empty_result_writes_empty_parquet(
        self, mock_conn_m, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="mssql-python")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_m.return_value = mock_conn
        mock_cursor.description = [("col1", pyodbc.SQL_INTEGER, None, 4, None, None)]
        mock_cursor.fetchmany.return_value = []

        filepath = tmp_path / "empty_mssql.parquet"
        exp._export_partition_with_mssql("SELECT 1", filepath, "empty_mssql.parquet")

        assert filepath.exists()
        table = pq.read_table(filepath)
        assert table.num_rows == 0

    @patch.object(Exporter, "connection_m")
    def test_execute_error_raises_data_export_error(
        self, mock_conn_m, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="mssql-python")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_m.return_value = mock_conn
        mock_cursor.execute.side_effect = RuntimeError("connection timeout")

        with pytest.raises(DataExportError, match="connection timeout"):
            exp._export_partition_with_mssql(
                "SELECT 1", tmp_path / "fail.parquet", "fail.parquet"
            )

        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


class TestExportPartitionWithDuckdb:
    """Test _export_partition_with_duckdb."""

    @patch.object(Exporter, "connection_d")
    @patch.object(Exporter, "_write_parquet_from_record_batches")
    def test_calls_write_with_arrow_result(
        self, mock_write, mock_conn_d, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="duckdb")

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_result = MagicMock()
        mock_result.arrow.return_value = "arrow_reader"
        mock_conn.execute.return_value = mock_result
        mock_conn_d.return_value = mock_conn

        exp._export_partition_with_duckdb(
            "SELECT 1", tmp_path / "duck.parquet", "duck.parquet"
        )

        mock_write.assert_called_once_with(
            "arrow_reader", tmp_path / "duck.parquet", "duck.parquet"
        )

    @patch.object(Exporter, "connection_d")
    def test_duckdb_error_raises_data_export_error(
        self, mock_conn_d, mock_config, tmp_path
    ):
        exp = _make_exporter(mock_config, tmp_path, engine="duckdb")

        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.side_effect = RuntimeError("odbc failure")
        mock_conn_d.return_value = mock_conn

        with pytest.raises(DataExportError, match="odbc failure"):
            exp._export_partition_with_duckdb(
                "SELECT 1", tmp_path / "duck.parquet", "duck.parquet"
            )
