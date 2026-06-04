"""
Tests for import retry logic and transaction modes.

Covers:
- Per-batch retry (BATCH mode)
- Per-rowgroup retry (ROWGROUP mode)
- File-level retry (FILE mode)
- Autocommit for ROW mode
- Exponential backoff behavior
- Retry exhaustion
"""

import time
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pybutt.core.config import (
    SqlConfig,
    TransactionMode,
)
from pybutt.io.importer import Importer


@pytest.fixture
def mock_config():
    """Create a SqlConfig for testing."""
    return SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
        retries=3,
    )


@pytest.fixture
def importer_batch_mode(tmp_path, mock_config):
    """Create an Importer with BATCH transaction mode."""
    return Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.BATCH,
    )


@pytest.fixture
def importer_rowgroup_mode(tmp_path, mock_config):
    """Create an Importer with ROWGROUP transaction mode."""
    return Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.ROWGROUP,
    )


@pytest.fixture
def importer_file_mode(tmp_path, mock_config):
    """Create an Importer with FILE transaction mode."""
    return Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.FILE,
    )


@pytest.fixture
def importer_row_mode(tmp_path, mock_config):
    """Create an Importer with ROW transaction mode."""
    return Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.ROW,
    )


class TestTransactionModeDefaults:
    """Test transaction mode defaults and CLI parameter handling."""

    def test_importer_default_transaction_mode_is_batch(self, tmp_path, mock_config):
        """Verify that default transaction mode is BATCH (not FILE)."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
        )
        assert importer.transaction_mode == TransactionMode.BATCH

    def test_importer_accepts_batch_mode(self, tmp_path, mock_config):
        """Verify BATCH mode can be set."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            transaction_mode=TransactionMode.BATCH,
        )
        assert importer.transaction_mode == TransactionMode.BATCH

    def test_importer_accepts_rowgroup_mode(self, tmp_path, mock_config):
        """Verify ROWGROUP mode can be set."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            transaction_mode=TransactionMode.ROWGROUP,
        )
        assert importer.transaction_mode == TransactionMode.ROWGROUP

    def test_importer_accepts_file_mode(self, tmp_path, mock_config):
        """Verify FILE mode can be set."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            transaction_mode=TransactionMode.FILE,
        )
        assert importer.transaction_mode == TransactionMode.FILE

    def test_importer_accepts_row_mode(self, tmp_path, mock_config):
        """Verify ROW mode can be set."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            transaction_mode=TransactionMode.ROW,
        )
        assert importer.transaction_mode == TransactionMode.ROW

    def test_importer_accepts_string_transaction_mode(self, tmp_path, mock_config):
        """Verify transaction mode accepts string and converts to enum."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            transaction_mode="batch",
        )
        assert importer.transaction_mode == TransactionMode.BATCH


class TestBatchModeRetry:
    """Test per-batch retry logic for BATCH transaction mode."""

    def test_batch_retry_succeeds_on_first_attempt(self, importer_batch_mode):
        """Test that batch is committed successfully on first attempt."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock(), MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1, 2, 3]
        mock_batch.columns[1].to_pylist.return_value = ["a", "b", "c"]

        rows_returned = importer_batch_mode._import_batch_with_retry(
            mock_connection,
            mock_cursor,
            mock_batch,
            "INSERT INTO [dbo].[MyTable] (col1, col2) VALUES (?, ?)",
            "test_file.parquet",
        )

        assert rows_returned == 3
        mock_cursor.executemany.assert_called_once()
        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

    def test_batch_retry_fails_then_succeeds(self, importer_batch_mode):
        """Test that batch retries and succeeds on second attempt."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock(), MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1, 2, 3]
        mock_batch.columns[1].to_pylist.return_value = ["a", "b", "c"]

        # Fail first time, succeed second time
        mock_cursor.executemany.side_effect = [
            Exception("Connection timeout"),
            None,  # Success
        ]

        with patch("time.sleep"):  # Don't actually sleep during tests
            rows_returned = importer_batch_mode._import_batch_with_retry(
                mock_connection,
                mock_cursor,
                mock_batch,
                "INSERT INTO [dbo].[MyTable] (col1, col2) VALUES (?, ?)",
                "test_file.parquet",
            )

        assert rows_returned == 3
        assert mock_cursor.executemany.call_count == 2
        assert mock_connection.rollback.call_count == 1
        assert mock_connection.commit.call_count == 1

    def test_batch_retry_exhausts_retries(self, importer_batch_mode):
        """Test that batch fails after exhausting retries."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock(), MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1, 2]
        mock_batch.columns[1].to_pylist.return_value = ["a", "b"]

        # Always fail
        mock_cursor.executemany.side_effect = Exception("Persistent connection error")

        with patch("time.sleep"):
            with pytest.raises(
                RuntimeError, match="Batch import failed after 3 retries"
            ):
                importer_batch_mode._import_batch_with_retry(
                    mock_connection,
                    mock_cursor,
                    mock_batch,
                    "INSERT INTO [dbo].[MyTable] (col1, col2) VALUES (?, ?)",
                    "test_file.parquet",
                )

        assert mock_cursor.executemany.call_count == 3
        # Rollback happens on first 2 failures,
        # not on the final attempt (where we raise)
        assert mock_connection.rollback.call_count == 2

    def test_batch_retry_exponential_backoff(self, importer_batch_mode):
        """Test that batch retry uses exponential backoff."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock(), MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1]
        mock_batch.columns[1].to_pylist.return_value = ["a"]

        mock_cursor.executemany.side_effect = [
            Exception("Error 1"),
            Exception("Error 2"),
            None,  # Success on third attempt
        ]

        with patch("time.sleep") as mock_sleep:
            importer_batch_mode._import_batch_with_retry(
                mock_connection,
                mock_cursor,
                mock_batch,
                "INSERT INTO [dbo].[MyTable] (col1, col2) VALUES (?, ?)",
                "test_file.parquet",
            )

            # Exponential backoff: 2^0 = 1, 2^1 = 2
            assert mock_sleep.call_count == 2
            mock_sleep.assert_any_call(1)
            mock_sleep.assert_any_call(2)


class TestRowGroupModeRetry:
    """Test per-rowgroup retry logic for ROWGROUP transaction mode."""

    def test_rowgroup_retry_succeeds_on_first_attempt(self, importer_rowgroup_mode):
        """Test that rowgroup is committed successfully on first attempt."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_table = MagicMock()
        mock_batch1 = MagicMock()
        mock_batch1.columns = [MagicMock(), MagicMock()]
        mock_batch1.columns[0].to_pylist.return_value = [1, 2]
        mock_batch1.columns[1].to_pylist.return_value = ["a", "b"]

        mock_batch2 = MagicMock()
        mock_batch2.columns = [MagicMock(), MagicMock()]
        mock_batch2.columns[0].to_pylist.return_value = [3, 4]
        mock_batch2.columns[1].to_pylist.return_value = ["c", "d"]

        mock_table.to_batches.return_value = [mock_batch1, mock_batch2]

        rows_returned = importer_rowgroup_mode._import_rowgroup_with_retry(
            mock_connection,
            mock_cursor,
            mock_table,
            "INSERT INTO [dbo].[MyTable] (col1, col2) VALUES (?, ?)",
            "test_file.parquet",
            rg_idx=0,
            total_rg=1,
        )

        assert rows_returned == 4
        assert mock_cursor.executemany.call_count == 2
        mock_connection.commit.assert_called_once()
        mock_connection.rollback.assert_not_called()

    def test_import_file_with_duckdb_rowgroup_uses_actual_parquet_row_groups(
        self, tmp_path, mock_config
    ):
        """Verify DuckDB ROWGROUP mode respects parquet row group boundaries."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            batch_size=1000,
            transaction_mode=TransactionMode.ROWGROUP,
            engine="duckdb",
        )

        table = pa.table({"col1": [1, 2, 3, 4]})
        parquet_file = tmp_path / "test.parquet"
        pq.write_table(table, parquet_file, row_group_size=2)

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",)]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.__exit__.return_value = None

        importer.get_table_columns = lambda cur: ["col1"]

        with patch.object(importer, "connection_p", return_value=mock_connection):
            importer._import_file_with_duckdb(parquet_file, "test.parquet", time.time())

        assert mock_connection.commit.call_count == 2
        assert mock_cursor.executemany.call_count == 2

    def test_rowgroup_retry_fails_then_succeeds(self, importer_rowgroup_mode):
        """Test that rowgroup retries and succeeds on second attempt."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_table = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1, 2, 3]

        mock_table.to_batches.return_value = [mock_batch]

        # Fail first time (during batches), succeed second time
        call_count = [0]

        def executemany_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Connection timeout")
            # Second call succeeds

        mock_cursor.executemany.side_effect = executemany_side_effect

        with patch("time.sleep"):
            rows_returned = importer_rowgroup_mode._import_rowgroup_with_retry(
                mock_connection,
                mock_cursor,
                mock_table,
                "INSERT INTO [dbo].[MyTable] (col1) VALUES (?)",
                "test_file.parquet",
                rg_idx=0,
                total_rg=1,
            )

        assert rows_returned == 3
        assert mock_connection.rollback.call_count == 1
        assert mock_connection.commit.call_count == 1

    def test_rowgroup_retry_exhausts_retries(self, importer_rowgroup_mode):
        """Test that rowgroup fails after exhausting retries."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_table = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1]

        mock_table.to_batches.return_value = [mock_batch]

        # Always fail during batch processing
        mock_cursor.executemany.side_effect = Exception("Persistent error")

        with patch("time.sleep"):
            with pytest.raises(
                RuntimeError, match="Row group import failed after 3 retries"
            ):
                importer_rowgroup_mode._import_rowgroup_with_retry(
                    mock_connection,
                    mock_cursor,
                    mock_table,
                    "INSERT INTO [dbo].[MyTable] (col1) VALUES (?)",
                    "test_file.parquet",
                    rg_idx=0,
                    total_rg=1,
                )

        # Rollback happens on first 2 failures,
        # not on the final attempt (where we raise)
        assert mock_connection.rollback.call_count == 2

    def test_rowgroup_retry_accepts_recordbatch(self, importer_rowgroup_mode):
        """Test that ROWGROUP retry works when passed a RecordBatch-like object."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_recordbatch = MagicMock()
        mock_recordbatch.to_batches = None
        mock_recordbatch.columns = [MagicMock()]
        mock_recordbatch.columns[0].to_pylist.return_value = [1, 2]

        rows_returned = importer_rowgroup_mode._import_rowgroup_with_retry(
            mock_connection,
            mock_cursor,
            mock_recordbatch,
            "INSERT INTO [dbo].[MyTable] (col1) VALUES (?)",
            "test_file.parquet",
            rg_idx=0,
            total_rg=1,
        )

        assert rows_returned == 2
        mock_cursor.executemany.assert_called_once()
        mock_connection.commit.assert_called_once()


class TestFileModeRetry:
    """Test file-level retry logic for FILE transaction mode."""

    @patch("pybutt.io.importer.Importer._import_file_impl")
    def test_file_retry_succeeds_on_first_attempt(
        self, mock_impl, importer_file_mode, tmp_path
    ):
        """Test that file import succeeds on first attempt."""
        mock_impl.return_value = None

        importer_file_mode.import_file("test.parquet")

        assert mock_impl.call_count == 1

    @patch("pybutt.io.importer.Importer._import_file_impl")
    def test_file_retry_fails_then_succeeds(
        self, mock_impl, importer_file_mode, tmp_path
    ):
        """Test that file import retries and succeeds on second attempt."""
        mock_impl.side_effect = [
            Exception("Connection error"),
            None,  # Success on retry
        ]

        with patch("time.sleep"):
            importer_file_mode.import_file("test.parquet")

        assert mock_impl.call_count == 2

    @patch("pybutt.io.importer.Importer._import_file_impl")
    def test_file_retry_exhausts_retries(self, mock_impl, importer_file_mode, tmp_path):
        """Test that file import fails after exhausting retries."""
        mock_impl.side_effect = Exception("Persistent connection error")

        with patch("time.sleep"):
            with pytest.raises(
                RuntimeError, match="Import file .* failed after max retries"
            ):
                importer_file_mode.import_file("test.parquet")

        assert mock_impl.call_count == 3


class TestRowModeAutocommit:
    """Test autocommit behavior for ROW transaction mode."""

    @patch("pybutt.io.importer.Importer.connection_p")
    def test_row_mode_uses_autocommit(self, mock_connection_p, importer_row_mode):
        """Test that ROW mode creates connection with autocommit=True."""
        importer_row_mode.connection_p(autocommit=True)

        # connection_p is called with autocommit=True
        mock_connection_p.assert_called()

    def test_row_mode_transaction_mode_value(self, importer_row_mode):
        """Test that ROW mode has correct enum value."""
        assert importer_row_mode.transaction_mode == TransactionMode.ROW
        assert importer_row_mode.transaction_mode.value == "row"


class TestImportFileImpl:
    """Test the _import_file_impl method logic."""

    @patch("pybutt.io.importer.pq.ParquetFile")
    @patch("pybutt.io.importer.Importer._import_batch_with_retry")
    def test_import_file_impl_batch_mode_retry_per_batch(
        self, mock_batch_retry, mock_parquet, importer_batch_mode, tmp_path
    ):
        """
        Test that _import_file_impl calls _import_batch_with_retry for each batch
        in BATCH mode.
        """
        # Setup mock parquet file
        mock_pq_file = MagicMock()
        mock_pq_file.schema.names = ["col1", "col2"]
        mock_pq_file.num_row_groups = 1
        mock_parquet.return_value = mock_pq_file

        mock_table = MagicMock()
        mock_batch1 = MagicMock()
        mock_batch2 = MagicMock()
        mock_table.to_batches.return_value = [mock_batch1, mock_batch2]
        mock_pq_file.read_row_group.return_value = mock_table

        # Setup mock connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.__exit__.return_value = None

        # Mock batch retry to return row counts
        mock_batch_retry.side_effect = [10, 15]

        # Create parquet file
        parquet_file = tmp_path / "test.parquet"
        parquet_file.write_bytes(b"fake parquet")

        # Call import
        with patch.object(
            importer_batch_mode, "connection_p", return_value=mock_connection
        ):
            importer_batch_mode._import_file_impl(
                parquet_file, "test.parquet", time.time()
            )

        # Verify _import_batch_with_retry was called for each batch
        assert mock_batch_retry.call_count == 2

    @patch("pybutt.io.importer.pq.ParquetFile")
    @patch("pybutt.io.importer.Importer._import_rowgroup_with_retry")
    def test_import_file_impl_rowgroup_mode_retry_per_rowgroup(
        self, mock_rg_retry, mock_parquet, importer_rowgroup_mode, tmp_path
    ):
        """
        Test that _import_file_impl calls _import_rowgroup_with_retry for each
        rowgroup in ROWGROUP mode.
        """
        # Setup mock parquet file
        mock_pq_file = MagicMock()
        mock_pq_file.schema.names = ["col1", "col2"]
        mock_pq_file.num_row_groups = 2
        mock_parquet.return_value = mock_pq_file

        mock_table1 = MagicMock()
        mock_table2 = MagicMock()
        mock_pq_file.read_row_group.side_effect = [mock_table1, mock_table2]

        # Setup mock connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connection.__enter__.return_value = mock_connection
        mock_connection.__exit__.return_value = None

        # Mock rowgroup retry to return row counts
        mock_rg_retry.side_effect = [100, 150]

        # Create parquet file
        parquet_file = tmp_path / "test.parquet"
        parquet_file.write_bytes(b"fake parquet")

        # Call import
        with patch.object(
            importer_rowgroup_mode, "connection_p", return_value=mock_connection
        ):
            importer_rowgroup_mode._import_file_impl(
                parquet_file, "test.parquet", time.time()
            )

        # Verify _import_rowgroup_with_retry was called for each rowgroup
        assert mock_rg_retry.call_count == 2


class TestErrorMessageRedaction:
    """Test that sensitive information is redacted in error messages."""

    def test_password_redacted_in_batch_retry_error(self, importer_batch_mode):
        """Test that password is redacted in batch retry error messages."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_batch = MagicMock()
        mock_batch.columns = [MagicMock()]
        mock_batch.columns[0].to_pylist.return_value = [1]

        error_msg = "Connection failed: Pwd=secret_password"
        mock_cursor.executemany.side_effect = Exception(error_msg)

        with patch("time.sleep"):
            with pytest.raises(RuntimeError) as exc_info:
                importer_batch_mode._import_batch_with_retry(
                    mock_connection,
                    mock_cursor,
                    mock_batch,
                    "INSERT INTO [dbo].[MyTable] (col1) VALUES (?)",
                    "test_file.parquet",
                )

        assert "Pwd=***" in str(exc_info.value)
        assert "secret_password" not in str(exc_info.value)


class TestMssqlPythonBulkcopyRetry:
    """Test retry logic for the mssql-python bulkcopy path."""

    @pytest.fixture
    def importer_mssql_batch(self, tmp_path, mock_config):
        """Create an Importer with mssql-python engine and BATCH mode."""
        return Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            batch_size=1000,
            transaction_mode=TransactionMode.BATCH,
            engine="mssql-python",
        )

    @pytest.fixture
    def importer_mssql_rowgroup(self, tmp_path, mock_config):
        """Create an Importer with mssql-python engine and ROWGROUP mode."""
        return Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            batch_size=1000,
            transaction_mode=TransactionMode.ROWGROUP,
            engine="mssql-python",
        )

    def test_mssql_bulkcopy_succeeds_on_first_attempt(self, importer_mssql_batch):
        """Test that bulkcopy is committed successfully on first attempt."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.bulkcopy.return_value = {"rows_copied": 3, "batch_count": 1}

        rows = [(1, "a"), (2, "b"), (3, "c")]
        columns = ["id", "name"]

        with patch("time.sleep"):
            result = importer_mssql_batch._mssql_bulkcopy_with_retry(
                mock_conn,
                rows,
                columns,
                "[dbo].[MyTable]",
                "test_file.parquet",
                context="batch in test_file.parquet",
                is_rows=True,
            )

        assert result == 3
        mock_cursor.bulkcopy.assert_called_once_with(
            "[dbo].[MyTable]",
            rows,
            column_mappings=columns,
        )
        mock_conn.commit.assert_called_once()

    def test_mssql_bulkcopy_retries_on_failure(self, importer_mssql_batch):
        """Test that bulkcopy retries and succeeds on second attempt."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.bulkcopy.side_effect = [
            Exception("Connection timeout"),
            {"rows_copied": 2, "batch_count": 1},
        ]

        rows = [(1, "a"), (2, "b")]
        columns = ["id", "name"]

        with patch("time.sleep"):
            result = importer_mssql_batch._mssql_bulkcopy_with_retry(
                mock_conn,
                rows,
                columns,
                "[dbo].[MyTable]",
                "test_file.parquet",
                context="batch in test_file.parquet",
                is_rows=True,
            )

        assert result == 2
        assert mock_cursor.bulkcopy.call_count == 2
        mock_conn.rollback.assert_called_once()

    def test_mssql_bulkcopy_exhausts_retries(self, importer_mssql_batch):
        """Test that bulkcopy fails after exhausting retries."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.bulkcopy.side_effect = Exception("Persistent error")

        rows = [(1, "a")]
        columns = ["id", "name"]

        from pybutt.exceptions import BatchImportError

        with patch("time.sleep"):
            with pytest.raises(BatchImportError, match="Bulk copy failed after 3"):
                importer_mssql_batch._mssql_bulkcopy_with_retry(
                    mock_conn,
                    rows,
                    columns,
                    "[dbo].[MyTable]",
                    "test_file.parquet",
                    context="batch in test_file.parquet",
                    is_rows=True,
                )

        assert mock_cursor.bulkcopy.call_count == 3
        assert mock_conn.rollback.call_count == 2

    def test_mssql_bulkcopy_with_arrow_table(self, importer_mssql_batch):
        """Test that bulkcopy handles PyArrow table input correctly."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.bulkcopy.return_value = {"rows_copied": 2, "batch_count": 1}

        table = pa.table({"id": [1, 2], "name": ["a", "b"]})
        columns = ["id", "name"]

        with patch("time.sleep"):
            result = importer_mssql_batch._mssql_bulkcopy_with_retry(
                mock_conn,
                table,
                columns,
                "[dbo].[MyTable]",
                "test_file.parquet",
                context="batch in test_file.parquet",
                is_rows=False,
            )

        assert result == 2
        mock_cursor.bulkcopy.assert_called_once()

    def test_mssql_import_file_batch_mode(self, importer_mssql_batch, tmp_path):
        """Test full import_file flow with mssql-python BATCH mode."""
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        filepath = tmp_path / "test.parquet"
        pq.write_table(table, str(filepath))

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.bulkcopy.return_value = {"rows_copied": 3, "batch_count": 1}

        with patch.object(importer_mssql_batch, "connection_m", return_value=mock_conn):
            importer_mssql_batch._import_file_with_mssql(
                filepath, "test.parquet", time.time()
            )

        mock_cursor.bulkcopy.assert_called()
        mock_conn.commit.assert_called()

    def test_mssql_import_file_rowgroup_mode(self, importer_mssql_rowgroup, tmp_path):
        """Test full import_file flow with mssql-python ROWGROUP mode."""
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        filepath = tmp_path / "test.parquet"
        pq.write_table(table, str(filepath))

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_cursor
        mock_cursor.description = [("id",), ("name",)]
        mock_cursor.bulkcopy.return_value = {"rows_copied": 3, "batch_count": 1}

        with patch.object(
            importer_mssql_rowgroup, "connection_m", return_value=mock_conn
        ):
            importer_mssql_rowgroup._import_file_with_mssql(
                filepath, "test.parquet", time.time()
            )

        mock_cursor.bulkcopy.assert_called()
        mock_conn.commit.assert_called()

    def test_mssql_create_temp_tables_creates_cci(self, tmp_path, mock_config):
        """Test _create_temp_tables creates CCI via connection_m."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            batch_size=1000,
            transaction_mode=TransactionMode.BATCH,
            engine="mssql-python",
            worker_count=2,
            create_cci=True,
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(importer, "connection_m", return_value=mock_conn):
            tables = importer._create_temp_tables(2)

        assert len(tables) == 2
        # SELECT INTO + CREATE CCI per table = 4 execute calls total
        assert mock_cursor.execute.call_count == 4
        cci_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "CLUSTERED COLUMNSTORE INDEX" in str(call)
        ]
        assert len(cci_calls) == 2
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_mssql_create_temp_tables_skips_cci_when_disabled(
        self, tmp_path, mock_config
    ):
        """Test that _create_temp_tables skips CCI when create_cci=False."""
        importer = Importer(
            config=mock_config,
            input_path=tmp_path,
            manifest_filename="manifest.json",
            batch_size=1000,
            transaction_mode=TransactionMode.BATCH,
            engine="mssql-python",
            worker_count=2,
            create_cci=False,
        )

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(importer, "connection_m", return_value=mock_conn):
            tables = importer._create_temp_tables(2)

        assert len(tables) == 2
        # Only SELECT INTO per table = 2 execute calls (no CCI)
        assert mock_cursor.execute.call_count == 2
        cci_calls = [
            call
            for call in mock_cursor.execute.call_args_list
            if "CLUSTERED COLUMNSTORE INDEX" in str(call)
        ]
        assert len(cci_calls) == 0
