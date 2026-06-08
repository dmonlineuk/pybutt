"""
Tests for pybutt.io.merger module.

Covers:
- TableMerger initialization (valid engines, invalid engines, transaction mode coercion)
- _parse_schema_table validation
- _ensure_target_exists_and_schema (create target, schema match, schema mismatch)
- merge orchestration
"""

from unittest.mock import MagicMock, patch

import pytest

from pybutt.core.config import SqlConfig, TransactionMode
from pybutt.exceptions import EngineSelectionError, SchemaMismatchError
from pybutt.io.merger import TableMerger


@pytest.fixture
def mock_config():
    return SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="TargetTable",
        trusted_connection=True,
        retries=3,
    )


class TestTableMergerInit:
    """Test TableMerger.__init__ validation."""

    def test_valid_pyodbc_engine(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A", "dbo.B"], engine="pyodbc")
        assert merger.engine == "pyodbc"
        assert merger.sources == ["dbo.A", "dbo.B"]

    def test_valid_duckdb_engine(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A"], engine="duckdb")
        assert merger.engine == "duckdb"

    def test_invalid_engine_raises(self, mock_config):
        with pytest.raises(EngineSelectionError):
            TableMerger(mock_config, sources=["dbo.A"], engine="mssql-python")

    def test_invalid_engine_nonsense_raises(self, mock_config):
        with pytest.raises(EngineSelectionError):
            TableMerger(mock_config, sources=["dbo.A"], engine="sqlite")

    def test_transaction_mode_default_is_batch(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A"])
        assert merger.transaction_mode == TransactionMode.BATCH

    def test_transaction_mode_from_string(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A"], transaction_mode="file")
        assert merger.transaction_mode == TransactionMode.FILE

    def test_transaction_mode_enum(self, mock_config):
        merger = TableMerger(
            mock_config, sources=["dbo.A"], transaction_mode=TransactionMode.ROWGROUP
        )
        assert merger.transaction_mode == TransactionMode.ROWGROUP

    def test_sources_converted_to_list(self, mock_config):
        merger = TableMerger(mock_config, sources=iter(["dbo.X", "dbo.Y"]))
        assert merger.sources == ["dbo.X", "dbo.Y"]


class TestParseSchemaTable:
    """Test _parse_schema_table helper."""

    def test_valid_schema_table(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A"])
        assert merger._parse_schema_table("dbo.MyTable") == ("dbo", "MyTable")

    def test_invalid_no_dot(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A"])
        with pytest.raises(ValueError, match="Invalid source table name"):
            merger._parse_schema_table("MyTable")

    def test_invalid_too_many_dots(self, mock_config):
        merger = TableMerger(mock_config, sources=["dbo.A"])
        with pytest.raises(ValueError, match="Invalid source table name"):
            merger._parse_schema_table("catalog.dbo.MyTable")


class TestEnsureTargetExistsAndSchema:
    """Test _ensure_target_exists_and_schema logic."""

    def test_target_does_not_exist_creates_table(self, mock_config):
        merger = TableMerger(mock_config, sources=["staging.Src"])

        mock_cur = MagicMock()
        # First call: OBJECT_ID returns None (target doesn't exist)
        obj_id_row = MagicMock()
        obj_id_row.__getitem__ = lambda self, idx: None
        # Second call: SELECT TOP 0 from source for column list
        mock_cur.description = [("col_a",), ("col_b",)]
        mock_cur.fetchone.return_value = obj_id_row
        # Mock connection for commit
        mock_cur.connection = MagicMock()

        result = merger._ensure_target_exists_and_schema(
            mock_cur, "staging.Src", "dbo", "Target"
        )

        assert result == ["col_a", "col_b"]
        # Should have called commit after creating the table
        mock_cur.connection.commit.assert_called_once()

    def test_target_exists_schema_matches(self, mock_config):
        merger = TableMerger(mock_config, sources=["staging.Src"])

        mock_cur = MagicMock()
        # OBJECT_ID returns non-None (target exists)
        obj_id_row = MagicMock()
        obj_id_row.__getitem__ = lambda self, idx: 12345

        mock_cur.fetchone.return_value = obj_id_row

        # Source description, then target description (same columns)
        call_count = [0]

        def side_effect_description():
            call_count[0] += 1
            if call_count[0] == 1:
                return [("col_a",), ("col_b",)]
            return [("col_b",), ("col_a",)]  # same set, different order

        type(mock_cur).description = property(lambda self: side_effect_description())

        result = merger._ensure_target_exists_and_schema(
            mock_cur, "staging.Src", "dbo", "Target"
        )

        assert set(result) == {"col_a", "col_b"}

    def test_target_exists_schema_mismatch_raises(self, mock_config):
        merger = TableMerger(mock_config, sources=["staging.Src"])

        mock_cur = MagicMock()
        obj_id_row = MagicMock()
        obj_id_row.__getitem__ = lambda self, idx: 12345
        mock_cur.fetchone.return_value = obj_id_row

        descriptions = iter(
            [
                [("col_a",), ("col_b",)],  # source columns
                [("col_x",), ("col_y",)],  # target columns - different!
            ]
        )
        type(mock_cur).description = property(lambda self: next(descriptions))

        with pytest.raises(SchemaMismatchError):
            merger._ensure_target_exists_and_schema(
                mock_cur, "staging.Src", "dbo", "Target"
            )


class TestMerge:
    """Test merge orchestration."""

    @patch.object(TableMerger, "connection_p")
    def test_merge_inserts_from_all_sources(self, mock_conn_p, mock_config):
        sources = ["staging.Part1", "staging.Part2", "staging.Part3"]
        merger = TableMerger(mock_config, sources=sources)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_p.return_value = mock_conn

        # Mock _ensure_target_exists_and_schema to succeed
        with patch.object(
            merger, "_ensure_target_exists_and_schema", return_value=["col_a"]
        ):
            merger.merge("dbo", "FinalTable")

        # 3 INSERT executes + 1 for _ensure (called on real object, but we patched it)
        # Check commit was called for each source
        assert mock_conn.commit.call_count == 3

    @patch.object(TableMerger, "connection_p")
    def test_merge_rollback_on_error(self, mock_conn_p, mock_config):
        sources = ["staging.Part1"]
        merger = TableMerger(mock_config, sources=sources)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor
        mock_conn_p.return_value = mock_conn

        # Make the INSERT execute raise
        mock_cursor.execute.side_effect = RuntimeError("deadlock")

        with patch.object(
            merger, "_ensure_target_exists_and_schema", return_value=["col_a"]
        ):
            with pytest.raises(RuntimeError, match="deadlock"):
                merger.merge("dbo", "FinalTable")

        mock_conn.rollback.assert_called_once()
