"""Tests for the purge command."""

import json
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from pybutt.cli import app, purge_command
from pybutt.io.purger import TablePurger

runner = CliRunner()


class DummyTablePurger:
    """Test double for TablePurger."""

    last_instance = None

    def __init__(self, config, sources):
        self.config = config
        self.sources = sources
        self.purge_called = False
        DummyTablePurger.last_instance = self

    def purge(self):
        self.purge_called = True
        return list(self.sources)


# --- File purge tests ---


def test_purge_files_deletes_entries_and_manifest(tmp_path, create_parquet):
    """File purge deletes listed files and the manifest itself."""
    create_parquet(tmp_path, "a.parquet", rows=3)
    create_parquet(tmp_path, "b.parquet", rows=2)
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet", "b.parquet"]')

    result = runner.invoke(app, ["purge", str(manifest)])

    assert result.exit_code == 0
    assert "2 file(s) deleted" in result.output
    assert "manifest removed" in result.output
    assert not (tmp_path / "a.parquet").exists()
    assert not (tmp_path / "b.parquet").exists()
    assert not manifest.exists()


def test_purge_files_handles_missing_files(tmp_path, create_parquet):
    """File purge warns about missing files but still succeeds."""
    create_parquet(tmp_path, "a.parquet", rows=3)
    # b.parquet does not exist
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet", "b.parquet"]')

    result = runner.invoke(app, ["purge", str(manifest)])

    assert result.exit_code == 0
    assert "1 file(s) deleted" in result.output
    assert "1 file(s) not found (skipped)" in result.output
    assert not (tmp_path / "a.parquet").exists()
    assert not manifest.exists()


def test_purge_files_v2_manifest(tmp_path, create_parquet):
    """File purge works with v2 manifest format."""
    create_parquet(tmp_path, "part_00000.parquet", rows=5)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {"version": 2, "type": "files", "entries": ["part_00000.parquet"]}
        )
    )

    result = runner.invoke(app, ["purge", str(manifest)])

    assert result.exit_code == 0
    assert "1 file(s) deleted" in result.output
    assert not (tmp_path / "part_00000.parquet").exists()
    assert not manifest.exists()


def test_purge_files_empty_manifest(tmp_path):
    """File purge with no entries still deletes the manifest."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"version": 2, "type": "files", "entries": []}))

    result = runner.invoke(app, ["purge", str(manifest)])

    assert result.exit_code == 0
    assert "0 file(s) deleted" in result.output
    assert not manifest.exists()


# --- Table purge tests ---


def test_purge_tables_invokes_table_purger(monkeypatch, tmp_path):
    """Table purge delegates to TablePurger and deletes the manifest."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "version": 2,
                "type": "tables",
                "entries": ["dbo.TableA", "dbo.TableB"],
            }
        )
    )

    monkeypatch.setattr(purge_command, "TablePurger", DummyTablePurger)

    result = runner.invoke(
        app,
        [
            "purge",
            "--server",
            "localhost",
            "--database",
            "TestDb",
            "--trusted-connection",
            str(manifest),
        ],
    )

    assert result.exit_code == 0
    purger = DummyTablePurger.last_instance
    assert purger is not None
    assert purger.sources == ["dbo.TableA", "dbo.TableB"]
    assert purger.purge_called
    assert "2 table(s) dropped" in result.output
    assert "manifest removed" in result.output
    assert not manifest.exists()


def test_purge_tables_requires_server_and_database(tmp_path):
    """Table purge fails if --server and --database are not provided."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {"version": 2, "type": "tables", "entries": ["dbo.TableA"]}
        )
    )

    result = runner.invoke(
        app,
        ["purge", "--trusted-connection", str(manifest)],
    )

    assert result.exit_code != 0
    assert "--server and --database are required" in result.output


def test_purge_tables_requires_username_without_trusted_connection(tmp_path):
    """Table purge requires --username when not using --trusted-connection."""
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {"version": 2, "type": "tables", "entries": ["dbo.TableA"]}
        )
    )

    result = runner.invoke(
        app,
        [
            "purge",
            "--server",
            "localhost",
            "--database",
            "TestDb",
            str(manifest),
        ],
    )

    assert result.exit_code != 0
    assert "--username is required" in result.output


def test_purge_manifest_not_found(tmp_path):
    """Purge fails gracefully when manifest file does not exist."""
    missing = tmp_path / "nonexistent.json"

    result = runner.invoke(app, ["purge", str(missing)])

    assert result.exit_code != 0
    assert "Purge failed" in result.output


# --- TablePurger unit tests ---


def test_table_purger_drops_tables():
    """TablePurger executes DROP TABLE IF EXISTS for each entry."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch.object(TablePurger, "__init__", return_value=None):
        purger = TablePurger.__new__(TablePurger)
        purger.sources = ["dbo.TableA", "schema1.TableB"]

    with patch.object(purger, "connection_p", return_value=mock_conn):
        dropped = purger.purge()

    assert dropped == ["dbo.TableA", "schema1.TableB"]
    assert mock_cursor.execute.call_count == 2
    mock_cursor.execute.assert_any_call("DROP TABLE IF EXISTS [dbo].[TableA]")
    mock_cursor.execute.assert_any_call(
        "DROP TABLE IF EXISTS [schema1].[TableB]"
    )
    mock_conn.close.assert_called_once()


def test_table_purger_invalid_table_name():
    """TablePurger raises ValueError for malformed schema.table entries."""
    with patch.object(TablePurger, "__init__", return_value=None):
        purger = TablePurger.__new__(TablePurger)
        purger.sources = ["invalid_no_dot"]

    mock_conn = MagicMock()
    with patch.object(purger, "connection_p", return_value=mock_conn):
        with pytest.raises(ValueError, match="Invalid source table name"):
            purger.purge()

    mock_conn.close.assert_called_once()
