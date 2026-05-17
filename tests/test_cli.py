import pytest
from typer.testing import CliRunner

import pybutt.cli  as cli


runner = CliRunner()


# ============================================================
# ✅ BASIC CLI EXECUTION
# ============================================================

def test_cli_export_runs(monkeypatch, tmp_path):
    called = {"ran": False}

    class FakeExporter:
        def __init__(self, *args, **kwargs):
            # basic sanity checks on wiring
            assert kwargs["server"] == "srv"
            assert kwargs["database"] == "db"
            assert kwargs["schema"] == "dbo"
            assert kwargs["table"] == "tbl"

        def perform_work(self):
            called["ran"] = True

    monkeypatch.setattr(cli, "Exporter", FakeExporter)

    result = runner.invoke(cli.app, [
        "--server", "srv",
        "--database", "db",
        "--schema", "dbo",
        "--table", "tbl",
        "--output-path", str(tmp_path),
    ])

    assert result.exit_code == 0, result.stdout
    assert called["ran"] is True
    assert "Export complete" in result.stdout


# ============================================================
# ✅ CLI PASSES OPTIONAL PARAMETERS
# ============================================================

def test_cli_passes_optional_args(monkeypatch, tmp_path):
    captured = {}

    class FakeExporter:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def perform_work(self):
            pass

    monkeypatch.setattr(cli, "Exporter", FakeExporter)

    result = runner.invoke(cli.app, [
        "--server", "srv",
        "--database", "db",
        "--schema", "dbo",
        "--table", "tbl",
        "--output-path", str(tmp_path),
        "--username", "user",
        "--password", "pass",
        "--pk-column", "id",
        "--columns", "col1",
        "--columns", "col2",
        "--worker-count", "4",
        "--max-rows-per-file", "500",
        "--trusted-connection",
        "--trust-cert",
        "--encrypt",
        "--retries", "5",
    ])

    assert result.exit_code == 0

    assert captured["username"] == "user"
    assert captured["password"] == "pass"
    assert captured["pk_column"] == "id"
    assert captured["columns"] == ["col1", "col2"]
    assert captured["worker_count"] == 4
    assert captured["max_rows_per_file"] == 500
    assert captured["trusted_connection"] is True
    assert captured["trust_cert"] is True
    assert captured["encrypt"] is True
    assert captured["retries"] == 5


# ============================================================
# ✅ CLI VALIDATION ERROR PROPAGATION
# ============================================================

def test_cli_invalid_identifier_fails(tmp_path):
    result = runner.invoke(cli.app, [
        "--server", "srv",
        "--database", "db",
        "--schema", "invalid-schema!",  # invalid
        "--table", "tbl",
        "--output-path", str(tmp_path),
    ])

    # Typer will catch exception and return non-zero
    assert result.exit_code != 0
    assert "Invalid identifier" in result.stderr


# ============================================================
# ✅ CLI HANDLES EXPORT FAILURE
# ============================================================

def test_cli_export_failure(monkeypatch, tmp_path):

    class FakeExporter:
        def __init__(self, *args, **kwargs):
            pass

        def perform_work(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(cli, "Exporter", FakeExporter)

    result = runner.invoke(cli.app, [
        "--server", "srv",
        "--database", "db",
        "--schema", "dbo",
        "--table", "tbl",
        "--output-path", str(tmp_path),
    ])

    assert result.exit_code != 0
    assert "boom" in result.stderr


# ============================================================
# ✅ CLI HELP TEXT
# ============================================================

def test_cli_help():
    result = runner.invoke(cli.app, ["--help"])

    assert result.exit_code == 0
    assert "Export a SQL Server table to Parquet using DuckDB + ODBC." in result.stdout
    assert "export" in result.stdout