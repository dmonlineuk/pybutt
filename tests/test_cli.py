from pathlib import Path

import pytest
from typer.testing import CliRunner

from pybutt import cli

runner = CliRunner()


class DummyExporter:
    last_instance = None

    def __init__(self, config, output_path, pk_column=None, columns=None, worker_count=1, file_count=1):
        self.config = config
        self.output_path = Path(output_path)
        self.pk_column = pk_column
        self.columns = columns
        self.worker_count = worker_count
        self.file_count = file_count
        DummyExporter.last_instance = self

    def perform_work(self):
        self.performed = True


class DummyImporter:
    last_instance = None

    def __init__(self, config, input_path, manifest_filename, worker_count=1, batch_size=1000):
        self.config = config
        self.input_path = Path(input_path)
        self.manifest_filename = manifest_filename
        self.worker_count = worker_count
        self.batch_size = batch_size
        DummyImporter.last_instance = self

    def perform_work(self):
        self.performed = True


def test_cli_help_contains_commands():
    result = runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0
    assert "export" in result.output
    assert "import" in result.output


def test_export_help_includes_server_option():
    result = runner.invoke(cli.app, ["export", "--help"])
    assert result.exit_code == 0
    assert "--server" in result.output
    assert "--output-path" in result.output


def test_import_help_includes_input_path_option():
    result = runner.invoke(cli.app, ["import", "--help"])
    assert result.exit_code == 0
    assert "--input-path" in result.output
    assert "manifest" in result.output


def test_export_command_parses_options(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "Exporter", DummyExporter)

    output_dir = tmp_path / "out"
    result = runner.invoke(
        cli.app,
        [
            "export",
            "--server",
            "localhost",
            "--database",
            "TestDb",
            "--schema",
            "dbo",
            "--table",
            "MyTable",
            "--output-path",
            str(output_dir),
            "--trusted-connection",
            "--driver",
            "ODBC Driver 18 for SQL Server",
            "--trust-cert",
            "--no-encrypt",
            "--retries",
            "5",
            "--pk-column",
            "id",
            "--columns",
            "a,b,c",
            "--worker-count",
            "2",
            "--file-count",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert "Export completed successfully" in result.output
    exporter = DummyExporter.last_instance
    assert exporter is not None
    assert exporter.config.server == "localhost"
    assert exporter.config.database == "TestDb"
    assert exporter.config.schema == "dbo"
    assert exporter.config.table == "MyTable"
    assert exporter.pk_column == "id"
    assert exporter.columns == ["a", "b", "c"]
    assert exporter.worker_count == 2
    assert exporter.file_count == 3
    assert exporter.output_path == output_dir


def test_import_command_parses_options(monkeypatch, tmp_path):
    monkeypatch.setattr(cli, "Importer", DummyImporter)

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    manifest = "manifest.json"
    result = runner.invoke(
        cli.app,
        [
            "import",
            "--server",
            "localhost",
            "--database",
            "TestDb",
            "--schema",
            "dbo",
            "--table",
            "MyTable",
            "--input-path",
            str(input_dir),
            "--manifest-filename",
            manifest,
            "--trusted-connection",
            "--worker-count",
            "4",
            "--batch-size",
            "2500",
        ],
    )

    assert result.exit_code == 0
    assert "Import completed successfully" in result.output
    importer = DummyImporter.last_instance
    assert importer is not None
    assert importer.config.server == "localhost"
    assert importer.config.database == "TestDb"
    assert importer.config.schema == "dbo"
    assert importer.config.table == "MyTable"
    assert importer.input_path == input_dir
    assert importer.manifest_filename == manifest
    assert importer.worker_count == 4
    assert importer.batch_size == 2500


def test_export_command_requires_password_without_trusted_connection():
    result = runner.invoke(
        cli.app,
        [
            "export",
            "--server",
            "localhost",
            "--database",
            "TestDb",
            "--schema",
            "dbo",
            "--table",
            "MyTable",
            "--output-path",
            "out",
            "--username",
            "user",
        ],
    )

    assert result.exit_code != 0
    assert "username and password are required" in result.output


def test_import_command_requires_password_without_trusted_connection(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    result = runner.invoke(
        cli.app,
        [
            "import",
            "--server",
            "localhost",
            "--database",
            "TestDb",
            "--schema",
            "dbo",
            "--table",
            "MyTable",
            "--input-path",
            str(input_dir),
            "--manifest-filename",
            "manifest.json",
            "--username",
            "user",
        ],
    )

    assert result.exit_code != 0
    assert "username and password are required" in result.output
