import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from typer.testing import CliRunner

from pybutt.cli import cli
from pybutt.core.config import TransactionMode

runner = CliRunner()


class DummyExporter:
    last_instance = None

    def __init__(
        self,
        config,
        output_path,
        pk_column=None,
        columns=None,
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
        fetch_size=None,
        engine="duckdb",
    ):
        self.config = config
        self.output_path = Path(output_path)
        self.pk_column = pk_column
        self.columns = columns
        self.worker_count = worker_count
        self.file_count = file_count
        self.rowgroup_size = rowgroup_size
        self.fetch_size = fetch_size
        self.engine = engine
        DummyExporter.last_instance = self

    def perform_work(self):
        self.performed = True


class DummyImporter:
    last_instance = None

    def __init__(
        self,
        config,
        input_path,
        manifest_filename,
        worker_count=1,
        batch_size=1000,
        transaction_mode=None,
        engine="pyodbc",
    ):
        self.config = config
        self.input_path = Path(input_path)
        self.manifest_filename = manifest_filename
        self.worker_count = worker_count
        self.batch_size = batch_size
        self.transaction_mode = transaction_mode
        self.engine = engine
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
    assert exporter.engine == "duckdb"
    assert exporter.output_path == output_dir


def test_export_command_parses_engine_option(monkeypatch, tmp_path):
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
            "--engine",
            "pyodbc",
        ],
    )

    assert result.exit_code == 0
    exporter = DummyExporter.last_instance
    assert exporter.engine == "pyodbc"


def test_import_command_parses_engine_option(monkeypatch, tmp_path):
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
            "--engine",
            "duckdb",
        ],
    )

    assert result.exit_code == 0
    importer = DummyImporter.last_instance
    assert importer.engine == "duckdb"


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
    assert importer.engine == "pyodbc"


def test_export_command_requires_username_without_trusted_connection():
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
            "--password",
            "user",
        ],
    )

    assert result.exit_code != 0
    assert "username is required" in result.output


def test_import_command_requires_username_without_trusted_connection(tmp_path):
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
            "--password",
            "user",
        ],
    )

    assert result.exit_code != 0
    assert "username is required" in result.output


class TestTransactionModeCliParameter:
    """Test transaction mode CLI parameter handling."""

    def test_import_command_default_transaction_mode_is_batch(
        self, monkeypatch, tmp_path
    ):
        """Test that default transaction mode is BATCH."""
        monkeypatch.setattr(cli, "Importer", DummyImporter)

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
                "--table",
                "MyTable",
                "--input-path",
                str(input_dir),
                "--manifest-filename",
                "manifest.json",
                "--trusted-connection",
            ],
        )

        assert result.exit_code == 0
        importer = DummyImporter.last_instance
        assert importer is not None
        assert importer.transaction_mode == TransactionMode.BATCH

    def test_import_command_accepts_batch_mode(self, monkeypatch, tmp_path):
        """Test that BATCH transaction mode is accepted via CLI."""
        monkeypatch.setattr(cli, "Importer", DummyImporter)

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
                "--table",
                "MyTable",
                "--input-path",
                str(input_dir),
                "--manifest-filename",
                "manifest.json",
                "--trusted-connection",
                "--transaction-mode",
                "batch",
            ],
        )

        assert result.exit_code == 0
        importer = DummyImporter.last_instance
        assert importer is not None
        assert importer.transaction_mode == TransactionMode.BATCH

    def test_import_command_accepts_rowgroup_mode(self, monkeypatch, tmp_path):
        """Test that ROWGROUP transaction mode is accepted via CLI."""
        monkeypatch.setattr(cli, "Importer", DummyImporter)

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
                "--table",
                "MyTable",
                "--input-path",
                str(input_dir),
                "--manifest-filename",
                "manifest.json",
                "--trusted-connection",
                "--transaction-mode",
                "rowgroup",
            ],
        )

        assert result.exit_code == 0
        importer = DummyImporter.last_instance
        assert importer is not None
        assert importer.transaction_mode == TransactionMode.ROWGROUP

    def test_import_command_accepts_file_mode(self, monkeypatch, tmp_path):
        """Test that FILE transaction mode is accepted via CLI."""
        monkeypatch.setattr(cli, "Importer", DummyImporter)

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
                "--table",
                "MyTable",
                "--input-path",
                str(input_dir),
                "--manifest-filename",
                "manifest.json",
                "--trusted-connection",
                "--transaction-mode",
                "file",
            ],
        )

        assert result.exit_code == 0
        importer = DummyImporter.last_instance
        assert importer is not None
        assert importer.transaction_mode == TransactionMode.FILE

    def test_import_command_accepts_row_mode(self, monkeypatch, tmp_path):
        """Test that ROW transaction mode is accepted via CLI."""
        monkeypatch.setattr(cli, "Importer", DummyImporter)

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
                "--table",
                "MyTable",
                "--input-path",
                str(input_dir),
                "--manifest-filename",
                "manifest.json",
                "--trusted-connection",
                "--transaction-mode",
                "row",
            ],
        )

        assert result.exit_code == 0
        importer = DummyImporter.last_instance
        assert importer is not None
        assert importer.transaction_mode == TransactionMode.ROW

    def test_import_help_displays_transaction_mode_option(self):
        """Test that --transaction-mode option appears in help."""
        result = runner.invoke(cli.app, ["import", "--help"])
        assert result.exit_code == 0
        assert "transaction" in result.output.lower()
        assert "batch" in result.output.lower()
        assert "[default: batch]" in result.output.lower()


def create_parquet(tmp_path: Path, name: str, rows: int = 10, rowgroup_size: int = 5):
    data = {
        "id": list(range(rows)),
        "value": [f"v{i}" for i in range(rows)],
    }
    table = pa.Table.from_pydict(data)

    file_path = tmp_path / name
    pq.write_table(table, file_path, row_group_size=rowgroup_size)
    return file_path


def test_cli_files_inspect(tmp_path):
    create_parquet(tmp_path, "x.parquet", rows=8, rowgroup_size=4)

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(cli.app, ["inspect", str(manifest)])

    assert result.exit_code == 0
    assert "x.parquet" in result.stdout
    assert "rows: 8" in result.stdout
    assert "row groups: 2" in result.stdout


def test_cli_files_inspect_verbose(tmp_path):
    create_parquet(tmp_path, "x.parquet")

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(cli.app, ["inspect", str(manifest), "--verbose"])

    assert result.exit_code == 0
    assert "columns:" in result.stdout
    assert "id:" in result.stdout
    assert "value:" in result.stdout
