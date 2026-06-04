import json
from pathlib import Path

from typer.testing import CliRunner

from pybutt.cli import cli
from pybutt.core.config import TransactionMode

runner = CliRunner()


class DummyMerger:
    last_instance = None

    def __init__(self, config, sources):
        self.config = config
        self.sources = sources
        self.merge_called = False
        DummyMerger.last_instance = self

    def merge(self, target_schema, target_table):
        self.merge_called = True


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
        manifest_filename=None,
        parameters=None,
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
        self.manifest_filename = manifest_filename
        self.parameters = parameters
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
        temp_manifest_filename=None,
        delete_files=False,
    ):
        self.config = config
        self.input_path = Path(input_path)
        self.manifest_filename = manifest_filename
        self.temp_manifest_filename = temp_manifest_filename
        self.worker_count = worker_count
        self.batch_size = batch_size
        self.transaction_mode = transaction_mode
        self.engine = engine
        self.delete_files = delete_files
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
    assert "--delete-files" in result.output


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


def test_export_command_manifest_version_is_2(monkeypatch, tmp_path):
    import pybutt.io.exporter as exporter_module

    def fake_partition_meta(self):
        self.partition_count = 1
        self.chunk_size = 1

    monkeypatch.setattr(exporter_module.Exporter, "partition_meta", fake_partition_meta)

    def fake_export_partition(self, n):
        filename = f"{self.config.schema}_{self.config.table}_part_{n:05d}.parquet"
        filepath = self.output_path / filename
        filepath.write_text("dummy")
        return filename

    monkeypatch.setattr(
        exporter_module.Exporter, "export_partition", fake_export_partition
    )

    class DummyPool:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, func, args):
            return [func(arg) for arg in args]

    class DummyContext:
        def Pool(self, count):
            return DummyPool()

    monkeypatch.setattr(exporter_module, "get_context", lambda _: DummyContext())

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
            str(tmp_path),
            "--trusted-connection",
            "--file-count",
            "1",
        ],
    )

    assert result.exit_code == 0
    manifest_file = tmp_path / "dbo_MyTable_manifest.json"
    assert manifest_file.exists()
    with open(manifest_file) as f:
        manifest_data = json.load(f)

    assert manifest_data["version"] == 2
    assert manifest_data["type"] == "files"
    assert manifest_data["entries"] == ["dbo_MyTable_part_00000.parquet"]


def test_export_command_supports_view_like_objects(monkeypatch, tmp_path):
    import pybutt.io.exporter as exporter_module

    class DummyResult:
        def __init__(self, value):
            self.value = value

        def fetchone(self):
            return (self.value,)

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

    def fake_export_partition(self, n):
        filename = f"{self.config.schema}_{self.config.table}_part_{n:05d}.parquet"
        filepath = self.output_path / filename
        filepath.write_text("dummy")
        return filename

    class DummyPool:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def map(self, func, args):
            return [func(arg) for arg in args]

    class DummyContext:
        def Pool(self, count):
            return DummyPool()

    monkeypatch.setattr(
        exporter_module.Exporter, "connection_d", lambda self: DummyConnection()
    )
    monkeypatch.setattr(
        exporter_module.Exporter, "export_partition", fake_export_partition
    )
    monkeypatch.setattr(exporter_module, "get_context", lambda _: DummyContext())

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
            "MyView",
            "--output-path",
            str(tmp_path),
            "--trusted-connection",
            "--file-count",
            "1",
        ],
    )

    assert result.exit_code == 0
    manifest_file = tmp_path / "dbo_MyView_manifest.json"
    assert manifest_file.exists()
    with open(manifest_file) as f:
        manifest_data = json.load(f)

    assert manifest_data["entries"] == ["dbo_MyView_part_00000.parquet"]


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


def test_export_command_passes_manifest_filename(monkeypatch, tmp_path):
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
            "--manifest-filename",
            "custom_manifest.json",
        ],
    )

    assert result.exit_code == 0
    exporter = DummyExporter.last_instance
    assert exporter.manifest_filename == "custom_manifest.json"


def test_export_command_passes_function_parameters(monkeypatch, tmp_path):
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
            "export",
            "--table",
            "tvf_users",
            "--output-path",
            str(output_dir),
            "--trusted-connection",
            "--parameters",
            "12,'fred','1989'",
        ],
    )

    assert result.exit_code == 0
    exporter = DummyExporter.last_instance
    assert exporter.parameters == "12,'fred','1989'"


def test_import_command_uses_default_manifest_filename(monkeypatch, tmp_path):
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
            "--schema",
            "dbo",
            "--table",
            "MyTable",
            "--input-path",
            str(input_dir),
            "--trusted-connection",
        ],
    )

    assert result.exit_code == 0
    importer = DummyImporter.last_instance
    assert importer is not None
    assert importer.manifest_filename is None


def test_merge_command_files_invokes_merge_helper(
    monkeypatch, tmp_path, create_parquet
):
    create_parquet(tmp_path, "a.parquet", rows=3)
    create_parquet(tmp_path, "b.parquet", rows=2)
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet", "b.parquet"]')

    called = {}

    def fake_merge(
        manifest_path,
        output_file,
        rowgroup_size,
        delete_originals=False,
        new_manifest_name=None,
    ):
        called["args"] = (
            manifest_path,
            output_file,
            rowgroup_size,
            delete_originals,
            new_manifest_name,
        )

    monkeypatch.setattr(cli, "merge_parquet_files", fake_merge)

    output_file = tmp_path / "merged.parquet"
    result = runner.invoke(
        cli.app,
        [
            "merge",
            "--manifest",
            str(manifest),
            "--output-file",
            str(output_file),
            "--trusted-connection",
        ],
    )

    assert result.exit_code == 0
    assert "File merge completed successfully" in result.output
    assert called["args"] == (manifest, output_file, 1048576, False, None)


def test_import_command_uses_local_temp_tables_by_default(monkeypatch, tmp_path):
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
        ],
    )

    assert result.exit_code == 0
    importer = DummyImporter.last_instance
    assert importer is not None


def test_import_command_parses_delete_files_option(monkeypatch, tmp_path):
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
            "--delete-files",
        ],
    )

    assert result.exit_code == 0
    importer = DummyImporter.last_instance
    assert importer is not None
    assert importer.delete_files is True


def test_merge_command_tables_invokes_table_merger(monkeypatch, tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        '{"version": 2, "type": "tables", "entries": ["dbo.TableA", "dbo.TableB"]}'
    )

    monkeypatch.setattr(cli, "TableMerger", DummyMerger)

    result = runner.invoke(
        cli.app,
        [
            "merge",
            "--manifest",
            str(manifest),
            "--server",
            "localhost",
            "--database",
            "TestDb",
            "--schema",
            "dbo",
            "--table",
            "MyTable",
            "--trusted-connection",
        ],
    )

    assert result.exit_code == 0
    merger = DummyMerger.last_instance
    assert merger is not None
    assert merger.sources == ["dbo.TableA", "dbo.TableB"]
    assert merger.merge_called


def test_import_command_passes_temp_manifest_filename(monkeypatch, tmp_path):
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
            "--output-manifest-filename",
            "custom_temp_manifest.json",
        ],
    )

    assert result.exit_code == 0
    importer = DummyImporter.last_instance
    assert importer.temp_manifest_filename == "custom_temp_manifest.json"


def test_merge_command_files_requires_output_file(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text('["a.parquet"]')

    result = runner.invoke(
        cli.app,
        ["merge", "--manifest", str(manifest), "--trusted-connection"],
    )

    assert result.exit_code != 0
    assert "--output-file is required for file manifests" in result.output


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


def test_cli_files_inspect(tmp_path, create_parquet):
    create_parquet(tmp_path, "x.parquet", rows=8, rowgroup_size=4)

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(cli.app, ["inspect", str(manifest)])

    assert result.exit_code == 0
    assert "x.parquet" in result.stdout
    assert "rows: 8" in result.stdout
    assert "row groups: 2" in result.stdout


def test_cli_rewrite_defaults_manifest_name(tmp_path, create_parquet):
    create_parquet(tmp_path, "x.parquet", rows=8, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(
        cli.app,
        [
            "rewrite",
            str(manifest),
            "--rowgroup-size",
            "4",
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "manifest_new.json").exists()


def test_cli_rewrite_manifest_version_is_2(tmp_path, create_parquet):
    create_parquet(tmp_path, "x.parquet", rows=8, rowgroup_size=2)

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(
        cli.app,
        [
            "rewrite",
            str(manifest),
            "--rowgroup-size",
            "4",
        ],
    )

    assert result.exit_code == 0
    manifest_file = tmp_path / "manifest_new.json"
    assert manifest_file.exists()

    with open(manifest_file) as f:
        manifest_data = json.load(f)

    assert manifest_data["version"] == 2
    assert manifest_data["type"] == "files"
    assert manifest_data["entries"] == ["x_new.parquet"]


def test_cli_files_inspect_verbose(tmp_path, create_parquet):
    create_parquet(tmp_path, "x.parquet")

    manifest = tmp_path / "manifest.json"
    with open(manifest, "w") as f:
        json.dump(["x.parquet"], f)

    result = runner.invoke(cli.app, ["inspect", str(manifest), "--verbose"])

    assert result.exit_code == 0
    assert "columns:" in result.stdout
    assert "id:" in result.stdout
    assert "value:" in result.stdout
