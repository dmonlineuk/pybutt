from typer.testing import CliRunner
import pybutt.cli as cli

runner = CliRunner()


# ------------------------------------------------------------
# ✅ BASIC RUN
# ------------------------------------------------------------

def test_cli_runs(monkeypatch, tmp_path):
    called = {"ran": False}

    class FakeExporter:
        def __init__(self, *args, **kwargs):
            cfg = kwargs["config"]
            assert cfg.server == "srv"

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

    assert result.exit_code == 0
    assert called["ran"]


# ------------------------------------------------------------
# ✅ OPTIONS
# ------------------------------------------------------------

def test_cli_options(monkeypatch, tmp_path):
    captured = {}

    class FakeExporter:
        def __init__(self, *args, **kwargs):
            captured.update(kwargs)

        def perform_work(self): pass

    monkeypatch.setattr(cli, "Exporter", FakeExporter)

    runner.invoke(cli.app, [
        "--server", "srv",
        "--database", "db",
        "--schema", "dbo",
        "--table", "tbl",
        "--output-path", str(tmp_path),
        "--username", "u",
        "--password", "p",
        "--pk-column", "id",
        "--columns", "c1",
        "--columns", "c2",
        "--worker-count", "4"
    ])

    cfg = captured["config"]

    assert cfg.username == "u"
    assert captured["columns"] == ["c1", "c2"]
    assert captured["worker_count"] == 4


# ------------------------------------------------------------
# ✅ ERROR HANDLING
# ------------------------------------------------------------

def test_cli_error(monkeypatch, tmp_path):

    class FakeExporter:
        def __init__(self, *a, **k): pass
        def perform_work(self): raise RuntimeError("boom")

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


# ------------------------------------------------------------
# ✅ HELP
# ------------------------------------------------------------

def test_cli_help():
    result = runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0
    assert "Python Bulk Transfer Tool for MS SQL Server CLI" in result.stdout