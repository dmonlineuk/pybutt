from typer.testing import CliRunner

from pybutt.cli import cli

runner = CliRunner()


def test_cli_help_contains_commands():
    result = runner.invoke(cli.app, ["--help"])
    assert result.exit_code == 0
    assert " export " in result.output
    assert " import " in result.output
    assert " combine " in result.output
    assert " inspect " in result.output
    assert " purge " in result.output


def test_export_help_includes_options():
    result = runner.invoke(cli.app, ["export", "--help"])
    assert result.exit_code == 0
    assert " Arguments " not in result.output
    assert "--verbose" in result.output
    assert "Server Connection Options" in result.output
    assert "--server" in result.output
    assert "--database" in result.output
    assert "--driver" in result.output
    assert "--engine" in result.output
    assert "SQL Data Object Options" in result.output
    assert "--schema" in result.output
    assert "--table" in result.output
    assert "--parameters" in result.output
    assert "--columns" in result.output
    assert "--pk-column" in result.output
    assert "Server Security Options" in result.output
    assert "--username" in result.output
    assert "--password" in result.output
    assert "--trusted-connection" in result.output
    assert "--trust-cert" in result.output
    assert "--encrypt" in result.output
    assert "File Options" in result.output
    assert "--output-path" in result.output
    assert "--manifest-filename" in result.output
    assert "--file-count" in result.output
    assert "Transport Tuning Options" in result.output
    assert "--fetch-size" in result.output
    assert "--packet-size" in result.output
    assert "--rowgroup-size" in result.output
    assert "--retries" in result.output
    assert "--worker-count" in result.output
    assert "Memory Tuning Options" in result.output
    assert "--mem-heartbeat" in result.output
    assert "--mem-threshold" in result.output
    assert "--mem-sleep" in result.output
    assert "--mem-max-wait" in result.output
    assert "--mem-cooldown" in result.output


def test_import_help_includes_options():
    result = runner.invoke(cli.app, ["import", "--help"])
    assert result.exit_code == 0
    assert " Arguments " in result.output
    assert "--verbose" in result.output
    assert "Server Connection Options" in result.output
    assert "--server" in result.output
    assert "--database" in result.output
    assert "--driver" in result.output
    assert "--engine" in result.output
    assert "--transaction-mode" in result.output
    assert "SQL Data Object Options" in result.output
    assert "--schema" in result.output
    assert "--table" in result.output
    assert "--cci" in result.output
    assert "Server Security Options" in result.output
    assert "--username" in result.output
    assert "--password" in result.output
    assert "--trusted-connection" in result.output
    assert "--trust-cert" in result.output
    assert "--encrypt" in result.output
    assert "File Options" in result.output
    assert "--imported-manifest-filename" in result.output
    assert "Transport Tuning Options" in result.output
    assert "--batch-size" in result.output
    assert "--packet-size" in result.output
    assert "--retries" in result.output
    assert "--worker-count" in result.output
    assert "Memory Tuning Options" in result.output
    assert "--mem-heartbeat" in result.output
    assert "--mem-threshold" in result.output
    assert "--mem-sleep" in result.output
    assert "--mem-max-wait" in result.output
    assert "--mem-cooldown" in result.output


def test_combine_help_includes_options():
    result = runner.invoke(cli.app, ["combine", "--help"])
    assert result.exit_code == 0
    assert " Arguments " in result.output
    assert "--verbose" in result.output
    assert "Server Connection Options" in result.output
    assert "--server" in result.output
    assert "--database" in result.output
    assert "--driver" in result.output
    assert "SQL Data Object Options" in result.output
    assert "--schema" in result.output
    assert "--table" in result.output
    assert "Server Security Options" in result.output
    assert "--username" in result.output
    assert "--password" in result.output
    assert "--trusted-connection" in result.output
    assert "--trust-cert" in result.output
    assert "--encrypt" in result.output
    assert "File Options" in result.output
    assert "--rowgroup-size" in result.output
    assert "Transport Tuning Options" in result.output
    assert "--packet-size" in result.output
    assert "--retries" in result.output


def test_inspect_help_includes_options():
    result = runner.invoke(cli.app, ["inspect", "--help"])
    assert result.exit_code == 0
    assert " Arguments " in result.output
    assert "--verbose" in result.output


def test_purge_help_includes_options():
    result = runner.invoke(cli.app, ["purge", "--help"])
    assert result.exit_code == 0
    assert " Arguments " in result.output
    assert "--verbose" in result.output
