"""Tests for the centralised logging/observability helpers (Phases 1 & 2).

Covers:
- ``context`` structured key=value rendering (and None skipping)
- ``configure_logging`` idempotency, level, and propagation
- ``get_logger`` naming under the ``pybutt`` hierarchy
- ``MemoryError`` is logged and re-raised (never retried) in ``base.retry``
  and the importer batch/rowgroup retry helpers
- worker-failure surfacing names the failing unit before re-raising
- memory observability: ``_human_bytes`` formatting, ``rss_bytes`` peak
  tracking, ``mem_fields`` shape, and ``MemoryHeartbeat`` lifecycle
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import SqlConfig, TransactionMode
from pybutt.core.logobs import (
    LOGGER_NAME,
    MemoryGate,
    MemoryHeartbeat,
    WorkerMonitor,
    _human_bytes,
    configure_logging,
    context,
    get_logger,
    init_worker_logging,
    log_failure_summary,
    log_memory_budget,
    mem_fields,
    rss_bytes,
    sys_mem_fields,
)
from pybutt.io.importer import Importer


@pytest.fixture
def mock_config():
    return SqlConfig(
        server="localhost",
        database="TestDb",
        schema="dbo",
        table="MyTable",
        trusted_connection=True,
        retries=3,
    )


@pytest.fixture(autouse=True)
def _reset_pybutt_logger():
    """Restore the pybutt logger to a clean state around each test."""
    logger = logging.getLogger(LOGGER_NAME)
    saved_handlers = logger.handlers[:]
    saved_level = logger.level
    saved_propagate = logger.propagate
    logger.handlers = []
    yield
    logger.handlers = saved_handlers
    logger.level = saved_level
    logger.propagate = saved_propagate


# --- context() ------------------------------------------------------------


def test_context_renders_key_values():
    assert context(file="a.parquet", rg="3/40", batch=12) == (
        "file=a.parquet rg=3/40 batch=12"
    )


def test_context_skips_none_but_keeps_zero():
    # offset=0 must be kept (it is a valid identifier); only None is dropped.
    assert context(file="a.parquet", rg=None, offset=0) == "file=a.parquet offset=0"


def test_context_empty_when_all_none():
    assert context(rg=None, batch=None) == ""


# --- get_logger / configure_logging --------------------------------------


def test_get_logger_names_are_under_pybutt():
    assert get_logger().name == "pybutt"
    assert get_logger("importer").name == "pybutt.importer"


def test_configure_logging_is_idempotent():
    configure_logging()
    configure_logging()
    configure_logging(verbose=True)
    logger = logging.getLogger(LOGGER_NAME)
    pybutt_handlers = [
        h for h in logger.handlers if getattr(h, "_pybutt_handler", False)
    ]
    assert len(pybutt_handlers) == 1


def test_configure_logging_sets_level_and_disables_propagation():
    logger = configure_logging(verbose=False)
    assert logger.level == logging.INFO
    assert logger.propagate is False

    logger = configure_logging(verbose=True)
    assert logger.level == logging.DEBUG


def test_init_worker_logging_configures_level():
    init_worker_logging(logging.DEBUG)
    logger = logging.getLogger(LOGGER_NAME)
    assert logger.level == logging.DEBUG
    assert any(getattr(h, "_pybutt_handler", False) for h in logger.handlers)


def test_formatted_line_contains_identity_and_context():
    # propagate is False by design, so format a record through the handler's
    # own formatter rather than relying on caplog (which sits on the root).
    configure_logging()
    logger = logging.getLogger(LOGGER_NAME)
    handler = next(h for h in logger.handlers if getattr(h, "_pybutt_handler", False))
    record = logging.LogRecord(
        name="pybutt.importer",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="Importing " + context(file="a.parquet", rg="1/2"),
        args=(),
        exc_info=None,
    )
    line = handler.formatter.format(record)
    assert "INFO" in line
    assert "pybutt.importer" in line
    assert "[MainProcess/" in line  # processName/threadName identity
    assert "file=a.parquet" in line
    assert "rg=1/2" in line


# --- MemoryError fail-fast -------------------------------------------------


def test_base_retry_does_not_retry_memoryerror(mock_config):
    base = SqlServerIOBase(mock_config)
    fn = MagicMock(side_effect=MemoryError("oom"))
    with patch("time.sleep") as sleep:
        with pytest.raises(MemoryError):
            base.retry(fn, context="unit")
    assert fn.call_count == 1  # not retried
    sleep.assert_not_called()


def test_importer_batch_retry_does_not_retry_memoryerror(tmp_path, mock_config):
    importer = Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.BATCH,
    )
    cur = MagicMock()
    cur.executemany.side_effect = MemoryError("oom")
    conn = MagicMock()
    with patch("time.sleep") as sleep:
        with pytest.raises(MemoryError):
            importer._import_batch_with_retry(
                conn, cur, [(1,)], "INSERT", "f.parquet", batch=0, offset=0
            )
    assert cur.executemany.call_count == 1
    sleep.assert_not_called()


def test_importer_batch_retry_retries_other_errors(tmp_path, mock_config):
    importer = Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.BATCH,
    )
    cur = MagicMock()
    cur.executemany.side_effect = [ValueError("boom"), None]
    conn = MagicMock()
    with patch("time.sleep"):
        result = importer._import_batch_with_retry(
            conn, cur, [(1,)], "INSERT", "f.parquet", batch=0, offset=0
        )
    assert result == 1
    assert cur.executemany.call_count == 2  # retried once then succeeded


# --- worker failure surfacing ---------------------------------------------


def test_await_futures_logs_failing_unit(tmp_path, mock_config, caplog):
    importer = Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        batch_size=1000,
        transaction_mode=TransactionMode.BATCH,
    )

    def boom():
        raise ValueError("worker died")

    # Attach caplog's handler to the pybutt logger directly: the logger has
    # propagate=False once configured, so it would not reach caplog's root handler.
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.ERROR)

    with ThreadPoolExecutor(max_workers=1) as ex:
        futures = {ex.submit(boom): "dbo_Posts_part_00000.parquet"}
        with pytest.raises(ValueError):
            importer._await_futures(futures, label="file")

    messages = [r.message for r in caplog.records]
    assert any(
        "Worker failed" in m and "file=dbo_Posts_part_00000.parquet" in m
        for m in messages
    )


# --- memory observability (Phase 2) ---------------------------------------


@pytest.mark.parametrize(
    "num,expected",
    [
        (0, "0B"),
        (900, "900B"),
        (1024, "1.0KB"),
        (1536, "1.5KB"),
        (1024 * 1024, "1.0MB"),
        (int(1.8 * 1024**3), "1.8GB"),
        (5 * 1024**4, "5120.0GB"),  # no TB unit; GB is the cap
    ],
)
def test_human_bytes(num, expected):
    assert _human_bytes(num) == expected


def test_rss_bytes_is_positive_and_tracks_peak():
    first = rss_bytes()
    assert first > 0
    # Allocate ~40MB; peak must not decrease and current should be observable.
    blob = bytearray(40 * 1024 * 1024)
    second = rss_bytes()
    assert second >= first
    del blob
    fields = mem_fields()
    assert {"rss", "peak", "sys_pct", "sys_avail"} <= set(fields)
    assert fields["rss"].endswith(("B", "KB", "MB", "GB"))
    assert fields["peak"].endswith(("B", "KB", "MB", "GB"))
    assert fields["sys_pct"].endswith("%")
    assert fields["sys_avail"].endswith(("B", "KB", "MB", "GB"))


def test_sys_mem_fields_returns_system_info():
    fields = sys_mem_fields()
    assert "sys_pct" in fields
    assert "sys_avail" in fields
    assert fields["sys_pct"].endswith("%")
    assert fields["sys_avail"].endswith(("B", "KB", "MB", "GB"))


def test_memory_heartbeat_disabled_starts_no_thread():
    hb = MemoryHeartbeat(0, unit="import")
    with hb:
        time.sleep(0.05)
    assert hb._thread is None


def test_memory_heartbeat_emits_and_stops(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.INFO)

    hb = MemoryHeartbeat(0.05, unit="import")
    with hb:
        time.sleep(0.2)
    # Thread is stopped/joined on exit.
    assert hb._thread is not None
    assert not hb._thread.is_alive()

    heartbeats = [r.message for r in caplog.records if "Memory heartbeat" in r.message]
    assert heartbeats, "expected at least one heartbeat line"
    assert "unit=import" in heartbeats[0]
    assert "rss=" in heartbeats[0] and "peak=" in heartbeats[0]


def test_memory_heartbeat_includes_progress(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.INFO)

    progress = {"rows_buffered": 0}
    hb = MemoryHeartbeat(0.05, unit="export", progress=progress)
    with hb:
        progress["rows_buffered"] = 42_000
        time.sleep(0.2)

    heartbeats = [r.message for r in caplog.records if "Memory heartbeat" in r.message]
    assert heartbeats, "expected at least one heartbeat line"
    assert any("rows_buffered=42000" in m for m in heartbeats)


def test_memory_heartbeat_includes_sys_mem(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.INFO)

    hb = MemoryHeartbeat(0.05, unit="import")
    with hb:
        time.sleep(0.2)

    heartbeats = [r.message for r in caplog.records if "Memory heartbeat" in r.message]
    assert heartbeats, "expected at least one heartbeat line"
    assert "sys_pct=" in heartbeats[0]
    assert "sys_avail=" in heartbeats[0]


def test_worker_monitor_logs_alive(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.DEBUG)

    # Monitor our own PID
    with WorkerMonitor([os.getpid()], interval=0.05):
        time.sleep(0.2)

    health_msgs = [r.message for r in caplog.records if "Worker health" in r.message]
    assert health_msgs, "expected at least one worker health line"
    assert f"pid={os.getpid()}" in health_msgs[0]
    assert "status=alive" in health_msgs[0]


def test_worker_monitor_detects_vanished(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.WARNING)

    # Use a PID that doesn't exist
    fake_pid = 999999
    with WorkerMonitor([fake_pid], interval=0.05):
        time.sleep(0.2)

    vanished_msgs = [
        r.message for r in caplog.records if "Worker vanished" in r.message
    ]
    assert vanished_msgs, "expected at least one worker vanished line"
    assert f"pid={fake_pid}" in vanished_msgs[0]
    assert "GONE" in vanished_msgs[0]


def test_worker_monitor_disabled_when_zero_interval():
    monitor = WorkerMonitor([os.getpid()], interval=0)
    with monitor:
        time.sleep(0.05)
    assert monitor._thread is None


def test_memory_gate_noop_when_disabled():
    gate = MemoryGate(threshold_pct=0)
    assert gate.check("test") == 0.0


def test_memory_gate_noop_when_below_threshold():
    gate = MemoryGate(threshold_pct=99.9)
    assert gate.check("test") == 0.0


def test_memory_gate_logs_warning_when_triggered(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.WARNING)

    # threshold=1% means it will always trigger (system is always >1%)
    gate = MemoryGate(threshold_pct=1.0, sleep_seconds=0.05, max_wait=0.1)
    waited = gate.check("unit_test")

    assert waited > 0
    pressure_msgs = [
        r.message for r in caplog.records if "Memory pressure" in r.message
    ]
    assert pressure_msgs, "expected a throttle warning"
    assert "unit_test" in pressure_msgs[0]


def test_memory_gate_respects_max_wait():
    gate = MemoryGate(threshold_pct=1.0, sleep_seconds=0.05, max_wait=0.1)
    waited = gate.check("timeout_test")
    assert waited <= 0.2  # max_wait + one sleep cycle


def test_memory_gate_cooldown_skips_recheck():
    gate = MemoryGate(
        threshold_pct=1.0, sleep_seconds=0.05, max_wait=0.1, cooldown_seconds=60.0
    )
    # First check triggers (system is always >1%)
    waited1 = gate.check("first")
    assert waited1 > 0
    # Second check should be skipped due to cooldown
    waited2 = gate.check("second")
    assert waited2 == 0.0


def test_log_memory_budget_emits_info(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.INFO)

    log_memory_budget(operation="export", workers=4, threshold_pct=85.0)

    budget_msgs = [r.message for r in caplog.records if "Memory budget" in r.message]
    assert budget_msgs, "expected a memory budget line"
    assert "operation=export" in budget_msgs[0]
    assert "workers=4" in budget_msgs[0]
    assert "sys_total=" in budget_msgs[0]
    assert "headroom=" in budget_msgs[0]


def test_log_memory_budget_includes_total_rows(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.INFO)

    log_memory_budget(operation="import", workers=2, total_rows=1_000_000)

    budget_msgs = [r.message for r in caplog.records if "Memory budget" in r.message]
    assert budget_msgs
    assert "total_rows=1000000" in budget_msgs[0]


def test_log_failure_summary_emits_error(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.ERROR)

    log_failure_summary(
        operation="export",
        workers=4,
        completed=["part_0", "part_1"],
        failed_error="worker died",
    )

    error_msgs = [r.message for r in caplog.records if "FAILURE SUMMARY" in r.message]
    assert error_msgs, "expected a failure summary line"
    assert "operation=export" in error_msgs[0]
    assert "completed=2/4" in error_msgs[0]
    assert "error=worker died" in error_msgs[0]

    completed_msgs = [
        r.message for r in caplog.records if "Completed units" in r.message
    ]
    assert completed_msgs
    assert "part_0" in completed_msgs[0]


def test_log_failure_summary_no_completed(caplog):
    pybutt_logger = logging.getLogger(LOGGER_NAME)
    pybutt_logger.addHandler(caplog.handler)
    pybutt_logger.setLevel(logging.ERROR)

    log_failure_summary(operation="import", workers=2, failed_error="timeout")

    error_msgs = [r.message for r in caplog.records if "FAILURE SUMMARY" in r.message]
    assert error_msgs
    assert "completed=0/2" in error_msgs[0]


def test_importer_accepts_mem_heartbeat(tmp_path, mock_config):
    importer = Importer(
        config=mock_config,
        input_path=tmp_path,
        manifest_filename="manifest.json",
        mem_heartbeat=1.5,
    )
    assert importer.mem_heartbeat == 1.5
