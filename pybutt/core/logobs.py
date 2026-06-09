"""Centralised logging/observability helpers for PyButt.

All PyButt modules log through the ``pybutt`` logger (via :func:`get_logger`)
rather than the root logger. The CLI calls :func:`configure_logging` once at
startup; spawned export worker processes call it again through the pool
initialiser (see ``Exporter.perform_work``) so their output is formatted
identically on every platform (``spawn`` is the default on Windows/macOS and is
forced here on all OSes).

Library/API users who want PyButt's formatted output should call
:func:`configure_logging` themselves; otherwise standard ``logging`` rules apply.
"""

import logging
import threading

import psutil

LOGGER_NAME = "pybutt"

# Timestamp + level + process/thread identity so concurrent workers' lines can be
# told apart and ordered. Identity matters because a single import run fans out
# across threads and an export run across (spawned) processes.
LOG_FORMAT = (
    "%(asctime)s %(levelname)s [%(processName)s/%(threadName)s] %(name)s: %(message)s"
)
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a child of the ``pybutt`` logger (or the root pybutt logger)."""
    if name is None:
        return logging.getLogger(LOGGER_NAME)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")


def configure_logging(verbose: bool = False) -> logging.Logger:
    """Configure the ``pybutt`` logger. Idempotent and safe to call repeatedly.

    Adds a single stderr handler with :data:`LOG_FORMAT`, sets the level
    (``DEBUG`` when ``verbose`` else ``INFO``), and disables propagation so we
    don't double-emit through the root logger or fight library handlers.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    if not any(getattr(h, "_pybutt_handler", False) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        handler._pybutt_handler = True  # marker so we never add a duplicate
        logger.addHandler(handler)

    logger.propagate = False
    return logger


def init_worker_logging(level: int) -> None:
    """Pool initialiser: configure logging inside a spawned worker process."""
    configure_logging(verbose=level <= logging.DEBUG)
    logging.getLogger(LOGGER_NAME).setLevel(level)


def context(**fields: object) -> str:
    """Render structured ``key=value`` context, skipping ``None`` values.

    Example: ``context(file="a.parquet", rg="3/40", batch=12)`` ->
    ``"file=a.parquet rg=3/40 batch=12"``.
    """
    return " ".join(
        f"{key}={value}" for key, value in fields.items() if value is not None
    )


# --- memory observability --------------------------------------------------
#
# psutil gives a uniform *current* RSS on Windows/Linux/BSD/macOS (stdlib
# ``resource`` is Unix-only and its units differ by OS). There is no portable
# "peak RSS", so we track a running peak ourselves, per process. Export workers
# are separate processes, so each tracks (and reports) its own peak.

_process = psutil.Process()
_peak_rss = 0


def _human_bytes(num: float) -> str:
    """Render a byte count compactly, e.g. ``1.8GB`` / ``512.0MB`` / ``900B``."""
    value = float(num)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{int(value)}{unit}" if unit == "B" else f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}GB"


def rss_bytes() -> int:
    """Return current process RSS in bytes, updating the per-process peak.

    Returns 0 if the platform/process info is unavailable, so logging never
    fails because of a memory probe.
    """
    global _peak_rss
    try:
        rss = _process.memory_info().rss
    except Exception:
        return 0
    if rss > _peak_rss:
        _peak_rss = rss
    return rss


def peak_rss_bytes() -> int:
    """Return the highest RSS observed in this process (refreshes first)."""
    rss_bytes()
    return _peak_rss


def sys_mem_fields() -> dict[str, str]:
    """System-wide memory fields for :func:`context`.

    Returns ``{"sys_pct": "78%", "sys_avail": "4.2GB"}`` so log lines show
    how close the *machine* is to the OOM-kill threshold — not just this
    process's own RSS.
    """
    try:
        vm = psutil.virtual_memory()
        return {
            "sys_pct": f"{vm.percent:.0f}%",
            "sys_avail": _human_bytes(vm.available),
        }
    except Exception:
        return {}


def mem_fields() -> dict[str, str]:
    """RSS + system-wide memory fields for :func:`context`.

    Splat into ``context`` at boundary log points so the last line before an
    OOM-kill shows the memory trend and exactly where it died, e.g.::

        context(file=fn, rows=n, **mem_fields())
    """
    rss = rss_bytes()
    return {
        "rss": _human_bytes(rss),
        "peak": _human_bytes(_peak_rss),
        **sys_mem_fields(),
    }


class MemoryHeartbeat:
    """Periodically log process RSS while a long operation runs.

    Use as a context manager. A no-op when ``interval <= 0`` so callers can pass
    a user-configured value unconditionally. The thread is a daemon and is
    stopped/joined on exit. Runs in whichever process enters it, so for export
    it must be entered inside the worker (where the memory actually lives).
    """

    def __init__(
        self,
        interval: float,
        unit: str | None = None,
        progress: dict[str, object] | None = None,
    ):
        self.interval = interval or 0
        self.progress = progress
        self.unit = unit
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "MemoryHeartbeat":
        if self.interval > 0:
            self._thread = threading.Thread(
                target=self._run, name="mem-heartbeat", daemon=True
            )
            self._thread.start()
        return self

    def __exit__(self, *exc: object) -> bool:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1)
        return False

    def _run(self) -> None:
        log = get_logger("mem")
        while not self._stop.wait(self.interval):
            extra = dict(self.progress) if self.progress else {}
            log.info(
                "Memory heartbeat " + context(unit=self.unit, **extra, **mem_fields())
            )


class WorkerMonitor:
    """Monitor child worker processes from the parent and log their RSS.

    Runs a daemon thread that polls each worker PID via ``psutil``. When a
    worker disappears (e.g. OOM-killed by SIGKILL), the monitor logs the last
    known RSS and system memory state so the operator has a breadcrumb trail
    even though the child had no chance to log anything itself.

    Use as a context manager. A no-op when ``interval <= 0``.
    """

    def __init__(self, pids: list[int], interval: float):
        self.interval = interval or 0
        self._pids = list(pids)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_rss: dict[int, int] = {}

    def __enter__(self) -> "WorkerMonitor":
        if self.interval > 0 and self._pids:
            self._thread = threading.Thread(
                target=self._run, name="worker-monitor", daemon=True
            )
            self._thread.start()
        return self

    def __exit__(self, *exc: object) -> bool:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval + 1)
        return False

    def _run(self) -> None:
        log = get_logger("monitor")
        procs: dict[int, psutil.Process] = {}
        for pid in self._pids:
            try:
                procs[pid] = psutil.Process(pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        gone: set[int] = set()

        while not self._stop.wait(self.interval):
            sys_fields = sys_mem_fields()
            for pid in self._pids:
                if pid in gone:
                    continue
                proc = procs.get(pid)
                if proc is None:
                    gone.add(pid)
                    log.warning(
                        "Worker vanished "
                        + context(
                            pid=pid,
                            last_rss=_human_bytes(self._last_rss.get(pid, 0)),
                            status="GONE",
                            **sys_fields,
                        )
                        + " — likely OOM-killed"
                    )
                    continue
                try:
                    rss = proc.memory_info().rss
                    self._last_rss[pid] = rss
                    log.debug(
                        "Worker health "
                        + context(
                            pid=pid,
                            rss=_human_bytes(rss),
                            status="alive",
                            **sys_fields,
                        )
                    )
                except psutil.NoSuchProcess:
                    gone.add(pid)
                    log.warning(
                        "Worker vanished "
                        + context(
                            pid=pid,
                            last_rss=_human_bytes(self._last_rss.get(pid, 0)),
                            status="GONE",
                            **sys_fields,
                        )
                        + " — likely OOM-killed"
                    )
                except (psutil.AccessDenied, Exception):
                    pass
