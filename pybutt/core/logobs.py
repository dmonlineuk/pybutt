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
