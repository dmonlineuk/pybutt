import re
from dataclasses import dataclass
from enum import StrEnum

from pybutt.exceptions import (
    EngineSelectionError,
    InvalidIdentifierError,
    InvalidParameterError,
)


ENGINE_CHOICES = frozenset({"duckdb", "pyodbc", "mssql-python"})

class TransactionMode(StrEnum):
    """Control how transactions are handled during import."""

    BATCH = "batch"  # Each batch of batch_size rows in its own transaction
    ROWGROUP = "rowgroup"  # Each row group in the parquet file in its own transaction
    FILE = "file"  # Entire file in one transaction

# Global defaults
DRIVER_DEFAULT = "ODBC Driver 18 for SQL Server"
SCHEMA_DEFAULT = 'dbo'
TRUSTED_CONNECTION_DEFAULT= False
TRUST_CERT_DEFAULT = False
ENCRYPT_DEFAULT = True
RETRIES_DEFAULT = 3

# Default memory heartbeat interval in seconds. Set to 30 so operators always
# have a recent RSS breadcrumb trail when a worker is OOM-killed.
MEM_HEARTBEAT_DEFAULT: float = 30.0

# Default memory-pressure throttle threshold (% system memory used). When system
# memory exceeds this %, workers sleep until pressure drops. Set to 85% so OOM
# kill is avoided without throttling during normal operation.
MEM_THRESHOLD_DEFAULT: float = 85.0

# Seconds to sleep per throttle cycle and max total wait before giving up.
MEM_SLEEP_DEFAULT: float = 5.0
MEM_MAX_WAIT_DEFAULT: float = 300.0

# Cooldown seconds after a throttle event before the gate re-checks. Prevents
# the gate from firing on every loop iteration and serialising workers.
MEM_COOLDOWN_DEFAULT: float = 30.0

# Default TDS packet size in bytes. 16383 is the maximum for encrypted
# connections (SQL Server caps encrypted packets at this size). Valid range
# for all drivers is 512–32767.
PACKET_SIZE_DEFAULT: int = 4_096

# Import specific defaults
IMPORT_ENGINE_DEFAULT = "mssql-python"
BATCH_SIZE_DEFAULT = 1_000
TRANSACTION_MODE_DEFAULT = TransactionMode.ROWGROUP
CCI_DEFAULT = True

# Export specific defaults
EXPORT_ENGINE_DEFAULT = "pyodbc"
FETCH_SIZE_DEFAULT = 1_000
ROWGROUP_SIZE_DEFAULT = 1_048_576


def validate_engine(engine: str, allowed: frozenset[str] | None = None) -> str:
    """Raise :class:`EngineSelectionError` if *engine* is not in *allowed*."""
    choices = allowed if allowed is not None else ENGINE_CHOICES
    if engine not in choices:
        raise EngineSelectionError(f"engine must be one of {sorted(choices)}")
    return engine


def coerce_transaction_mode(mode: TransactionMode | str) -> TransactionMode:
    """Accept a :class:`TransactionMode` or its string value and return the enum."""
    if isinstance(mode, str):
        return TransactionMode(mode)
    return mode


IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# TVF parameters must be a comma-separated list of literals:
# integers/decimals, single-quoted strings (no nested quotes), NULLs.
_PARAM_TOKEN_RE = re.compile(
    r"\s*(?:" r"NULL" r"|[+-]?\d+(?:\.\d+)?" r"|'[^']*'" r")\s*",
    re.IGNORECASE,
)


def validate_parameters(params: str) -> str:
    """Reject parameter strings that could contain SQL injection payloads.

    Accepts only comma-separated SQL literals: numbers, single-quoted
    strings (no embedded quotes), and NULL.
    """
    tokens = params.split(",")
    for token in tokens:
        if not _PARAM_TOKEN_RE.fullmatch(token):
            raise InvalidParameterError(
                f"Unsafe TVF parameter token: {token.strip()!r}. "
                "Only numeric literals, single-quoted strings, and NULL are allowed."
            )
    return params


def validate_identifier(name: str) -> str:
    if not IDENTIFIER_REGEX.match(name):
        raise InvalidIdentifierError(f"Invalid identifier: {name}")
    return name


def quote_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


def sanitise_dsn_value(value: str) -> str:
    """Escape ODBC connection-string metacharacters in a value.

    Braces and semicolons are special in ODBC DSN strings. If the value
    contains any of them, wrap it in ``{…}`` (doubling any literal
    ``}`` inside) so the driver interprets the whole token as one value.
    """
    if not value:
        return value
    if any(ch in value for ch in (";", "{", "}", "=")):
        return "{" + value.replace("}", "}}") + "}"
    return value


@dataclass
class SqlConfig:
    server: str
    database: str
    table: str
    username: str | None = None
    password: str | None = None
    schema: str = SCHEMA_DEFAULT
    driver: str = DRIVER_DEFAULT
    trusted_connection: bool = TRUSTED_CONNECTION_DEFAULT
    trust_cert: bool = TRUST_CERT_DEFAULT
    encrypt: bool = ENCRYPT_DEFAULT
    retries: int = RETRIES_DEFAULT
    packet_size: int = PACKET_SIZE_DEFAULT


if __name__ == "__main__":
    pass
