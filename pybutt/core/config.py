import re
from dataclasses import dataclass
from enum import StrEnum

from pybutt.exceptions import (
    EngineSelectionError,
    InvalidIdentifierError,
    InvalidParameterError,
)

ENGINE_CHOICES = frozenset({"duckdb", "pyodbc", "mssql-python"})

# Generic (engine-independent) fallback for the import batch size.
DEFAULT_IMPORT_BATCH_SIZE = 1_000

# Per-engine default overrides, keyed by tunable name then engine. Only values
# that diverge from the generic fallback are listed; everything else falls back.
# See docs/defaults.md for the rationale behind each entry.
ENGINE_DEFAULTS: dict[str, dict[str, int]] = {
    # mssql-python import uses bulkcopy, where each batch closes a columnstore
    # rowgroup, so default to a full rowgroup instead of the generic batch size.
    "batch_size": {"mssql-python": 1_048_576},
}


def resolve_engine_default(
    tunable: str, engine: str, value: int | None, fallback: int
) -> int:
    """Resolve a tunable: explicit value wins, else engine default, else fallback."""
    if value is not None:
        return value
    return ENGINE_DEFAULTS.get(tunable, {}).get(engine, fallback)


class TransactionMode(StrEnum):
    """Control how transactions are handled during import."""

    ROW = "row"  # Each row commits individually (no transaction)
    BATCH = "batch"  # Each batch of batch_size rows in its own transaction
    ROWGROUP = "rowgroup"  # Each row group in the parquet file in its own transaction
    FILE = "file"  # Entire file in one transaction


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
    r"\s*(?:"
    r"NULL"
    r"|[+-]?\d+(?:\.\d+)?"
    r"|'[^']*'"
    r")\s*",
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
    schema: str
    table: str
    username: str | None = None
    password: str | None = None
    driver: str = "ODBC Driver 18 for SQL Server"
    trusted_connection: bool = False
    trust_cert: bool = False
    encrypt: bool = True
    retries: int = 3


if __name__ == "__main__":
    pass
