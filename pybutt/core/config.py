import re
from dataclasses import dataclass
from enum import StrEnum

ENGINE_CHOICES = frozenset({"duckdb", "pyodbc"})


class TransactionMode(StrEnum):
    """Control how transactions are handled during import."""

    ROW = "row"  # Each row commits individually (no transaction)
    BATCH = "batch"  # Each batch of batch_size rows in its own transaction
    ROWGROUP = "rowgroup"  # Each row group in the parquet file in its own transaction
    FILE = "file"  # Entire file in one transaction


IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(name: str) -> str:
    if not IDENTIFIER_REGEX.match(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name


def quote_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"


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
