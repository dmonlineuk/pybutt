import re
import time

import duckdb as d
import mssql_python
import pyodbc

from pybutt.exceptions import ConfigurationError, RetryExceededError

from .config import (
    SqlConfig,
    quote_identifier,
    sanitise_dsn_value,
    validate_identifier,
)
from .logobs import get_logger

logger = get_logger("base")


def rows_from_arrow(arrow_obj) -> list[tuple]:
    """Convert an Arrow Table or RecordBatch to a list of row-tuples.

    Works with both ``pyarrow.Table`` and ``pyarrow.RecordBatch`` (anything
    with a ``.columns`` attribute whose elements support ``.to_pylist()``).
    """
    return list(zip(*[col.to_pylist() for col in arrow_obj.columns], strict=True))


class SqlServerIOBase:
    def __init__(self, config: SqlConfig):
        self.config = config

        self.schema = validate_identifier(config.schema)
        self.table = validate_identifier(config.table)

        self.dsn = self.build_dsn()

    def _connection_parts(self, *, include_driver: bool = True) -> list[str]:
        """Build the common connection-string parts shared by all drivers."""
        cfg = self.config
        parts: list[str] = []
        if include_driver:
            parts.append(f"Driver={{{cfg.driver}}}")
        parts.append(f"Server={sanitise_dsn_value(cfg.server)}")
        parts.append(f"Database={sanitise_dsn_value(cfg.database)}")

        if cfg.trusted_connection:
            parts.append("Trusted_Connection=Yes")
        else:
            if include_driver:
                if not cfg.username or not cfg.password:
                    raise ConfigurationError(
                        "Username/password required when not using trusted connection"
                    )
                parts.append(f"Uid={sanitise_dsn_value(cfg.username)}")
                parts.append(f"Pwd={sanitise_dsn_value(cfg.password)}")
            else:
                if cfg.username:
                    parts.append(f"UID={sanitise_dsn_value(cfg.username)}")
                if cfg.password:
                    parts.append(f"PWD={sanitise_dsn_value(cfg.password)}")

        parts.append(f"TrustServerCertificate={'Yes' if cfg.trust_cert else 'No'}")

        if cfg.encrypt:
            parts.append("Encrypt=Yes")

        # Add the PacketSize parameter
        parts.append("PacketSize=32768")

        return parts

    def build_dsn(self):
        return ";".join(self._connection_parts(include_driver=True)) + ";"

    def connection_d(self):
        conn = d.connect()
        conn.execute("INSTALL odbc_scanner; LOAD odbc_scanner;")
        return conn

    def connection_p(self, autocommit=False):
        conn = pyodbc.connect(self.dsn)
        conn.autocommit = autocommit
        return conn

    def connection_m(self, autocommit=False):
        conn_str = ";".join(self._connection_parts(include_driver=False)) + ";"
        conn = mssql_python.connect(conn_str)
        conn.setautocommit(autocommit)
        return conn

    def full_table_name(self):
        return f"{quote_identifier(self.schema)}.{quote_identifier(self.table)}"

    def safe_error_message(self, e: Exception) -> str:
        msg = str(e)

        # redact common sensitive tokens
        msg = re.sub(r"(Pwd|Password)=[^;]+", r"\1=***", msg, flags=re.IGNORECASE)
        msg = re.sub(r"(Uid|User ID)=[^;]+", r"\1=***", msg, flags=re.IGNORECASE)

        return msg

    def retry(self, fn, context="operation"):
        last_error: Exception | None = None
        for attempt in range(self.config.retries):
            try:
                return fn()
            except MemoryError:
                logger.error(f"{context} out of memory - not retrying (fatal)")
                raise
            except Exception as e:
                last_error = e
                safe_msg = self.safe_error_message(e)
                logger.warning(
                    f"{context} attempt {attempt + 1}/{self.config.retries} "
                    f"failed: {safe_msg}"
                )
                time.sleep(2**attempt)
        if last_error is not None:
            raise RetryExceededError(
                f"{context} failed after max retries: "
                f"{self.safe_error_message(last_error)}"
            ) from last_error
        raise RetryExceededError(f"{context} failed after max retries")


if __name__ == "__main__":
    pass
