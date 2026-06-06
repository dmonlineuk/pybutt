import logging
import re
import time

import duckdb as d
import mssql_python
import pyodbc

from pybutt.exceptions import ConfigurationError, RetryExceededError

from .config import (
    SqlConfig,
    quote_identifier,
    validate_identifier,
)

logging.basicConfig(level=logging.INFO)


class SqlServerIOBase:
    def __init__(self, config: SqlConfig):
        self.config = config

        self.schema = validate_identifier(config.schema)
        self.table = validate_identifier(config.table)

        self.dsn = self.build_dsn()

    def build_dsn(self):
        cfg = self.config

        parts = [
            f"Driver={{{cfg.driver}}}",
            f"Server={cfg.server}",
            f"Database={cfg.database}",
        ]

        if cfg.trusted_connection:
            parts.append("Trusted_Connection=Yes")
        else:
            if not cfg.username or not cfg.password:
                raise ConfigurationError(
                    "Username/password required when not using trusted connection"
                )
            parts.append(f"Uid={cfg.username}")
            parts.append(f"Pwd={cfg.password}")

        parts.append(f"TrustServerCertificate={'Yes' if cfg.trust_cert else 'No'}")

        if cfg.encrypt:
            parts.append("Encrypt=Yes")

        return ";".join(parts) + ";"

    def connection_d(self):
        conn = d.connect()
        conn.execute("INSTALL odbc_scanner; LOAD odbc_scanner;")
        return conn

    def connection_p(self, autocommit=False):
        conn = pyodbc.connect(self.dsn)
        conn.autocommit = autocommit
        return conn

    def connection_m(self, autocommit=False):
        cfg = self.config

        parts = [
            f"Server={cfg.server}",
            f"Database={cfg.database}",
        ]

        if cfg.trusted_connection:
            parts.append("Trusted_Connection=Yes")
        else:
            if cfg.username:
                parts.append(f"UID={cfg.username}")
            if cfg.password:
                parts.append(f"PWD={cfg.password}")

        parts.append(f"TrustServerCertificate={'Yes' if cfg.trust_cert else 'No'}")

        if cfg.encrypt:
            parts.append("Encrypt=Yes")

        conn_str = ";".join(parts) + ";"
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
            except Exception as e:
                last_error = e
                safe_msg = self.safe_error_message(e)
                logging.warning(
                    f"{context} retry {attempt+1}/{self.config.retries} "
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
