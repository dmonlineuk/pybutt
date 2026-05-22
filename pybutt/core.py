import json as j
import logging
import math as m
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing import get_context
from pathlib import Path

import duckdb as d
import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

logging.basicConfig(level=logging.INFO)

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
                raise ValueError(
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

    def full_table_name(self):
        return f"{quote_identifier(self.schema)}.{quote_identifier(self.table)}"

    def safe_error_message(self, e: Exception) -> str:
        msg = str(e)

        # redact common sensitive tokens
        msg = re.sub(r"(Pwd|Password)=[^;]+", r"\1=***", msg, flags=re.IGNORECASE)
        msg = re.sub(r"(Uid|User ID)=[^;]+", r"\1=***", msg, flags=re.IGNORECASE)

        return msg

    def retry(self, fn, context="operation"):
        for attempt in range(self.config.retries):
            try:
                return fn()
            except Exception as e:
                safe_msg = self.safe_error_message(e)
                logging.warning(
                    f"{context} retry {attempt+1}/{self.config.retries} "
                    f"failed: {safe_msg}"
                )
                time.sleep(2**attempt)
        raise RuntimeError(f"{context} failed after max retries")


class Exporter(SqlServerIOBase):
    def __init__(
        self,
        config: SqlConfig,
        output_path,
        pk_column=None,
        columns=None,
        worker_count=1,
        file_count=1,
        rowgroup_size=1_048_576,
        engine="duckdb",
    ):
        super().__init__(config)

        self.pk_column = validate_identifier(pk_column) if pk_column else None
        self.columns = [validate_identifier(c) for c in columns] if columns else None

        if engine not in ENGINE_CHOICES:
            raise ValueError(f"engine must be one of {sorted(ENGINE_CHOICES)}")

        if file_count < 1:
            raise ValueError("file_count must be at least 1")

        self.worker_count = worker_count
        self.file_count = file_count
        self.rowgroup_size = rowgroup_size
        self.engine = engine

        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        self.total_rows = 0
        self.partition_count = 0
        self.chunk_size = 0

        self.partition_meta()

    def partition_meta(self):
        def _work():
            with self.connection_d() as c:
                query = f"""
                    SELECT SUM(row_count)
                    FROM sys.dm_db_partition_stats
                    WHERE object_id = OBJECT_ID('{self.full_table_name()}')
                    AND index_id IN (0,1)
                """

                return (
                    c.execute(
                        f"FROM odbc_query('{self.dsn}', $$ {query} $$)"
                    ).fetchone()[0]
                    or 0
                )

        self.total_rows = self.retry(_work, context="Fetching partition strategy")

        if self.total_rows == 0:
            raise RuntimeError("Table empty or not found")

        self.partition_count = self.file_count
        self.chunk_size = m.ceil(self.total_rows / self.partition_count)

        logging.info(
            f"Partitioning table={self.schema}.{self.table} "
            f"total_rows={self.total_rows} "
            f"file_count={self.file_count} "
            f"chunk_size={self.chunk_size}"
        )

        if self.pk_column:
            logging.info(f"Partition strategy=ROW_NUMBER pk={self.pk_column}")
        else:
            logging.info(f"Partition strategy=CHECKSUM modulo={self.partition_count}")

    def get_table_columns(self):
        query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.schema}'
              AND TABLE_NAME = '{self.table}'
            ORDER BY ORDINAL_POSITION
        """

        with self.connection_d() as c:
            rows = c.execute(f"FROM odbc_query('{self.dsn}', $$ {query} $$)").fetchall()

        return [row[0] for row in rows]

    def build_partition_query(self, n):
        if self.pk_column:
            start = n * self.chunk_size
            end = (n + 1) * self.chunk_size

            if self.columns is None:
                column_names = self.get_table_columns()
                selected_columns = ", ".join(quote_identifier(c) for c in column_names)
            else:
                selected_columns = ", ".join(quote_identifier(c) for c in self.columns)

            return (
                f"SELECT {selected_columns} "
                "FROM ( "
                f"SELECT {selected_columns}, "
                "ROW_NUMBER() OVER ("
                f"ORDER BY {quote_identifier(self.pk_column)}"
                ") AS rn "
                f"FROM {self.full_table_name()} "
                ") t "
                f"WHERE rn > {start} AND rn <= {end}"
            )
        else:
            selected_columns = (
                ", ".join(quote_identifier(c) for c in self.columns)
                if self.columns is not None
                else "*"
            )
            return f"""
                SELECT {selected_columns}
                FROM {self.full_table_name()}
                WHERE ABS(CHECKSUM(*)) % {self.partition_count} = {n}
            """

    def _pyodbc_type_code_to_pyarrow(self, type_code, precision, scale, internal_size):
        if type_code in (pyodbc.SQL_TINYINT, pyodbc.SQL_SMALLINT, pyodbc.SQL_INTEGER):
            return pa.int32()
        if type_code == pyodbc.SQL_BIGINT:
            return pa.int64()
        if type_code in (pyodbc.SQL_REAL, pyodbc.SQL_FLOAT):
            return pa.float32()
        if type_code == pyodbc.SQL_DOUBLE:
            return pa.float64()
        if type_code in (pyodbc.SQL_DECIMAL, pyodbc.SQL_NUMERIC):
            precision = precision or 38
            scale = scale or 0
            return pa.decimal128(precision, scale)
        if type_code in (
            pyodbc.SQL_CHAR,
            pyodbc.SQL_VARCHAR,
            pyodbc.SQL_LONGVARCHAR,
            pyodbc.SQL_WCHAR,
            pyodbc.SQL_WVARCHAR,
            pyodbc.SQL_WLONGVARCHAR,
        ):
            return pa.string()
        if type_code in (
            pyodbc.SQL_BINARY,
            pyodbc.SQL_VARBINARY,
            pyodbc.SQL_LONGVARBINARY,
        ):
            return pa.binary()
        if type_code == pyodbc.SQL_BIT:
            return pa.bool_()
        if type_code == pyodbc.SQL_TYPE_DATE:
            return pa.date32()
        if type_code == pyodbc.SQL_TYPE_TIME:
            return pa.time64("us")
        if type_code == pyodbc.SQL_TYPE_TIMESTAMP:
            return pa.timestamp("us")
        return pa.string()

    def _pyodbc_schema_from_description(self, description):
        return pa.schema(
            [
                pa.field(
                    column[0],
                    self._pyodbc_type_code_to_pyarrow(
                        column[1],
                        column[5],
                        column[6],
                        column[3],
                    ),
                    nullable=column[6],
                )
                for column in description
            ]
        )

    def _export_partition_with_duckdb(self, query, filepath, filename):
        with self.connection_d() as c:
            try:
                c.execute(f"""
                    COPY (
                        FROM odbc_query('{self.dsn}', $$ {query} $$)
                    )
                    TO '{str(filepath).replace('\\', '/')}'
                    (
                        FORMAT parquet,
                        COMPRESSION snappy,
                        ROW_GROUP_SIZE {self.rowgroup_size}
                    )
                """)
            except Exception as e:
                raise RuntimeError(
                    f"Failed exporting {filename}: {self.safe_error_message(e)}"
                ) from e

    def _export_partition_with_pyodbc(self, query, filepath, filename):
        with self.connection_p() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query)
                except Exception as e:
                    raise RuntimeError(
                        f"Failed exporting {filename}: {self.safe_error_message(e)}"
                    ) from e

                columns = [desc[0] for desc in cur.description]
                schema = self._pyodbc_schema_from_description(cur.description)
                writer = pq.ParquetWriter(
                    str(filepath.as_posix()), schema, compression="snappy"
                )
                fetch_size = min(max(1024, self.rowgroup_size), 8192)

                while True:
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    batch = [dict(zip(columns, row, strict=True)) for row in rows]
                    table = pa.Table.from_pylist(batch, schema=schema)
                    writer.write_table(table, row_group_size=self.rowgroup_size)

                writer.close()

    def export_partition(self, n):
        thread_id = threading.get_ident()
        start = time.time()
        safe_name = f"{self.schema}_{self.table}"
        filename = f"{safe_name}_part_{n:05d}.parquet"
        filepath = self.output_path / filename
        query = self.build_partition_query(n)

        logging.debug(f"Partition {n} query: {query}")
        logging.info(
            f"Thread={thread_id} "
            f"Exporting file={filename} "
            f"partition={n}/{self.partition_count-1} "
            f"table={self.schema}.{self.table} "
            f"engine={self.engine}"
        )

        def _work():
            if self.engine == "duckdb":
                self._export_partition_with_duckdb(query, filepath, filename)
            else:
                self._export_partition_with_pyodbc(query, filepath, filename)

        self.retry(_work, context=f"Export partition {n}")

        duration = time.time() - start
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
        else:
            size_mb = 0
        logging.info(
            f"Completed file={filename} "
            f"rows~{self.chunk_size} "
            f"size={size_mb:.2f} MB "
            f"time={duration:.2f}s "
            f"Progress: {n+1}/{self.partition_count}"
        )

        return filename

    def perform_work(self):
        start = time.time()
        safe_name = f"{self.schema}_{self.table}"
        manifest_file = self.output_path / f"{safe_name}_manifest.json"

        with get_context("spawn").Pool(self.worker_count) as p:
            filenames = p.map(self.export_partition, range(self.partition_count))

        duration = time.time() - start

        logging.info(
            f"Export complete table={self.schema}.{self.table} "
            f"files={len(filenames)} "
            f"time={duration:.2f}s"
        )

        logging.info(f"Writing manifest: {manifest_file}")
        try:
            with open(manifest_file, "w") as f:
                j.dump(filenames, f, indent=4)
            logging.info(
                f"Manifest written: {manifest_file} " f"files={len(filenames)}"
            )

        except Exception as e:
            logging.error(
                f"Failed to write manifest {manifest_file}: "
                f"{self.safe_error_message(e)}"
            )


class Importer(SqlServerIOBase):
    def __init__(
        self,
        config: SqlConfig,
        input_path,
        manifest_filename,
        worker_count=1,
        batch_size=1_000,
        transaction_mode: TransactionMode = TransactionMode.BATCH,
        engine="pyodbc",
    ):
        super().__init__(config)

        self.input_path = Path(input_path)
        self.manifest_filename = manifest_filename

        self.worker_count = worker_count
        self.batch_size = batch_size
        self.transaction_mode = (
            TransactionMode(transaction_mode)
            if isinstance(transaction_mode, str)
            else transaction_mode
        )
        if engine not in ENGINE_CHOICES:
            raise ValueError(f"engine must be one of {sorted(ENGINE_CHOICES)}")
        self.engine = engine

    def load_manifest(self):
        manifest_file = self.input_path / self.manifest_filename

        if not manifest_file.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_file}")

        with open(manifest_file) as f:
            data = j.load(f)

        if not isinstance(data, list):
            raise ValueError("Manifest must be a list of filenames")

        seen = set()
        validated = []

        for item in data:
            if not isinstance(item, str):
                raise ValueError(f"Invalid manifest entry (not string): {item}")

            if item in seen:
                raise ValueError(f"Duplicate file in manifest: {item}")

            filepath = self.input_path / item
            if not filepath.exists():
                raise FileNotFoundError(f"Missing file: {filepath}")

            seen.add(item)
            validated.append(item)

        return validated

    def get_table_columns(self, cur):
        cur.execute(f"SELECT TOP 0 * FROM {self.full_table_name()}")
        return [column[0] for column in cur.description]

    def validate_schema(self, parquet_columns, table_columns, filename):
        parquet_set = set(parquet_columns)
        table_set = set(table_columns)

        if parquet_set != table_set:
            missing_in_sql = parquet_set - table_set
            missing_in_parquet = table_set - parquet_set

            raise ValueError(
                f"Schema mismatch in {filename}:\n"
                f"  Columns in parquet but not SQL: {missing_in_sql}\n"
                f"  Columns in SQL but not parquet: {missing_in_parquet}"
            )

    def import_file(self, filename):
        filepath = self.input_path / filename
        thread_id = threading.get_ident()
        start = time.time()

        logging.info(
            f"Thread={thread_id} "
            f"Importing file={filename} "
            f"table={self.schema}.{self.table} "
            f"batch_size={self.batch_size} "
            f"transaction_mode={self.transaction_mode.value}"
        )

        try:
            if self.transaction_mode == TransactionMode.FILE:
                # For FILE mode, wrap entire operation in retry logic
                def _file_operation():
                    if self.engine == "duckdb":
                        return self._import_file_with_duckdb(filepath, filename, start)
                    return self._import_file_impl(filepath, filename, start)

                self.retry(_file_operation, context=f"Import file {filename}")
            else:
                # For BATCH, ROWGROUP, and ROW modes, retries happen at granular level
                if self.engine == "duckdb":
                    self._import_file_with_duckdb(filepath, filename, start)
                else:
                    self._import_file_impl(filepath, filename, start)
        except Exception as e:
            logging.error(f"Failed importing {filename}: {self.safe_error_message(e)}")
            raise

        return True

    def _import_file_impl(self, filepath, filename, start):
        """Implementation of file import with transaction management."""
        # For ROW mode, use autocommit; for others, manual commit control
        with self.connection_p(
            autocommit=(self.transaction_mode == TransactionMode.ROW)
        ) as c:
            with c.cursor() as cur:
                cur.fast_executemany = True
                parquet_file = pq.ParquetFile(filepath)
                columns = parquet_file.schema.names
                table_columns = self.get_table_columns(cur)
                self.validate_schema(columns, table_columns, filename)
                column_list = ", ".join(quote_identifier(col) for col in columns)
                placeholders = ", ".join("?" for _ in columns)

                insert_sql = f"""
                    INSERT INTO {self.full_table_name()} ({column_list})
                    VALUES ({placeholders})
                """

                total_rows = 0

                for rg_idx in range(parquet_file.num_row_groups):
                    table = parquet_file.read_row_group(rg_idx)

                    if self.transaction_mode == TransactionMode.ROWGROUP:
                        # Wrap rowgroup processing in retry logic
                        rows_in_rg = self._import_rowgroup_with_retry(
                            c,
                            cur,
                            table,
                            insert_sql,
                            filename,
                            rg_idx,
                            parquet_file.num_row_groups,
                        )
                        total_rows += rows_in_rg
                    else:
                        # Process batches within the rowgroup
                        for batch in table.to_batches(max_chunksize=self.batch_size):
                            if self.transaction_mode == TransactionMode.BATCH:
                                # Wrap batch processing in retry logic
                                rows_in_batch = self._import_batch_with_retry(
                                    c, cur, batch, insert_sql, filename
                                )
                                total_rows += rows_in_batch
                            else:
                                # ROW or FILE mode: just process the batch
                                rows = list(
                                    zip(
                                        *[col.to_pylist() for col in batch.columns],
                                        strict=True,
                                    )
                                )
                                cur.executemany(insert_sql, rows)
                                total_rows += len(rows)

                        if self.transaction_mode != TransactionMode.BATCH:
                            logging.info(
                                f"{filename}: processed row group {rg_idx+1}/"
                                f"{parquet_file.num_row_groups}"
                            )

                # Commit after entire file if in FILE mode
                if self.transaction_mode == TransactionMode.FILE:
                    c.commit()

                logging.info(
                    f"Completed file={filename}, total rows: {total_rows}, in "
                    f"{time.time() - start:.2f}s"
                )

    def _load_parquet_with_duckdb(self, filepath):
        with self.connection_d() as dconn:
            sanitized_path = str(filepath.as_posix()).replace("'", "''")
            return dconn.execute(
                f"SELECT * FROM read_parquet('{sanitized_path}')"
            ).fetch_arrow_table()

    def _import_file_with_duckdb(self, filepath, filename, start):
        with self.connection_p(
            autocommit=(self.transaction_mode == TransactionMode.ROW)
        ) as c:
            with c.cursor() as cur:
                cur.fast_executemany = True
                parquet_table = self._load_parquet_with_duckdb(filepath)
                columns = parquet_table.schema.names
                table_columns = self.get_table_columns(cur)
                self.validate_schema(columns, table_columns, filename)
                column_list = ", ".join(quote_identifier(col) for col in columns)
                placeholders = ", ".join("?" for _ in columns)

                insert_sql = f"""
                    INSERT INTO {self.full_table_name()} ({column_list})
                    VALUES ({placeholders})
                """

                total_rows = 0

                if self.transaction_mode == TransactionMode.ROWGROUP:
                    batches = list(
                        parquet_table.to_batches(max_chunksize=self.batch_size)
                    )
                    for rg_idx, batch in enumerate(batches):
                        rows_in_rg = self._import_rowgroup_with_retry(
                            c,
                            cur,
                            batch,
                            insert_sql,
                            filename,
                            rg_idx,
                            len(batches),
                        )
                        total_rows += rows_in_rg
                else:
                    for batch in parquet_table.to_batches(
                        max_chunksize=self.batch_size
                    ):
                        if self.transaction_mode == TransactionMode.BATCH:
                            rows_in_batch = self._import_batch_with_retry(
                                c, cur, batch, insert_sql, filename
                            )
                            total_rows += rows_in_batch
                        else:
                            rows = list(
                                zip(
                                    *[col.to_pylist() for col in batch.columns],
                                    strict=True,
                                )
                            )
                            cur.executemany(insert_sql, rows)
                            total_rows += len(rows)

                if self.transaction_mode == TransactionMode.FILE:
                    c.commit()

                logging.info(
                    f"Completed file={filename}, total rows: {total_rows}, in "
                    f"{time.time() - start:.2f}s"
                )

    def _import_batch_with_retry(self, c, cur, batch, insert_sql, filename):
        """Import a single batch with retry logic for BATCH mode."""
        rows = list(zip(*[col.to_pylist() for col in batch.columns], strict=True))

        for attempt in range(self.config.retries):
            try:
                cur.executemany(insert_sql, rows)
                c.commit()
                return len(rows)
            except Exception as e:
                safe_msg = self.safe_error_message(e)

                if attempt < self.config.retries - 1:
                    logging.warning(
                        f"Batch retry {attempt+1}/{self.config.retries} failed in "
                        f"{filename}: {safe_msg}"
                    )
                    c.rollback()
                    time.sleep(2**attempt)
                else:
                    raise RuntimeError(
                        f"Batch import failed after {self.config.retries} retries: "
                        f"{safe_msg}"
                    ) from None

    def _import_rowgroup_with_retry(
        self, c, cur, table, insert_sql, filename, rg_idx, total_rg
    ):
        """Import a single row group with retry logic for ROWGROUP mode."""
        for attempt in range(self.config.retries):
            try:
                total_rows = 0
                for batch in table.to_batches(max_chunksize=self.batch_size):
                    rows = list(
                        zip(*[col.to_pylist() for col in batch.columns], strict=True)
                    )
                    cur.executemany(insert_sql, rows)
                    total_rows += len(rows)

                c.commit()
                logging.info(f"{filename}: processed row group {rg_idx+1}/{total_rg}")
                return total_rows
            except Exception as e:
                safe_msg = self.safe_error_message(e)

                if attempt < self.config.retries - 1:
                    logging.warning(
                        f"Row group retry {attempt+1}/{self.config.retries} failed in "
                        f"{filename}: {safe_msg}"
                    )
                    c.rollback()
                    time.sleep(2**attempt)
                else:
                    raise RuntimeError(
                        f"Row group import failed after {self.config.retries} retries: "
                        f"{safe_msg}"
                    ) from None

    def perform_work(self):
        filenames = self.load_manifest()

        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            futures = [
                executor.submit(self.import_file, filename) for filename in filenames
            ]

            for future in as_completed(futures):
                future.result()


if __name__ == "__main__":
    pass
