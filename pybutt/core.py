import duckdb as d
import math as m
import json as j
from pathlib import Path
from multiprocessing import get_context
import pyodbc
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import re
import time
import logging
import threading

logging.basicConfig(level=logging.INFO)

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
    driver: str = 'ODBC Driver 18 for SQL Server'
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
                raise ValueError("Username/password required when not using trusted connection")
            parts.append(f"Uid={cfg.username}")
            parts.append(f"Pwd={cfg.password}")

        parts.append(f"TrustServerCertificate={'Yes' if cfg.trust_cert else 'No'}")

        if cfg.encrypt:
            parts.append("Encrypt=Yes")

        return ";".join(parts) + ";"

    def connection_d(self):
        conn = d.connect()
        conn.execute('INSTALL odbc_scanner; LOAD odbc_scanner;')
        return conn

    def connection_p(self):
        conn = pyodbc.connect(self.dsn)
        conn.autocommit = False
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
                logging.warning(f"{context} retry {attempt+1}/{self.config.retries} failed: {safe_msg}")
                time.sleep(2 ** attempt)
        raise RuntimeError(f"{context} failed after max retries")

class Exporter(SqlServerIOBase):
    def __init__(
        self,
        config: SqlConfig,
        output_path,
        pk_column=None,
        columns=None,
        worker_count=1,
        max_rows_per_file=1_000_000,
    ):
        super().__init__(config)

        self.pk_column = validate_identifier(pk_column) if pk_column else None
        self.columns = (
            ", ".join(validate_identifier(c) for c in columns)
            if columns else "*"
        )

        self.worker_count = worker_count
        self.max_rows_per_file = max_rows_per_file

        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        self.total_rows = 0
        self.partition_count = 0

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

                return c.execute(
                    f"FROM odbc_query('{self.dsn}', $$ {query} $$)"
                ).fetchone()[0] or 0

        self.total_rows = self.retry(_work, context="Fetching partition strategy")

        if self.total_rows == 0:
            raise RuntimeError("Table empty or not found")

        self.partition_count = m.ceil(self.total_rows / self.max_rows_per_file)

        logging.info(
            f"Partitioning table={self.schema}.{self.table} "
            f"total_rows={self.total_rows} "
            f"max_rows_per_file={self.max_rows_per_file}"
        )

        if self.pk_column:
            logging.info(f"Partition strategy=ROW_NUMBER pk={self.pk_column}")
        else:
            logging.info(f"Partition strategy=CHECKSUM modulo={self.partition_count}")

    def build_partition_query(self, n):
        if self.pk_column:
            start = n * self.max_rows_per_file
            end = (n + 1) * self.max_rows_per_file

            return f"""
                SELECT {self.columns}
                FROM (
                    SELECT {self.columns},
                           ROW_NUMBER() OVER (ORDER BY {quote_identifier(self.pk_column)}) AS rn
                    FROM {self.full_table_name()}
                ) t
                WHERE rn > {start} AND rn <= {end}
            """
        else:
            return f"""
                SELECT {self.columns}
                FROM {self.full_table_name()}
                WHERE ABS(CHECKSUM(*)) % {self.partition_count} = {n}
            """

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
            f"table={self.schema}.{self.table}"
        )

        def _work():
            with self.connection_d() as c:
                
                try:
                    c.execute(f"""
                        COPY (
                            FROM odbc_query('{self.dsn}', $$ {query} $$)
                        ) 
                        TO '{str(filepath).replace("\\", "/")}'
                        (FORMAT parquet, COMPRESSION snappy)
                    """)
                except Exception as e:
                    raise RuntimeError(f"Failed exporting {filename}: {self.safe_error_message(e)}")


        self.retry(_work, context=f"Export partition {n}")
        
        duration = time.time() - start
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
        else:
            size_mb = 0
        logging.info(
            f"Completed file={filename} "
            f"rows~{self.max_rows_per_file} "
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
                f"Manifest written: {manifest_file} "
                f"files={len(filenames)}"
            )

        except Exception as e:
            logging.error(f"Failed to write manifest {manifest_file}: {self.safe_error_message(e)}")

class Importer(SqlServerIOBase):
    def __init__(
        self,
        config: SqlConfig,
        input_path,
        manifest_filename,
        worker_count=1,
        batch_size=1_000,
    ):
        super().__init__(config)

        self.input_path = Path(input_path)
        self.manifest_filename = manifest_filename

        self.worker_count = worker_count
        self.batch_size = batch_size

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
            f"batch_size={self.batch_size}"
        )

        def _work():
            with self.connection_p() as c:
                try:
                    with c.cursor() as cur:
                        cur.fast_executemany = True
                        parquet_file = pq.ParquetFile(filepath)
                        columns = parquet_file.schema.names
                        table_columns = self.get_table_columns(cur)
                        self.validate_schema(columns, table_columns, filename)
                        column_list = ', '.join(quote_identifier(col) for col in columns)
                        placeholders = ', '.join('?' for _ in columns)

                        insert_sql = f"""
                            INSERT INTO {self.full_table_name()} ({column_list})
                            VALUES ({placeholders})
                        """

                        total_rows = 0

                        for rg_idx in range(parquet_file.num_row_groups):
                            table = parquet_file.read_row_group(rg_idx)

                            for batch in table.to_batches(max_chunksize=self.batch_size):
                                rows = list(zip(*[col.to_pylist() for col in batch.columns]))
                                cur.executemany(insert_sql, rows)
                                total_rows += len(rows)

                            logging.info(
                                f"{filename}: processed row group {rg_idx+1}/"
                                f"{parquet_file.num_row_groups}"
                            )
                                
                    c.commit()

                    logging.info(f"Completed file={filename}, total rows: {total_rows}, in {time.time() - start:.2f}s")

                except Exception as e:
                    c.rollback()
                    logging.error(f"Failed importing {filename}: {self.safe_error_message(e)}")
                    raise

        self.retry(_work, context=f"Import file {filename}")
        return True

    def perform_work(self):
        filenames = self.load_manifest()

        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            futures = [
                executor.submit(self.import_file, filename)
                for filename in filenames
            ]

            for future in as_completed(futures):
                future.result()

if __name__ == '__main__':
    pass
