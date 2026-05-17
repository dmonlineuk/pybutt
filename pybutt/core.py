import duckdb as d
import math as m
import json as j
from pathlib import Path
from multiprocessing import Pool
import re
import time
import logging

logging.basicConfig(level=logging.INFO)

IDENTIFIER_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def validate_identifier(name: str) -> str:
    if not IDENTIFIER_REGEX.match(name):
        raise ValueError(f"Invalid identifier: {name}")
    return name
    
def quote_identifier(name: str) -> str:
    return f"[{name.replace(']', ']]')}]"

class Exporter:
    
    def build_dsn(self):
        parts = [
            f"Driver={{{self.driver}}}",
            f"Server={self.server}",
            f"Database={self.database}"
        ]

        if self.trusted_connection:
            parts.append("Trusted_Connection=Yes")
        else:
            parts.append(f"Uid={self.username}")
            parts.append(f"Pwd={self.password}")

        parts.append(f"TrustServerCertificate={'Yes' if self.trust_cert else 'No'}")

        if self.encrypt:
            parts.append("Encrypt=Yes")

        return ";".join(parts) + ";"
    
    def connection(self):
        conn = d.connect()
        conn.execute('INSTALL odbc_scanner; LOAD odbc_scanner;')
        return conn

    def __init__(
        self,
        server,
        database,
        schema,
        table,
        output_path,
        username = None,
        password = None,
        pk_column = None,
        columns=None,
        worker_count = 1,
        max_rows_per_file = 1_000_000,
        driver = 'ODBC Driver 18 for SQL Server',
        trusted_connection = False,
        trust_cert = False,
        encrypt = True,
        retries = 3,
        conn_debug = False,
    ):
        self.server = server
        self.database = database
        self.schema = validate_identifier(schema)
        self.table = validate_identifier(table)
        self.pk_column = validate_identifier(pk_column) if pk_column else None
        self.columns = (
            ", ".join(validate_identifier(c) for c in columns)
            if columns else "*"
        )

        self.username = username
        self.password = password        
        
        self.driver = driver
        self.trusted_connection = trusted_connection
        self.trust_cert = trust_cert
        self.encrypt = encrypt

        self.worker_count = worker_count
        self.max_rows_per_file = max_rows_per_file
        self.retries = retries
        
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        self.dsn = self.build_dsn()
        
        self.total_rows = 0
        self.partition_count = 0

        self.partition_meta()
    
    def full_table_name(self):
        return f"{quote_identifier(self.schema)}.{quote_identifier(self.table)}"
    
    def partition_meta(self):
        with self.connection() as c:
            query = f"""
                SELECT SUM(row_count)
                FROM sys.dm_db_partition_stats
                WHERE object_id = OBJECT_ID('{self.full_table_name}')
                AND index_id IN (0,1)
            """

            self.total_rows = c.execute(
                f"FROM odbc_query('{self.dsn}', $$ {query} $$)"
            ).fetchone()[0] or 0

            if self.total_rows == 0:
                raise RuntimeError("Table empty or not found")

            self.partition_count = m.ceil(
                self.total_rows / self.max_rows_per_file
            )

            logging.info(f"Total rows: {self.total_rows}")
            logging.info(f"Partitions: {self.partition_count}")

    def build_partition_query(self, n):
        if self.pk_column:
            # Range partitioning
            start = n * self.max_rows_per_file
            end = (n + 1) * self.max_rows_per_file

            return f"""
                SELECT {self.columns}
                FROM (
                    SELECT {self.columns},
                           ROW_NUMBER() OVER (ORDER BY {quote_identifier(self.pk_column)}) AS rn
                    FROM {self.full_table_name()}
                t
                WHERE rn > {start} AND rn <= {end}
            """
        else:
            # fallback hash
            return f"""
                SELECT {self.columns}
                FROM {self.full_table_name()}
                WHERE ABS(CHECKSUM(*)) % {self.partition_count} = {n}
            """

    def export_partition(self, n):
        safe_name = f"{self.schema}_{self.table}"
        filename = self.output_path / f"{safe_name}_part_{n:05d}.parquet"
        query = self.build_partition_query(n)

        logging.info(f"Starting {filename}")

        for attempt in range(self.retries):        
            try:
                with self.connection() as c:
                    c.execute(f"""
                        COPY (
                            FROM odbc_query('{self.dsn}', $$ {query} $$)
                        ) 
                        TO '{filename}' 
                        (FORMAT parquet, COMPRESSION snappy)
                    """)
                return filename
            except Exception as e:
                logging.warning(f"Retry {attempt+1} failed: {e}")
                time.sleep(2 ** attempt)

        logging.error(f"Failed exporting {filename}")

    def perform_work(self):
        written_files = []
        safe_name = f"{self.schema}_{self.table}"
        manifest_file = self.output_path / f"{safe_name}_manifest.json"
        with Pool(self.worker_count) as p:
            written_files = p.map(self.export_partition, range(self.partition_count))

        try:
            with open(manifest_file, "w") as f:
                f.write(j.dumps(written_files, indent=4))
        except Exception as e:
            logging.error(f"Failed to write manifest {mainfest_file}: {e}")

if __name__ == '__main__':
    pass
