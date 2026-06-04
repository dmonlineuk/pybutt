import json as j
import logging
import math as m
import threading
import time
from multiprocessing import get_context
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import (
    ENGINE_CHOICES,
    SqlConfig,
    quote_identifier,
    validate_identifier,
)
from pybutt.exceptions import (
    ConfigurationError,
    DataExportError,
    EngineSelectionError,
    TableEmptyError,
)
from pybutt.files.files import (
    MANIFEST_VERSION_2,
    default_manifest_filename,
)

logging.basicConfig(level=logging.INFO)


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
        fetch_size=None,
        engine="duckdb",
        manifest_filename: str | None = None,
    ):
        super().__init__(config)

        self.pk_column = validate_identifier(pk_column) if pk_column else None
        self.columns = [validate_identifier(c) for c in columns] if columns else None

        if engine not in ENGINE_CHOICES:
            raise EngineSelectionError(
                f"engine must be one of {sorted(ENGINE_CHOICES)}"
            )

        if file_count < 1:
            raise ConfigurationError("file_count must be at least 1")

        if fetch_size is not None and fetch_size < 1:
            raise ConfigurationError("fetch_size must be at least 1")

        self.worker_count = worker_count
        self.file_count = file_count
        self.rowgroup_size = rowgroup_size
        self.fetch_size = (
            fetch_size
            if fetch_size is not None
            else min(max(1024, self.rowgroup_size), 8192)
        )
        self.engine = engine

        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.manifest_filename = (
            manifest_filename
            if manifest_filename
            else default_manifest_filename(self.schema, self.table)
        )

        self.total_rows = 0
        self.partition_count = 0
        self.chunk_size = 0

        self.partition_meta()

    def partition_meta(self):
        def _work():
            with self.connection_d() as c:
                partition_query = f"""
                    SELECT SUM(row_count)
                    FROM sys.dm_db_partition_stats
                    WHERE object_id = OBJECT_ID('{self.full_table_name()}')
                    AND index_id IN (0,1)
                """

                row_count = (
                    c.execute(
                        f"FROM odbc_query('{self.dsn}', $$ {partition_query} $$)"
                    ).fetchone()[0]
                    or 0
                )

                if row_count == 0:
                    logging.info(
                        "Partition stats returned zero rows; falling back to COUNT(*)"
                    )
                    count_query = f"SELECT COUNT(*) FROM {self.full_table_name()}"
                    row_count = (
                        c.execute(
                            f"FROM odbc_query('{self.dsn}', $$ {count_query} $$)"
                        ).fetchone()[0]
                        or 0
                    )

                return row_count

        self.total_rows = self.retry(_work, context="Fetching partition strategy")

        if self.total_rows == 0:
            raise TableEmptyError("Table empty or not found")

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
        fields = []
        for column in description:
            name = column[0]
            type_code = column[1]
            precision = column[5] if len(column) > 5 else None
            scale = column[6] if len(column) > 6 else None
            nullable = column[6] if len(column) > 6 else True
            fields.append(
                pa.field(
                    name,
                    self._pyodbc_type_code_to_pyarrow(
                        type_code,
                        precision,
                        scale,
                        column[3] if len(column) > 3 else None,
                    ),
                    nullable=nullable,
                )
            )
        return pa.schema(fields)

    def _write_parquet_from_record_batches(self, reader, filepath, filename):
        try:
            schema = reader.schema
            with pq.ParquetWriter(
                str(filepath.as_posix()), schema, compression="snappy"
            ) as writer:
                buffered_table = None
                for batch in reader:
                    table = pa.Table.from_batches([batch])
                    if buffered_table is None:
                        buffered_table = table
                    else:
                        buffered_table = pa.concat_tables([buffered_table, table])

                    while (
                        buffered_table is not None
                        and buffered_table.num_rows >= self.rowgroup_size
                    ):
                        chunk = buffered_table.slice(0, self.rowgroup_size)
                        writer.write_table(chunk, row_group_size=self.rowgroup_size)
                        buffered_table = buffered_table.slice(self.rowgroup_size)

                if buffered_table is None:
                    writer.write_table(pa.Table.from_batches([], schema=schema))
                elif buffered_table.num_rows > 0:
                    writer.write_table(
                        buffered_table, row_group_size=self.rowgroup_size
                    )
        except Exception as e:
            raise DataExportError(
                f"Failed exporting {filename}: {self.safe_error_message(e)}"
            ) from e

    def _export_partition_with_duckdb(self, query, filepath, filename):
        with self.connection_d() as c:
            try:
                result = c.execute(f"FROM odbc_query('{self.dsn}', $$ {query} $$)")
                reader = result.arrow()
                self._write_parquet_from_record_batches(reader, filepath, filename)
            except Exception as e:
                raise DataExportError(
                    f"Failed exporting {filename}: {self.safe_error_message(e)}"
                ) from e

    def _export_partition_with_pyodbc(self, query, filepath, filename):
        with self.connection_p() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query)
                except Exception as e:
                    raise DataExportError(
                        f"Failed exporting {filename}: {self.safe_error_message(e)}"
                    ) from e

                if cur.description is None:
                    raise DataExportError(
                        f"Failed exporting {filename}: query returned no column "
                        "metadata"
                    )

                columns = [desc[0] for desc in cur.description]
                fetch_size = self.fetch_size

                # Read first non-empty batch to infer a stable schema
                try:
                    first_rows = cur.fetchmany(fetch_size)
                    if not first_rows:
                        # No rows: create an empty file with string columns
                        empty_schema = pa.schema(
                            [pa.field(c, pa.string()) for c in columns]
                        )
                        with pq.ParquetWriter(
                            str(filepath.as_posix()), empty_schema, compression="snappy"
                        ) as writer:
                            writer.write_table(
                                pa.Table.from_pydict(
                                    {c: [] for c in columns}, schema=empty_schema
                                )
                            )
                        return

                    batch_dicts = [
                        dict(zip(columns, row, strict=True)) for row in first_rows
                    ]
                    target_schema = pa.Table.from_pylist(batch_dicts).schema

                    def _rows_to_table(rows_to_write):
                        batch = [
                            dict(zip(columns, row, strict=True))
                            for row in rows_to_write
                        ]
                        tbl = pa.Table.from_pylist(batch)
                        if tbl.schema != target_schema:
                            arrays = []
                            for field in target_schema:
                                name = field.name
                                col_type = field.type
                                vals = [r.get(name) for r in batch]
                                arrays.append(pa.array(vals, type=col_type))
                            tbl = pa.Table.from_arrays(
                                arrays, names=[f.name for f in target_schema]
                            )
                        return tbl

                    with pq.ParquetWriter(
                        str(filepath.as_posix()), target_schema, compression="snappy"
                    ) as writer:
                        buffered_rows = list(first_rows)

                        while True:
                            if len(buffered_rows) >= self.rowgroup_size:
                                rows_to_write = buffered_rows[: self.rowgroup_size]
                                writer.write_table(
                                    _rows_to_table(rows_to_write),
                                    row_group_size=self.rowgroup_size,
                                )
                                buffered_rows = buffered_rows[self.rowgroup_size :]
                                continue

                            rows = cur.fetchmany(fetch_size)
                            if not rows:
                                break
                            buffered_rows.extend(rows)

                        if buffered_rows:
                            writer.write_table(
                                _rows_to_table(buffered_rows),
                                row_group_size=self.rowgroup_size,
                            )
                except Exception as e:
                    raise DataExportError(
                        f"Failed exporting {filename}: {self.safe_error_message(e)}"
                    ) from e

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
        manifest_file = self.output_path / self.manifest_filename

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
                j.dump(
                    {
                        "version": MANIFEST_VERSION_2,
                        "type": "files",
                        "entries": filenames,
                    },
                    f,
                    indent=4,
                )
            logging.info(
                f"Manifest written: {manifest_file} " f"files={len(filenames)}"
            )

        except Exception as e:
            logging.error(
                f"Failed to write manifest {manifest_file}: "
                f"{self.safe_error_message(e)}"
            )


if __name__ == "__main__":
    pass
