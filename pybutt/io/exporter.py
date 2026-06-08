import math as m
import time
from multiprocessing import get_context
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyodbc

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import (
    SqlConfig,
    quote_identifier,
    resolve_engine_default,
    validate_engine,
    validate_identifier,
)
from pybutt.core.logobs import (
    MemoryHeartbeat,
    context,
    get_logger,
    init_worker_logging,
    mem_fields,
)
from pybutt.exceptions import (
    ConfigurationError,
    DataExportError,
    TableEmptyError,
)
from pybutt.files.files import (
    default_manifest_filename,
    write_manifest,
)

logger = get_logger("exporter")


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
        parameters: str | None = None,
        mem_heartbeat: float = 0,
    ):
        super().__init__(config)

        self.mem_heartbeat = mem_heartbeat

        self.pk_column = validate_identifier(pk_column) if pk_column else None
        self.columns = [validate_identifier(c) for c in columns] if columns else None
        self.parameters = parameters

        validate_engine(engine)

        if file_count < 1:
            raise ConfigurationError("file_count must be at least 1")

        if fetch_size is not None and fetch_size < 1:
            raise ConfigurationError("fetch_size must be at least 1")

        self.worker_count = worker_count
        self.file_count = file_count
        self.rowgroup_size = rowgroup_size
        self.engine = engine
        self.fetch_size = resolve_engine_default(
            "fetch_size",
            self.engine,
            fetch_size,
            min(max(1024, self.rowgroup_size), 8192),
        )

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
                    logger.info(
                        "Partition stats returned zero rows; falling back to COUNT(*)"
                    )
                    count_query = f"SELECT COUNT(*) FROM {self._source_reference()}"
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

        logger.info(
            "Partitioning "
            + context(
                table=f"{self.schema}.{self.table}",
                total_rows=self.total_rows,
                file_count=self.file_count,
                chunk_size=self.chunk_size,
            )
        )

        if self.pk_column:
            logger.info("Partition strategy=ROW_NUMBER " + context(pk=self.pk_column))
        else:
            logger.info(
                "Partition strategy=CHECKSUM " + context(modulo=self.partition_count)
            )

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

    def _source_reference(self) -> str:
        if self.parameters is None:
            return self.full_table_name()
        return f"{self.full_table_name()}({self.parameters})"

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
                f"FROM {self._source_reference()} "
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
                FROM {self._source_reference()}
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

    def _export_cursor_to_parquet(self, cur, filepath, filename):
        """Shared fetch-buffer-write logic for cursor-based engines (pyodbc / mssql)."""
        if cur.description is None:
            raise DataExportError(
                f"Failed exporting {filename}: query returned no column metadata"
            )

        columns = [desc[0] for desc in cur.description]
        fetch_size = self.fetch_size

        first_rows = cur.fetchmany(fetch_size)
        if not first_rows:
            empty_schema = pa.schema([pa.field(c, pa.string()) for c in columns])
            with pq.ParquetWriter(
                str(filepath.as_posix()), empty_schema, compression="snappy"
            ) as writer:
                writer.write_table(
                    pa.Table.from_pydict({c: [] for c in columns}, schema=empty_schema)
                )
            return

        batch_dicts = [dict(zip(columns, row, strict=True)) for row in first_rows]
        target_schema = pa.Table.from_pylist(batch_dicts).schema

        def _rows_to_table(rows_to_write):
            batch = [dict(zip(columns, row, strict=True)) for row in rows_to_write]
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

    def _export_partition_with_pyodbc(self, query, filepath, filename):
        with self.connection_p() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(query)
                    self._export_cursor_to_parquet(cur, filepath, filename)
                except DataExportError:
                    raise
                except Exception as e:
                    raise DataExportError(
                        f"Failed exporting {filename}: {self.safe_error_message(e)}"
                    ) from e

    def _export_partition_with_mssql(self, query, filepath, filename):
        conn = self.connection_m()
        try:
            cur = conn.cursor()
            try:
                cur.execute(query)
                self._export_cursor_to_parquet(cur, filepath, filename)
            except DataExportError:
                raise
            except Exception as e:
                raise DataExportError(
                    f"Failed exporting {filename}: {self.safe_error_message(e)}"
                ) from e
            finally:
                cur.close()
        finally:
            conn.close()

    def export_partition(self, n):
        start = time.time()
        safe_name = f"{self.schema}_{self.table}"
        filename = f"{safe_name}_part_{n:05d}.parquet"
        filepath = self.output_path / filename
        query = self.build_partition_query(n)

        logger.debug("Partition query " + context(partition=n) + f": {query}")
        logger.info(
            "Exporting "
            + context(
                file=filename,
                partition=f"{n}/{self.partition_count - 1}",
                table=f"{self.schema}.{self.table}",
                engine=self.engine,
                **mem_fields(),
            )
        )

        def _work():
            if self.engine == "duckdb":
                self._export_partition_with_duckdb(query, filepath, filename)
            elif self.engine == "mssql-python":
                self._export_partition_with_mssql(query, filepath, filename)
            else:
                self._export_partition_with_pyodbc(query, filepath, filename)

        try:
            # Heartbeat runs inside the worker process where the memory lives.
            with MemoryHeartbeat(self.mem_heartbeat, unit=f"partition={n}"):
                self.retry(_work, context=f"Export partition {n}")
        except MemoryError:
            logger.error(
                "Out of memory during export - not retrying (fatal) "
                + context(partition=n, file=filename)
            )
            raise
        except Exception as e:
            logger.error(
                "Export partition failed "
                + context(partition=n, file=filename)
                + f": {self.safe_error_message(e)}"
            )
            logger.debug("Traceback for partition %s", n, exc_info=True)
            raise

        duration = time.time() - start
        if filepath.exists():
            size_mb = filepath.stat().st_size / (1024 * 1024)
        else:
            size_mb = 0
        logger.info(
            "Completed "
            + context(
                file=filename,
                rows_approx=self.chunk_size,
                size_mb=f"{size_mb:.2f}",
                seconds=f"{duration:.2f}",
                progress=f"{n + 1}/{self.partition_count}",
                **mem_fields(),
            )
        )

        return filename

    def perform_work(self):
        start = time.time()
        manifest_file = self.output_path / self.manifest_filename

        # Spawned worker processes re-import modules and do NOT inherit the
        # parent's logging config, so configure it in each via the initialiser
        # (spawn is the default on Windows/macOS and is forced here on all OSes).
        worker_level = get_logger().getEffectiveLevel()
        try:
            with get_context("spawn").Pool(
                self.worker_count,
                initializer=init_worker_logging,
                initargs=(worker_level,),
            ) as p:
                filenames = p.map(self.export_partition, range(self.partition_count))
        except Exception as e:
            # A worker killed abruptly (e.g. OOM/SIGKILL) surfaces here without a
            # partition context; make the likely cause explicit.
            logger.error(
                "Export pool failed - a worker may have terminated abnormally "
                "(possible out-of-memory/SIGKILL); check earlier per-partition "
                f"logs: {self.safe_error_message(e)}"
            )
            raise

        duration = time.time() - start

        logger.info(
            "Export complete "
            + context(
                table=f"{self.schema}.{self.table}",
                files=len(filenames),
                seconds=f"{duration:.2f}",
            )
        )

        logger.info("Writing manifest " + context(file=manifest_file))
        try:
            write_manifest(manifest_file, filenames)
            logger.info(
                "Manifest written " + context(file=manifest_file, files=len(filenames))
            )

        except Exception as e:
            logger.error(
                "Failed to write manifest "
                + context(file=manifest_file)
                + f": {self.safe_error_message(e)}"
            )


if __name__ == "__main__":
    pass
