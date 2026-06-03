import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pyarrow.parquet as pq

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import (
    ENGINE_CHOICES,
    SqlConfig,
    TransactionMode,
    quote_identifier,
)
from pybutt.exceptions import (
    BatchImportError,
    EngineSelectionError,
    RowGroupImportError,
    SchemaMismatchError,
    UnsupportedManifestTypeError,
)
from pybutt.files.files import (
    default_manifest_filename,
    default_temp_manifest_filename,
    load_manifest,
    validate_manifest_entries,
)

logging.basicConfig(level=logging.INFO)


class Importer(SqlServerIOBase):
    def __init__(
        self,
        config: SqlConfig,
        input_path,
        manifest_filename: str | None,
        worker_count=1,
        batch_size=1_000,
        transaction_mode: TransactionMode = TransactionMode.BATCH,
        engine="pyodbc",
        use_tempdb: bool = True,
        temp_manifest_filename: str | None = None,
    ):
        super().__init__(config)

        self.input_path = Path(input_path)
        self.manifest_filename = (
            manifest_filename
            if manifest_filename
            else default_manifest_filename(self.schema, self.table)
        )
        self.temp_manifest_filename = (
            temp_manifest_filename
            if temp_manifest_filename
            else default_temp_manifest_filename(self.schema, self.table)
        )

        self.worker_count = worker_count
        self.batch_size = batch_size
        self.transaction_mode = (
            TransactionMode(transaction_mode)
            if isinstance(transaction_mode, str)
            else transaction_mode
        )
        if engine not in ENGINE_CHOICES:
            raise EngineSelectionError(
                f"engine must be one of {sorted(ENGINE_CHOICES)}"
            )
        self.engine = engine
        self.use_tempdb = use_tempdb

    def load_manifest(self):
        manifest_file = self.input_path / self.manifest_filename
        return load_manifest(manifest_file)

    def load_manifest_entries(self):
        manifest = self.load_manifest()
        if manifest["type"] != "files":
            raise UnsupportedManifestTypeError(
                f"Importer only supports file manifests, got: {manifest['type']}"
            )
        return validate_manifest_entries(manifest, self.input_path)

    def _build_insert_sql(
        self, columns: list[str], target_table: str | None = None
    ) -> str:
        column_list = ", ".join(quote_identifier(col) for col in columns)
        placeholders = ", ".join("?" for _ in columns)
        table_name = target_table or self.full_table_name()
        return f"INSERT INTO {table_name} " f"({column_list}) VALUES ({placeholders})"

    def _rows_from_batch(self, batch):
        return list(zip(*[col.to_pylist() for col in batch.columns], strict=True))

    def _validate_and_build_insert(
        self, cur, columns, filename, target_table: str | None = None
    ):
        try:
            table_columns = self.get_table_columns(cur, target_table=target_table)
        except TypeError:
            table_columns = self.get_table_columns(cur)

        self.validate_schema(columns, table_columns, filename)
        return self._build_insert_sql(columns, target_table=target_table)

    def get_table_columns(self, cur, target_table: str | None = None):
        target_table = target_table or self.full_table_name()
        cur.execute(f"SELECT TOP 0 * FROM {target_table}")
        return [column[0] for column in cur.description]

    def validate_schema(self, parquet_columns, table_columns, filename):
        parquet_set = set(parquet_columns)
        table_set = set(table_columns)

        if parquet_set != table_set:
            missing_in_sql = parquet_set - table_set
            missing_in_parquet = table_set - parquet_set

            raise SchemaMismatchError(
                f"Schema mismatch in {filename}:\n"
                f"  Columns in parquet but not SQL: {missing_in_sql}\n"
                f"  Columns in SQL but not parquet: {missing_in_parquet}"
            )

    def import_file(self, filename, target_table: str | None = None):
        filepath = self.input_path / filename
        thread_id = threading.get_ident()
        start = time.time()
        target_table_name = target_table or self.full_table_name()

        logging.info(
            f"Thread={thread_id} "
            f"Importing file={filename} "
            f"table={target_table_name} "
            f"batch_size={self.batch_size} "
            f"transaction_mode={self.transaction_mode.value}"
        )

        try:
            if self.transaction_mode == TransactionMode.FILE:
                # For FILE mode, wrap entire operation in retry logic
                def _file_operation():
                    if self.engine == "duckdb":
                        return self._import_file_with_duckdb(
                            filepath, filename, start, target_table=target_table
                        )
                    return self._import_file_impl(
                        filepath, filename, start, target_table=target_table
                    )

                self.retry(_file_operation, context=f"Import file {filename}")
            else:
                # For BATCH, ROWGROUP, and ROW modes, retries happen at granular level
                if self.engine == "duckdb":
                    self._import_file_with_duckdb(
                        filepath, filename, start, target_table=target_table
                    )
                else:
                    self._import_file_impl(
                        filepath, filename, start, target_table=target_table
                    )
        except Exception as e:
            logging.error(f"Failed importing {filename}: {self.safe_error_message(e)}")
            raise

        return True

    def _import_file_impl(
        self, filepath, filename, start, target_table: str | None = None
    ):
        """Implementation of file import with transaction management."""
        # For ROW mode, use autocommit; for others, manual commit control
        with self.connection_p(
            autocommit=(self.transaction_mode == TransactionMode.ROW)
        ) as c:
            with c.cursor() as cur:
                cur.fast_executemany = True
                parquet_file = pq.ParquetFile(filepath)
                columns = parquet_file.schema.names
                insert_sql = self._validate_and_build_insert(
                    cur, columns, filename, target_table=target_table
                )

                total_rows = 0

                for rg_idx in range(parquet_file.num_row_groups):
                    table = parquet_file.read_row_group(rg_idx)

                    if self.transaction_mode == TransactionMode.ROWGROUP:
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
                        for batch in table.to_batches(max_chunksize=self.batch_size):
                            if self.transaction_mode == TransactionMode.BATCH:
                                rows = self._rows_from_batch(batch)
                                rows_in_batch = self._import_batch_with_retry(
                                    c, cur, rows, insert_sql, filename
                                )
                                total_rows += rows_in_batch
                            else:
                                rows = self._rows_from_batch(batch)
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

    def _import_file_with_duckdb(
        self, filepath, filename, start, target_table: str | None = None
    ):
        with self.connection_p(
            autocommit=(self.transaction_mode == TransactionMode.ROW)
        ) as c:
            with c.cursor() as cur:
                cur.fast_executemany = True
                if self.transaction_mode == TransactionMode.ROWGROUP:
                    parquet_file = pq.ParquetFile(filepath)
                    columns = parquet_file.schema.names
                else:
                    parquet_table = self._load_parquet_with_duckdb(filepath)
                    columns = parquet_table.schema.names

                insert_sql = self._validate_and_build_insert(
                    cur, columns, filename, target_table=target_table
                )

                total_rows = 0

                if self.transaction_mode == TransactionMode.ROWGROUP:
                    for rg_idx in range(parquet_file.num_row_groups):
                        rowgroup_table = parquet_file.read_row_group(rg_idx)
                        rows_in_rg = self._import_rowgroup_with_retry(
                            c,
                            cur,
                            rowgroup_table,
                            insert_sql,
                            filename,
                            rg_idx,
                            parquet_file.num_row_groups,
                        )
                        total_rows += rows_in_rg
                else:
                    for batch in parquet_table.to_batches(
                        max_chunksize=self.batch_size
                    ):
                        rows = self._rows_from_batch(batch)
                        if self.transaction_mode == TransactionMode.BATCH:
                            rows_in_batch = self._import_batch_with_retry(
                                c, cur, rows, insert_sql, filename
                            )
                            total_rows += rows_in_batch
                        else:
                            cur.executemany(insert_sql, rows)
                            total_rows += len(rows)

                if self.transaction_mode == TransactionMode.FILE:
                    c.commit()

                logging.info(
                    f"Completed file={filename}, total rows: {total_rows}, in "
                    f"{time.time() - start:.2f}s"
                )

    def _import_batch_with_retry(self, c, cur, rows_or_batch, insert_sql, filename):
        """Import a single batch with retry logic for BATCH mode."""
        rows = (
            rows_or_batch
            if isinstance(rows_or_batch, list)
            else self._rows_from_batch(rows_or_batch)
        )

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
                    raise BatchImportError(
                        f"Batch import failed after {self.config.retries} retries: "
                        f"{safe_msg}"
                    ) from None

    def _import_rowgroup_with_retry(
        self, c, cur, table_or_batch, insert_sql, filename, rg_idx, total_rg
    ):
        """Import a single row group with retry logic for ROWGROUP mode."""
        for attempt in range(self.config.retries):
            try:
                total_rows = 0
                to_batches = getattr(table_or_batch, "to_batches", None)
                rowgroup_batches = (
                    table_or_batch.to_batches(max_chunksize=self.batch_size)
                    if callable(to_batches)
                    else [table_or_batch]
                )

                for batch in rowgroup_batches:
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
                    raise RowGroupImportError(
                        f"Row group import failed after {self.config.retries} retries: "
                        f"{safe_msg}"
                    ) from None

    def _make_temp_table_name(self, worker_index: int) -> str:
        suffix = uuid.uuid4().hex[:8]
        if self.use_tempdb:
            return f"##{self.schema}_{self.table}_{worker_index+1:02d}_{suffix}"
        return f"{self.schema}.{self.table}_{worker_index+1:02d}_{suffix}"

    def _create_temp_tables(self, count: int) -> list[str]:
        temp_tables = []
        with self.connection_p(autocommit=True) as conn:
            with conn.cursor() as cur:
                for i in range(count):
                    temp_table_name = self._make_temp_table_name(i)
                    cur.execute(
                        "SELECT TOP 0 * "
                        f"INTO {temp_table_name} "
                        f"FROM {self.full_table_name()}"
                    )
                    temp_tables.append(temp_table_name)
        return temp_tables

    def _assign_files_to_workers(
        self, filenames: list[str], temp_tables: list[str]
    ) -> dict[str, list[str]]:
        assignments: dict[str, list[str]] = {tbl: [] for tbl in temp_tables}
        for index, filename in enumerate(filenames):
            target_table = temp_tables[index % len(temp_tables)]
            assignments[target_table].append(filename)
        return assignments

    def _write_temp_manifest(self, temp_tables: list[str]) -> Path:
        manifest_path = (
            self.input_path / f"{self.schema}_{self.table}_temp_manifest.json"
        )
        with open(manifest_path, "w") as f:
            json.dump(
                {
                    "version": 2,
                    "type": "tables",
                    "entries": temp_tables,
                },
                f,
                indent=4,
            )
        return manifest_path

    def _import_files_to_temp_table(self, target_table: str, filenames: list[str]):
        for filename in filenames:
            self.import_file(filename, target_table=target_table)

    def perform_work(self):
        filenames = self.load_manifest_entries()

        if self.worker_count > 1 and len(filenames) > 1:
            worker_count = min(self.worker_count, len(filenames))
            temp_tables = self._create_temp_tables(worker_count)
            assignments = self._assign_files_to_workers(filenames, temp_tables)

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(
                        self._import_files_to_temp_table, target_table, assigned
                    )
                    for target_table, assigned in assignments.items()
                    if assigned
                ]

                for future in as_completed(futures):
                    future.result()

            manifest_file = self._write_temp_manifest(temp_tables)
            logging.info(f"Wrote temporary table manifest: {manifest_file}")
            return

        with ThreadPoolExecutor(max_workers=self.worker_count) as executor:
            futures = [
                executor.submit(self.import_file, filename) for filename in filenames
            ]

            for future in as_completed(futures):
                future.result()


if __name__ == "__main__":
    pass
