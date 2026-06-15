import queue
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from pybutt.core.base import SqlServerIOBase, rows_from_arrow
from pybutt.core.config import (
    BATCH_SIZE_DEFAULT,
    CCI_DEFAULT,
    IMPORT_ENGINE_DEFAULT,
    MEM_COOLDOWN_DEFAULT,
    MEM_HEARTBEAT_DEFAULT,
    MEM_MAX_WAIT_DEFAULT,
    MEM_SLEEP_DEFAULT,
    MEM_THRESHOLD_DEFAULT,
    TRANSACTION_MODE_DEFAULT,
    SqlConfig,
    TransactionMode,
    coerce_transaction_mode,
    quote_identifier,
    validate_engine,
)
from pybutt.core.logobs import (
    MemoryGate,
    MemoryHeartbeat,
    context,
    get_logger,
    log_failure_summary,
    log_memory_budget,
    mem_fields,
)
from pybutt.exceptions import (
    BatchImportError,
    RowGroupImportError,
    SchemaMismatchError,
)
from pybutt.files import (
    default_import_manifest_filename,
    default_manifest_filename,
    load_file_manifest,
    load_manifest,
    validate_manifest_entries,
    write_manifest,
)

logger = get_logger("importer")

_SENTINEL = object()


@dataclass(slots=True)
class _QueueItem:
    """Payload passed from the reader thread to the writer."""

    rows: list[tuple[Any, ...]]
    rg_label: str
    batch_idx: int | None
    row_count: int


class Importer(SqlServerIOBase):
    def __init__(
        self,
        config: SqlConfig,
        input_path,
        manifest_filename: str | None,
        worker_count=1,
        batch_size: int = BATCH_SIZE_DEFAULT,
        transaction_mode: TransactionMode = TRANSACTION_MODE_DEFAULT,
        engine=IMPORT_ENGINE_DEFAULT,
        temp_manifest_filename: str | None = None,
        create_cci: bool = CCI_DEFAULT,
        mem_heartbeat: float = MEM_HEARTBEAT_DEFAULT,
        mem_threshold: float = MEM_THRESHOLD_DEFAULT,
        mem_sleep: float = MEM_SLEEP_DEFAULT,
        mem_max_wait: float = MEM_MAX_WAIT_DEFAULT,
        mem_cooldown: float = MEM_COOLDOWN_DEFAULT,
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
            else default_import_manifest_filename(self.schema, self.table)
        )

        self.worker_count = worker_count
        self.transaction_mode = coerce_transaction_mode(transaction_mode)
        validate_engine(engine)
        self.engine = engine
        self.batch_size = batch_size
        self.create_cci = create_cci
        self.mem_heartbeat = mem_heartbeat
        self.mem_gate = MemoryGate(mem_threshold, mem_sleep, mem_max_wait, mem_cooldown)

    def load_manifest(self):
        manifest_file = self.input_path / self.manifest_filename
        return load_manifest(manifest_file)

    def load_manifest_entries(self):
        manifest = load_file_manifest(
            self.input_path / self.manifest_filename, operation="Importer"
        )
        return validate_manifest_entries(manifest, self.input_path)

    def _build_insert_sql(
        self, columns: list[str], target_table: str | None = None
    ) -> str:
        column_list = ", ".join(quote_identifier(col) for col in columns)
        placeholders = ", ".join("?" for _ in columns)
        table_name = target_table or self.full_table_name()
        return f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

    def _rows_from_batch(self, batch):
        return rows_from_arrow(batch)

    def _validate_and_build_insert(
        self, cur, columns, filename, target_table: str | None = None
    ):
        table_columns = self.get_table_columns(cur, target_table=target_table)
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
        start = time.time()
        target_table_name = target_table or self.full_table_name()

        logger.info(
            "Importing "
            + context(
                file=filename,
                table=target_table_name,
                engine=self.engine,
                batch_size=self.batch_size,
                transaction_mode=self.transaction_mode.value,
                **mem_fields(),
            )
        )

        try:
            if self.transaction_mode == TransactionMode.FILE:
                # For FILE mode, wrap entire operation in retry logic
                def _file_operation():
                    if self.engine == "duckdb":
                        return self._import_file_with_duckdb(
                            filepath, filename, start, target_table=target_table
                        )
                    elif self.engine == "mssql-python":
                        return self._import_file_with_mssql(
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
                elif self.engine == "mssql-python":
                    self._import_file_with_mssql(
                        filepath, filename, start, target_table=target_table
                    )
                else:
                    self._import_file_impl(
                        filepath, filename, start, target_table=target_table
                    )
        except MemoryError:
            logger.error(
                "Out of memory - not retrying (fatal) " + context(file=filename)
            )
            raise
        except Exception as e:
            logger.error(
                "Failed importing "
                + context(file=filename)
                + f": {self.safe_error_message(e)}"
            )
            logger.debug("Traceback for failed import of %s", filename, exc_info=True)
            raise

        return True

    def _import_file_impl(
        self, filepath, filename, start, target_table: str | None = None
    ):
        """Implementation of file import with transaction management.

        Uses a producer-consumer pattern: a reader thread pre-reads
        rowgroups/batches into a bounded queue while the caller thread
        pushes rows to SQL Server via ``cur.executemany``.  This keeps
        the TDS pipe fed and reduces ASYNC_NETWORK_IO waits.
        """
        with self.connection_p() as c:
            with c.cursor() as cur:
                cur.fast_executemany = True
                parquet_file = pq.ParquetFile(filepath)
                columns = parquet_file.schema.names
                insert_sql = self._validate_and_build_insert(
                    cur, columns, filename, target_table=target_table
                )

                total_rows = 0
                buf: queue.Queue[_QueueItem | object] = queue.Queue(maxsize=2)
                cancel = threading.Event()

                reader = threading.Thread(
                    target=self._parquet_reader_thread,
                    args=(parquet_file, buf, filename, cancel),
                    daemon=True,
                    name=f"pyodbc-reader-{filename}",
                )
                reader.start()

                try:
                    while True:
                        item = buf.get()
                        if item is _SENTINEL:
                            break
                        if isinstance(item, Exception):
                            raise item
                        assert isinstance(item, _QueueItem)

                        if self.transaction_mode == TransactionMode.ROWGROUP:
                            rows_in_rg = self._import_rowgroup_with_retry(
                                c,
                                cur,
                                item.rows,
                                insert_sql,
                                filename,
                                rg=item.rg_label,
                            )
                            total_rows += rows_in_rg
                        elif self.transaction_mode == TransactionMode.BATCH:
                            rows_in_batch = self._import_batch_with_retry(
                                c,
                                cur,
                                item.rows,
                                insert_sql,
                                filename,
                                rg=item.rg_label,
                                batch=item.batch_idx,
                                offset=total_rows,
                            )
                            total_rows += rows_in_batch
                        else:
                            # FILE / ROW modes — no per-item commit
                            cur.executemany(insert_sql, item.rows)
                            total_rows += item.row_count
                except BaseException:
                    cancel.set()
                    raise
                finally:
                    reader.join(timeout=5)

                # Commit after entire file if in FILE mode
                if self.transaction_mode == TransactionMode.FILE:
                    c.commit()

                logger.info(
                    "Completed "
                    + context(
                        file=filename,
                        rows=total_rows,
                        seconds=f"{time.time() - start:.2f}",
                        **mem_fields(),
                    )
                )

    def _load_parquet_with_duckdb(self, filepath):
        logger.debug(
            "Loading parquet via DuckDB " + context(file=str(filepath), **mem_fields())
        )
        with self.connection_d() as dconn:
            sanitized_path = str(filepath.as_posix()).replace("'", "''")
            table = dconn.execute(
                f"SELECT * FROM read_parquet('{sanitized_path}')"
            ).fetch_arrow_table()
            logger.debug(
                "Loaded parquet via DuckDB "
                + context(
                    file=str(filepath),
                    rows=table.num_rows,
                    **mem_fields(),
                )
            )
            return table

    def _import_file_with_duckdb(
        self, filepath, filename, start, target_table: str | None = None
    ):
        with self.connection_p() as c:
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
                        self.mem_gate.check(
                            f"read_row_group file={filename}"
                            f" rg={rg_idx + 1}/{parquet_file.num_row_groups}"
                        )
                        logger.debug(
                            "Reading row group "
                            + context(
                                file=filename,
                                rg=f"{rg_idx + 1}/{parquet_file.num_row_groups}",
                                **mem_fields(),
                            )
                        )
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
                    for batch_idx, batch in enumerate(
                        parquet_table.to_batches(max_chunksize=self.batch_size)
                    ):
                        rows = self._rows_from_batch(batch)
                        if self.transaction_mode == TransactionMode.BATCH:
                            rows_in_batch = self._import_batch_with_retry(
                                c,
                                cur,
                                rows,
                                insert_sql,
                                filename,
                                batch=batch_idx,
                                offset=total_rows,
                            )
                            total_rows += rows_in_batch
                        else:
                            cur.executemany(insert_sql, rows)
                            total_rows += len(rows)

                if self.transaction_mode == TransactionMode.FILE:
                    c.commit()

                logger.info(
                    "Completed "
                    + context(
                        file=filename,
                        rows=total_rows,
                        seconds=f"{time.time() - start:.2f}",
                        **mem_fields(),
                    )
                )

    # ------------------------------------------------------------------
    # producer-consumer helpers
    # ------------------------------------------------------------------

    def _parquet_reader_thread(
        self,
        parquet_file: pq.ParquetFile,
        buf: queue.Queue[_QueueItem | object],
        filename: str,
        cancel: threading.Event,
    ) -> None:
        """Producer: read rowgroups/batches and enqueue row tuples."""
        total_rg = parquet_file.num_row_groups
        try:
            for rg_idx in range(total_rg):
                if cancel.is_set():
                    return
                rg_label = f"{rg_idx + 1}/{total_rg}"
                self.mem_gate.check(f"read_row_group file={filename} rg={rg_label}")
                logger.debug(
                    "Reading row group "
                    + context(file=filename, rg=rg_label, **mem_fields())
                )
                table = parquet_file.read_row_group(rg_idx)

                if self.transaction_mode == TransactionMode.ROWGROUP:
                    rows = rows_from_arrow(table)
                    buf.put(
                        _QueueItem(
                            rows=rows,
                            rg_label=rg_label,
                            batch_idx=None,
                            row_count=len(rows),
                        )
                    )
                else:
                    for batch_idx, batch in enumerate(
                        table.to_batches(max_chunksize=self.batch_size)
                    ):
                        if cancel.is_set():
                            return
                        rows = self._rows_from_batch(batch)
                        buf.put(
                            _QueueItem(
                                rows=rows,
                                rg_label=rg_label,
                                batch_idx=batch_idx,
                                row_count=len(rows),
                            )
                        )
        except Exception as exc:
            buf.put(exc)
            return
        finally:
            buf.put(_SENTINEL)

    def _import_file_with_mssql(
        self, filepath, filename, start, target_table: str | None = None
    ):
        """Import a parquet file using mssql-python's bulkcopy API.

        Uses a producer-consumer pattern: a reader thread pre-reads
        rowgroups/batches into a bounded queue while the caller thread
        pushes rows to SQL Server via ``cursor.bulkcopy``.  This keeps
        the TDS pipe fed and avoids ASYNC_NETWORK_IO waits on the
        server.
        """
        parquet_file = pq.ParquetFile(filepath)
        columns = parquet_file.schema.names
        target_table_name = target_table or self.full_table_name()

        conn = self.connection_m()
        try:
            cur = conn.cursor()
            try:
                table_columns = [
                    col[0]
                    for col in cur.execute(
                        f"SELECT TOP 0 * FROM {target_table_name}"
                    ).description
                ]
                self.validate_schema(columns, table_columns, filename)
            finally:
                cur.close()

            total_rows = 0
            buf: queue.Queue[_QueueItem | object] = queue.Queue(maxsize=2)
            cancel = threading.Event()

            reader = threading.Thread(
                target=self._parquet_reader_thread,
                args=(parquet_file, buf, filename, cancel),
                daemon=True,
                name=f"mssql-reader-{filename}",
            )
            reader.start()

            try:
                while True:
                    item = buf.get()
                    if item is _SENTINEL:
                        break
                    if isinstance(item, Exception):
                        raise item
                    assert isinstance(item, _QueueItem)

                    if self.transaction_mode == TransactionMode.ROWGROUP:
                        rows_in_rg = self._mssql_bulkcopy_with_retry(
                            conn,
                            item.rows,
                            columns,
                            target_table_name,
                            filename,
                            op="bulkcopy(rowgroup)",
                            rg=item.rg_label,
                            offset=total_rows,
                            is_rows=True,
                        )
                        total_rows += rows_in_rg
                        logger.debug(
                            "Processed row group "
                            + context(
                                file=filename,
                                rg=item.rg_label,
                                **mem_fields(),
                            )
                        )
                    elif self.transaction_mode == TransactionMode.BATCH:
                        rows_in_batch = self._mssql_bulkcopy_with_retry(
                            conn,
                            item.rows,
                            columns,
                            target_table_name,
                            filename,
                            op="bulkcopy(batch)",
                            rg=item.rg_label,
                            batch=item.batch_idx,
                            offset=total_rows,
                            is_rows=True,
                        )
                        total_rows += rows_in_batch
                    else:
                        # FILE / ROW modes — no per-item retry
                        cursor = conn.cursor()
                        try:
                            cursor.bulkcopy(
                                target_table_name,
                                item.rows,
                                column_mappings=columns,
                            )
                        finally:
                            cursor.close()
                        total_rows += item.row_count
            except BaseException:
                cancel.set()
                raise
            finally:
                reader.join(timeout=5)

            if self.transaction_mode == TransactionMode.FILE:
                conn.commit()

            logger.info(
                "Completed "
                + context(
                    file=filename,
                    rows=total_rows,
                    seconds=f"{time.time() - start:.2f}",
                    **mem_fields(),
                )
            )
        finally:
            conn.close()

    def _mssql_bulkcopy_with_retry(
        self,
        conn,
        data,
        columns,
        target_table_name,
        filename,
        op="bulkcopy",
        rg=None,
        batch=None,
        offset=None,
        is_rows=False,
    ):
        """Execute bulkcopy with retry logic."""
        if not is_rows:
            rows = self._rows_from_arrow_table(data)
        else:
            rows = data

        for attempt in range(self.config.retries):
            try:
                cursor = conn.cursor()
                try:
                    result = cursor.bulkcopy(
                        target_table_name,
                        rows,
                        column_mappings=columns,
                    )
                finally:
                    cursor.close()
                if self.transaction_mode in (
                    TransactionMode.BATCH,
                    TransactionMode.ROWGROUP,
                ):
                    conn.commit()
                return (
                    result.get("rows_copied", len(rows))
                    if isinstance(result, dict)
                    else len(rows)
                )
            except MemoryError:
                logger.error(
                    f"Out of memory during {op} - not retrying (fatal) "
                    + context(file=filename, rg=rg, batch=batch, offset=offset)
                )
                raise
            except Exception as e:
                safe_msg = self.safe_error_message(e)
                if attempt < self.config.retries - 1:
                    logger.warning(
                        f"{op} attempt {attempt + 1}/{self.config.retries} failed "
                        + context(
                            file=filename,
                            rg=rg,
                            batch=batch,
                            rows=len(rows),
                            offset=offset,
                        )
                        + f": {safe_msg}"
                    )
                    conn.rollback()
                    time.sleep(2**attempt)
                else:
                    raise BatchImportError(
                        f"Bulk copy failed after {self.config.retries} retries "
                        + context(file=filename, rg=rg, batch=batch, offset=offset)
                        + f": {safe_msg}"
                    ) from e

    def _rows_from_arrow_table(self, table):
        """Convert a PyArrow table to a list of tuples for bulkcopy."""
        return rows_from_arrow(table)

    def _import_batch_with_retry(
        self,
        c,
        cur,
        rows_or_batch,
        insert_sql,
        filename,
        rg=None,
        batch=None,
        offset=None,
    ):
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
            except MemoryError:
                logger.error(
                    "Out of memory during batch insert - not retrying (fatal) "
                    + context(file=filename, rg=rg, batch=batch, offset=offset)
                )
                raise
            except Exception as e:
                safe_msg = self.safe_error_message(e)

                if attempt < self.config.retries - 1:
                    logger.warning(
                        f"batch insert attempt {attempt + 1}/{self.config.retries} "
                        "failed "
                        + context(
                            file=filename,
                            rg=rg,
                            batch=batch,
                            rows=len(rows),
                            offset=offset,
                        )
                        + f": {safe_msg}"
                    )
                    c.rollback()
                    time.sleep(2**attempt)
                else:
                    raise BatchImportError(
                        f"Batch import failed after {self.config.retries} retries "
                        + context(file=filename, rg=rg, batch=batch, offset=offset)
                        + f": {safe_msg}"
                    ) from e

    def _import_rowgroup_with_retry(
        self,
        c,
        cur,
        table_or_batch,
        insert_sql,
        filename,
        rg_idx=None,
        total_rg=None,
        *,
        rg=None,
    ):
        """Import a single row group with retry logic for ROWGROUP mode.

        ``table_or_batch`` may be a PyArrow Table, RecordBatch, or a
        pre-converted ``list[tuple]`` of rows (from the reader thread).
        Pass *rg* to supply a pre-formatted label; otherwise it is
        derived from *rg_idx* / *total_rg*.
        """
        if rg is None:
            rg = f"{rg_idx + 1}/{total_rg}"
        is_rows = isinstance(table_or_batch, list)
        for attempt in range(self.config.retries):
            try:
                total_rows = 0
                if is_rows:
                    for i in range(0, len(table_or_batch), self.batch_size):
                        chunk = table_or_batch[i : i + self.batch_size]
                        cur.executemany(insert_sql, chunk)
                        total_rows += len(chunk)
                else:
                    to_batches = getattr(table_or_batch, "to_batches", None)
                    rowgroup_batches = (
                        table_or_batch.to_batches(max_chunksize=self.batch_size)
                        if callable(to_batches)
                        else [table_or_batch]
                    )
                    for batch in rowgroup_batches:
                        rows = rows_from_arrow(batch)
                        cur.executemany(insert_sql, rows)
                        total_rows += len(rows)

                c.commit()
                logger.debug("Processed row group " + context(file=filename, rg=rg))
                return total_rows
            except MemoryError:
                logger.error(
                    "Out of memory during rowgroup insert - not retrying (fatal) "
                    + context(file=filename, rg=rg)
                )
                raise
            except Exception as e:
                safe_msg = self.safe_error_message(e)

                if attempt < self.config.retries - 1:
                    logger.warning(
                        f"rowgroup insert attempt {attempt + 1}/{self.config.retries} "
                        "failed " + context(file=filename, rg=rg) + f": {safe_msg}"
                    )
                    c.rollback()
                    time.sleep(2**attempt)
                else:
                    raise RowGroupImportError(
                        f"Row group import failed after {self.config.retries} retries "
                        + context(file=filename, rg=rg)
                        + f": {safe_msg}"
                    ) from e

    def _make_temp_table_name(self, worker_index: int) -> str:
        suffix = uuid.uuid4().hex[:8]
        return f"{self.schema}.{self.table}_{worker_index + 1:02d}_{suffix}"

    def _make_columnstore_index_name(self, temp_table_name: str) -> str:
        table_part = temp_table_name.split(".", 1)[-1]
        return quote_identifier(f"cci_{table_part}")

    def _execute_temp_table_ddl(self, cur, count: int) -> list[str]:
        """Run the CREATE TABLE / CCI DDL on a cursor, returning table names."""
        temp_tables: list[str] = []
        for i in range(count):
            temp_table_name = self._make_temp_table_name(i)
            cur.execute(
                f"SELECT TOP 0 * INTO {temp_table_name} FROM {self.full_table_name()}"
            )
            if self.create_cci:
                index_name = self._make_columnstore_index_name(temp_table_name)
                cur.execute(
                    f"CREATE CLUSTERED COLUMNSTORE INDEX {index_name} "
                    f"ON {temp_table_name}"
                )
            temp_tables.append(temp_table_name)
        return temp_tables

    def _create_temp_tables(self, count: int) -> list[str]:
        if self.engine == "mssql-python":
            conn = self.connection_m(autocommit=True)
            try:
                cur = conn.cursor()
                try:
                    return self._execute_temp_table_ddl(cur, count)
                finally:
                    cur.close()
            finally:
                conn.close()
        else:
            with self.connection_p(autocommit=True) as conn:
                with conn.cursor() as cur:
                    return self._execute_temp_table_ddl(cur, count)

    def _assign_files_to_workers(
        self, filenames: list[str], temp_tables: list[str]
    ) -> dict[str, list[str]]:
        assignments: dict[str, list[str]] = {tbl: [] for tbl in temp_tables}
        for index, filename in enumerate(filenames):
            target_table = temp_tables[index % len(temp_tables)]
            assignments[target_table].append(filename)
        return assignments

    def _write_temp_manifest(self, temp_tables: list[str]) -> Path:
        return write_manifest(
            self.input_path / self.temp_manifest_filename,
            temp_tables,
            manifest_type="tables",
        )

    def _import_files_to_temp_table(self, target_table: str, filenames: list[str]):
        for filename in filenames:
            self.import_file(filename, target_table=target_table)

    def _delete_original_files(self, filenames: list[str]):
        for filename in filenames:
            path = self.input_path / filename
            if path.exists():
                path.unlink()

        manifest_path = self.input_path / self.manifest_filename
        if manifest_path.exists():
            manifest_path.unlink()

    def perform_work(self):
        # Import runs in a single process (worker threads share its memory), so
        # one heartbeat here reports the whole run's RSS trend.
        with MemoryHeartbeat(self.mem_heartbeat, unit="import"):
            self._perform_work()

    def _perform_work(self):
        filenames = self.load_manifest_entries()

        log_memory_budget(
            operation="import",
            workers=self.worker_count,
            threshold_pct=self.mem_gate.threshold_pct,
        )

        if self.worker_count > 1 and len(filenames) > 1:
            worker_count = min(self.worker_count, len(filenames))
            temp_tables = self._create_temp_tables(worker_count)
            assignments = self._assign_files_to_workers(filenames, temp_tables)

            with ThreadPoolExecutor(
                max_workers=worker_count, thread_name_prefix="import"
            ) as executor:
                futures = {
                    executor.submit(
                        self._import_files_to_temp_table, target_table, assigned
                    ): target_table
                    for target_table, assigned in assignments.items()
                    if assigned
                }

                self._await_futures(futures, label="table")

            manifest_file = self._write_temp_manifest(temp_tables)
            logger.info("Wrote temporary table manifest " + context(file=manifest_file))
            return

        with ThreadPoolExecutor(
            max_workers=self.worker_count, thread_name_prefix="import"
        ) as executor:
            futures = {
                executor.submit(self.import_file, filename): filename
                for filename in filenames
            }

            self._await_futures(futures, label="file")

    def _await_futures(self, futures, label):
        """Wait for worker futures, surfacing *all* failures before re-raising.

        Without this, a worker exception only re-raises as a bare traceback with
        no indication of *which* file/table the dead worker was handling.  We now
        wait for every future so errors from all workers are logged, then raise
        the first failure.
        """
        first_error: Exception | None = None
        completed_units: list[str] = []
        for future in as_completed(futures):
            unit = futures[future]
            try:
                future.result()
                completed_units.append(str(unit))
            except Exception as e:
                logger.error(
                    "Worker failed "
                    + context(**{label: unit})
                    + f": {self.safe_error_message(e)}"
                )
                if first_error is None:
                    first_error = e
        if first_error is not None:
            log_failure_summary(
                operation="import",
                workers=len(futures),
                completed=completed_units,
                failed_error=self.safe_error_message(first_error),
            )
            raise first_error


if __name__ == "__main__":
    pass
