from collections.abc import Iterable

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import TransactionMode, coerce_transaction_mode, validate_engine
from pybutt.core.logobs import context, get_logger
from pybutt.exceptions import SchemaMismatchError

logger = get_logger("merger")


class TableMerger(SqlServerIOBase):
    """Merge multiple SQL tables into a single target table.

    Sources should be provided as fully-qualified schema.table strings.
    """

    def __init__(
        self,
        config,
        sources: Iterable[str],
        transaction_mode: TransactionMode = TransactionMode.BATCH,
        engine: str = "pyodbc",
    ):
        super().__init__(config)
        self.sources: list[str] = list(sources)
        self.transaction_mode = coerce_transaction_mode(transaction_mode)
        validate_engine(engine, allowed=frozenset({"pyodbc", "duckdb"}))
        self.engine = engine

    def _parse_schema_table(self, fq: str) -> tuple[str, str]:
        parts = fq.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid source table name: {fq}")
        return parts[0], parts[1]

    def _ensure_target_exists_and_schema(
        self, cur, first_source: str, target_schema: str, target_table: str
    ):
        # If target exists, validate schema equality;
        # otherwise create from first source (no rows)
        cur.execute("SELECT OBJECT_ID(?)", (f"{target_schema}.{target_table}",))
        exists = cur.fetchone()[0] is not None

        # Get column list for source
        src_schema, src_table = self._parse_schema_table(first_source)
        cur.execute(f"SELECT TOP 0 * FROM {src_schema}.{src_table}")
        src_cols = [c[0] for c in cur.description]

        if not exists:
            # Create target table with same schema as source
            cur.execute(
                "SELECT TOP 0 * "
                f"INTO {target_schema}.{target_table} "
                f"FROM {src_schema}.{src_table}"
            )
            cur.connection.commit()
            return src_cols

        # Target exists: get target columns
        cur.execute(f"SELECT TOP 0 * FROM {target_schema}.{target_table}")
        tgt_cols = [c[0] for c in cur.description]

        if set(src_cols) != set(tgt_cols):
            raise SchemaMismatchError(
                "Source and target schemas differ for "
                f"{first_source} vs {target_schema}.{target_table}"
            )

        return src_cols

    def merge(self, target_schema: str, target_table: str):
        """Merge all source tables into the target table.

        Implementation: create target if missing using first source schema, then
        run `INSERT INTO target SELECT * FROM source` for each source.
        """
        with self.connection_p(autocommit=False) as conn:
            with conn.cursor() as cur:
                # Ensure first source schema compatible / create target
                first_source = self.sources[0]
                self._ensure_target_exists_and_schema(
                    cur, first_source, target_schema, target_table
                )

                # Insert from each source
                for src in self.sources:
                    src_schema, src_table = self._parse_schema_table(src)
                    logger.info(
                        "Merging "
                        + context(
                            source=f"{src_schema}.{src_table}",
                            target=f"{target_schema}.{target_table}",
                        )
                    )
                    try:
                        cur.execute(
                            f"INSERT INTO {target_schema}.{target_table} "
                            f"SELECT * FROM {src_schema}.{src_table}"
                        )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise

        logger.info("Table merge completed")
