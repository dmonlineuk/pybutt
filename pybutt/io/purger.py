from collections.abc import Iterable

from pybutt.core.base import SqlServerIOBase
from pybutt.core.config import quote_identifier, validate_identifier
from pybutt.core.logobs import get_logger

logger = get_logger("purger")


class TablePurger(SqlServerIOBase):
    """Drop SQL tables listed in a manifest.

    Sources should be provided as fully-qualified schema.table strings.
    """

    def __init__(self, config, sources: Iterable[str]):
        super().__init__(config)
        self.sources: list[str] = list(sources)

    def _parse_schema_table(self, fq: str) -> tuple[str, str]:
        parts = fq.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid source table name: {fq}")
        schema, table = parts
        validate_identifier(schema)
        validate_identifier(table)
        return schema, table

    def purge(self) -> list[str]:
        """Drop all tables in sources. Returns list of dropped table names."""
        dropped: list[str] = []
        conn = self.connection_p(autocommit=True)
        try:
            cur = conn.cursor()
            for fq_name in self.sources:
                schema, table = self._parse_schema_table(fq_name)
                qualified = f"{quote_identifier(schema)}.{quote_identifier(table)}"
                logger.info(f"Dropping table {qualified}")
                cur.execute(f"DROP TABLE IF EXISTS {qualified}")  # noqa: S608
                dropped.append(fq_name)
                logger.info(f"Dropped table {qualified}")
        finally:
            conn.close()
        return dropped
