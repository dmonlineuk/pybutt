from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def create_parquet():
    def _create_parquet(
        tmp_path: Path, name: str, rows: int = 10, rowgroup_size: int = 5
    ):
        data = {
            "id": list(range(rows)),
            "value": [f"v{i}" for i in range(rows)],
        }
        table = pa.Table.from_pydict(data)
        file_path = tmp_path / name
        pq.write_table(table, file_path, row_group_size=rowgroup_size)
        return file_path

    return _create_parquet
