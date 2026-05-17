import pytest
from pybutt.core import Exporter, SqlConfig


def make_exporter(**overrides):
    cfg = SqlConfig(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        username="user",
        password="pass",
    )

    output_path = overrides.pop("output_path", "/tmp")

    return Exporter(
        config=cfg,
        output_path="/tmp",
        **overrides
    )


# ------------------------------------------------------------
# ✅ INIT / PARTITION
# ------------------------------------------------------------

def test_partition_meta():
    e = make_exporter(max_rows_per_file=100)
    assert e.total_rows == 1000
    assert e.partition_count == 10


# ------------------------------------------------------------
# ✅ QUERY GENERATION
# ------------------------------------------------------------

def test_query_pk():
    e = make_exporter(pk_column="id")
    e.max_rows_per_file = 100

    q = e.build_partition_query(1)

    assert "ROW_NUMBER()" in q
    assert "ORDER BY [id]" in q


def test_query_no_pk():
    e = make_exporter()
    q = e.build_partition_query(0)

    assert "[dbo].[tbl]" in q


# ------------------------------------------------------------
# ✅ EXPORT PARTITION SUCCESS
# ------------------------------------------------------------

def test_export_partition(monkeypatch, tmp_path):
    calls = {"count": 0}

    monkeypatch.setattr(Exporter, "partition_meta", lambda self: None)

    def mock_conn(self):
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *_): pass
                
            def execute(self, *_):
                calls["count"] += 1
                return self

        return Conn()

    monkeypatch.setattr(
        "pybutt.core.SqlServerIOBase.connection_d",
        mock_conn
    )

    e = make_exporter(output_path=tmp_path)
    e.total_rows = 1000
    e.partition_count = 1

    e.export_partition(0)

    assert calls["count"] >= 1


# ------------------------------------------------------------
# ✅ RETRIES
# ------------------------------------------------------------

def test_export_retries(monkeypatch):

    def fake_partition_meta(self):
        self.total_rows = 1000
        self.partition_count = 1

    monkeypatch.setattr(Exporter, "partition_meta", fake_partition_meta)

    calls = {"count": 0}

    def failing_conn(self):
        class Conn:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def execute(self, *_):
                calls["count"] += 1
                raise Exception("fail")
        return Conn()

    monkeypatch.setattr(
        "pybutt.core.SqlServerIOBase.connection_d",
        failing_conn
    )

    e = make_exporter()

    with pytest.raises(RuntimeError):
        e.export_partition(0)

    assert calls["count"] == e.config.retries


# ------------------------------------------------------------
# ✅ MULTIPROCESSING
# ------------------------------------------------------------

def test_perform_work(monkeypatch):
    calls = {"count": 0}

    def fake_export(self, n):
        calls["count"] += 1
        return f"file_{n}.parquet"

    class FakePool:
        def __init__(self, *_): pass
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def map(self, fn, iterable):
            return [fn(i) for i in iterable]

    monkeypatch.setattr(
        "pybutt.core.get_context",
        lambda *_: type("X", (), {"Pool": FakePool})()
    )

    monkeypatch.setattr(Exporter, "export_partition", fake_export)

    e = make_exporter()
    e.partition_count = 3

    e.perform_work()

    assert calls["count"] == 3