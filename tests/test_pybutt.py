import pytest
from pathlib import Path

from pybutt.core import Exporter, validate_identifier, quote_identifier


# ============================================================
# ✅ GLOBAL MOCK: prevent ALL real DB calls
# ============================================================

@pytest.fixture(autouse=True)
def mock_connection(monkeypatch):
    """
    Automatically replaces Exporter.connection for ALL tests.
    Prevents real SQL Server / ODBC calls.
    """

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return (1000,)  # pretend table has 1000 rows

    monkeypatch.setattr(Exporter, "connection", lambda self: Conn())


# ============================================================
# ✅ IDENTIFIER VALIDATION
# ============================================================

@pytest.mark.parametrize("valid", [
    "table1",
    "my_table",
    "TableName",
    "t123"
])
def test_validate_identifier_valid(valid):
    assert validate_identifier(valid) == valid


@pytest.mark.parametrize("invalid", [
    "table-name",
    "table name",
    "123table",
    "table;DROP",
])
def test_validate_identifier_invalid(invalid):
    with pytest.raises(ValueError):
        validate_identifier(invalid)


# ============================================================
# ✅ IDENTIFIER QUOTING
# ============================================================

def test_quote_identifier_basic():
    assert quote_identifier("dbo") == "[dbo]"


def test_quote_identifier_escapes_brackets():
    assert quote_identifier("a]b") == "[a]]b]"


# ============================================================
# ✅ DSN CONSTRUCTION
# ============================================================

def test_dsn_sql_auth():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        username="user",
        password="pass",
        output_path="/tmp"
    )

    assert "Uid=user" in e.dsn
    assert "Pwd=pass" in e.dsn
    assert "Trusted_Connection" not in e.dsn


def test_dsn_trusted_connection():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        trusted_connection=True,
        output_path="/tmp"
    )

    assert "Trusted_Connection=Yes" in e.dsn
    assert "Uid=" not in e.dsn


def test_dsn_certificate_flag():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        trust_cert=True,
        output_path="/tmp"
    )

    assert "TrustServerCertificate=Yes" in e.dsn


# ============================================================
# ✅ TABLE NAMING
# ============================================================

def test_full_table_name():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        output_path="/tmp"
    )

    assert e.full_table_name() == "[dbo].[tbl]"


# ============================================================
# ✅ PARTITION METADATA
# ============================================================

def test_partition_meta_calculation():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        output_path="/tmp",
        max_rows_per_file=100
    )

    assert e.total_rows == 1000
    assert e.partition_count == 10


# ============================================================
# ✅ QUERY GENERATION
# ============================================================

def test_build_partition_query_range():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        pk_column="id",
        output_path="/tmp"
    )

    e.max_rows_per_file = 100

    q = e.build_partition_query(1)

    assert "ROW_NUMBER()" in q
    assert "ORDER BY [id]" in q
    assert "rn > 100" in q
    assert "rn <= 200" in q
    assert "[dbo].[tbl]" in q


def test_build_partition_query_no_pk():
    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        output_path="/tmp"
    )

    q = e.build_partition_query(0)

    assert "[dbo].[tbl]" in q
    # we no longer rely on CHECKSUM(*)
    assert "SELECT" in q


# ============================================================
# ✅ EXPORT PARTITION (SUCCESS)
# ============================================================

def test_export_partition_success(monkeypatch, tmp_path):
    called = {"count": 0}

    def mock_conn(self):
        class Conn:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def execute(self, *_):
                called["count"] += 1
                return self

            def fetchone(self):
                return (1000,)

        return Conn()

    monkeypatch.setattr(Exporter, "connection", mock_conn)

    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        output_path=tmp_path
    )

    e.partition_count = 1

    e.export_partition(0)

    assert called["count"] >= 1


# ============================================================
# ✅ EXPORT PARTITION (RETRIES)
# ============================================================

def test_export_partition_retries(monkeypatch):
    calls = {"count": 0}

    # ✅ Prevent __init__ from touching the database
    def fake_partition_meta(self):
        self.total_rows = 1000
        self.partition_count = 1

    monkeypatch.setattr(Exporter, "partition_meta", fake_partition_meta)

    # ✅ Now mock connection to FAIL (only affects export phase)
    def mock_conn(self):
        class FailingConn:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def execute(self, *_):
                calls["count"] += 1
                raise Exception("fail")

        return FailingConn()

    monkeypatch.setattr(Exporter, "connection", mock_conn)

    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        output_path="/tmp",
        retries=3
    )
    
    e.export_partition(0)

    assert calls["count"] == 3


# ============================================================
# ✅ MULTIPROCESSING
# ============================================================

def test_perform_work(monkeypatch):
    called = {"count": 0}

    def fake_export(self, n):
        called["count"] += 1

    class FakePool:
        def __init__(self, *_):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def map(self, func, iterable):
            for i in iterable:
                func(i)

    monkeypatch.setattr("pybutt.core.Pool", FakePool)
    monkeypatch.setattr(Exporter, "export_partition", fake_export)

    e = Exporter(
        server="srv",
        database="db",
        schema="dbo",
        table="tbl",
        output_path="/tmp"
    )

    e.partition_count = 4
    e.perform_work()

    assert called["count"] == 4
