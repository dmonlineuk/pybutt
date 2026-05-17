import pytest


# ------------------------------------------------------------
# ✅ GLOBAL EXPORT MOCK (DuckDB / ODBC read)
# ------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_connection_d(monkeypatch):

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return (1000,)  # simulate row count

    monkeypatch.setattr(
        "pybutt.core.SqlServerIOBase.connection_d",
        lambda self: Conn()
    )


# ------------------------------------------------------------
# ✅ GLOBAL IMPORT MOCK (pyodbc write)
# ------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_connection_p(monkeypatch):

    class Cursor:
        description = [("col1",), ("col2",)]

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, *_):
            return self
        def executemany(self, *_):
            return None

        fast_executemany = False  # important: attribute exists
    
    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def cursor(self):
            return Cursor()

        def commit(self): pass
        def rollback(self): pass

        autocommit = False

    monkeypatch.setattr(
        "pybutt.core.SqlServerIOBase.connection_p",
        lambda self: Conn()
    )