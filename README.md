# pybutt
Python Bulk Transfer Tool

## Getting started

First, ensure you have `libodbc.so.2`, usually supplied with the `unixodbc` package;
as well as `msodbcsql` version 18; and duckdb (see https://duckdb.org/install/?platform=linux&environment=cli)

```bash
ldconfig -p | grep libodbc # check libodbc.so.2 is listed
odbcinst -q -d # Check we have ODBC Driver 18 for SQL Server
git clone https://github.com/dmonlineuk/pybutt && cd pybutt
python -m venv .venv
. .venv/bin.activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

