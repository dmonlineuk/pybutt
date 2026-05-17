import typer
from typing import Optional, List
from pybutt.core import Exporter

app = typer.Typer(help="Python Bulk Transfer Tool for MS SQL Server CLI")

@app.command()
def export(
    server: str = typer.Option(..., help="SQL Server hostname"),
    database: str = typer.Option(..., help="Database name"),
    schema: str = typer.Option(..., help="Schema name"),
    table: str = typer.Option(..., help="Table name"),
    output_path: str = typer.Option(..., help="Output directory"),
    username: Optional[str] = typer.Option(None, help="SQL username"),
    password: Optional[str] = typer.Option(None, help="SQL password"),
    pk_column: Optional[str] = typer.Option(None, help="Primary key for partitioning"),
    columns: Optional[List[str]] = typer.Option(None, help="Columns to export"),
    worker_count: int = typer.Option(1, help="Parallel workers"),
    max_rows_per_file: int = typer.Option(1_000_000),
    trusted_connection: bool = typer.Option(False),
    trust_cert: bool = typer.Option(False),
    encrypt: bool = typer.Option(True),
    retries: int = typer.Option(3),
):
    """
    Export a SQL Server table to Parquet using DuckDB + ODBC.
    """
    try:
        exporter = Exporter(
            server=server,
            database=database,
            schema=schema,
            table=table,
            output_path=output_path,
            username=username,
            password=password,
            pk_column=pk_column,
            columns=columns,
            worker_count=worker_count,
            max_rows_per_file=max_rows_per_file,
            trusted_connection=trusted_connection,
            trust_cert=trust_cert,
            encrypt=encrypt,
            retries=retries,
        )
    
        exporter.perform_work()
        typer.echo("Export complete")
    except ValueError as e:
        typer.echo(f"{e}", err=True)
        raise typer.Exit(code=1)

    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()