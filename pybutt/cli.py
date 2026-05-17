import typer
from typing import Optional, List
from pybutt.core import Exporter, SqlConfig

app = typer.Typer(help="Python Bulk Transfer Tool for MS SQL Server CLI")


@app.command(help="Python Bulk Transfer Tool for MS SQL Server CLI")
def export(
    server: str = typer.Option(...),
    database: str = typer.Option(...),
    schema: str = typer.Option(...),
    table: str = typer.Option(...),
    output_path: str = typer.Option(...),

    username: Optional[str] = typer.Option(None),
    password: Optional[str] = typer.Option(None),

    pk_column: Optional[str] = typer.Option(None),
    columns: Optional[List[str]] = typer.Option(None),

    worker_count: int = typer.Option(1),
    max_rows_per_file: int = typer.Option(1_000_000),

    trusted_connection: bool = typer.Option(False),
    trust_cert: bool = typer.Option(False),
    encrypt: bool = typer.Option(True),
    retries: int = typer.Option(3),
):
    try:
        cfg = SqlConfig(
            server=server,
            database=database,
            schema=schema,
            table=table,
            username=username,
            password=password,
            trusted_connection=trusted_connection,
            trust_cert=trust_cert,
            encrypt=encrypt,
            retries=retries,
        )

        exporter = Exporter(
            config=cfg,
            output_path=output_path,
            pk_column=pk_column,
            columns=columns,
            worker_count=worker_count,
            max_rows_per_file=max_rows_per_file,
        )

        exporter.perform_work()
        typer.echo("Export complete")

    except ValueError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=1)

    except Exception as e:
        typer.echo(f"Unexpected error: {e}", err=True)
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()