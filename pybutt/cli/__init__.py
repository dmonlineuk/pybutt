# Import command modules so @app.command decorators register
from . import combine_command  # noqa: F401, I001
from . import export_command   # noqa: F401, I001
from . import import_command   # noqa: F401, I001
from . import inspect_command  # noqa: F401, I001
from . import purge_command    # noqa: F401, I001
from .app import app

__all__ = ["app"]
