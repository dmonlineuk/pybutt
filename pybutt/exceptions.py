class PyButtError(Exception):
    """Base class for all PyButt-specific errors."""


class ConfigurationError(PyButtError, ValueError):
    """Raised for invalid application configuration."""


class EngineSelectionError(ConfigurationError):
    """Raised when an unsupported engine is selected."""


class InvalidIdentifierError(ConfigurationError):
    """Raised when a SQL identifier is invalid."""


class ManifestError(PyButtError, ValueError):
    """Base class for manifest validation errors."""


class ManifestNotFoundError(FileNotFoundError, ManifestError):
    """Raised when a manifest file cannot be found."""


class InvalidManifestError(ManifestError):
    """Raised when a manifest file contains invalid data."""


class InvalidManifestEntryError(InvalidManifestError):
    """Raised when a manifest entry is malformed."""


class DuplicateManifestEntryError(InvalidManifestError):
    """Raised when a manifest contains duplicate file entries."""


class UnsupportedManifestVersionError(InvalidManifestError):
    """Raised when a manifest has an unsupported version."""


class UnsupportedManifestTypeError(InvalidManifestError):
    """Raised when a manifest type is not supported."""


class MissingManifestEntryError(FileNotFoundError, InvalidManifestError):
    """Raised when a manifest references a missing Parquet file."""


class SchemaMismatchError(PyButtError, ValueError):
    """Raised when Parquet schema does not match the destination table schema."""


class DataExportError(PyButtError, RuntimeError):
    """Raised when exporting data fails."""


class DataImportError(PyButtError, RuntimeError):
    """Raised when importing data fails."""


class BatchImportError(DataImportError):
    """Raised when a batch import fails after retries."""


class RowGroupImportError(DataImportError):
    """Raised when a row group import fails after retries."""


class RetryExceededError(PyButtError, RuntimeError):
    """Raised when retry logic exhausts all attempts."""


class TableEmptyError(DataExportError):
    """Raised when the source table is empty or missing."""
