import json
from pathlib import Path

from pybutt.core.config import validate_identifier
from pybutt.exceptions import (
    DuplicateManifestEntryError,
    InvalidManifestEntryError,
    InvalidManifestError,
    ManifestNotFoundError,
    MissingManifestEntryError,
    PathTraversalError,
    UnsupportedManifestTypeError,
    UnsupportedManifestVersionError,
)

MANIFEST_VERSION_1 = 1
MANIFEST_VERSION_2 = 2
SUPPORTED_MANIFEST_TYPES = frozenset({"files", "tables"})


def _parse_manifest_dict(data):
    if not isinstance(data, dict):
        raise InvalidManifestError(
            "Manifest must be a list or an object with version, type, and entries"
        )

    version = data.get("version")
    if version not in {MANIFEST_VERSION_1, MANIFEST_VERSION_2}:
        raise UnsupportedManifestVersionError(
            f"Unsupported manifest version: {version}"
        )

    manifest_type = data.get("type")
    if version == MANIFEST_VERSION_1:
        manifest_type = manifest_type or "files"
        if manifest_type != "files":
            raise UnsupportedManifestTypeError(
                "Version 1 manifests support only type 'files'"
            )
    else:
        if manifest_type not in SUPPORTED_MANIFEST_TYPES:
            raise InvalidManifestError(
                "Manifest type must be 'files' or 'tables' for version 2"
            )

    entries = data.get("entries")
    if not isinstance(entries, list):
        raise InvalidManifestError("Manifest entries must be a list")

    if manifest_type == "tables":
        return {
            "version": version,
            "type": manifest_type,
            "entries": [_validate_table_name(e) for e in entries],
        }

    return {"version": version, "type": manifest_type, "entries": entries}


def _validate_table_name(value):
    if not isinstance(value, str):
        raise InvalidManifestEntryError(
            f"Invalid manifest table entry (not string): {value}"
        )

    parts = value.split(".")
    if len(parts) != 2:
        raise InvalidManifestEntryError(
            f"Invalid table name format, expected schema.table: {value}"
        )

    schema, table = parts
    validate_identifier(schema)
    validate_identifier(table)
    return value


def default_manifest_filename(
    schema: str, table: str, op_type: str = "", suffix: str = "manifest"
) -> str:
    return f"{schema}_{table}_{op_type}{suffix}.json"


def default_import_manifest_filename(
    schema: str,
    table: str,
) -> str:
    return default_manifest_filename(schema=schema, table=table, op_type="import_")


def write_manifest(
    path: str | Path,
    entries: list[str],
    manifest_type: str = "files",
    version: int = MANIFEST_VERSION_2,
) -> Path:
    """Write a versioned manifest JSON file and return its :class:`Path`."""
    path = Path(path)
    with open(path, "w") as f:
        json.dump(
            {"version": version, "type": manifest_type, "entries": entries},
            f,
            indent=4,
        )
    return path


def load_manifest(manifest_path: str | Path) -> dict:
    manifest_path = Path(manifest_path)

    if not manifest_path.exists():
        raise ManifestNotFoundError(f"Manifest not found: {manifest_path}")

    with open(manifest_path) as f:
        data = json.load(f)

    if isinstance(data, list):
        return {"version": MANIFEST_VERSION_1, "type": "files", "entries": data}

    return _parse_manifest_dict(data)


def load_file_manifest(
    manifest_path: str | Path, *, operation: str = "Operation"
) -> dict:
    """Load a manifest and raise if it is not a file manifest."""
    manifest = load_manifest(manifest_path)
    if manifest["type"] != "files":
        raise UnsupportedManifestTypeError(
            f"{operation} only supports file manifests, got: {manifest['type']}"
        )
    return manifest


def validate_manifest_entries(manifest: dict, base_dir: Path) -> list[str]:
    seen = set()
    validated = []

    for item in manifest["entries"]:
        if not isinstance(item, str):
            raise InvalidManifestEntryError(
                f"Invalid manifest entry (not string): {item}"
            )

        if item in seen:
            raise DuplicateManifestEntryError(f"Duplicate file in manifest: {item}")

        if manifest["type"] == "files":
            filepath = (base_dir / item).resolve()
            if not filepath.is_relative_to(base_dir.resolve()):
                raise PathTraversalError(
                    f"Manifest entry escapes base directory: {item}"
                )
            if not filepath.exists():
                raise MissingManifestEntryError(f"Missing file: {filepath}")

        seen.add(item)
        validated.append(item)

    return validated
