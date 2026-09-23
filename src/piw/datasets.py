"""Generic local dataset substrate: descriptors, import configuration, registry.

Identity is deterministic: source bytes plus the normalized import configuration.
No timestamp, random identifier, or machine path participates in dataset identity.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .events import SourceContractError

IMPORT_CONTRACT_VERSION = "dataset-import-v1"
SOURCE_FORMAT_XES = "xes"
SOURCE_FORMAT_CSV = "csv"
SOURCE_FORMATS = (SOURCE_FORMAT_XES, SOURCE_FORMAT_CSV)
REGISTRY_FILENAME = "registry.json"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REGISTRY_ROOT = PROJECT_ROOT / "data" / "datasets"
_LOG_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_FIXED_OFFSET_PATTERN = re.compile(r"^([+-])(\d{2}):?(\d{2})$")


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_timezone(name: str) -> tzinfo:
    """Resolve a fixed ISO-8601 UTC offset or an IANA time zone name.

    Fixed offsets need no tz database; IANA names require one to be available
    locally. Anything else fails closed.
    """

    text = name.strip()
    upper = text.upper()
    if upper in {"Z", "UTC", "GMT"}:
        return timezone.utc
    if upper.startswith(("UTC", "GMT")):
        text = text[3:].strip()
    match = _FIXED_OFFSET_PATTERN.match(text)
    if match:
        sign, hours, minutes = match.groups()
        offset = timedelta(hours=int(hours), minutes=int(minutes))
        if offset >= timedelta(hours=24) or int(minutes) > 59:
            raise SourceContractError(f"unsupported UTC offset {name!r}")
        return timezone(-offset if sign == "-" else offset)
    try:
        return ZoneInfo(text)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise SourceContractError(
            f"unsupported time zone {name!r}: use a fixed ISO-8601 offset such as "
            "'+02:00' or an IANA time zone name available in the local tz database"
        ) from exc


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    """Optional attribution metadata; arbitrary local datasets have none."""

    dataset_url: str | None = None
    doi: str | None = None
    source_url: str | None = None


@dataclass(frozen=True, slots=True)
class SourceFingerprint:
    filename: str
    sha256: str
    size_bytes: int
    provenance: SourceProvenance = SourceProvenance()

    def as_dict(self) -> dict[str, Any]:
        """Report form: base fingerprint plus provenance that actually exists."""

        value: dict[str, Any] = {
            "filename": self.filename,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }
        for key in ("dataset_url", "doi", "source_url"):
            item = getattr(self.provenance, key)
            if item is not None:
                value[key] = item
        return value


def source_fingerprint(
    path: Path, provenance: SourceProvenance | None = None
) -> SourceFingerprint:
    """Fingerprint exact source bytes; never a path, timestamp, or identifier."""

    path = Path(path)
    return SourceFingerprint(
        filename=path.name,
        sha256=_sha256_file(path),
        size_bytes=path.stat().st_size,
        provenance=provenance or SourceProvenance(),
    )


@dataclass(frozen=True, slots=True)
class TimestampConfig:
    """Explicit interpretation rules for source timestamps.

    ``assume_timezone`` applies only to timestamps that carry no UTC offset;
    offset-less values are never silently treated as UTC.
    """

    assume_timezone: str | None = None
    timestamp_format: str | None = None

    def __post_init__(self) -> None:
        if self.assume_timezone is not None:
            if not self.assume_timezone or not isinstance(self.assume_timezone, str):
                raise SourceContractError(
                    "assume_timezone must be a non-empty ISO-8601 offset or IANA time zone name"
                )
            resolve_timezone(self.assume_timezone)
        if self.timestamp_format is not None and (
            not isinstance(self.timestamp_format, str) or not self.timestamp_format
        ):
            raise SourceContractError("timestamp_format must be a non-empty strptime pattern")

    def as_record(self) -> dict[str, Any]:
        return {
            "assume_timezone": self.assume_timezone,
            "timestamp_format": self.timestamp_format,
        }


@dataclass(frozen=True, slots=True)
class CsvColumnMapping:
    """Canonical field to source column mapping; source column names are free."""

    case_id: str
    activity: str
    timestamp: str
    lifecycle: str | None = None
    resource: str | None = None

    def __post_init__(self) -> None:
        for name in ("case_id", "activity", "timestamp"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise SourceContractError(
                    f"CSV mapping requires a non-empty {name} source column"
                )
        for name in ("lifecycle", "resource"):
            value = getattr(self, name)
            if value is not None and (not isinstance(value, str) or not value):
                raise SourceContractError(
                    f"CSV mapping {name} source column must be non-empty when present"
                )
        columns = [
            self.case_id,
            self.activity,
            self.timestamp,
            *[column for column in (self.lifecycle, self.resource) if column is not None],
        ]
        if len(set(columns)) != len(columns):
            raise SourceContractError(
                "CSV mapping must map each canonical field to a distinct source column"
            )

    def as_record(self) -> dict[str, Any]:
        return {
            "activity": self.activity,
            "case_id": self.case_id,
            "lifecycle": self.lifecycle,
            "resource": self.resource,
            "timestamp": self.timestamp,
        }


def validate_delimiter(value: Any) -> str:
    """Validate one CSV delimiter; anything ambiguous fails closed."""

    if not isinstance(value, str) or len(value) != 1:
        raise SourceContractError("CSV delimiter must be exactly one character")
    if value in {'"', "\r", "\n"}:
        raise SourceContractError(f"unsupported CSV delimiter {value!r}")
    return value


@dataclass(frozen=True, slots=True)
class CsvImportConfig:
    mapping: CsvColumnMapping
    timestamp: TimestampConfig = TimestampConfig()
    delimiter: str = ","

    def __post_init__(self) -> None:
        validate_delimiter(self.delimiter)

    def as_record(self) -> dict[str, Any]:
        return {
            "delimiter": self.delimiter,
            "mapping": self.mapping.as_record(),
            "timestamp": self.timestamp.as_record(),
        }


def dataset_identity(
    *,
    source_format: str,
    sha256: str,
    size_bytes: int,
    csv_import: CsvImportConfig | None = None,
) -> str:
    """Stable identity of source bytes plus the normalized import configuration.

    The source path, display name, and `log_id` deliberately do not participate:
    identical bytes plus identical configuration keep one identity anywhere.
    """

    payload = {
        "csv_import": None if csv_import is None else csv_import.as_record(),
        "import_contract_version": IMPORT_CONTRACT_VERSION,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "source_format": source_format,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class DatasetDescriptor:
    """One registered local dataset and everything needed to rebuild it."""

    log_id: str
    display_name: str
    source_format: str
    source_path: Path
    database_path: Path
    sha256: str
    size_bytes: int
    provenance: SourceProvenance = SourceProvenance()
    csv_import: CsvImportConfig | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.log_id, str) or not _LOG_ID_PATTERN.match(self.log_id):
            raise SourceContractError(
                "log_id must be lowercase alphanumeric with '.', '_', or '-' and at most 64 characters"
            )
        if not isinstance(self.display_name, str) or not self.display_name:
            raise SourceContractError("display_name must be a non-empty string")
        if self.source_format not in SOURCE_FORMATS:
            raise SourceContractError(
                f"source_format must be one of {SOURCE_FORMATS}, got {self.source_format!r}"
            )
        if not isinstance(self.sha256, str) or not _SHA256_PATTERN.match(self.sha256):
            raise SourceContractError("sha256 must be 64 lowercase hexadecimal characters")
        if not isinstance(self.size_bytes, int) or isinstance(self.size_bytes, bool) or self.size_bytes <= 0:
            raise SourceContractError("size_bytes must be a positive integer")
        if self.source_format == SOURCE_FORMAT_CSV and self.csv_import is None:
            raise SourceContractError("csv datasets require an explicit CSV import configuration")
        if self.source_format != SOURCE_FORMAT_CSV and self.csv_import is not None:
            raise SourceContractError(
                f"{self.source_format} datasets must not carry a CSV import configuration"
            )

    @property
    def dataset_id(self) -> str:
        """Stable identity of source bytes plus the normalized import configuration."""

        return dataset_identity(
            source_format=self.source_format,
            sha256=self.sha256,
            size_bytes=self.size_bytes,
            csv_import=self.csv_import,
        )

    @property
    def build_state(self) -> str:
        return "built" if self.database_path.is_file() else "registered"

    def csv_config(self) -> CsvImportConfig:
        if self.csv_import is None:
            raise SourceContractError(
                f"{self.log_id}: source format {self.source_format!r} has no CSV import configuration"
            )
        return self.csv_import

    def as_record(self, root: Path) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "database_path": _portable_path(self.database_path, root),
            "display_name": self.display_name,
            "log_id": self.log_id,
            "provenance": {
                "dataset_url": self.provenance.dataset_url,
                "doi": self.provenance.doi,
                "source_url": self.provenance.source_url,
            },
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "source_format": self.source_format,
            "source_path": _portable_path(self.source_path, root),
            "csv_import": None if self.csv_import is None else self.csv_import.as_record(),
        }

    @staticmethod
    def from_record(record: dict[str, Any], root: Path) -> DatasetDescriptor:
        csv_import = record.get("csv_import")
        provenance = record.get("provenance") or {}
        return DatasetDescriptor(
            log_id=record["log_id"],
            display_name=record["display_name"],
            source_format=record["source_format"],
            source_path=_resolve_path(record["source_path"], root),
            database_path=_resolve_path(record["database_path"], root),
            sha256=record["sha256"],
            size_bytes=record["size_bytes"],
            provenance=SourceProvenance(
                dataset_url=provenance.get("dataset_url"),
                doi=provenance.get("doi"),
                source_url=provenance.get("source_url"),
            ),
            csv_import=(
                None
                if csv_import is None
                else CsvImportConfig(
                    mapping=CsvColumnMapping(**csv_import["mapping"]),
                    timestamp=TimestampConfig(**csv_import["timestamp"]),
                    delimiter=csv_import["delimiter"],
                )
            ),
        )


def _portable_path(path: Path, root: Path) -> str:
    resolved = Path(path).resolve()
    try:
        return resolved.relative_to(Path(root).resolve()).as_posix()
    except ValueError:
        return resolved.as_posix()


def _resolve_path(value: str, root: Path) -> Path:
    candidate = Path(value)
    return candidate if candidate.is_absolute() else Path(root).resolve() / candidate


class DatasetRegistryError(ValueError):
    """Raised when registry state cannot be resolved deterministically."""


class DatasetRegistry:
    """Minimal local registry: log_id -> dataset descriptor -> local DuckDB path."""

    def __init__(self, root: Path = DEFAULT_REGISTRY_ROOT) -> None:
        self._root = Path(root).resolve()

    @property
    def root(self) -> Path:
        return self._root

    @property
    def registry_path(self) -> Path:
        return self._root / REGISTRY_FILENAME

    def default_database_path(self, log_id: str) -> Path:
        return self._root / f"{log_id}.duckdb"

    def _load(self) -> dict[str, Any]:
        if not self.registry_path.is_file():
            return {}
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
            records = payload["datasets"]
        except (json.JSONDecodeError, KeyError, OSError, TypeError, UnicodeError) as exc:
            raise DatasetRegistryError(
                "registry state is not a readable dataset registry document"
            ) from exc
        if payload.get("import_contract_version") != IMPORT_CONTRACT_VERSION:
            raise DatasetRegistryError(
                "registry was written by a different import contract version: "
                f"{payload.get('import_contract_version')!r}"
            )
        if not isinstance(records, dict):
            raise DatasetRegistryError("registry datasets must be a mapping of log_id")
        return records

    def _write(self, records: dict[str, Any]) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        payload = {
            "datasets": records,
            "import_contract_version": IMPORT_CONTRACT_VERSION,
        }
        self.registry_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )

    def register(self, descriptor: DatasetDescriptor) -> DatasetDescriptor:
        """Register idempotently.

        Re-registering an unchanged dataset writes nothing. Re-registering the
        same identity from a new path refreshes the stored resolution metadata
        (paths, display name, provenance) without changing `dataset_id`, so a
        moved or renamed source file rebuilds instead of failing to resolve.
        """

        records = self._load()
        existing = records.get(descriptor.log_id)
        if existing is not None:
            current = DatasetDescriptor.from_record(existing, self._root)
            if current.dataset_id != descriptor.dataset_id:
                raise DatasetRegistryError(
                    f"log_id {descriptor.log_id!r} is already registered with a different "
                    "dataset identity; register the changed source or mapping under a new log_id"
                )
            if current == descriptor:
                return current
        records[descriptor.log_id] = descriptor.as_record(self._root)
        self._write(records)
        return descriptor

    def find(self, log_id: str) -> DatasetDescriptor | None:
        """Resolve a `log_id`, or return `None` when it is not registered."""

        record = self._load().get(log_id)
        if record is None:
            return None
        return DatasetDescriptor.from_record(record, self._root)

    def get(self, log_id: str) -> DatasetDescriptor:
        descriptor = self.find(log_id)
        if descriptor is None:
            raise DatasetRegistryError(f"unknown log_id {log_id!r}")
        return descriptor

    def log_ids(self) -> list[str]:
        return sorted(self._load())


def register_csv(
    registry: DatasetRegistry,
    *,
    log_id: str,
    display_name: str,
    source_path: Path,
    config: CsvImportConfig,
    provenance: SourceProvenance | None = None,
    database_path: Path | None = None,
) -> DatasetDescriptor:
    source_path = Path(source_path).resolve()
    fingerprint = source_fingerprint(source_path, provenance)
    descriptor = DatasetDescriptor(
        log_id=log_id,
        display_name=display_name,
        source_format=SOURCE_FORMAT_CSV,
        source_path=source_path,
        database_path=(
            Path(database_path).resolve() if database_path is not None else registry.default_database_path(log_id)
        ),
        sha256=fingerprint.sha256,
        size_bytes=fingerprint.size_bytes,
        provenance=provenance or SourceProvenance(),
        csv_import=config,
    )
    return registry.register(descriptor)


def register_xes(
    registry: DatasetRegistry,
    *,
    log_id: str,
    display_name: str,
    source_path: Path,
    provenance: SourceProvenance | None = None,
    database_path: Path | None = None,
) -> DatasetDescriptor:
    source_path = Path(source_path).resolve()
    fingerprint = source_fingerprint(source_path, provenance)
    descriptor = DatasetDescriptor(
        log_id=log_id,
        display_name=display_name,
        source_format=SOURCE_FORMAT_XES,
        source_path=source_path,
        database_path=(
            Path(database_path).resolve() if database_path is not None else registry.default_database_path(log_id)
        ),
        sha256=fingerprint.sha256,
        size_bytes=fingerprint.size_bytes,
        provenance=provenance or SourceProvenance(),
    )
    return registry.register(descriptor)
