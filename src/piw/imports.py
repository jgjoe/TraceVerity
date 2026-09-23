"""Local content-addressed import area for browser-selected event-log bytes.

A browser posts the selected file's bytes to the localhost server, which
streams them into a gitignored local workspace. Localhost, local filesystem
only: nothing is uploaded, and no external service is contacted.

Identity is deterministic: the stored import is named after the SHA-256 of its
content plus the source filename, so re-importing the same file is idempotent,
no random identifier is created, and no machine path participates.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from .datasets import (
    IMPORT_CONTRACT_VERSION,
    SOURCE_FORMAT_CSV,
    SOURCE_FORMAT_XES,
    canonical_json,
)

IMPORT_ID_PREFIX = "imp_"
MANIFEST_FILENAME = "import.json"
MAX_IMPORT_BYTES = 1024 * 1024 * 1024
MAX_FILENAME_LENGTH = 200
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_UPLOAD_ROOT = PROJECT_ROOT / "data" / "datasets" / "imports"
_IMPORT_ID_PATTERN = re.compile(rf"^{IMPORT_ID_PREFIX}[0-9a-f]{{64}}$")
_EXTENSIONS = {
    ".csv": SOURCE_FORMAT_CSV,
    ".xes": SOURCE_FORMAT_XES,
    ".xes.gz": SOURCE_FORMAT_XES,
}
SOURCE_EXTENSIONS = (".csv", ".xes", ".xes.gz")
_WINDOWS_INVALID_CHARACTERS = '<>:"|?*'
_WINDOWS_RESERVED_NAMES = frozenset(
    {"aux", "con", "nul", "prn"}
    | {f"com{index}" for index in range(1, 10)}
    | {f"lpt{index}" for index in range(1, 10)}
)


class ImportContractError(ValueError):
    """Raised when a browser import violates the local import contract."""

    def __init__(
        self, code: str, message: str, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


def safe_filename(value: Any) -> str:
    """Return a plain basename or fail closed on traversal and control characters.

    The browser supplies this name. It is never used as a filesystem path
    component, but it is rejected loudly rather than stored, so an unsafe name
    cannot reach the local workspace at all. The Windows rules apply on every
    platform, because the local workspace is portable and the name participates
    in the deterministic import identity.
    """

    if not isinstance(value, str) or not value:
        raise ImportContractError("INVALID_FILENAME", "a source filename is required")
    if value != value.strip():
        raise ImportContractError(
            "INVALID_FILENAME", "the source filename must not carry surrounding whitespace"
        )
    if len(value) > MAX_FILENAME_LENGTH:
        raise ImportContractError(
            "INVALID_FILENAME",
            f"the source filename must be at most {MAX_FILENAME_LENGTH} characters",
        )
    if value in {".", ".."} or "/" in value or "\\" in value:
        raise ImportContractError(
            "UNSAFE_FILENAME", "the source filename must be a plain file name, not a path"
        )
    if any(character < " " or character == "\x7f" for character in value):
        raise ImportContractError(
            "UNSAFE_FILENAME", "the source filename must not contain control characters"
        )
    invalid = sorted(
        character for character in _WINDOWS_INVALID_CHARACTERS if character in value
    )
    if invalid:
        raise ImportContractError(
            "UNSAFE_FILENAME",
            f"the source filename must not contain the path character(s) {invalid}",
            {"characters": invalid},
        )
    if value.endswith("."):
        raise ImportContractError(
            "UNSAFE_FILENAME",
            "the source filename must not end with '.'; a trailing dot is not a stable name",
        )
    stem = value.split(".", 1)[0].lower()
    if stem in _WINDOWS_RESERVED_NAMES:
        raise ImportContractError(
            "UNSAFE_FILENAME",
            f"{value!r} is a reserved device name and cannot be used as a source file name",
            {"filename": value},
        )
    return value


def source_extension(filename: str) -> str:
    """Return the supported source extension of a filename, or fail closed."""

    lowered = filename.lower()
    for extension in SOURCE_EXTENSIONS:
        if lowered.endswith(extension):
            return extension
    raise ImportContractError(
        "UNSUPPORTED_SOURCE_FORMAT",
        f"unsupported source file; supported formats are CSV, XES, and XES.GZ ({', '.join(SOURCE_EXTENSIONS)})",
        {"filename": filename, "supported_extensions": list(SOURCE_EXTENSIONS)},
    )


def _bucket(import_id: str) -> str:
    return import_id[len(IMPORT_ID_PREFIX) : len(IMPORT_ID_PREFIX) + 2]


def _import_id(filename: str, sha256: str, size_bytes: int) -> str:
    digest = hashlib.sha256(
        canonical_json(
            {"filename": filename, "sha256": sha256, "size_bytes": size_bytes}
        ).encode("utf-8")
    ).hexdigest()
    return f"{IMPORT_ID_PREFIX}{digest}"


def _write_manifest(path: Path, record: dict[str, Any]) -> None:
    staging = path.with_name(f"{path.name}.partial")
    staging.write_text(
        json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    staging.replace(path)


@dataclass(frozen=True, slots=True)
class StoredImport:
    """One local import: source bytes plus the deterministic reference to them."""

    import_id: str
    filename: str
    source_format: str
    extension: str
    sha256: str
    size_bytes: int
    path: Path

    def as_summary(self) -> dict[str, Any]:
        """Browser-facing metadata; never a machine path."""

        return {
            "filename": self.filename,
            "import_id": self.import_id,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "source_format": self.source_format,
        }


class ImportSink:
    """Streaming writer for one browser import.

    Bytes are hashed while they are written, so the finished file is named
    after its content and the same file never lands twice. While the upload is
    unfinished the bytes live under their own source name inside a `*.partial`
    staging directory, so a parser can validate exactly the bytes that would be
    promoted — and a source the validation rejects is discarded from staging
    without ever replacing the promoted import of the same content. Promotion
    itself is one rename, so an aborted, rejected, or unvalidated upload never
    appears as an import.
    """

    def __init__(self, upload_root: Path, filename: str) -> None:
        self._root = Path(upload_root).resolve()
        self._filename = safe_filename(filename)
        self._extension = source_extension(self._filename)
        self._source_format = _EXTENSIONS[self._extension]
        self._root.mkdir(parents=True, exist_ok=True)
        self._staging_directory = self._root / ".staging" / (
            "upload-"
            + hashlib.sha256(self._filename.encode("utf-8")).hexdigest()[:32]
            + ".partial"
        )
        self._staging_directory.mkdir(parents=True, exist_ok=True)
        self._staging = self._staging_directory / self._filename
        self._handle: BinaryIO | None = self._staging.open("wb")
        self._digest = hashlib.sha256()
        self._size = 0

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def source_format(self) -> str:
        return self._source_format

    def seal(self) -> Path:
        """Flush the staged bytes and return them for pre-promotion validation.

        The bytes a parser reads here are exactly the bytes that promotion would
        name, under the source file name, so a diagnostic names the source and
        never the staging area. An empty upload fails closed exactly as it does
        at promotion time, because no parser can read a header from it.
        """

        self._close()
        if self._size == 0:
            self.discard()
            raise ImportContractError(
                "EMPTY_IMPORT", "the selected file is empty and carries no event log"
            )
        return self._staging

    def write(self, chunk: bytes) -> None:
        if not chunk:
            return
        if self._handle is None:
            raise ImportContractError(
                "INVALID_IMPORT", "the import was already finished or discarded"
            )
        self._size += len(chunk)
        if self._size > MAX_IMPORT_BYTES:
            raise ImportContractError(
                "IMPORT_TOO_LARGE",
                f"the selected file exceeds the {MAX_IMPORT_BYTES} byte local import limit",
            )
        self._digest.update(chunk)
        self._handle.write(chunk)

    def finish(self) -> StoredImport:
        self._close()
        if self._size == 0:
            self.discard()
            raise ImportContractError(
                "EMPTY_IMPORT", "the selected file is empty and carries no event log"
            )
        sha256 = self._digest.hexdigest()
        import_id = _import_id(self._filename, sha256, self._size)
        directory = self._root / _bucket(import_id) / import_id
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / self._filename
        self._staging.replace(path)
        _write_manifest(
            directory / MANIFEST_FILENAME,
            {
                "extension": self._extension,
                "filename": self._filename,
                "import_contract_version": IMPORT_CONTRACT_VERSION,
                "import_id": import_id,
                "sha256": sha256,
                "size_bytes": self._size,
                "source_format": self._source_format,
            },
        )
        self._remove_staging_directory()
        return StoredImport(
            extension=self._extension,
            filename=self._filename,
            import_id=import_id,
            path=path,
            sha256=sha256,
            size_bytes=self._size,
            source_format=self._source_format,
        )

    def discard(self) -> None:
        """Remove an unfinished import; safe to call more than once.

        Only the staging area of this sink is removed: a promoted import is
        never named here, so discarding a rejected upload cannot delete the
        stored bytes of an import the local area already holds.
        """

        self._close()
        self._staging.unlink(missing_ok=True)
        self._remove_staging_directory()

    def _remove_staging_directory(self) -> None:
        """Remove the staging directory once no staged bytes remain in it."""

        if self._staging_directory.is_dir() and not any(
            self._staging_directory.iterdir()
        ):
            self._staging_directory.rmdir()

    def _close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def resolve_import(upload_root: Path, import_id: Any) -> StoredImport:
    """Resolve a deterministic import reference to its local bytes."""

    if not isinstance(import_id, str) or not _IMPORT_ID_PATTERN.match(import_id):
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            "import_id must be the reference returned by the preview step",
        )
    directory = Path(upload_root).resolve() / _bucket(import_id) / import_id
    manifest_path = directory / MANIFEST_FILENAME
    if not manifest_path.is_file():
        raise ImportContractError(
            "UNKNOWN_IMPORT",
            "the referenced import is not in the local import area",
            {"import_id": import_id},
        )
    record = json.loads(manifest_path.read_text(encoding="utf-8"))
    if record.get("import_contract_version") != IMPORT_CONTRACT_VERSION:
        raise ImportContractError(
            "UNKNOWN_IMPORT",
            "the referenced import was written by a different import contract version",
            {"import_id": import_id},
        )
    filename = safe_filename(record.get("filename"))
    extension = source_extension(filename)
    sha256 = str(record.get("sha256"))
    size_bytes = int(record.get("size_bytes", 0))
    if _import_id(filename, sha256, size_bytes) != import_id:
        raise ImportContractError(
            "UNKNOWN_IMPORT",
            "the referenced import does not match its deterministic identity",
            {"import_id": import_id},
        )
    path = directory / filename
    if not path.is_file() or path.stat().st_size != size_bytes:
        raise ImportContractError(
            "UNKNOWN_IMPORT",
            "the stored bytes of the referenced import are missing or changed",
            {"import_id": import_id},
        )
    return StoredImport(
        extension=extension,
        filename=filename,
        import_id=import_id,
        path=path,
        sha256=sha256,
        size_bytes=size_bytes,
        source_format=_EXTENSIONS[extension],
    )
