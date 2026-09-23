"""Generic CSV source adapter: mapped columns into the canonical event model."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO

from .datasets import (
    CsvColumnMapping,
    CsvImportConfig,
    TimestampConfig,
    resolve_timezone,
    validate_delimiter,
)
from .events import COMPLETE_LIFECYCLE, Event, SourceContractError, utc_epoch_ms


class CsvContractError(SourceContractError):
    """Raised when a CSV source violates the canonical import contract."""


class CsvSource:
    """One-pass, fail-closed reader over a CSV source.

    The header is read and validated (present, named, fixed width) on entry, so
    a caller can inspect it before any data row is consumed, and every data row
    must match the header width. Both the preview and the canonical ingest path
    read a source through this single implementation.
    """

    def __init__(self, path: Path, delimiter: str = ",") -> None:
        self._path = Path(path)
        self._delimiter = validate_delimiter(delimiter)
        self._handle: TextIO | None = None
        self._reader: Any = None
        self._header: list[str] = []

    @property
    def path(self) -> Path:
        return self._path

    @property
    def header(self) -> list[str]:
        return list(self._header)

    def __enter__(self) -> CsvSource:
        try:
            self._handle = self._path.open("r", encoding="utf-8", newline="")
            self._reader = csv.reader(
                self._handle, delimiter=self._delimiter, strict=True
            )
            try:
                self._header = next(self._reader)
            except StopIteration:
                raise CsvContractError(
                    f"{self._path.name}: CSV source has no header row"
                ) from None
            for position, name in enumerate(self._header, 1):
                if not name:
                    raise CsvContractError(
                        f"{self._path.name}: CSV header column {position} has no name"
                    )
        except UnicodeDecodeError as exc:
            self.__exit__(None, None, None)
            raise CsvContractError(
                f"{self._path.name}: CSV source must be UTF-8 encoded: {exc}"
            ) from exc
        except csv.Error as exc:
            self.__exit__(None, None, None)
            raise CsvContractError(f"{self._path.name}: malformed CSV: {exc}") from exc
        except BaseException:
            # A header the contract rejects (no header row, an unnamed column)
            # leaves through `__enter__`, where no `with` body can run
            # `__exit__`: without this the source handle stays open, and on
            # Windows an open handle keeps the staged bytes from being
            # discarded once the caller rejects the source.
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *_: object) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def rows(self) -> Iterator[tuple[str, list[str]]]:
        """Yield `(location, row)` in source order, failing closed on any defect."""

        width = len(self._header)
        try:
            for row in self._reader:
                if len(row) != width:
                    raise CsvContractError(
                        f"{self._path.name} line {self._reader.line_num}: expected "
                        f"{width} columns, found {len(row)}"
                    )
                yield f"line {self._reader.line_num}", row
        except UnicodeDecodeError as exc:
            raise CsvContractError(
                f"{self._path.name}: CSV source must be UTF-8 encoded: {exc}"
            ) from exc
        except csv.Error as exc:
            raise CsvContractError(f"{self._path.name}: malformed CSV: {exc}") from exc


@dataclass(frozen=True, slots=True)
class CsvPreview:
    """Header-level description of one CSV source; no data row is exposed."""

    delimiter: str
    header: list[str]
    duplicate_columns: list[str]
    data_row_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "data_row_count": self.data_row_count,
            "delimiter": self.delimiter,
            "duplicate_columns": list(self.duplicate_columns),
            "header": list(self.header),
        }


def preview_csv(path: Path, delimiter: str = ",") -> CsvPreview:
    """Describe a CSV source for mapping without reading its records.

    The strict parser still runs over the data rows, so a wrong delimiter or a
    ragged row is reported here instead of at build time, but only the exact
    ordered header, the duplicated names, and the data row count are returned.
    """

    source_path = Path(path)
    with CsvSource(source_path, delimiter) as source:
        header = source.header
        duplicates = sorted({name for name in header if header.count(name) > 1})
        data_row_count = sum(1 for _ in source.rows())
    return CsvPreview(
        delimiter=delimiter,
        duplicate_columns=duplicates,
        data_row_count=data_row_count,
        header=header,
    )


def _validate_mapping(
    header: list[str], mapping: CsvColumnMapping, path: Path
) -> None:
    """Fail closed when the declared mapping cannot be resolved unambiguously."""

    fields = (
        ("case_id", mapping.case_id),
        ("activity", mapping.activity),
        ("timestamp", mapping.timestamp),
        ("lifecycle", mapping.lifecycle),
        ("resource", mapping.resource),
    )
    mapped_columns = {column for _, column in fields if column is not None}
    duplicates = sorted(
        {
            name
            for name in header
            if header.count(name) > 1 and name in mapped_columns
        }
    )
    if duplicates:
        raise CsvContractError(
            f"{path.name}: duplicate CSV header column(s) {duplicates} make the mapping ambiguous"
        )
    for canonical, column in fields:
        if column is not None and column not in header:
            raise CsvContractError(
                f"{path.name}: mapped {canonical} source column {column!r} is absent from the CSV header"
            )


def _required_cell(path: Path, location: str, column: str, value: str) -> str:
    if value == "":
        raise CsvContractError(
            f"{path.name} {location}: required source column {column!r} must not be empty"
        )
    return value


def _timestamp_ms(
    path: Path, location: str, value: str, config: TimestampConfig
) -> int:
    text = value.strip()
    if not text:
        raise CsvContractError(f"{path.name} {location}: timestamp value must not be empty")
    try:
        if config.timestamp_format is None:
            moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
        else:
            moment = datetime.strptime(text, config.timestamp_format)
    except ValueError as exc:
        raise CsvContractError(f"{path.name} {location}: invalid timestamp {value!r}") from exc
    if moment.tzinfo is None or moment.utcoffset() is None:
        if config.assume_timezone is None:
            raise CsvContractError(
                f"{path.name} {location}: timestamp {value!r} carries no UTC offset and no "
                "explicit timezone interpretation is configured"
            )
        moment = moment.replace(tzinfo=resolve_timezone(config.assume_timezone))
    return utc_epoch_ms(moment)


def iter_csv_events(path: Path, config: CsvImportConfig) -> Iterator[Event]:
    """Yield canonical events in source order while validating the mapping contract."""

    path = Path(path)
    mapping = config.mapping
    positions: dict[str, int] = {}
    row_count = 0
    completion_seen = False
    with CsvSource(path, config.delimiter) as source:
        _validate_mapping(source.header, mapping, path)
        index = {name: position for position, name in enumerate(source.header)}
        for location, row in source.rows():
            case_id = _required_cell(
                path, location, mapping.case_id, row[index[mapping.case_id]]
            )
            activity = _required_cell(
                path, location, mapping.activity, row[index[mapping.activity]]
            )
            event_ts_utc_ms = _timestamp_ms(
                path, location, row[index[mapping.timestamp]], config.timestamp
            )
            lifecycle = None
            if mapping.lifecycle is not None:
                source_lifecycle = row[index[mapping.lifecycle]]
                if source_lifecycle != "":
                    lifecycle = source_lifecycle
                    if source_lifecycle.upper() == COMPLETE_LIFECYCLE:
                        completion_seen = True
            resource = (
                None
                if mapping.resource is None
                else row[index[mapping.resource]] or None
            )
            event_pos = positions.get(case_id, 0)
            positions[case_id] = event_pos + 1
            row_count += 1
            yield Event(
                case_id=case_id,
                activity=activity,
                event_ts_utc_ms=event_ts_utc_ms,
                event_pos=event_pos,
                lifecycle=lifecycle,
                resource=resource,
            )
    if row_count == 0:
        raise CsvContractError(f"{path.name}: CSV source contains no data rows")
    if mapping.lifecycle is not None and not completion_seen:
        raise CsvContractError(
            f"{path.name}: mapped lifecycle source column {mapping.lifecycle!r} carries no value "
            f"that the canonical completion rule (UPPER(lifecycle) = '{COMPLETE_LIFECYCLE}') "
            "includes, so the control-flow perspective would be empty; map a column that marks "
            "completion or leave lifecycle unmapped"
        )


def csv_trace_count(path: Path, config: CsvImportConfig) -> int:
    """Count distinct source cases without interpreting events."""

    path = Path(path)
    mapping = config.mapping
    case_ids: set[str] = set()
    with CsvSource(path, config.delimiter) as source:
        _validate_mapping(source.header, mapping, path)
        index = {name: position for position, name in enumerate(source.header)}
        for location, row in source.rows():
            case_ids.add(
                _required_cell(path, location, mapping.case_id, row[index[mapping.case_id]])
            )
    return len(case_ids)
