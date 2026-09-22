from __future__ import annotations

import gzip
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, Iterator
from xml.etree import ElementTree


@dataclass(frozen=True, slots=True)
class Event:
    case_id: str
    activity: str
    event_ts_utc_ms: int
    event_pos: int
    lifecycle: str | None
    resource: str | None


class XesContractError(ValueError):
    """Raised when a required XES value violates the canonical contract."""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _direct_values(element: ElementTree.Element, key: str) -> list[str]:
    return [
        child.attrib["value"]
        for child in element
        if child.attrib.get("key") == key and "value" in child.attrib
    ]


def _required_value(element: ElementTree.Element, key: str, location: str) -> str:
    values = _direct_values(element, key)
    if len(values) != 1 or not values[0]:
        raise XesContractError(
            f"{location}: required {key!r} must occur exactly once and be non-empty"
        )
    return values[0]


def _optional_value(element: ElementTree.Element, key: str, location: str) -> str | None:
    values = _direct_values(element, key)
    if len(values) > 1:
        raise XesContractError(f"{location}: optional {key!r} occurs more than once")
    return values[0] if values else None


def _utc_epoch_ms(value: str, location: str) -> int:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise XesContractError(f"{location}: invalid time:timestamp {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise XesContractError(f"{location}: timestamp must include a UTC offset")
    delta = parsed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000 + delta.microseconds // 1_000


def _open_xes(path: Path) -> BinaryIO:
    return gzip.open(path, "rb") if path.suffix.lower() == ".gz" else path.open("rb")


def iter_xes_events(path: Path) -> Iterator[Event]:
    """Yield source events in trace order while validating the required contract."""

    seen_case_ids: set[str] = set()
    trace_number = 0
    with _open_xes(path) as source:
        for _, element in ElementTree.iterparse(source, events=("end",)):
            if _local_name(element.tag) != "trace":
                continue
            trace_number += 1
            trace_location = f"trace {trace_number}"
            case_id = _required_value(element, "concept:name", trace_location)
            if case_id in seen_case_ids:
                raise XesContractError(f"{trace_location}: duplicate case_id {case_id!r}")
            seen_case_ids.add(case_id)

            event_pos = 0
            for child in element:
                if _local_name(child.tag) != "event":
                    continue
                location = f"case {case_id!r} event_pos {event_pos}"
                activity = _required_value(child, "concept:name", location)
                timestamp = _required_value(child, "time:timestamp", location)
                lifecycle = _optional_value(child, "lifecycle:transition", location)
                resource = _optional_value(child, "org:resource", location)
                yield Event(
                    case_id=case_id,
                    activity=activity,
                    event_ts_utc_ms=_utc_epoch_ms(timestamp, location),
                    event_pos=event_pos,
                    lifecycle=lifecycle,
                    resource=resource,
                )
                event_pos += 1
            element.clear()


def source_trace_count(path: Path) -> int:
    """Count traces without interpreting events; useful for fail-closed validation."""

    traces = 0
    with _open_xes(path) as source:
        for _, element in ElementTree.iterparse(source, events=("end",)):
            if _local_name(element.tag) == "trace":
                traces += 1
                element.clear()
    return traces
