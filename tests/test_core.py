from __future__ import annotations

import hashlib
import json

import duckdb

from piw.core import (
    complete_traces,
    install_events,
    rework_summary,
    sla_summary,
    transition_summary,
    variant_id,
)
from piw.events import Event


def event(
    case_id: str,
    activity: str,
    timestamp: int,
    position: int,
    lifecycle: str | None = "COMPLETE",
) -> Event:
    return Event(case_id, activity, timestamp, position, lifecycle, None)


def connection_with(events: list[Event]) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(":memory:")
    install_events(connection, events)
    return connection


def test_complete_filter_and_same_timestamp_position_order() -> None:
    connection = connection_with(
        [
            event("case-1", "third", 100, 2, None),
            event("case-1", "ignored-start", 50, 0, "START"),
            event("case-1", "first", 100, 0, "complete"),
            event("case-1", "second", 100, 1, "COMPLETE"),
        ]
    )
    try:
        assert complete_traces(connection) == [
            ("case-1", ["first", "second", "third"])
        ]
        assert transition_summary(connection)["transition_count"] == 2
    finally:
        connection.close()


def test_repeated_source_events_are_preserved_as_rework() -> None:
    connection = connection_with(
        [
            event("case-1", "A", 1, 0),
            event("case-1", "A", 1, 1),
            event("case-1", "A", 2, 2),
            event("case-2", "A", 1, 0),
        ]
    )
    try:
        summary = rework_summary(connection)
        assert summary["aggregate_rework_event_count"] == 2
        assert summary["cases_with_rework"] == 1
        assert summary["activities"] == [
            {"activity": "A", "cases_with_rework": 1, "rework_event_count": 2}
        ]
    finally:
        connection.close()


def test_sla_equality_is_not_a_violation() -> None:
    connection = connection_with(
        [
            event("equal", "A", 0, 0),
            event("equal", "B", 100, 1),
            event("over", "A", 0, 0),
            event("over", "B", 101, 1),
        ]
    )
    try:
        summary = sla_summary(connection, 100)
        assert summary["strict_operator"] == ">"
        assert summary["violation_case_count"] == 1
    finally:
        connection.close()


def test_variant_id_is_canonical_utf8_json_sha256() -> None:
    canonical = '["A","é"]'.encode("utf-8")
    assert variant_id(["A", "é"]) == hashlib.sha256(canonical).hexdigest()
    assert json.loads(canonical) == ["A", "é"]
