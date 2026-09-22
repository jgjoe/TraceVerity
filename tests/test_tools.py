from __future__ import annotations

from pathlib import Path

import duckdb

from piw.core import install_events
from piw.tools import CoreToolSurface, canonical_bytes
from piw.xes import Event


def event(case_id: str, activity: str, timestamp: int, position: int) -> Event:
    return Event(case_id, activity, timestamp, position, "COMPLETE", None)


def surface(tmp_path: Path) -> CoreToolSurface:
    database = tmp_path / "fixture.duckdb"
    connection = duckdb.connect(str(database))
    try:
        install_events(
            connection,
            [
                event("case-1", "A", 0, 0),
                event("case-1", "A", 10, 1),
                event("case-1", "B", 20, 2),
                event("case-2", "A", 0, 0),
                event("case-2", "C", 30, 1),
            ],
            {
                "doi": "fixture-doi",
                "filename": "fixture.xes",
                "sha256": "a" * 64,
                "size_bytes": 1,
                "source_url": "https://example.invalid/fixture",
            },
        )
    finally:
        connection.close()
    return CoreToolSurface(database)


def assert_envelope(response: dict) -> None:
    assert set(
        [
            "facts",
            "log_fingerprint",
            "metric_definition_version",
            "parameters",
            "query_id",
            "result",
            "schema_version",
        ]
    ).issubset(response)
    assert response["log_fingerprint"] == "a" * 64
    assert response["query_id"].startswith("q_")
    assert all(fact["fact_id"].startswith("f_") for fact in response["facts"])


def test_all_five_tools_are_deterministic_and_grounded(tmp_path: Path) -> None:
    tools = surface(tmp_path)
    calls = [
        ("describe_log", {"log_id": "bpic2012", "sla_threshold_ms": 20}),
        (
            "list_variants",
            {"log_id": "bpic2012", "order_by": "case_count_desc", "limit": 2},
        ),
        (
            "list_transitions",
            {
                "log_id": "bpic2012",
                "order_by": "transition_count_desc",
                "limit": 2,
            },
        ),
        (
            "list_activities",
            {"log_id": "bpic2012", "order_by": "event_count_desc", "limit": 2},
        ),
        (
            "get_case_trace",
            {"log_id": "bpic2012", "case_id": "case-1", "perspective": "complete"},
        ),
    ]
    for name, arguments in calls:
        first = tools.dispatch(name, arguments)
        second = tools.dispatch(name, arguments)
        assert_envelope(first)
        assert canonical_bytes(first) == canonical_bytes(second)

    summary = tools.describe_log("bpic2012", 20)
    assert summary["result"]["case_count"] == 2
    assert summary["result"]["sla_violation_case_count"] == 1
    assert tools.list_activities("bpic2012", "event_count_desc", 1)["result"][
        "items"
    ][0] == {
        "activity": "A",
        "case_count": 2,
        "event_count": 3,
        "rework_event_count": 1,
    }
    assert tools.get_case_trace("bpic2012", "case-1", "complete")["result"][
        "activities"
    ] == ["A", "A", "B"]


def test_tool_dispatch_fails_closed(tmp_path: Path) -> None:
    tools = surface(tmp_path)
    assert tools.dispatch("run_sql", {"sql": "SELECT 1"})["error"]["code"] == (
        "FORBIDDEN_TOOL"
    )
    assert tools.dispatch(
        "describe_log", {"log_id": "bpic2012", "path": "secret"}
    )["error"]["code"] == "UNSUPPORTED_PARAMETER"
    assert tools.dispatch(
        "list_variants",
        {"log_id": "bpic2012", "order_by": "case_count_desc", "limit": 101},
    )["error"]["code"] == "INVALID_ARGUMENT"
    assert tools.dispatch(
        "get_case_trace",
        {"log_id": "bpic2012", "case_id": "missing", "perspective": "complete"},
    )["error"]["code"] == "NOT_FOUND"
