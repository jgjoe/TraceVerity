"""The five-tool boundary over the built-in baseline and registered datasets."""

from __future__ import annotations

import ast
import json
from pathlib import Path

import duckdb
import pytest

from piw.core import build_dataset, install_events
from piw.core_surface import TOOL_SCHEMA_VERSION
from piw.datasets import (
    CsvColumnMapping,
    CsvImportConfig,
    DatasetRegistry,
    SourceFingerprint,
    TimestampConfig,
    register_csv,
)
from piw.events import Event
from piw.tools import CoreToolSurface, canonical_bytes, canonical_json

HEADER = ("Case ID", "Activity", "Complete Timestamp", "Resource")
ROWS = [
    ("Case 1", "Assign seriousness", "2012/10/09 14:50:17.000", "Value 1"),
    ("Case 1", "Take in charge ticket", "2012/10/09 14:50:17.500", "Value 1"),
    ("Case 1", "Closed", "2012/10/10 08:00:00.000", "Value 3"),
    ("Case 2", "Assign seriousness", "2012/10/09 15:00:00.000", "Value 2"),
    ("Case 2", "Resolve ticket", "2012/10/09 16:00:00.000", "Value 2"),
]
CONFIG = CsvImportConfig(
    mapping=CsvColumnMapping(
        case_id="Case ID",
        activity="Activity",
        timestamp="Complete Timestamp",
        resource="Resource",
    ),
    timestamp=TimestampConfig(
        assume_timezone="UTC", timestamp_format="%Y/%m/%d %H:%M:%S.%f"
    ),
)
# The same five events as `ROWS`, so both datasets must yield identical facts.
CANONICAL_EVENTS = [
    Event("Case 1", "Assign seriousness", 1_349_794_217_000, 0, None, "Value 1"),
    Event("Case 1", "Take in charge ticket", 1_349_794_217_500, 1, None, "Value 1"),
    Event("Case 1", "Closed", 1_349_856_000_000, 2, None, "Value 3"),
    Event("Case 2", "Assign seriousness", 1_349_797_200_000, 0, None, "Value 2"),
    Event("Case 2", "Resolve ticket", 1_349_800_800_000, 1, None, "Value 2"),
]
EXPECTED_FACTS = {
    "aggregate_rework_event_count": 0,
    "analysis_event_count": 5,
    "case_count": 2,
    "cases_with_rework": 0,
    "direct_follow_count": 3,
    "distinct_activity_count": 4,
    "raw_event_count": 5,
    "variant_count": 2,
}


def write_source(tmp_path: Path, name: str = "tickets.csv") -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / name
    lines = [",".join(HEADER), *[",".join(row) for row in ROWS]]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return path


def write_other_source(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "other.csv"
    lines = [
        ",".join(HEADER),
        "Case 9,Register,2012/10/09 09:00:00.000,Value 9",
        "Case 9,Approve,2012/10/09 10:00:00.000,Value 9",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return path


def canonical_surface(tmp_path: Path, registry_root: Path) -> CoreToolSurface:
    database = tmp_path / "canonical.duckdb"
    connection = duckdb.connect(str(database))
    try:
        install_events(
            connection,
            CANONICAL_EVENTS,
            SourceFingerprint(filename="fixture.xes", sha256="a" * 64, size_bytes=1),
        )
    finally:
        connection.close()
    return CoreToolSurface(database, registry_root)


def registered_surface(
    tmp_path: Path,
    *,
    log_id: str = "tickets",
    built: bool = True,
    source: Path | None = None,
) -> CoreToolSurface:
    """Register (and optionally build) one CSV dataset into an isolated registry."""

    root = tmp_path / "registry"
    descriptor = register_csv(
        DatasetRegistry(root),
        log_id=log_id,
        display_name="Ticket log",
        source_path=source if source is not None else write_source(tmp_path),
        config=CONFIG,
    )
    if built:
        assert build_dataset(descriptor)["validation"]["status"] == "PASS"
    return CoreToolSurface(tmp_path / "canonical.duckdb", root)


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
    assert response["query_id"].startswith("q_")
    assert all(fact["fact_id"].startswith("f_") for fact in response["facts"])


def test_tool_contract_publishes_five_tools_with_a_runtime_log_id() -> None:
    definitions = CoreToolSurface.definitions()
    assert TOOL_SCHEMA_VERSION == "slice-d-tool-v2"
    assert [definition["name"] for definition in definitions] == [
        "describe_log",
        "list_variants",
        "list_transitions",
        "list_activities",
        "get_case_trace",
    ]
    for definition in definitions:
        parameters = definition["parameters"]
        assert parameters["properties"]["log_id"] == {"type": "string", "minLength": 1}
        assert "log_id" in parameters["required"]
        assert parameters["additionalProperties"] is False


def test_built_in_dataset_serves_all_five_tools_deterministically(tmp_path: Path) -> None:
    tools = canonical_surface(tmp_path, tmp_path / "registry")
    calls = [
        ("describe_log", {"log_id": "bpic2012"}),
        ("list_variants", {"log_id": "bpic2012", "order_by": "case_count_desc", "limit": 2}),
        (
            "list_transitions",
            {"log_id": "bpic2012", "order_by": "transition_count_desc", "limit": 2},
        ),
        ("list_activities", {"log_id": "bpic2012", "order_by": "event_count_desc", "limit": 2}),
        (
            "get_case_trace",
            {"log_id": "bpic2012", "case_id": "Case 1", "perspective": "complete"},
        ),
    ]
    for tool, arguments in calls:
        first = tools.dispatch(tool, arguments)
        second = tools.dispatch(tool, arguments)
        assert_envelope(first)
        assert "error" not in first
        assert canonical_bytes(first) == canonical_bytes(second)
    assert tools.describe_log("bpic2012")["result"] == {
        **EXPECTED_FACTS,
        "cycle_time_max_ms": 61_783_000,
        "cycle_time_p50_ms": 3_600_000,
        "cycle_time_p90_ms": 61_783_000,
        "observed_gap_p90_ms": 61_782_500,
    }


def test_registered_dataset_serves_the_same_five_tools(tmp_path: Path) -> None:
    registered = registered_surface(tmp_path)
    canonical = canonical_surface(tmp_path, tmp_path / "registry")

    summary = registered.describe_log("tickets")
    assert "error" not in summary
    assert summary["parameters"]["log_id"] == "tickets"
    assert summary["log_fingerprint"] != canonical.describe_log("bpic2012")["log_fingerprint"]
    # Identical events, identical facts: only the dataset identifier differs.
    assert summary["result"] == canonical.describe_log("bpic2012")["result"]
    assert {name: summary["result"][name] for name in EXPECTED_FACTS} == EXPECTED_FACTS

    variants = registered.list_variants("tickets", "case_count_desc", 2)
    assert [item["case_count"] for item in variants["result"]["items"]] == [1, 1]
    transitions = registered.list_transitions("tickets", "transition_count_desc", 2)
    assert transitions["result"]["items"][0]["transition_count"] == 1
    activities = registered.list_activities("tickets", "event_count_desc", 2)
    assert activities["result"]["items"][0]["activity"] == "Assign seriousness"
    trace = registered.get_case_trace("tickets", "Case 1", "raw")
    assert trace["result"]["activities"] == [
        "Assign seriousness",
        "Take in charge ticket",
        "Closed",
    ]
    complete = registered.get_case_trace("tickets", "Case 1", "complete")
    assert complete["result"]["activities"] == trace["result"]["activities"]
    assert complete["result"]["perspective"] == "complete"


def test_unknown_log_id_fails_closed(tmp_path: Path) -> None:
    tools = registered_surface(tmp_path)
    response = tools.dispatch("describe_log", {"log_id": "not-registered"})
    assert response["error"] == {
        "code": "UNKNOWN_LOG",
        "details": {"log_id": "not-registered"},
        "message": "log_id is not a known local dataset",
    }


def test_missing_and_corrupt_databases_fail_closed(tmp_path: Path) -> None:
    unbuilt = registered_surface(tmp_path / "unbuilt", built=False)
    response = unbuilt.dispatch("describe_log", {"log_id": "tickets"})
    assert response["error"]["code"] == "DATASET_UNAVAILABLE"
    assert response["error"]["details"] == {"log_id": "tickets", "status": "registered"}

    corrupt_root = tmp_path / "corrupt"
    corrupt_root.mkdir()
    corrupt = registered_surface(corrupt_root, built=False)
    DatasetRegistry(corrupt_root / "registry").default_database_path("tickets").write_bytes(
        b"not a database"
    )
    response = corrupt.dispatch("describe_log", {"log_id": "tickets"})
    assert response["error"]["code"] == "DATASET_UNAVAILABLE"
    assert response["error"]["details"] == {"log_id": "tickets", "status": "unavailable"}


def test_inconsistent_registered_database_fails_closed(tmp_path: Path) -> None:
    """A database that no longer carries the registered fingerprint is never served."""

    root = tmp_path / "registry"
    registry = DatasetRegistry(root)
    register_csv(
        registry,
        log_id="other",
        display_name="Other log",
        source_path=write_other_source(tmp_path),
        config=CONFIG,
    )
    built = registered_surface(tmp_path)
    assert build_dataset(registry.get("other"))["validation"]["status"] == "PASS"

    registry.default_database_path("tickets").write_bytes(
        registry.default_database_path("other").read_bytes()
    )
    response = built.dispatch("describe_log", {"log_id": "tickets"})
    assert response["error"]["code"] == "DATASET_UNAVAILABLE"
    assert response["error"]["details"] == {"log_id": "tickets", "status": "unavailable"}


def test_reserved_builtin_shadow_and_unreadable_registry_fail_closed(tmp_path: Path) -> None:
    """The built-in identifier is never ambiguous, and broken registry state is fatal."""

    root = tmp_path / "registry"
    register_csv(
        DatasetRegistry(root),
        log_id="bpic2012",
        display_name="Shadow baseline",
        source_path=write_source(tmp_path),
        config=CONFIG,
    )
    shadowed = CoreToolSurface(tmp_path / "canonical.duckdb", root)
    for log_id in ("bpic2012", "tickets"):
        response = shadowed.dispatch("describe_log", {"log_id": log_id})
        assert response["error"]["code"] == "DATA_CONTRACT_ERROR"
        assert response["error"]["details"] == {"log_id": log_id}

    broken = tmp_path / "broken"
    broken.mkdir()
    (broken / "registry.json").write_text("{not json", encoding="utf-8")
    surface = CoreToolSurface(tmp_path / "canonical.duckdb", broken)
    response = surface.dispatch("describe_log", {"log_id": "tickets"})
    assert response["error"]["code"] == "DATA_CONTRACT_ERROR"


def test_describe_log_applies_no_threshold_unless_one_is_requested(tmp_path: Path) -> None:
    tools = registered_surface(tmp_path)
    facts_without_threshold = set(tools.describe_log("tickets")["result"])
    assert not [name for name in facts_without_threshold if name.startswith("sla_")]
    assert tools.describe_log("tickets")["parameters"]["sla_threshold_ms"] is None

    # The strict rule is `cycle_time_ms > threshold_ms`; equality is not a violation.
    for threshold, violations, share in (
        (0, 2, 1.0),
        (3_600_000, 1, 0.5),
        (604_800_000, 0, 0.0),
    ):
        result = tools.describe_log("tickets", threshold)["result"]
        assert result["sla_threshold_ms"] == threshold
        assert result["sla_violation_case_count"] == violations
        assert result["sla_violation_case_share"] == share

    assert canonical_bytes(tools.describe_log("tickets", 1000)) == canonical_bytes(
        tools.describe_log("tickets", 1000)
    )
    assert tools.dispatch("describe_log", {"log_id": "tickets", "sla_threshold_ms": -1})[
        "error"
    ]["code"] == "INVALID_ARGUMENT"


def test_tool_dispatch_fails_closed(tmp_path: Path) -> None:
    tools = registered_surface(tmp_path)
    assert tools.dispatch("run_sql", {"sql": "SELECT 1"})["error"]["code"] == (
        "FORBIDDEN_TOOL"
    )
    assert tools.dispatch(
        "describe_log", {"log_id": "tickets", "path": "secret"}
    )["error"]["code"] == "UNSUPPORTED_PARAMETER"
    assert tools.dispatch(
        "list_variants",
        {"log_id": "tickets", "order_by": "case_count_desc", "limit": 101},
    )["error"]["code"] == "INVALID_ARGUMENT"
    assert tools.dispatch(
        "get_case_trace",
        {"log_id": "tickets", "case_id": "missing", "perspective": "complete"},
    )["error"]["code"] == "NOT_FOUND"
    assert tools.dispatch("describe_log", {"log_id": ""})["error"]["code"] == (
        "INVALID_ARGUMENT"
    )


def test_error_envelopes_never_expose_machine_paths(tmp_path: Path) -> None:
    tools = registered_surface(tmp_path)
    calls = [
        ("describe_log", {"log_id": "not-registered"}),
        ("describe_log", {"log_id": "not-registered", "sla_threshold_ms": -1}),
        ("run_sql", {"sql": "SELECT 1"}),
        (
            "get_case_trace",
            {"log_id": "not-registered", "case_id": "missing", "perspective": "raw"},
        ),
    ]
    for tool, arguments in calls:
        response = tools.dispatch(tool, arguments)
        assert "error" in response
        text = canonical_json(response)
        assert str(tmp_path) not in text
        assert str(tmp_path / "registry" / "tickets.duckdb") not in text
        assert json.dumps(response["error"]).count("\\\\") == 0
        assert "\\" not in json.dumps(response["error"])
        assert "/" not in json.dumps(response["error"])


def test_tool_and_mcp_layers_contain_no_metric_implementation() -> None:
    """Every read goes through `CoreReadSurface`; the boundary only validates and routes."""

    for module in ("src/piw/tools.py", "src/piw/mcp_server.py", "src/piw/mcp_runtime.py"):
        tree = ast.parse(Path(module).read_text(encoding="utf-8"), module)
        imported = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        }
        assert ".core" not in imported, module
        assert "piw.core" not in imported, module
        assert ".agent" not in imported, module
    assert "core_surface" in Path("src/piw/tools.py").read_text(encoding="utf-8")
