"""MCP stdio transport over the generalized dataset surface."""

from __future__ import annotations

from pathlib import Path

import duckdb
import mcp.types as types
import pytest

from piw.agent import GroundedAgent
from piw.core import build_dataset, install_events
from piw.datasets import (
    CsvColumnMapping,
    CsvImportConfig,
    DatasetRegistry,
    SourceFingerprint,
    TimestampConfig,
    register_csv,
)
from piw.events import Event
from piw import mcp_server
from piw.mcp_runtime import McpProtocolError, McpToolRuntime, _structured_envelope
from piw.tools import CoreToolSurface, canonical_bytes, canonical_json

CSV_HEADER = "Case ID,Activity,Complete Timestamp"
CSV_ROWS = [
    "T-1,Register,2012/10/09 14:50:17.000",
    "T-1,Approve,2012/10/09 15:50:17.000",
    "T-2,Register,2012/10/09 16:50:17.000",
]
CSV_CONFIG = CsvImportConfig(
    mapping=CsvColumnMapping(
        case_id="Case ID", activity="Activity", timestamp="Complete Timestamp"
    ),
    timestamp=TimestampConfig(
        assume_timezone="UTC", timestamp_format="%Y/%m/%d %H:%M:%S.%f"
    ),
)


def _database(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    try:
        install_events(
            connection,
            [
                Event("case-1", "A", 0, 0, "COMPLETE", None),
                Event("case-1", "B", 10, 1, "COMPLETE", None),
                Event("case-2", "A", 0, 0, "COMPLETE", None),
            ],
            SourceFingerprint(filename="fixture.xes", sha256="c" * 64, size_bytes=1),
        )
    finally:
        connection.close()
    return path


def _registry(root: Path) -> Path:
    """One built registered dataset plus one registered-but-unbuilt probe."""

    root.mkdir(parents=True, exist_ok=True)
    source = root / "tickets.csv"
    source.write_text(
        "\n".join([CSV_HEADER, *CSV_ROWS]) + "\n", encoding="utf-8", newline="\n"
    )
    registry = DatasetRegistry(root)
    built = register_csv(
        registry,
        log_id="tickets",
        display_name="Ticket log",
        source_path=source,
        config=CSV_CONFIG,
    )
    assert build_dataset(built)["validation"]["status"] == "PASS"
    register_csv(
        registry,
        log_id="unbuilt",
        display_name="Unbuilt log",
        source_path=source,
        config=CSV_CONFIG,
    )
    return root


@pytest.fixture(scope="module")
def runtimes(tmp_path_factory: pytest.TempPathFactory):
    workspace = tmp_path_factory.mktemp("mcp")
    database = _database(workspace / "fixture.duckdb")
    registry_root = _registry(workspace / "registry")
    direct = CoreToolSurface(database, registry_root)
    runtime = McpToolRuntime(database, registry_root)
    try:
        yield direct, runtime
    finally:
        runtime.close()
    assert runtime.closed
    assert not runtime.is_alive


def test_mcp_lists_exact_core_tools_and_schemas(runtimes) -> None:
    direct, mcp = runtimes
    definitions = mcp.definitions()
    assert len(definitions) == 5
    assert definitions == direct.definitions()
    assert [definition["name"] for definition in definitions] == [
        "describe_log",
        "list_variants",
        "list_transitions",
        "list_activities",
        "get_case_trace",
    ]
    assert definitions[0]["parameters"]["properties"]["log_id"] == {
        "type": "string",
        "minLength": 1,
    }


def test_direct_and_mcp_agent_prompts_use_identical_tool_definitions(runtimes) -> None:
    direct, mcp = runtimes
    client = object()
    assert GroundedAgent(direct, client)._system_prompt() == GroundedAgent(
        mcp, client
    )._system_prompt()


def test_mcp_normalizes_malformed_model_requests_like_the_direct_route(runtimes) -> None:
    direct, mcp = runtimes
    for tool, arguments in ((None, {}), ("describe_log", "not-an-object")):
        assert mcp.dispatch(tool, arguments) == direct.dispatch(tool, arguments)


def test_mcp_success_envelope_and_ids_are_exact(runtimes) -> None:
    direct, mcp = runtimes
    arguments = {"log_id": "bpic2012"}
    direct_result = direct.dispatch("describe_log", arguments)
    mcp_result = mcp.dispatch("describe_log", arguments)
    assert canonical_bytes(mcp_result) == canonical_bytes(direct_result)
    assert mcp_result["query_id"] == direct_result["query_id"]
    assert [fact["fact_id"] for fact in mcp_result["facts"]] == [
        fact["fact_id"] for fact in direct_result["facts"]
    ]


def test_mcp_serves_a_registered_dataset_exactly_like_the_direct_route(runtimes) -> None:
    direct, mcp = runtimes
    calls = [
        ("describe_log", {"log_id": "tickets"}),
        (
            "list_variants",
            {"log_id": "tickets", "order_by": "case_count_desc", "limit": 2},
        ),
        (
            "list_transitions",
            {"log_id": "tickets", "order_by": "transition_count_desc", "limit": 2},
        ),
        (
            "list_activities",
            {"log_id": "tickets", "order_by": "event_count_desc", "limit": 2},
        ),
        (
            "get_case_trace",
            {"log_id": "tickets", "case_id": "T-1", "perspective": "raw"},
        ),
    ]
    for tool, arguments in calls:
        direct_result = direct.dispatch(tool, arguments)
        mcp_result = mcp.dispatch(tool, arguments)
        assert "error" not in direct_result, (tool, direct_result)
        assert canonical_bytes(mcp_result) == canonical_bytes(direct_result), tool
    summary = mcp.dispatch("describe_log", {"log_id": "tickets"})
    assert summary["parameters"]["log_id"] == "tickets"
    assert summary["result"]["case_count"] == 2
    assert [name for name in summary["result"] if name.startswith("sla_")] == []


@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        ("describe_log", {"log_id": "bpic2012", "path": "forbidden"}),
        ("run_sql", {"sql": "SELECT 1"}),
        (
            "list_variants",
            {"log_id": "bpic2012", "order_by": "invalid", "limit": 1},
        ),
        (
            "list_variants",
            {"log_id": "bpic2012", "order_by": "case_count_desc", "limit": 101},
        ),
        (
            "get_case_trace",
            {"log_id": "bpic2012", "case_id": "missing", "perspective": "complete"},
        ),
        ("describe_log", {"log_id": "not-registered"}),
        ("describe_log", {"log_id": "unbuilt"}),
        (
            "get_case_trace",
            {"log_id": "tickets", "case_id": "missing", "perspective": "complete"},
        ),
    ],
)
def test_mcp_error_envelopes_are_exact(runtimes, tool: str, arguments: dict) -> None:
    direct, mcp = runtimes
    mcp_result = mcp.dispatch(tool, arguments)
    assert "error" in mcp_result
    assert canonical_bytes(mcp_result) == canonical_bytes(direct.dispatch(tool, arguments))


def test_mcp_reports_unresolvable_datasets_as_structured_errors(runtimes) -> None:
    _, mcp = runtimes
    assert mcp.dispatch("describe_log", {"log_id": "not-registered"})["error"] == {
        "code": "UNKNOWN_LOG",
        "details": {"log_id": "not-registered"},
        "message": "log_id is not a known local dataset",
    }
    assert mcp.dispatch("describe_log", {"log_id": "unbuilt"})["error"]["details"] == {
        "log_id": "unbuilt",
        "status": "registered",
    }


def test_mcp_repeated_calls_are_deterministic(runtimes) -> None:
    _, mcp = runtimes
    for arguments in (
        {"log_id": "bpic2012", "order_by": "transition_count_desc", "limit": 2},
        {"log_id": "tickets", "order_by": "transition_count_desc", "limit": 2},
    ):
        assert canonical_bytes(mcp.dispatch("list_transitions", arguments)) == canonical_bytes(
            mcp.dispatch("list_transitions", arguments)
        )


def test_mcp_adapter_fails_closed_without_structured_content() -> None:
    text_only = types.CallToolResult(
        content=[types.TextContent(text=canonical_json({"schema_version": "fake"}))]
    )
    with pytest.raises(McpProtocolError, match="structured JSON object"):
        _structured_envelope(text_only)

    malformed = types.CallToolResult(
        content=[types.TextContent(text="[]")], structured_content=[]
    )
    with pytest.raises(McpProtocolError, match="structured JSON object"):
        _structured_envelope(malformed)


def test_mcp_transport_is_stdio_only(runtimes) -> None:
    _, mcp = runtimes
    assert mcp.transport == "stdio"
    assert mcp.protocol_version
    assert not hasattr(mcp_server, "app")


def test_mcp_subprocess_lifecycle_terminates(tmp_path: Path) -> None:
    workspace = tmp_path / "lifecycle"
    runtime = McpToolRuntime(
        _database(workspace / "lifecycle.duckdb"), _registry(workspace / "registry")
    )
    assert runtime.is_alive
    runtime.close()
    assert runtime.closed
    assert not runtime.is_alive
