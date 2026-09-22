from __future__ import annotations

from pathlib import Path

import duckdb
import mcp.types as types
import pytest

from piw.agent import GroundedAgent
from piw.core import install_events
from piw import mcp_server
from piw.mcp_runtime import McpProtocolError, McpToolRuntime, _structured_envelope
from piw.tools import CoreToolSurface, canonical_bytes, canonical_json
from piw.xes import Event


def _database(path: Path) -> Path:
    connection = duckdb.connect(str(path))
    try:
        install_events(
            connection,
            [
                Event("case-1", "A", 0, 0, "COMPLETE", None),
                Event("case-1", "B", 10, 1, "COMPLETE", None),
                Event("case-2", "A", 0, 0, "COMPLETE", None),
            ],
            {
                "doi": "fixture",
                "filename": "fixture.xes",
                "sha256": "c" * 64,
                "size_bytes": 1,
                "source_url": "https://example.invalid/fixture",
            },
        )
    finally:
        connection.close()
    return path


@pytest.fixture(scope="module")
def runtimes(tmp_path_factory: pytest.TempPathFactory):
    database = _database(tmp_path_factory.mktemp("mcp") / "fixture.duckdb")
    direct = CoreToolSurface(database)
    runtime = McpToolRuntime(database)
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


def test_direct_and_mcp_agent_prompts_use_identical_tool_definitions(runtimes) -> None:
    direct, mcp = runtimes
    client = object()
    assert GroundedAgent(direct, client)._system_prompt() == GroundedAgent(
        mcp, client
    )._system_prompt()


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
    ],
)
def test_mcp_error_envelopes_are_exact(runtimes, tool: str, arguments: dict) -> None:
    direct, mcp = runtimes
    assert canonical_bytes(mcp.dispatch(tool, arguments)) == canonical_bytes(
        direct.dispatch(tool, arguments)
    )


def test_mcp_repeated_calls_are_deterministic(runtimes) -> None:
    _, mcp = runtimes
    arguments = {
        "log_id": "bpic2012",
        "order_by": "transition_count_desc",
        "limit": 2,
    }
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
    runtime = McpToolRuntime(_database(tmp_path / "lifecycle.duckdb"))
    assert runtime.is_alive
    runtime.close()
    assert runtime.closed
    assert not runtime.is_alive
