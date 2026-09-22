from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from piw.agent import GroundedAgent, LlamaServerClient, _ungrounded_parameter_error
from piw.core import install_events
from piw.tools import CoreToolSurface
from piw.xes import Event


def tool_surface(tmp_path: Path) -> CoreToolSurface:
    database = tmp_path / "agent.duckdb"
    connection = duckdb.connect(str(database))
    try:
        install_events(
            connection,
            [Event("case-1", "A", 0, 0, "COMPLETE", None)],
            {
                "doi": "fixture",
                "filename": "fixture.xes",
                "sha256": "b" * 64,
                "size_bytes": 1,
                "source_url": "https://example.invalid/fixture",
            },
        )
    finally:
        connection.close()
    return CoreToolSurface(database)


class GroundedClient:
    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012"},
            }
        tool_message = messages[-1]["content"].removeprefix("TOOL_RESULT=")
        response = json.loads(tool_message)
        fact = next(fact for fact in response["facts"] if fact["name"] == "case_count")
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": [fact],
            "supporting_fact_ids": [fact["fact_id"]],
        }


class ForbiddenClient:
    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {"action": "tool", "tool": "run_sql", "arguments": {"sql": "SELECT 1"}}
        return {
            "action": "final",
            "status": "UNAVAILABLE",
            "facts": [],
            "supporting_fact_ids": [],
        }


class CorrectingClient:
    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012"},
            }
        tool_message = next(
            message["content"].removeprefix("TOOL_RESULT=")
            for message in reversed(messages)
            if message["content"].startswith("TOOL_RESULT=")
        )
        fact = next(
            fact
            for fact in json.loads(tool_message)["facts"]
            if fact["name"] == "case_count"
        )
        if self.calls == 2:
            invented = dict(fact)
            invented["value"] = 999
            return {
                "action": "final",
                "status": "ANSWERED",
                "facts": [invented],
                "supporting_fact_ids": [invented["fact_id"]],
            }
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": [fact],
            "supporting_fact_ids": [fact["fact_id"]],
        }


class HydratingClient:
    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012"},
            }
        tool_message = next(
            message["content"].removeprefix("TOOL_RESULT=")
            for message in reversed(messages)
            if message["content"].startswith("TOOL_RESULT=")
        )
        fact = next(
            fact
            for fact in json.loads(tool_message)["facts"]
            if fact["name"] == "case_count"
        )
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": [{"fact_id": "mistyped", "name": fact["name"], "value": fact["value"]}],
            "supporting_fact_ids": ["mistyped"],
        }


class NumericStringClient:
    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012"},
            }
        tool_message = next(
            message["content"].removeprefix("TOOL_RESULT=")
            for message in reversed(messages)
            if message["content"].startswith("TOOL_RESULT=")
        )
        fact = next(
            fact
            for fact in json.loads(tool_message)["facts"]
            if fact["name"] == "case_count"
        )
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": [{**fact, "value": str(fact["value"])}],
            "supporting_fact_ids": [fact["fact_id"]],
        }


class NestedToolEnvelopeClient(GroundedClient):
    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "arguments": {
                    "tool": "describe_log",
                    "parameters": {"log_id": "bpic2012"},
                },
            }
        tool_message = messages[-1]["content"].removeprefix("TOOL_RESULT=")
        response = json.loads(tool_message)
        fact = next(fact for fact in response["facts"] if fact["name"] == "case_count")
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": [fact],
            "supporting_fact_ids": [fact["fact_id"]],
        }


class MissingFactThenRepairingClient:
    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012"},
            }
        tool_message = next(
            message["content"].removeprefix("TOOL_RESULT=")
            for message in reversed(messages)
            if message["content"].startswith("TOOL_RESULT=")
        )
        fact = next(
            fact
            for fact in json.loads(tool_message)["facts"]
            if fact["name"] == "case_count"
        )
        facts = [] if self.calls == 2 else [fact]
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": facts,
            "supporting_fact_ids": [item["fact_id"] for item in facts],
        }


class InventedSlaThenCorrectingClient(GroundedClient):
    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012", "sla_threshold_ms": 90},
            }
        if self.calls == 2:
            return {
                "action": "tool",
                "tool": "describe_log",
                "arguments": {"log_id": "bpic2012"},
            }
        tool_message = messages[-1]["content"].removeprefix("TOOL_RESULT=")
        response = json.loads(tool_message)
        fact = next(fact for fact in response["facts"] if fact["name"] == "case_count")
        return {
            "action": "final",
            "status": "ANSWERED",
            "facts": [fact],
            "supporting_fact_ids": [fact["fact_id"]],
        }


def test_agent_accepts_only_exact_returned_facts(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), GroundedClient()).answer(
        "How many cases?", expected_fact_names=["case_count"]
    )
    assert result["answer"]["status"] == "ANSWERED"
    assert result["answer"]["facts"][0]["value"] == 1
    assert result["grounding_violations"] == []
    assert result["forbidden_action_count"] == 0


def test_agent_rejects_forbidden_tool_without_accepting_action(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), ForbiddenClient()).answer(
        "Run SQL", expected_fact_names=[]
    )
    assert result["answer"]["status"] == "UNAVAILABLE"
    assert result["forbidden_attempt_count"] == 1
    assert result["forbidden_action_count"] == 0
    assert result["tool_calls"][0]["accepted"] is False


def test_agent_rejects_then_repairs_ungrounded_final(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), CorrectingClient()).answer(
        "How many cases?", expected_fact_names=["case_count"]
    )
    assert result["answer"]["facts"][0]["value"] == 1
    assert result["grounding_violations"] == []
    assert result["rejected_grounding_attempt_count"] == 1


def test_agent_hydrates_provenance_after_exact_name_value_match(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), HydratingClient()).answer(
        "How many cases?", expected_fact_names=["case_count"]
    )
    fact = result["answer"]["facts"][0]
    assert fact["value"] == 1
    assert fact["fact_id"].startswith("f_")
    assert result["answer"]["supporting_fact_ids"] == [fact["fact_id"]]
    assert result["provenance_hydration_count"] == 1
    assert result["grounding_violations"] == []


def test_agent_restores_lossless_numeric_string_from_exact_fact(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), NumericStringClient()).answer(
        "How many cases?", expected_fact_names=["case_count"]
    )
    fact = result["answer"]["facts"][0]
    assert fact["value"] == 1
    assert type(fact["value"]) is int
    assert result["provenance_hydration_count"] == 1


def test_agent_normalizes_bounded_nested_tool_envelope(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), NestedToolEnvelopeClient()).answer(
        "How many cases?", expected_fact_names=["case_count"]
    )
    assert result["answer"]["facts"][0]["value"] == 1
    assert result["tool_calls"][0]["accepted"] is True
    assert result["tool_calls"][0]["tool"] == "describe_log"


def test_agent_rejects_answered_final_with_missing_required_fact(tmp_path: Path) -> None:
    result = GroundedAgent(
        tool_surface(tmp_path), MissingFactThenRepairingClient()
    ).answer("How many cases?", expected_fact_names=["case_count"])
    assert result["answer"]["facts"][0]["name"] == "case_count"
    assert any("missing required facts" in error for error in result["protocol_errors"])


def test_agent_rejects_sla_threshold_not_explicitly_grounded_in_question(
    tmp_path: Path,
) -> None:
    result = GroundedAgent(
        tool_surface(tmp_path), InventedSlaThenCorrectingClient()
    ).answer("Return case_count and p90.", expected_fact_names=["case_count"])
    assert result["answer"]["facts"][0]["name"] == "case_count"
    assert result["tool_calls"][0]["accepted"] is False
    assert result["tool_calls"][0]["error"]["code"] == "UNGROUNDED_PARAMETER"
    assert result["tool_calls"][1]["accepted"] is True


def test_agent_rejects_optional_null_placeholders() -> None:
    error = _ungrounded_parameter_error(
        "Return the top transitions.",
        "list_transitions",
        {
            "log_id": "bpic2012",
            "order_by": "transition_count_desc",
            "limit": 2,
            "from_activity": None,
            "to_activity": None,
        },
    )
    assert error is not None
    assert error["error"]["code"] == "UNGROUNDED_PARAMETER"
    assert error["error"]["details"]["parameters"] == [
        "from_activity",
        "to_activity",
    ]


def test_llama_client_is_localhost_only() -> None:
    with pytest.raises(ValueError, match="localhost"):
        LlamaServerClient("https://example.com/v1", "model")
