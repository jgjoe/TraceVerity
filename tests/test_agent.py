from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from piw.agent import GroundedAgent, LlamaServerClient, _ungrounded_parameter_error
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
from piw.tools import CoreToolSurface

REGISTERED_LOG_ID = "tickets"
# The Agent may only use a dataset identifier the question states.
CANONICAL_QUESTION = "How many cases are in the bpic2012 log? Return case_count."
CSV_CONFIG = CsvImportConfig(
    mapping=CsvColumnMapping(
        case_id="Case ID", activity="Activity", timestamp="Complete Timestamp"
    ),
    timestamp=TimestampConfig(
        assume_timezone="UTC", timestamp_format="%Y/%m/%d %H:%M:%S.%f"
    ),
)


def tool_surface(tmp_path: Path) -> CoreToolSurface:
    database = tmp_path / "agent.duckdb"
    connection = duckdb.connect(str(database))
    try:
        install_events(
            connection,
            [Event("case-1", "A", 0, 0, "COMPLETE", None)],
            SourceFingerprint(filename="fixture.xes", sha256="b" * 64, size_bytes=1),
        )
    finally:
        connection.close()
    return CoreToolSurface(database, tmp_path / "registry")


def registered_surface(tmp_path: Path) -> CoreToolSurface:
    """A real registered CSV dataset: two cases, five events."""

    source = tmp_path / "tickets.csv"
    source.write_text(
        "Case ID,Activity,Complete Timestamp\n"
        "T-1,Register,2012/10/09 14:50:17.000\n"
        "T-1,Approve,2012/10/09 15:50:17.000\n"
        "T-1,Close,2012/10/10 09:50:17.000\n"
        "T-2,Register,2012/10/09 16:50:17.000\n"
        "T-2,Close,2012/10/09 17:50:17.000\n",
        encoding="utf-8",
        newline="\n",
    )
    root = tmp_path / "registry"
    descriptor = register_csv(
        DatasetRegistry(root),
        log_id=REGISTERED_LOG_ID,
        display_name="Ticket log",
        source_path=source,
        config=CSV_CONFIG,
    )
    assert build_dataset(descriptor)["validation"]["status"] == "PASS"
    return CoreToolSurface(tmp_path / "agent.duckdb", root)


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
        CANONICAL_QUESTION, expected_fact_names=["case_count"]
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
        CANONICAL_QUESTION, expected_fact_names=["case_count"]
    )
    assert result["answer"]["facts"][0]["value"] == 1
    assert result["grounding_violations"] == []
    assert result["rejected_grounding_attempt_count"] == 1


def test_agent_hydrates_provenance_after_exact_name_value_match(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), HydratingClient()).answer(
        CANONICAL_QUESTION, expected_fact_names=["case_count"]
    )
    fact = result["answer"]["facts"][0]
    assert fact["value"] == 1
    assert fact["fact_id"].startswith("f_")
    assert result["answer"]["supporting_fact_ids"] == [fact["fact_id"]]
    assert result["provenance_hydration_count"] == 1
    assert result["grounding_violations"] == []


def test_agent_restores_lossless_numeric_string_from_exact_fact(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), NumericStringClient()).answer(
        CANONICAL_QUESTION, expected_fact_names=["case_count"]
    )
    fact = result["answer"]["facts"][0]
    assert fact["value"] == 1
    assert type(fact["value"]) is int
    assert result["provenance_hydration_count"] == 1


def test_agent_normalizes_bounded_nested_tool_envelope(tmp_path: Path) -> None:
    result = GroundedAgent(tool_surface(tmp_path), NestedToolEnvelopeClient()).answer(
        CANONICAL_QUESTION, expected_fact_names=["case_count"]
    )
    assert result["answer"]["facts"][0]["value"] == 1
    assert result["tool_calls"][0]["accepted"] is True
    assert result["tool_calls"][0]["tool"] == "describe_log"


def test_agent_rejects_answered_final_with_missing_required_fact(tmp_path: Path) -> None:
    result = GroundedAgent(
        tool_surface(tmp_path), MissingFactThenRepairingClient()
    ).answer(CANONICAL_QUESTION, expected_fact_names=["case_count"])
    assert result["answer"]["facts"][0]["name"] == "case_count"
    assert any("missing required facts" in error for error in result["protocol_errors"])


def test_agent_rejects_sla_threshold_not_explicitly_grounded_in_question(
    tmp_path: Path,
) -> None:
    result = GroundedAgent(
        tool_surface(tmp_path), InventedSlaThenCorrectingClient()
    ).answer(
        "Return case_count and p90 for the bpic2012 log.", expected_fact_names=["case_count"]
    )
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


class RegisteredDatasetClient:
    """A grounded client that copies the stated dataset identifier."""

    calls = 0

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "arguments": {"log_id": REGISTERED_LOG_ID},
                "tool": "describe_log",
            }
        tool_message = messages[-1]["content"].removeprefix("TOOL_RESULT=")
        response = json.loads(tool_message)
        fact = next(fact for fact in response["facts"] if fact["name"] == "case_count")
        return {
            "action": "final",
            "facts": [fact],
            "status": "ANSWERED",
            "supporting_fact_ids": [fact["fact_id"]],
        }


class InventedLogIdClient:
    """A client that guesses or translates a dataset identifier instead of copying one."""

    calls = 0

    def __init__(self, log_id: str = "guessed-dataset") -> None:
        self._log_id = log_id

    def complete(self, messages: list[dict[str, str]]) -> dict:
        self.calls += 1
        if self.calls == 1:
            return {
                "action": "tool",
                "arguments": {"log_id": self._log_id},
                "tool": "describe_log",
            }
        return {"action": "final", "facts": [], "status": "UNAVAILABLE", "supporting_fact_ids": []}


def test_agent_answers_a_registered_dataset_from_returned_facts(tmp_path: Path) -> None:
    result = GroundedAgent(
        registered_surface(tmp_path), RegisteredDatasetClient()
    ).answer(
        f"How many cases are in the {REGISTERED_LOG_ID} log? Return case_count.",
        expected_fact_names=["case_count"],
    )
    assert result["answer"]["status"] == "ANSWERED"
    assert result["answer"]["facts"][0]["name"] == "case_count"
    assert result["answer"]["facts"][0]["value"] == 2
    assert result["grounding_violations"] == []
    assert result["forbidden_action_count"] == 0
    assert result["tool_calls"][0]["accepted"] is True
    assert result["tool_calls"][0]["arguments"] == {"log_id": REGISTERED_LOG_ID}


@pytest.mark.parametrize(
    "invented", ["guessed-dataset", f"{REGISTERED_LOG_ID}_log", "ticket"]
)
def test_agent_rejects_an_identifier_that_the_question_does_not_state(
    tmp_path: Path, invented: str
) -> None:
    """A translated identifier is rejected before it can resolve anything."""

    result = GroundedAgent(
        registered_surface(tmp_path), InventedLogIdClient(invented)
    ).answer(
        f"How many cases are in the {REGISTERED_LOG_ID} log? Return case_count.",
        expected_fact_names=["case_count"],
    )
    assert result["answer"]["status"] == "UNAVAILABLE"
    assert result["answer"]["facts"] == []
    assert result["tool_calls"][0]["accepted"] is False
    assert result["tool_calls"][0]["error"] == {
        "code": "UNGROUNDED_PARAMETER",
        "details": {"parameter": "log_id"},
        "message": (
            "log_id must be copied exactly from the question; a dataset "
            "identifier is never invented, guessed, translated, or defaulted"
        ),
    }
    assert result["grounding_violations"] == []
    assert result["forbidden_action_count"] == 0


def test_agent_reports_a_stated_but_unresolvable_identifier(tmp_path: Path) -> None:
    """A faithfully copied identifier that resolves to nothing fails closed."""

    stated = "not-a-registered-dataset"
    result = GroundedAgent(
        registered_surface(tmp_path), InventedLogIdClient(stated)
    ).answer(
        f"How many cases are in the {stated} log? Return case_count.",
        expected_fact_names=["case_count"],
    )
    assert result["answer"]["status"] == "UNAVAILABLE"
    assert result["tool_calls"][0]["accepted"] is False
    assert result["tool_calls"][0]["error"]["code"] == "UNKNOWN_LOG"
    assert result["tool_calls"][0]["error"]["details"] == {"log_id": stated}


def test_log_id_grounding_distinguishes_sentence_punctuation_from_id_suffixes() -> None:
    assert _ungrounded_parameter_error(
        "Return case_count for tickets.", "describe_log", {"log_id": "tickets"}
    ) is None
    error = _ungrounded_parameter_error(
        "Return case_count for tickets.archive.",
        "describe_log",
        {"log_id": "tickets"},
    )
    assert error is not None
    assert error["error"]["code"] == "UNGROUNDED_PARAMETER"
