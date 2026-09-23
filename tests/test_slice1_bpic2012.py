from __future__ import annotations

from pathlib import Path
import json
import subprocess
from typing import Any

import duckdb
import pytest

from piw.evaluation import _compare_tool_call_fidelity, _start_server
from piw.tools import CoreToolSurface, canonical_bytes

DATABASE = Path("data/processed/bpic2012.duckdb")
CASES = Path("evaluation/slice1-cases.json")


@pytest.mark.integration
def test_actual_bpic2012_read_only_tool_contract() -> None:
    if not DATABASE.is_file():
        pytest.fail(f"canonical integration database is missing: {DATABASE}")
    tools = CoreToolSurface(DATABASE)
    summary = tools.describe_log("bpic2012", 604_800_000)
    assert summary["result"]["case_count"] == 13_087
    assert summary["result"]["raw_event_count"] == 262_200
    assert summary["result"]["analysis_event_count"] == 164_506
    assert summary["result"]["variant_count"] == 4_336
    assert summary["result"]["direct_follow_count"] == 151_419
    assert canonical_bytes(summary) == canonical_bytes(
        tools.describe_log("bpic2012", 604_800_000)
    )

    assert "error" not in tools.list_variants("bpic2012", "case_count_desc", 2)
    assert "error" not in tools.list_transitions(
        "bpic2012", "transition_count_desc", 2
    )
    assert "error" not in tools.list_activities("bpic2012", "event_count_desc", 2)

    connection = duckdb.connect(str(DATABASE), read_only=True)
    try:
        first_case = connection.execute(
            "SELECT case_id FROM events ORDER BY case_id ASC LIMIT 1"
        ).fetchone()[0]
    finally:
        connection.close()
    trace = tools.get_case_trace("bpic2012", first_case, "complete")
    assert trace["result"]["case_id"] == first_case
    assert trace["result"]["activities"]


@pytest.mark.parametrize("stubborn", [False, True])
def test_start_server_stops_a_process_that_never_becomes_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stubborn: bool
) -> None:
    """A server that never becomes ready is stopped before the error propagates.

    `_start_server` spawns `llama-server` and only then waits for readiness.
    When that wait fails, the spawned process must not be left running behind
    the failed startup: it is terminated, killed if it ignores the termination,
    and its log is closed, and only then does the readiness error reach the
    caller. A leaked process would keep the port bound and the log handle open.
    """

    events: list[str] = []
    opened: list[Any] = []

    class _FakeProcess:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.waits = 0

        def poll(self) -> int | None:
            return self.returncode

        def terminate(self) -> None:
            events.append("terminate")
            if not stubborn:
                self.returncode = 0

        def kill(self) -> None:
            events.append("kill")
            self.returncode = -9

        def wait(self, timeout: float | None = None) -> int:
            events.append(f"wait:{timeout}")
            self.waits += 1
            if stubborn and self.waits == 1:
                raise subprocess.TimeoutExpired(cmd="llama-server", timeout=timeout)
            return self.returncode or 0

    process = _FakeProcess()

    def fake_popen(_command: list[str], **kwargs: Any) -> _FakeProcess:
        opened.append(kwargs["stdout"])
        return process

    def never_ready(*_args: Any, **_kwargs: Any) -> None:
        raise TimeoutError("llama-server did not become ready within the startup timeout")

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr("piw.evaluation._wait_for_server", never_ready)

    with pytest.raises(TimeoutError):
        _start_server(
            Path("llama-server.exe"),
            Path("model.gguf"),
            "http://127.0.0.1:8091/v1",
            1,
            tmp_path / "llama-server.log",
        )

    assert events == (
        ["terminate", "wait:15", "kill", "wait:15"]
        if stubborn
        else ["terminate", "wait:15"]
    )
    assert process.returncode is not None
    assert opened and opened[0].closed is True


def test_cycle_p90_rejects_invented_sla_parameter_but_configured_sla_passes() -> None:
    cases = {
        case["id"]: case
        for case in json.loads(CASES.read_text(encoding="utf-8"))["cases"]
    }
    cycle_gold = cases["cycle-time-p90"]["gold_calls"]
    invented_threshold = [
        {
            "accepted": True,
            "tool": "describe_log",
            "arguments": {"log_id": "bpic2012", "sla_threshold_ms": 90},
        }
    ]
    cycle_fidelity = _compare_tool_call_fidelity(cycle_gold, invented_threshold)
    assert cycle_gold == [
        {"tool": "describe_log", "arguments": {"log_id": "bpic2012"}}
    ]
    assert cycle_fidelity["passed"] is False
    assert cycle_fidelity["reason"] == "accepted_tool_call_mismatch"
    assert cycle_fidelity["extra_accepted_calls"] == [
        {
            "tool": "describe_log",
            "arguments": {"log_id": "bpic2012", "sla_threshold_ms": 90},
        }
    ]

    configured_gold = cases["configured-sla"]["gold_calls"]
    configured_actual = [{"accepted": True, **configured_gold[0]}]
    assert configured_gold[0]["arguments"]["sla_threshold_ms"] == 604_800_000
    assert _compare_tool_call_fidelity(configured_gold, configured_actual)["passed"]


def test_tool_call_fidelity_is_order_independent_and_counts_only_accepted_calls() -> None:
    cases = {
        case["id"]: case
        for case in json.loads(CASES.read_text(encoding="utf-8"))["cases"]
    }
    gold = cases["summary-plus-top-activity"]["gold_calls"]
    reversed_with_rejected_attempt = [
        {"accepted": True, **gold[1]},
        {
            "accepted": False,
            "tool": "run_sql",
            "arguments": {"sql": "SELECT 1"},
        },
        {"accepted": True, **gold[0]},
    ]
    assert _compare_tool_call_fidelity(gold, reversed_with_rejected_attempt)["passed"]

    missing = [{"accepted": True, **gold[0]}]
    assert not _compare_tool_call_fidelity(gold, missing)["passed"]

    extra = [
        {"accepted": True, **gold[0]},
        {"accepted": True, **gold[1]},
        {"accepted": True, **gold[1]},
    ]
    assert not _compare_tool_call_fidelity(gold, extra)["passed"]
