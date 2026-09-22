from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import duckdb

from .agent import GroundedAgent, LlamaServerClient
from .tools import CoreToolSurface, canonical_bytes, canonical_json

EVALUATION_SCHEMA_VERSION = "slice1-agent-evaluation-v2"
PROTECTED_SLICE0_SHA256 = (
    "bbdd8d716af40d7eece3c9914a9955b0800b16145fe9324796b0f271d5f2272b"
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _replace_placeholders(value: Any, context: dict[str, str]) -> Any:
    if isinstance(value, str):
        for name, replacement in context.items():
            value = value.replace("{{" + name + "}}", replacement)
        return value
    if isinstance(value, list):
        return [_replace_placeholders(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _replace_placeholders(item, context) for key, item in value.items()}
    return value


def _evaluation_context(
    database_path: Path, surface: CoreToolSurface
) -> dict[str, str]:
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        first_case_id = connection.execute(
            "SELECT case_id FROM events ORDER BY case_id ASC LIMIT 1"
        ).fetchone()[0]
    finally:
        connection.close()
    top_transition = surface.list_transitions(
        "bpic2012", "transition_count_desc", 1
    )["result"]["items"][0]
    return {
        "FIRST_CASE_ID": str(first_case_id),
        "TOP_TRANSITION_FROM": str(top_transition["from_activity"]),
    }


def _fact_map(response: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {fact["name"]: fact for fact in response.get("facts", [])}


def _gold_answer(
    surface: CoreToolSurface, case: dict[str, Any]
) -> dict[str, Any]:
    expected_status = case.get("expected_status", "ANSWERED")
    facts_by_name: dict[str, dict[str, Any]] = {}
    for call in case["gold_calls"]:
        response = surface.dispatch(call["tool"], call["arguments"])
        if "error" in response:
            raise RuntimeError(
                f"gold tool call failed for {case['id']}: {canonical_json(response)}"
            )
        facts_by_name.update(_fact_map(response))
    facts = []
    for name in case["expected_fact_names"]:
        if name not in facts_by_name:
            raise RuntimeError(f"gold fact {name!r} is missing for {case['id']}")
        facts.append(facts_by_name[name])
    return {
        "facts": facts,
        "status": expected_status,
        "supporting_fact_ids": [fact["fact_id"] for fact in facts],
    }


def _normalized_answer(answer: dict[str, Any]) -> dict[str, Any]:
    facts = answer.get("facts", [])
    if isinstance(facts, list):
        facts = sorted(
            [
                {"name": fact.get("name"), "value": fact.get("value")}
                for fact in facts
                if isinstance(fact, dict)
            ],
            key=lambda fact: str(fact.get("name")),
        )
    return {
        "facts": facts,
        "status": answer.get("status"),
    }


def _compare_answer(
    expected: dict[str, Any], actual: dict[str, Any], grounding: list[str], forbidden: int
) -> tuple[bool, str]:
    reasons = []
    if canonical_bytes(_normalized_answer(expected)) != canonical_bytes(
        _normalized_answer(actual)
    ):
        reasons.append("structured_answer_mismatch")
    if grounding:
        reasons.append("grounding_violation")
    if forbidden:
        reasons.append("forbidden_action_accepted")
    return not reasons, "exact_match" if not reasons else ",".join(reasons)


def _canonical_accepted_calls(calls: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = [
        {"arguments": call["arguments"], "tool": call["tool"]}
        for call in calls
        if call.get("accepted", True) is True
    ]
    return sorted(normalized, key=canonical_json)


def _compare_tool_call_fidelity(
    expected_calls: list[dict[str, Any]], actual_calls: list[dict[str, Any]]
) -> dict[str, Any]:
    expected = _canonical_accepted_calls(expected_calls)
    actual = _canonical_accepted_calls(actual_calls)
    expected_tokens = Counter(canonical_json(call) for call in expected)
    actual_tokens = Counter(canonical_json(call) for call in actual)
    missing = sorted(
        (expected_tokens - actual_tokens).elements()
    )
    extra = sorted(
        (actual_tokens - expected_tokens).elements()
    )
    passed = not missing and not extra
    return {
        "actual_accepted_calls": actual,
        "expected_accepted_calls": expected,
        "extra_accepted_calls": [json.loads(call) for call in extra],
        "missing_accepted_calls": [json.loads(call) for call in missing],
        "passed": passed,
        "reason": "exact_match" if passed else "accepted_tool_call_mismatch",
    }


def _health_url(base_url: str) -> str:
    value = base_url.rstrip("/")
    if value.endswith("/v1"):
        value = value[:-3]
    return value + "/health"


def _server_ready(base_url: str) -> bool:
    try:
        with urllib.request.urlopen(_health_url(base_url), timeout=2) as response:
            return response.status == 200
    except (urllib.error.URLError, TimeoutError):
        return False


def _wait_for_server(
    process: subprocess.Popen[bytes], base_url: str, timeout_seconds: int, log_path: Path
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _server_ready(base_url):
            return
        if process.poll() is not None:
            tail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
            raise RuntimeError(f"llama-server exited during startup:\n{tail}")
        time.sleep(1)
    raise TimeoutError("llama-server did not become ready within the startup timeout")


def _start_server(
    server_path: Path,
    model_path: Path,
    base_url: str,
    startup_timeout: int,
    log_path: Path,
) -> tuple[subprocess.Popen[bytes], Any]:
    parsed = urllib.parse.urlparse(base_url)
    port = parsed.port or 80
    command = [
        str(server_path),
        "-m",
        str(model_path),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "-ngl",
        "99",
        "-c",
        "8192",
        "--jinja",
        "-fa",
        "on",
    ]
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("wb")
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    process = subprocess.Popen(
        command,
        cwd=str(Path.cwd()),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        creationflags=creationflags,
    )
    try:
        _wait_for_server(process, base_url, startup_timeout, log_path)
    except Exception:
        log_file.close()
        raise
    return process, log_file


def _binary_version(path: Path) -> str:
    completed = subprocess.run(
        [str(path), "--version"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    output = (completed.stdout + "\n" + completed.stderr).strip()
    if completed.returncode != 0 or not output:
        raise RuntimeError("unable to read llama.cpp version")
    match = re.search(r"version:\s*([^\r\n]+)", output, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return output.splitlines()[0].strip()


def _run_final_validation(
    repository_root: Path, slice0_report_path: Path
) -> dict[str, Any]:
    before = slice0_report_path.read_bytes()
    before_hash = hashlib.sha256(before).hexdigest()
    pytest_result = subprocess.run(
        [sys.executable, "-m", "pytest"],
        cwd=repository_root,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    slice0_result = subprocess.run(
        [sys.executable, "-m", "piw"],
        cwd=repository_root,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    after = slice0_report_path.read_bytes()
    after_hash = hashlib.sha256(after).hexdigest()
    return {
        "actual_bpic2012_pipeline": {
            "command": ".venv/Scripts/python.exe -m piw",
            "exit_code": slice0_result.returncode,
            "passed": slice0_result.returncode == 0,
        },
        "pytest": {
            "command": ".venv/Scripts/python.exe -m pytest",
            "exit_code": pytest_result.returncode,
            "passed": pytest_result.returncode == 0,
        },
        "slice0_report": {
            "after_sha256": after_hash,
            "before_sha256": before_hash,
            "byte_identical": before == after,
            "protected_sha256": PROTECTED_SLICE0_SHA256,
            "protected_sha256_matches": after_hash == PROTECTED_SLICE0_SHA256,
        },
    }


def _tool_contract_checks(
    surface: CoreToolSurface, context: dict[str, str]
) -> dict[str, Any]:
    calls = [
        ("describe_log", {"log_id": "bpic2012", "sla_threshold_ms": 604800000}),
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
            {
                "log_id": "bpic2012",
                "case_id": context["FIRST_CASE_ID"],
                "perspective": "complete",
            },
        ),
    ]
    deterministic = True
    successful = True
    for name, arguments in calls:
        first = surface.dispatch(name, arguments)
        second = surface.dispatch(name, arguments)
        deterministic = deterministic and canonical_bytes(first) == canonical_bytes(second)
        successful = successful and "error" not in first
    forbidden = surface.dispatch("run_sql", {"sql": "SELECT 1"})
    unsupported = surface.dispatch(
        "describe_log", {"log_id": "bpic2012", "path": "forbidden"}
    )
    return {
        "all_five_tools_succeeded": successful,
        "deterministic_repeated_requests": deterministic,
        "forbidden_tool_failed_closed": forbidden.get("error", {}).get("code")
        == "FORBIDDEN_TOOL",
        "tool_count": len(CoreToolSurface.definitions()),
        "unsupported_parameter_failed_closed": unsupported.get("error", {}).get("code")
        == "UNSUPPORTED_PARAMETER",
    }


def run_evaluation(args: argparse.Namespace) -> dict[str, Any]:
    repository_root = Path.cwd().resolve()
    database_path = args.database.resolve()
    cases_path = args.cases.resolve()
    report_path = args.report.resolve()
    slice0_report_path = args.slice0_report.resolve()
    server_path = args.llama_server.resolve()
    model_path = args.model_file.resolve()
    for required in (database_path, cases_path, slice0_report_path, server_path, model_path):
        if not required.is_file():
            raise FileNotFoundError(required)

    surface = CoreToolSurface(database_path)
    context = _evaluation_context(database_path, surface)
    raw_cases = json.loads(cases_path.read_text(encoding="utf-8"))
    cases = [_replace_placeholders(case, context) for case in raw_cases["cases"]]
    tool_checks = _tool_contract_checks(surface, context)
    validation = _run_final_validation(repository_root, slice0_report_path)
    model_provenance = {
        "filename": model_path.name,
        "identifier": args.model_identifier,
        "quantization": args.quantization,
        "sha256": _sha256_file(model_path),
    }
    llama_version = _binary_version(server_path)

    owned_server: subprocess.Popen[bytes] | None = None
    log_file = None
    if not _server_ready(args.base_url):
        owned_server, log_file = _start_server(
            server_path,
            model_path,
            args.base_url,
            args.startup_timeout,
            args.server_log.resolve(),
        )
    client = LlamaServerClient(
        args.base_url,
        args.api_model,
        timeout_seconds=args.request_timeout,
        temperature=args.temperature,
        seed=args.seed,
    )
    agent = GroundedAgent(surface, client, max_steps=6)
    results = []
    total_grounding_violations = 0
    total_forbidden_actions = 0
    total_forbidden_attempts = 0
    total_rejected_grounding_attempts = 0
    total_provenance_hydrations = 0
    accepted_tool_call_mismatch_count = 0
    try:
        for case in cases:
            expected = _gold_answer(surface, case)
            actual_run = agent.answer(
                case["question"], expected_fact_names=case["expected_fact_names"]
            )
            actual = actual_run["answer"]
            grounding = actual_run["grounding_violations"]
            forbidden = actual_run["forbidden_action_count"]
            answer_passed, answer_reason = _compare_answer(
                expected, actual, grounding, forbidden
            )
            tool_call_fidelity = _compare_tool_call_fidelity(
                case["gold_calls"], actual_run["tool_calls"]
            )
            if not tool_call_fidelity["passed"]:
                accepted_tool_call_mismatch_count += 1
            passed = answer_passed and tool_call_fidelity["passed"]
            reasons = []
            if not answer_passed:
                reasons.append(answer_reason)
            if not tool_call_fidelity["passed"]:
                reasons.append("accepted_tool_call_mismatch")
            reason = "exact_match" if not reasons else ",".join(reasons)
            total_grounding_violations += len(grounding)
            total_forbidden_actions += forbidden
            total_forbidden_attempts += actual_run["forbidden_attempt_count"]
            total_rejected_grounding_attempts += actual_run[
                "rejected_grounding_attempt_count"
            ]
            total_provenance_hydrations += actual_run["provenance_hydration_count"]
            results.append(
                {
                    "actual": actual,
                    "actual_accepted_tool_calls": tool_call_fidelity[
                        "actual_accepted_calls"
                    ],
                    "answer_match_passed": answer_passed,
                    "expected": expected,
                    "expected_accepted_tool_calls": tool_call_fidelity[
                        "expected_accepted_calls"
                    ],
                    "forbidden_action_count": forbidden,
                    "forbidden_attempt_count": actual_run["forbidden_attempt_count"],
                    "grounding_violations": grounding,
                    "id": case["id"],
                    "mandatory": bool(case["mandatory"]),
                    "passed": passed,
                    "reason": reason,
                    "rejected_grounding_attempt_count": actual_run[
                        "rejected_grounding_attempt_count"
                    ],
                    "protocol_errors": actual_run["protocol_errors"],
                    "provenance_hydration_count": actual_run[
                        "provenance_hydration_count"
                    ],
                    "steps": actual_run["steps"],
                    "tool_calls": actual_run["tool_calls"],
                    "tool_call_fidelity_passed": tool_call_fidelity["passed"],
                    "tool_call_fidelity_reason": tool_call_fidelity["reason"],
                    "tool_call_fidelity_extra": tool_call_fidelity[
                        "extra_accepted_calls"
                    ],
                    "tool_call_fidelity_missing": tool_call_fidelity[
                        "missing_accepted_calls"
                    ],
                    "type": case["type"],
                }
            )
    finally:
        if owned_server is not None:
            owned_server.terminate()
            try:
                owned_server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                owned_server.kill()
                owned_server.wait(timeout=15)
        if log_file is not None:
            log_file.close()

    passed_count = sum(1 for result in results if result["passed"])
    mandatory = {
        result["id"]: result["passed"] for result in results if result["mandatory"]
    }
    deterministic_ok = all(
        [
            tool_checks["all_five_tools_succeeded"],
            tool_checks["deterministic_repeated_requests"],
            tool_checks["forbidden_tool_failed_closed"],
            tool_checks["tool_count"] == 5,
            tool_checks["unsupported_parameter_failed_closed"],
            validation["pytest"]["passed"],
            validation["actual_bpic2012_pipeline"]["passed"],
            validation["slice0_report"]["byte_identical"],
            validation["slice0_report"]["protected_sha256_matches"],
        ]
    )
    agent_ok = (
        len(results) == 10
        and passed_count >= 8
        and all(mandatory.values())
        and total_grounding_violations == 0
        and total_forbidden_actions == 0
        and accepted_tool_call_mismatch_count == 0
    )
    report = {
        "aggregate": {
            "accepted_tool_call_mismatch_count": accepted_tool_call_mismatch_count,
            "failed": len(results) - passed_count,
            "mandatory": mandatory,
            "passed": passed_count,
            "threshold": 8,
            "total": len(results),
        },
        "cases": results,
        "core": {
            "log_fingerprint": surface.describe_log("bpic2012")["log_fingerprint"],
            "metric_definition_version": surface.describe_log("bpic2012")[
                "metric_definition_version"
            ],
        },
        "evaluation_schema_version": EVALUATION_SCHEMA_VERSION,
        "forbidden_action_count": total_forbidden_actions,
        "forbidden_attempt_count": total_forbidden_attempts,
        "grounding_violation_count": total_grounding_violations,
        "rejected_grounding_attempt_count": total_rejected_grounding_attempts,
        "model": model_provenance,
        "provenance_hydration_count": total_provenance_hydrations,
        "runtime": {
            "api": "llama-server localhost /v1/chat/completions",
            "llama_cpp_version": llama_version,
            "max_steps": 6,
            "seed": args.seed,
            "temperature": args.temperature,
        },
        "slice1_status": "PASS" if deterministic_ok and agent_ok else "HOLD",
        "tool_contract": tool_checks,
        "tool_call_fidelity_passed": accepted_tool_call_mismatch_count == 0,
        "tool_schema_version": surface.describe_log("bpic2012")["schema_version"],
        "validation": validation,
        "zero_paid_services_or_apis": True,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return report


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Run the local Slice 1 Agent evaluation.")
    command.add_argument(
        "--database", type=Path, default=Path("data/processed/bpic2012.duckdb")
    )
    command.add_argument(
        "--cases", type=Path, default=Path("evaluation/slice1-cases.json")
    )
    command.add_argument(
        "--report", type=Path, default=Path("evidence/slice1/agent-eval-report.json")
    )
    command.add_argument(
        "--slice0-report",
        type=Path,
        default=Path("evidence/slice0/bpic2012-core-report.json"),
    )
    command.add_argument("--llama-server", type=Path, required=True)
    command.add_argument("--model-file", type=Path, required=True)
    command.add_argument(
        "--model-identifier", default="Qwen/Qwen3-8B-GGUF"
    )
    command.add_argument("--quantization", default="Q4_K_M")
    command.add_argument("--api-model", default="Qwen3-8B")
    command.add_argument("--base-url", default="http://127.0.0.1:8091/v1")
    command.add_argument("--server-log", type=Path, default=Path("data/processed/slice1-llama-server.log"))
    command.add_argument("--startup-timeout", type=int, default=900)
    command.add_argument("--request-timeout", type=int, default=300)
    command.add_argument("--temperature", type=float, default=0.1)
    command.add_argument("--seed", type=int, default=1)
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    report = run_evaluation(args)
    aggregate = report["aggregate"]
    print(
        f"{report['slice1_status']}: {aggregate['passed']}/{aggregate['total']} passed; "
        f"grounding={report['grounding_violation_count']} "
        f"forbidden={report['forbidden_action_count']} report={args.report}"
    )
    return 0 if report["slice1_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
