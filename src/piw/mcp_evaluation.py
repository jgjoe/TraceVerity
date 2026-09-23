from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .agent import GroundedAgent, LlamaServerClient, ToolRuntime
from .datasets import DEFAULT_REGISTRY_ROOT
from .evaluation import (
    _binary_version,
    _compare_answer,
    _compare_tool_call_fidelity,
    _evaluation_context,
    _gold_answer,
    _replace_placeholders,
    _server_ready,
    _sha256_file,
    _start_server,
)
from .mcp_runtime import McpToolRuntime
from .tools import CoreToolSurface, canonical_bytes

EVIDENCE_SCHEMA_VERSION = "slice5-mcp-integration-v1"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROTECTED_ARTIFACTS = {
    "slice0_core_report": (
        Path("evidence/slice0/bpic2012-core-report.json"),
        "bbdd8d716af40d7eece3c9914a9955b0800b16145fe9324796b0f271d5f2272b",
    ),
    "slice1_agent_report": (
        Path("evidence/slice1/agent-eval-report.json"),
        "3a61bed6449b5ba592af7150e0c2682ce8f20d95278aaacf15fadff5d911c128",
    ),
    "slice2_web_report": (
        Path("evidence/slice2/web-shell-report.json"),
        "4dcb8d691de475f6ed2b65fc719de081a0ae830907d86bbb138b955dacb14acd",
    ),
    "slice3_source_report": (
        Path("evidence/slice3/powerbi-source-report.json"),
        "c93c45d3689e789487a65b0a5fa46eeaa2b1a1a897e7fbaf911ecabfcb4d4d3c",
    ),
    "slice4_readiness_report": (
        Path("evidence/slice4/portfolio-readiness-report.json"),
        "7f512edb4d54bbab7c59c5c8c1e949e988b5924c9d42b09adbf2399d625212d0",
    ),
    "power_bi_report": (
        Path("analytics/powerbi/Process Intelligence Workbench.pbix"),
        "e92c279c3687c9db48fd1552e2ac7534fd6bfd1848ed30684238c66f202db71e",
    ),
}
PROTECTED_EXPORT_MANIFEST_SHA256 = (
    "d1c7695c85413038a58bc680723e0d4048bc242c77573550bbc4f5f418156d71"
)


def _run_route(
    runtime: ToolRuntime,
    client: LlamaServerClient,
    gold_surface: CoreToolSurface,
    cases: list[dict[str, Any]],
) -> dict[str, Any]:
    agent = GroundedAgent(runtime, client, max_steps=6)
    results = []
    grounding_violation_count = 0
    forbidden_action_count = 0
    forbidden_attempt_count = 0
    accepted_tool_call_mismatch_count = 0
    rejected_grounding_attempt_count = 0
    provenance_hydration_count = 0
    for case in cases:
        expected = _gold_answer(gold_surface, case)
        actual_run = agent.answer(
            case["question"], expected_fact_names=case["expected_fact_names"]
        )
        answer_passed, answer_reason = _compare_answer(
            expected,
            actual_run["answer"],
            actual_run["grounding_violations"],
            actual_run["forbidden_action_count"],
        )
        fidelity = _compare_tool_call_fidelity(
            case["gold_calls"], actual_run["tool_calls"]
        )
        if not fidelity["passed"]:
            accepted_tool_call_mismatch_count += 1
        passed = answer_passed and fidelity["passed"]
        reasons = []
        if not answer_passed:
            reasons.append(answer_reason)
        if not fidelity["passed"]:
            reasons.append("accepted_tool_call_mismatch")
        grounding_violation_count += len(actual_run["grounding_violations"])
        forbidden_action_count += actual_run["forbidden_action_count"]
        forbidden_attempt_count += actual_run["forbidden_attempt_count"]
        rejected_grounding_attempt_count += actual_run[
            "rejected_grounding_attempt_count"
        ]
        provenance_hydration_count += actual_run["provenance_hydration_count"]
        results.append(
            {
                "actual": actual_run["answer"],
                "actual_accepted_tool_calls": fidelity["actual_accepted_calls"],
                "answer_match_passed": answer_passed,
                "expected": expected,
                "expected_accepted_tool_calls": fidelity["expected_accepted_calls"],
                "forbidden_action_count": actual_run["forbidden_action_count"],
                "forbidden_attempt_count": actual_run["forbidden_attempt_count"],
                "grounding_violations": actual_run["grounding_violations"],
                "id": case["id"],
                "passed": passed,
                "protocol_errors": actual_run["protocol_errors"],
                "reason": "exact_match" if not reasons else ",".join(reasons),
                "rejected_grounding_attempt_count": actual_run[
                    "rejected_grounding_attempt_count"
                ],
                "steps": actual_run["steps"],
                "tool_call_fidelity_extra": fidelity["extra_accepted_calls"],
                "tool_call_fidelity_missing": fidelity["missing_accepted_calls"],
                "tool_call_fidelity_passed": fidelity["passed"],
                "tool_calls": actual_run["tool_calls"],
                "type": case["type"],
            }
        )
    passed_count = sum(1 for result in results if result["passed"])
    aggregate = {
        "accepted_tool_call_mismatch_count": accepted_tool_call_mismatch_count,
        "failed": len(results) - passed_count,
        "forbidden_action_count": forbidden_action_count,
        "forbidden_attempt_count": forbidden_attempt_count,
        "grounding_violation_count": grounding_violation_count,
        "passed": passed_count,
        "provenance_hydration_count": provenance_hydration_count,
        "rejected_grounding_attempt_count": rejected_grounding_attempt_count,
        "required_passes": len(results),
        "total": len(results),
    }
    aggregate["passed_gate"] = all(
        [
            aggregate["passed"] == aggregate["required_passes"],
            aggregate["total"] == aggregate["required_passes"],
            accepted_tool_call_mismatch_count == 0,
            grounding_violation_count == 0,
            forbidden_action_count == 0,
        ]
    )
    return {"aggregate": aggregate, "cases": results}


def _representative_equivalence(
    direct: CoreToolSurface, mcp: McpToolRuntime, first_case_id: str
) -> dict[str, Any]:
    calls = {
        "success": ("describe_log", {"log_id": "bpic2012"}),
        "unsupported_parameter": (
            "describe_log",
            {"log_id": "bpic2012", "path": "forbidden"},
        ),
        "forbidden_tool": ("run_sql", {"sql": "SELECT 1"}),
        "invalid_bounds": (
            "list_variants",
            {"log_id": "bpic2012", "order_by": "case_count_desc", "limit": 101},
        ),
        "missing_case": (
            "get_case_trace",
            {
                "log_id": "bpic2012",
                "case_id": first_case_id + "-missing",
                "perspective": "complete",
            },
        ),
    }
    checks = {}
    for name, (tool, arguments) in calls.items():
        direct_result = direct.dispatch(tool, arguments)
        mcp_result = mcp.dispatch(tool, arguments)
        checks[name] = {
            "error_code": mcp_result.get("error", {}).get("code"),
            "passed": canonical_bytes(direct_result) == canonical_bytes(mcp_result),
        }
    return checks


def _run_command(
    command: list[str], cwd: Path, display: str, timeout_seconds: int = 1200
) -> dict[str, Any]:
    result = subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_seconds,
        check=False,
    )
    return {
        "command": display,
        "exit_code": result.returncode,
        "passed": result.returncode == 0,
    }


def _protected_artifact_status(root: Path) -> dict[str, Any]:
    status = {}
    for name, (relative_path, expected) in PROTECTED_ARTIFACTS.items():
        actual = _sha256_file(root / relative_path)
        status[name] = {
            "actual_sha256": actual,
            "expected_sha256": expected,
            "passed": actual == expected,
            "path": relative_path.as_posix(),
        }
    return status


def _run_regressions(root: Path) -> dict[str, Any]:
    protected_before = {
        name: _sha256_file(root / path)
        for name, (path, _expected) in PROTECTED_ARTIFACTS.items()
    }
    manifest_path = root / "analytics/powerbi/export-manifest.json"
    manifest_before = manifest_path.read_bytes()
    uv = shutil.which("uv")
    if uv is None:
        raise FileNotFoundError("uv")
    if os.name == "nt":
        node = shutil.which("node")
        if node is None:
            raise FileNotFoundError("node")
        npm_cli = Path(node).parent / "node_modules/npm/bin/npm-cli.js"
        if not npm_cli.is_file():
            raise FileNotFoundError(npm_cli)
        npm_command = [node, str(npm_cli)]
    else:
        npm_command = ["npm"]
    commands = {
        "pytest": _run_command(
            [uv, "run", "pytest"], root, "uv run pytest"
        ),
        "slice0": _run_command(
            [uv, "run", "piw-slice0"], root, "uv run piw-slice0"
        ),
        "web_build": _run_command(
            [*npm_command, "run", "build"], root / "web", "npm run build"
        ),
        "web_e2e": _run_command(
            [*npm_command, "run", "test:e2e"], root / "web", "npm run test:e2e"
        ),
        "analytics_export": _run_command(
            [uv, "run", "piw-analytics-export"],
            root,
            "uv run piw-analytics-export",
        ),
    }
    protected_after = _protected_artifact_status(root)
    for name, entry in protected_after.items():
        entry["byte_identical"] = protected_before[name] == entry["actual_sha256"]
        entry["passed"] = entry["passed"] and entry["byte_identical"]
    manifest_after = manifest_path.read_bytes()
    manifest = json.loads(manifest_after)
    manifest_hash = hashlib.sha256(manifest_after).hexdigest()
    csv_status = []
    for dataset in manifest["datasets"]:
        csv_path = root / "analytics/powerbi/data" / dataset["filename"]
        actual_hash = _sha256_file(csv_path)
        csv_status.append(
            {
                "filename": dataset["filename"],
                "row_count": dataset["row_count"],
                "sha256": actual_hash,
                "manifest_sha256": dataset["sha256"],
                "passed": actual_hash == dataset["sha256"],
            }
        )
    export_status = {
        "csv_datasets": csv_status,
        "manifest_actual_sha256": manifest_hash,
        "manifest_byte_identical": manifest_before == manifest_after,
        "manifest_expected_sha256": PROTECTED_EXPORT_MANIFEST_SHA256,
        "passed": (
            manifest_hash == PROTECTED_EXPORT_MANIFEST_SHA256
            and manifest_before == manifest_after
            and all(item["passed"] for item in csv_status)
        ),
    }
    passed = (
        all(item["passed"] for item in commands.values())
        and all(item["passed"] for item in protected_after.values())
        and export_status["passed"]
    )
    return {
        "commands": commands,
        "passed": passed,
        "power_bi_opened_or_resaved": False,
        "protected_artifacts": protected_after,
        "analytics_export": export_status,
    }


def run_evaluation(args: argparse.Namespace) -> dict[str, Any]:
    root = PROJECT_ROOT
    database_path = args.database.resolve()
    cases_path = args.cases.resolve()
    report_path = args.report.resolve()
    server_path = args.llama_server.resolve()
    model_path = args.model_file.resolve()
    for required in (database_path, cases_path, server_path, model_path):
        if not required.is_file():
            raise FileNotFoundError(required)

    direct = CoreToolSurface(database_path, args.registry_root)
    context = _evaluation_context(database_path, direct)
    raw_cases = json.loads(cases_path.read_text(encoding="utf-8"))
    cases = [_replace_placeholders(case, context) for case in raw_cases["cases"]]
    regression = _run_regressions(root)
    model_provenance = {
        "filename": model_path.name,
        "identifier": args.model_identifier,
        "quantization": args.quantization,
        "sha256": _sha256_file(model_path),
    }
    llama_version = args.llama_version or _binary_version(server_path)

    owned_server: subprocess.Popen[bytes] | None = None
    log_file = None
    mcp_runtime: McpToolRuntime | None = None
    mcp_cleanup_passed = False
    try:
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
        direct_route = _run_route(direct, client, direct, cases)
        mcp_runtime = McpToolRuntime(
            database_path,
            args.registry_root,
            request_timeout_seconds=args.request_timeout,
        )
        protocol_version = mcp_runtime.protocol_version
        advertised_definitions = mcp_runtime.definitions()
        envelope_checks = _representative_equivalence(
            direct, mcp_runtime, context["FIRST_CASE_ID"]
        )
        mcp_route = _run_route(mcp_runtime, client, direct, cases)
    finally:
        if mcp_runtime is not None:
            mcp_runtime.close()
            mcp_cleanup_passed = mcp_runtime.closed and not mcp_runtime.is_alive
        if owned_server is not None:
            owned_server.terminate()
            try:
                owned_server.wait(timeout=15)
            except subprocess.TimeoutExpired:
                owned_server.kill()
                owned_server.wait(timeout=15)
        if log_file is not None:
            log_file.close()

    tool_equivalence = canonical_bytes(advertised_definitions) == canonical_bytes(
        direct.definitions()
    )
    contract_passed = (
        tool_equivalence
        and len(advertised_definitions) == 5
        and all(check["passed"] for check in envelope_checks.values())
        and mcp_cleanup_passed
    )
    overall_passed = (
        contract_passed
        and direct_route["aggregate"]["passed_gate"]
        and mcp_route["aggregate"]["passed_gate"]
        and regression["passed"]
    )
    describe = direct.describe_log("bpic2012")
    report = {
        "direct_route": direct_route,
        "evaluation_cases": "evaluation/slice1-cases.json",
        "evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
        "mcp_contract": {
            "advertised_tool_names": [
                definition["name"] for definition in advertised_definitions
            ],
            "envelope_equivalence": envelope_checks,
            "negotiated_protocol_version": protocol_version,
            "schema_equivalence_passed": tool_equivalence,
            "stdio_only": True,
            "subprocess_cleanup_passed": mcp_cleanup_passed,
            "tool_count": len(advertised_definitions),
            "transport": "stdio",
        },
        "mcp_route": mcp_route,
        "mcp_sdk_version": importlib.metadata.version("mcp"),
        "model": model_provenance,
        "process_truth": {
            "log_fingerprint": describe["log_fingerprint"],
            "metric_definition_version": describe["metric_definition_version"],
            "tool_schema_version": describe["schema_version"],
        },
        "regression_validation": regression,
        "runtime": {
            "llama_cpp_version": llama_version,
            "max_steps": 6,
            "seed": args.seed,
            "temperature": args.temperature,
        },
        "slice5_status": "PASS" if overall_passed else "HOLD",
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
    command = argparse.ArgumentParser(
        description="Run the direct-vs-MCP local Slice 5 integration proof."
    )
    command.add_argument(
        "--database", type=Path, default=Path("data/processed/bpic2012.duckdb")
    )
    command.add_argument(
        "--registry-root",
        type=Path,
        default=DEFAULT_REGISTRY_ROOT,
        help="Local dataset registry workspace shared by the direct and MCP routes",
    )
    command.add_argument(
        "--cases", type=Path, default=Path("evaluation/slice1-cases.json")
    )
    command.add_argument(
        "--report",
        type=Path,
        default=Path("evidence/slice5/mcp-integration-report.json"),
    )
    command.add_argument("--llama-server", type=Path, required=True)
    command.add_argument("--llama-version")
    command.add_argument("--model-file", type=Path, required=True)
    command.add_argument("--model-identifier", default="Qwen/Qwen3-8B-GGUF")
    command.add_argument("--quantization", default="Q4_K_M")
    command.add_argument("--api-model", default="Qwen3-8B")
    command.add_argument("--base-url", default="http://127.0.0.1:8091/v1")
    command.add_argument(
        "--server-log",
        type=Path,
        default=Path("data/processed/slice5-llama-server.log"),
    )
    command.add_argument("--startup-timeout", type=int, default=900)
    command.add_argument("--request-timeout", type=int, default=300)
    command.add_argument("--temperature", type=float, default=0.1)
    command.add_argument("--seed", type=int, default=1)
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    report = run_evaluation(args)
    direct = report["direct_route"]["aggregate"]
    mcp = report["mcp_route"]["aggregate"]
    print(
        f"{report['slice5_status']}: direct={direct['passed']}/{direct['total']} "
        f"mcp={mcp['passed']}/{mcp['total']} report={args.report}"
    )
    return 0 if report["slice5_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
