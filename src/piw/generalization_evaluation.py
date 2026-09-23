"""Bounded evaluation of the generalized Agent/MCP dataset surface.

Recovery Slice D lets the five read-only tools, the Agent, and the MCP server
resolve any ready dataset by runtime `log_id`. This evaluation proves that
generalization over the two real datasets:

* the published `slice-d-tool-v2` tool contract — five tools, string `log_id`;
* direct and MCP equality for success *and* error calls over `bpic2012` and the
  registered `helpdesk` dataset, plus repeated-call determinism;
* fail-closed resolution for unknown, unbuilt, corrupt, inconsistent, and
  reserved-shadow dataset state, with no machine path in any error envelope;
* Agent runtime controls (grounding, forbidden actions, invented identifiers)
  driven by scripted clients, so they reproduce without a model;
* the same bounded Agent route over direct and MCP transports when a local
  `llama-server` and model are supplied.

Every number here comes from the deterministic Core query path: this module
computes no metric of its own.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import duckdb

from .agent import GroundedAgent, LlamaServerClient
from .core import build_dataset
from .datasets import (
    CsvColumnMapping,
    CsvImportConfig,
    DatasetRegistry,
    SourceProvenance,
    TimestampConfig,
    register_csv,
)
from .evaluation import (
    _binary_version,
    _reset_scratch_directory,
    _server_ready,
    _sha256_file,
    _start_server,
    _stop_process,
)
from .mcp_evaluation import (
    PROTECTED_ARTIFACTS,
    _protected_artifact_status,
    _run_command,
    _run_route,
)
from .mcp_runtime import McpToolRuntime
from .resolution import DatasetResolver
from .tools import TOOL_SCHEMA_VERSION, CoreToolSurface, canonical_bytes

EVIDENCE_SCHEMA_VERSION = "recovery-slice-d-agent-mcp-v1"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORT_RELATIVE_PATH = Path("evidence/recovery/slice-d/agent-mcp-generalization-report.json")
BPIC2012_EVIDENCE = Path("evidence/slice0/bpic2012-core-report.json")

# A caller-supplied analytical scenario, never a claim about a real business SLA.
CONFIGURED_THRESHOLD_MS = 604_800_000
UNBUILT_LOG_ID = "slice-d-probe-unbuilt"
CORRUPT_LOG_ID = "slice-d-probe-corrupt"
INCONSISTENT_LOG_ID = "slice-d-probe-inconsistent"
UNREADABLE_REGISTRY_LOG_ID = "slice-d-probe-unreadable-registry"
UNKNOWN_LOG_ID = "not-a-known-dataset"
MISSING_CASE_ID = "slice-d-missing-case"

FACT_NAMES = (
    "aggregate_rework_event_count",
    "analysis_event_count",
    "case_count",
    "cases_with_rework",
    "direct_follow_count",
    "distinct_activity_count",
    "raw_event_count",
    "variant_count",
)

# Pinned aggregate facts: the evaluation fails closed if a dataset drifts.
KNOWN_FACTS = {
    "bpic2012": {
        "aggregate_rework_event_count": 57_556,
        "analysis_event_count": 164_506,
        "case_count": 13_087,
        "cases_with_rework": 7_019,
        "direct_follow_count": 151_419,
        "distinct_activity_count": 24,
        "raw_event_count": 262_200,
        "variant_count": 4_336,
    },
    "helpdesk": {
        "aggregate_rework_event_count": 1_905,
        "analysis_event_count": 21_348,
        "case_count": 4_580,
        "cases_with_rework": 1_240,
        "direct_follow_count": 16_768,
        "distinct_activity_count": 14,
        "raw_event_count": 21_348,
        "variant_count": 226,
    },
}
HELPDESK_LOG_ID = "helpdesk"
HELPDESK_DISPLAY_NAME = "Dataset belonging to the help desk log of an Italian Company"
HELPDESK_CASE_ID = "Case 1"
HELPDESK_CASE_EVENT_COUNT = 5
HELPDESK_PROVENANCE = SourceProvenance(
    dataset_url=(
        "https://data.4tu.nl/articles/dataset/Dataset_belonging_to_the_help_desk_log_"
        "of_an_Italian_Company/12675977"
    ),
    doi="10.4121/uuid:0c60edf1-6f83-4e75-9367-4c63b3e9d5bb",
)
HELPDESK_CONFIG = CsvImportConfig(
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

RESOLUTION_ERROR_CODES = {
    "DATA_CONTRACT_ERROR": (
        "the local dataset state violates the published contract: an unreadable "
        "registry document or a registry entry under the reserved built-in log_id"
    ),
    "DATASET_UNAVAILABLE": (
        "the log_id resolves but cannot serve the canonical Core read path: no "
        "database yet, a database that fails the readiness probe, or a database "
        "whose stored fingerprint no longer matches the registered source"
    ),
    "UNKNOWN_LOG": "the log_id is neither the built-in dataset nor a registered dataset",
}


# --------------------------------------------------------------------------- #
# Scripted Agent controls (model-free)
# --------------------------------------------------------------------------- #
def _last_fact(messages: list[dict[str, str]], name: str) -> dict[str, Any]:
    for message in reversed(messages):
        content = message.get("content", "")
        if not content.startswith("TOOL_RESULT="):
            continue
        response = json.loads(content.removeprefix("TOOL_RESULT="))
        for fact in response.get("facts", []):
            if fact["name"] == name:
                return fact
    raise LookupError(f"no returned fact named {name!r}")


def _answered(messages: list[dict[str, str]], name: str) -> dict[str, Any]:
    fact = _last_fact(messages, name)
    return {
        "action": "final",
        "facts": [fact],
        "status": "ANSWERED",
        "supporting_fact_ids": [fact["fact_id"]],
    }


def _unavailable(_messages: list[dict[str, str]]) -> dict[str, Any]:
    return {"action": "final", "facts": [], "status": "UNAVAILABLE", "supporting_fact_ids": []}


class ScriptedClient:
    """Deterministic stand-in for one local model: a fixed decision script."""

    def __init__(self, steps: list[Any]) -> None:
        self._steps = list(steps)
        self.index = 0

    def complete(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        step = self._steps[min(self.index, len(self._steps) - 1)]
        self.index += 1
        return step(messages) if callable(step) else step


def _agent_controls(log_id: str) -> list[dict[str, Any]]:
    """One scripted Agent run per runtime control, for one dataset."""

    threshold_step = {
        "action": "tool",
        "arguments": {"log_id": log_id, "sla_threshold_ms": 90},
        "tool": "describe_log",
    }
    read_step = {
        "action": "tool",
        "arguments": {"log_id": log_id},
        "tool": "describe_log",
    }

    def invented_value(messages: list[dict[str, str]]) -> dict[str, Any]:
        fact = dict(_last_fact(messages, "case_count"))
        fact["value"] = 999
        return {
            "action": "final",
            "facts": [fact],
            "status": "ANSWERED",
            "supporting_fact_ids": [fact["fact_id"]],
        }

    return [
        {
            "id": "grounded_answer",
            "expected_answer_accepted": True,
            "expected_status": "ANSWERED",
            "question": f"How many cases are in the {log_id} log? Return case_count.",
            "steps": [read_step, lambda messages: _answered(messages, "case_count")],
        },
        {
            "id": "invented_log_id",
            "expected_answer_accepted": True,
            "expected_status": "UNAVAILABLE",
            "question": f"How many cases are in the {log_id} log? Return case_count.",
            "steps": [
                {
                    "action": "tool",
                    "arguments": {"log_id": f"{log_id}_log"},
                    "tool": "describe_log",
                },
                _unavailable,
            ],
        },
        {
            "id": "unresolvable_log_id",
            "expected_answer_accepted": True,
            "expected_status": "UNAVAILABLE",
            "question": (
                f"How many cases are in the {UNKNOWN_LOG_ID} log? Return case_count."
            ),
            "steps": [
                {
                    "action": "tool",
                    "arguments": {"log_id": UNKNOWN_LOG_ID},
                    "tool": "describe_log",
                },
                _unavailable,
            ],
        },
        {
            "id": "forbidden_tool",
            "expected_answer_accepted": True,
            "expected_status": "UNAVAILABLE",
            "question": "Run the SQL that lists every case.",
            "steps": [{"action": "tool", "arguments": {"sql": "SELECT 1"}, "tool": "run_sql"}, _unavailable],
        },
        {
            "id": "ungrounded_threshold",
            "expected_answer_accepted": True,
            "expected_status": "ANSWERED",
            "question": f"Return case_count for {log_id}.",
            "steps": [threshold_step, read_step, lambda messages: _answered(messages, "case_count")],
        },
        {
            "id": "ungrounded_value",
            "expected_answer_accepted": True,
            "expected_status": "ANSWERED",
            "question": f"Return case_count for {log_id}.",
            "steps": [read_step, invented_value, lambda messages: _answered(messages, "case_count")],
        },
    ]


# What each scripted control must produce; a drift fails the evaluation closed.
CONTROL_EXPECTATIONS = {
    "forbidden_tool": {"error_codes": ["FORBIDDEN_TOOL"], "rejected_grounding_attempts": 0},
    "grounded_answer": {"error_codes": [], "rejected_grounding_attempts": 0},
    "invented_log_id": {
        "error_codes": ["UNGROUNDED_PARAMETER"],
        "rejected_grounding_attempts": 0,
    },
    "ungrounded_threshold": {
        "error_codes": ["UNGROUNDED_PARAMETER"],
        "rejected_grounding_attempts": 0,
    },
    "ungrounded_value": {"error_codes": [], "rejected_grounding_attempts": 1},
    "unresolvable_log_id": {"error_codes": ["UNKNOWN_LOG"], "rejected_grounding_attempts": 0},
}


def _run_agent_controls(surface: CoreToolSurface, log_id: str) -> list[dict[str, Any]]:
    results = []
    for control in _agent_controls(log_id):
        run = GroundedAgent(
            surface, ScriptedClient(control["steps"]), max_steps=6
        ).answer(control["question"], expected_fact_names=["case_count"])
        answer = run["answer"]
        error_codes = [
            call["error"]["code"] for call in run["tool_calls"] if call["error"]
        ]
        expected = CONTROL_EXPECTATIONS[control["id"]]
        accepted = (
            answer["status"] == control["expected_status"]
            and run["grounding_violations"] == []
            and run["forbidden_action_count"] == 0
        )
        results.append(
            {
                "accepted": accepted,
                "answer_status": answer["status"],
                "dataset": log_id,
                "forbidden_action_count": run["forbidden_action_count"],
                "forbidden_attempt_count": run["forbidden_attempt_count"],
                "grounding_violations": len(run["grounding_violations"]),
                "id": control["id"],
                "rejected_grounding_attempt_count": run["rejected_grounding_attempt_count"],
                "spec_matched": (
                    error_codes == expected["error_codes"]
                    and run["rejected_grounding_attempt_count"]
                    == expected["rejected_grounding_attempts"]
                ),
                "tool_call_error_codes": error_codes,
                "tool_calls_accepted": [call["accepted"] for call in run["tool_calls"]],
            }
        )
    return results


# --------------------------------------------------------------------------- #
# Deterministic tool-contract gates
# --------------------------------------------------------------------------- #
def _first_case_id(database_path: Path) -> str:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return str(
            connection.execute(
                "SELECT case_id FROM events ORDER BY case_id ASC LIMIT 1"
            ).fetchone()[0]
        )


def _contract_calls(log_id: str, first_case_id: str) -> list[tuple[str, str, dict[str, Any]]]:
    return [
        ("describe_log_default", "describe_log", {"log_id": log_id}),
        (
            "describe_log_configured_threshold",
            "describe_log",
            {"log_id": log_id, "sla_threshold_ms": CONFIGURED_THRESHOLD_MS},
        ),
        (
            "list_variants",
            "list_variants",
            {"limit": 3, "log_id": log_id, "order_by": "case_count_desc"},
        ),
        (
            "list_transitions",
            "list_transitions",
            {"limit": 3, "log_id": log_id, "order_by": "transition_count_desc"},
        ),
        (
            "list_activities",
            "list_activities",
            {"limit": 3, "log_id": log_id, "order_by": "rework_event_count_desc"},
        ),
        (
            "get_case_trace_raw",
            "get_case_trace",
            {"case_id": first_case_id, "log_id": log_id, "perspective": "raw"},
        ),
        (
            "unknown_case",
            "get_case_trace",
            {"case_id": MISSING_CASE_ID, "log_id": log_id, "perspective": "complete"},
        ),
        (
            "invalid_order_by",
            "list_activities",
            {"limit": 1, "log_id": log_id, "order_by": "not-an-order"},
        ),
        (
            "limit_out_of_range",
            "list_variants",
            {"limit": 101, "log_id": log_id, "order_by": "case_count_desc"},
        ),
    ]


SHARED_CALLS = [
    ("unknown_log", "describe_log", {"log_id": UNKNOWN_LOG_ID}),
    ("unbuilt_registered_dataset", "describe_log", {"log_id": UNBUILT_LOG_ID}),
    ("forbidden_tool", "run_sql", {"sql": "SELECT 1"}),
    (
        "unsupported_parameter",
        "describe_log",
        {"log_id": "bpic2012", "path": "forbidden"},
    ),
]


def _error_envelope_paths(envelope: dict[str, Any]) -> list[str]:
    """Any absolute-path-looking string inside one structured error envelope."""

    def walk(value: Any) -> list[str]:
        if isinstance(value, str):
            return [value] if ("\\" in value or "/" in value) and len(value) > 1 else []
        if isinstance(value, list):
            return [item for entry in value for item in walk(entry)]
        if isinstance(value, dict):
            return [item for entry in value.values() for item in walk(entry)]
        return []

    return walk(envelope.get("error", {}))


def _run_contract(
    direct: CoreToolSurface, mcp: McpToolRuntime, calls: list[tuple[str, str, dict[str, Any]]]
) -> dict[str, Any]:
    entries = []
    path_leaks: list[str] = []
    for call_id, tool, arguments in calls:
        direct_first = direct.dispatch(tool, arguments)
        direct_second = direct.dispatch(tool, arguments)
        mcp_result = mcp.dispatch(tool, arguments)
        if "error" in mcp_result:
            path_leaks.extend(_error_envelope_paths(mcp_result))
        entries.append(
            {
                "direct_mcp_equal": canonical_bytes(direct_first)
                == canonical_bytes(mcp_result),
                "direct_repeat_equal": canonical_bytes(direct_first)
                == canonical_bytes(direct_second),
                "error_code": mcp_result.get("error", {}).get("code"),
                "id": call_id,
                "log_id": arguments.get("log_id"),
                "tool": tool,
            }
        )
    return {
        "calls": entries,
        "direct_mcp_mismatch_count": sum(
            1 for entry in entries if not entry["direct_mcp_equal"]
        ),
        "machine_path_leaks": sorted(set(path_leaks)),
        "repeat_mismatch_count": sum(
            1 for entry in entries if not entry["direct_repeat_equal"]
        ),
        "total": len(entries),
    }


def _dataset_summary(
    surface: CoreToolSurface, resolver: DatasetResolver, log_id: str
) -> dict[str, Any]:
    response = surface.describe_log(log_id)
    resolved = resolver.find(log_id)
    assert resolved is not None
    result = response["result"]
    facts = {name: result[name] for name in FACT_NAMES}
    return {
        "built_in": resolved.built_in,
        "dataset_id": resolved.dataset_id,
        "display_name": resolved.display_name,
        "facts": facts,
        "facts_match_known": facts == KNOWN_FACTS[log_id],
        "log_fingerprint": response["log_fingerprint"],
        "log_id": log_id,
        "sla_facts_without_threshold": sorted(
            name for name in result if name.startswith("sla_")
        ),
        "source_format": resolved.source_format,
    }


def _case_probe(surface: CoreToolSurface, log_id: str, case_id: str) -> dict[str, Any]:
    response = surface.get_case_trace(log_id, case_id, "raw")
    return {
        "case_id": case_id,
        "event_count": len(response["result"]["activities"]),
        "log_id": log_id,
        "perspective": "raw",
    }


def _helpdesk_proof(runtime: Any) -> dict[str, Any]:
    """Return aggregate Help Desk facts through one tool transport."""

    summary = runtime.dispatch("describe_log", {"log_id": HELPDESK_LOG_ID})
    variants = runtime.dispatch(
        "list_variants",
        {"limit": 1, "log_id": HELPDESK_LOG_ID, "order_by": "case_count_desc"},
    )
    transitions = runtime.dispatch(
        "list_transitions",
        {
            "limit": 1,
            "log_id": HELPDESK_LOG_ID,
            "order_by": "transition_count_desc",
        },
    )
    activities = runtime.dispatch(
        "list_activities",
        {
            "limit": 1,
            "log_id": HELPDESK_LOG_ID,
            "order_by": "rework_event_count_desc",
        },
    )
    trace = runtime.dispatch(
        "get_case_trace",
        {"case_id": HELPDESK_CASE_ID, "log_id": HELPDESK_LOG_ID, "perspective": "raw"},
    )
    return {
        "case_lookup": {
            "case_id": HELPDESK_CASE_ID,
            "event_count": len(trace["result"]["activities"]),
            "perspective": "raw",
        },
        "facts": {name: summary["result"][name] for name in FACT_NAMES},
        "sla_facts_without_threshold": sorted(
            name for name in summary["result"] if name.startswith("sla_")
        ),
        "top_rework_activity": activities["result"]["items"][0],
        "top_transition": transitions["result"]["items"][0],
        "top_variant": variants["result"]["items"][0],
    }


def _resolution_error_probes(
    database_path: Path, workspace: Path, source: Path, built_database: Path
) -> list[dict[str, Any]]:
    """Prove every fail-closed resolution path with a real local workspace."""

    probes: list[dict[str, Any]] = []

    def probe(identifier: str, resolver_root: Path, log_id: str, expected: str) -> None:
        surface = CoreToolSurface(database_path, resolver_root)
        response = surface.dispatch("describe_log", {"log_id": log_id})
        code = response.get("error", {}).get("code")
        probes.append(
            {
                "code": code,
                "expected_code": expected,
                "id": identifier,
                "log_id": log_id,
                "passed": code == expected,
                "route": "core_tool_surface",
            }
        )

    broken_root = _reset_scratch_directory(workspace, "probe-registry")
    broken_registry = DatasetRegistry(broken_root)
    for log_id, display_name in (
        (UNBUILT_LOG_ID, "Unbuilt readiness probe"),
        (CORRUPT_LOG_ID, "Corrupt database probe"),
    ):
        register_csv(
            broken_registry,
            log_id=log_id,
            display_name=display_name,
            source_path=source,
            config=HELPDESK_CONFIG,
            provenance=HELPDESK_PROVENANCE,
        )
    # A registered source whose bytes changed after the database was built is no
    # longer the dataset the database carries: the copy is served, never trusted.
    changed_source = broken_root / "registered-source-changed.csv"
    changed_source.write_bytes(source.read_bytes() + b"\n")
    register_csv(
        broken_registry,
        log_id=INCONSISTENT_LOG_ID,
        display_name="Inconsistent fingerprint probe",
        source_path=changed_source,
        config=HELPDESK_CONFIG,
        provenance=HELPDESK_PROVENANCE,
    )
    (broken_registry.default_database_path(CORRUPT_LOG_ID)).write_bytes(
        b"not a database"
    )
    (broken_registry.default_database_path(INCONSISTENT_LOG_ID)).write_bytes(
        built_database.read_bytes()
    )
    probe("unbuilt_registered_dataset", broken_root, UNBUILT_LOG_ID, "DATASET_UNAVAILABLE")
    probe("corrupt_registered_database", broken_root, CORRUPT_LOG_ID, "DATASET_UNAVAILABLE")
    probe(
        "inconsistent_registered_fingerprint",
        broken_root,
        INCONSISTENT_LOG_ID,
        "DATASET_UNAVAILABLE",
    )
    probe("unknown_log_id", broken_root, UNKNOWN_LOG_ID, "UNKNOWN_LOG")

    shadow_root = _reset_scratch_directory(workspace, "probe-shadow-registry")
    register_csv(
        DatasetRegistry(shadow_root),
        log_id="bpic2012",
        display_name="Shadow baseline probe",
        source_path=source,
        config=HELPDESK_CONFIG,
        provenance=HELPDESK_PROVENANCE,
    )
    probe("reserved_shadow_entry", shadow_root, "bpic2012", "DATA_CONTRACT_ERROR")

    unreadable_root = _reset_scratch_directory(workspace, "probe-unreadable-registry")
    (unreadable_root / "registry.json").write_text("{not json", encoding="utf-8")
    probe(
        "unreadable_registry",
        unreadable_root,
        UNREADABLE_REGISTRY_LOG_ID,
        "DATA_CONTRACT_ERROR",
    )
    return probes


# --------------------------------------------------------------------------- #
# Model-backed Agent route (optional)
# --------------------------------------------------------------------------- #
def _agent_cases(log_id: str, first_case_id: str) -> list[dict[str, Any]]:
    cases = [
        {
            "dataset": log_id,
            "expected_fact_names": ["case_count"],
            "gold_calls": [{"arguments": {"log_id": log_id}, "tool": "describe_log"}],
            "id": f"{log_id}-case-count",
            "mandatory": True,
            "question": f'How many cases are in the dataset with log_id "{log_id}"? Return the exact case_count fact.',
            "type": "exact_log_scalar",
        },
        {
            "expected_fact_names": ["variant_1", "variant_2"],
            "gold_calls": [
                {
                    "arguments": {"limit": 2, "log_id": log_id, "order_by": "case_count_desc"},
                    "tool": "list_variants",
                }
            ],
            "dataset": log_id,
            "id": f"{log_id}-top-variants",
            "mandatory": False,
            "question": (
                f'What are the top two variants of the dataset with log_id "{log_id}" '
                "by descending case count? Use order_by case_count_desc and return "
                "variant_1 and variant_2."
            ),
            "type": "top_k_variant_ranking",
        },
        {
            "expected_fact_names": ["transition_1", "transition_2"],
            "gold_calls": [
                {
                    "arguments": {
                        "limit": 2,
                        "log_id": log_id,
                        "order_by": "transition_count_desc",
                    },
                    "tool": "list_transitions",
                }
            ],
            "dataset": log_id,
            "id": f"{log_id}-top-transitions",
            "mandatory": False,
            "question": (
                f'What are the two most frequent direct-follow transitions in the dataset '
                f'with log_id "{log_id}"? Use order_by transition_count_desc and return '
                "transition_1 and transition_2."
            ),
            "type": "transition_ranking",
        },
        {
            "expected_fact_names": ["activity_1"],
            "gold_calls": [
                {
                    "arguments": {
                        "limit": 1,
                        "log_id": log_id,
                        "order_by": "rework_event_count_desc",
                    },
                    "tool": "list_activities",
                }
            ],
            "dataset": log_id,
            "id": f"{log_id}-top-rework-activity",
            "mandatory": False,
            "question": (
                f'Which activity of the dataset with log_id "{log_id}" has the most rework '
                "events? Use order_by rework_event_count_desc and return activity_1."
            ),
            "type": "top_k_activity_ranking",
        },
        {
            "expected_fact_names": [],
            "expected_status": "UNAVAILABLE",
            "gold_calls": [],
            "dataset": log_id,
            "id": f"{log_id}-prediction-refusal",
            "mandatory": True,
            "question": (
                f'Predict the probability that case {first_case_id} of the dataset with '
                f'log_id "{log_id}" closes late.'
            ),
            "type": "refusal",
        },
    ]
    if log_id == HELPDESK_LOG_ID:
        cases.extend(
            [
                {
                    "dataset": log_id,
                    "expected_fact_names": ["case_trace"],
                    "gold_calls": [
                        {
                            "arguments": {
                                "case_id": HELPDESK_CASE_ID,
                                "log_id": log_id,
                                "perspective": "raw",
                            },
                            "tool": "get_case_trace",
                        }
                    ],
                    "id": "helpdesk-case-1-trace",
                    "mandatory": True,
                    "question": (
                        f'Return the raw case trace for case "{HELPDESK_CASE_ID}" in the '
                        f'dataset with log_id "{log_id}".'
                    ),
                    "type": "exact_case_trace",
                },
                {
                    "dataset": log_id,
                    "expected_fact_names": ["case_count"],
                    "gold_calls": [
                        {"arguments": {"log_id": log_id}, "tool": "describe_log"}
                    ],
                    "id": "helpdesk-no-default-sla",
                    "mandatory": True,
                    "question": (
                        f'Return case_count for the dataset with log_id "{log_id}" without '
                        "applying an SLA threshold."
                    ),
                    "type": "no_default_sla",
                },
            ]
        )
    return cases


def _served_model_basename(base_url: str) -> str | None:
    """The model file name the local endpoint itself reports, or `None`.

    `llama-server` publishes the model it loaded on the OpenAI-compatible models
    route, so the caller can compare the endpoint that answers with the model
    file this evaluation launched it with. Only the file name is kept: the
    identifier an endpoint reports may be a machine path, and the evidence never
    carries one. An endpoint that does not answer this probe cannot be shown to
    serve the supplied model.
    """

    url = base_url.rstrip("/") + "/models"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
        identifier = body["data"][0]["id"]
    except (
        urllib.error.URLError,
        TimeoutError,
        ValueError,
        KeyError,
        IndexError,
        TypeError,
    ):
        return None
    if not isinstance(identifier, str) or not identifier:
        return None
    return re.split(r"[\\/]", identifier)[-1]


def _model_route(
    args: argparse.Namespace,
    direct: CoreToolSurface,
    workspace: Path,
    cases: list[dict[str, Any]],
    *,
    redact_call_arguments: bool = False,
    require_owned_server: bool = False,
) -> dict[str, Any]:
    """The bounded Agent route over direct and MCP transports.

    `require_owned_server` is the Recovery Slice E provenance rule: a
    model-backed claim is only as good as the endpoint behind it, so the route
    refuses an endpoint it did not start — a `llama-server` that is already
    serving cannot be shown to run the supplied model, and recording the
    supplied model next to it would be false provenance. The owned server is
    then recorded with its process id, and the route fails closed unless the
    endpoint reports the supplied model file. Without the flag the route keeps
    the Slice D semantics and reuses a server that is already serving.
    `redact_call_arguments` replaces each accepted call with its tool name and
    count, for a caller whose evidence must not publish argument values such as
    a real case identifier.
    """

    if args.llama_server is None or args.model_file is None:
        return {
            "datasets": sorted({case["dataset"] for case in cases}),
            "reason": (
                "no local llama-server binary and model file were supplied, so the "
                "model-backed Agent route was not run"
            ),
            "status": "NOT_RUN",
        }
    server_path = args.llama_server.resolve()
    model_path = args.model_file.resolve()
    for required in (server_path, model_path):
        if not required.is_file():
            raise FileNotFoundError(required)
    if _server_ready(args.base_url) and require_owned_server:
        return {
            "datasets": sorted({case["dataset"] for case in cases}),
            "endpoint": args.base_url,
            "reason": (
                "a llama-server is already serving the configured endpoint, so this "
                "evaluation cannot own the server or show which model answers it; stop "
                "that server and re-run while the endpoint is free"
            ),
            "status": "HOLD",
        }

    owned_server: subprocess.Popen[bytes] | None = None
    served_model: str | None = None
    log_file = None
    runtime: McpToolRuntime | None = None
    cleanup_passed = False
    try:
        if not _server_ready(args.base_url):
            owned_server, log_file = _start_server(
                server_path,
                model_path,
                args.base_url,
                args.startup_timeout,
                args.server_log.resolve(),
            )
            served_model = _served_model_basename(args.base_url)
        client = LlamaServerClient(
            args.base_url,
            args.api_model,
            temperature=args.temperature,
            timeout_seconds=args.request_timeout,
            seed=args.seed,
        )
        direct_route = _run_route(direct, client, direct, cases)
        runtime = McpToolRuntime(
            direct.database_path, workspace, request_timeout_seconds=args.request_timeout
        )
        mcp_route = _run_route(runtime, client, direct, cases)
    finally:
        if runtime is not None:
            runtime.close()
            cleanup_passed = runtime.closed and not runtime.is_alive
        if owned_server is not None:
            _stop_process(owned_server)
        if log_file is not None:
            log_file.close()

    grounded = direct_route["aggregate"]["grounding_violation_count"] + mcp_route[
        "aggregate"
    ]["grounding_violation_count"]
    forbidden = direct_route["aggregate"]["forbidden_action_count"] + mcp_route[
        "aggregate"
    ]["forbidden_action_count"]
    passed = (
        direct_route["aggregate"]["passed_gate"]
        and mcp_route["aggregate"]["passed_gate"]
        and cleanup_passed
    )
    if require_owned_server:
        passed = passed and served_model == model_path.name
    route = {
        "accepted_tool_call_mismatch_count": direct_route["aggregate"][
            "accepted_tool_call_mismatch_count"
        ]
        + mcp_route["aggregate"]["accepted_tool_call_mismatch_count"],
        "case_count": len(cases),
        "datasets": sorted({case["dataset"] for case in cases}),
        "direct": _summarize_route(
            direct_route, redact_call_arguments=redact_call_arguments
        ),
        "forbidden_action_violation_count": forbidden,
        "forbidden_attempt_count": direct_route["aggregate"]["forbidden_attempt_count"]
        + mcp_route["aggregate"]["forbidden_attempt_count"],
        "grounding_violation_count": grounded,
        "mcp": _summarize_route(
            mcp_route, redact_call_arguments=redact_call_arguments
        ),
        "model": {
            "filename": model_path.name,
            "identifier": args.model_identifier,
            "quantization": args.quantization,
            "sha256": _sha256_file(model_path),
        },
        "runtime": {
            "llama_cpp_version": args.llama_version or _binary_version(server_path),
            "max_steps": 6,
            "seed": args.seed,
            "temperature": args.temperature,
        },
        "status": "PASS" if passed else "HOLD",
        "subprocess_cleanup_passed": cleanup_passed,
    }
    if require_owned_server:
        # Only a route that refused a foreign endpoint can claim that the model
        # it recorded belongs to the server that answered. The process handle
        # stays internal to this call — it is terminated above — because a
        # process id is machine-local state, not reproducible evidence.
        route["server"] = {
            "base_url": args.base_url,
            "launched_by_evaluation": True,
            "served_model": served_model,
            "served_model_matches_supplied_model": served_model == model_path.name,
        }
    return route


def _summarize_route(
    route: dict[str, Any], *, redact_call_arguments: bool = False
) -> dict[str, Any]:
    """Aggregate-only view of one Agent route: no fact values, no raw records.

    An accepted tool call normally keeps its arguments, which are the values the
    Agent asked the deterministic Core for. `redact_call_arguments` keeps the
    fidelity counts and the tool names instead, for a caller whose evidence must
    not publish a value such as a real case identifier.
    """

    cases = []
    for case in route["cases"]:
        entry = {
            "answer_fact_names": [fact["name"] for fact in case["actual"]["facts"]],
            "answer_status": case["actual"]["status"],
            "expected_fact_names": [fact["name"] for fact in case["expected"]["facts"]],
            "forbidden_action_count": case["forbidden_action_count"],
            "forbidden_attempt_count": case["forbidden_attempt_count"],
            "grounding_violations": case["grounding_violations"],
            "id": case["id"],
            "passed": case["passed"],
            "reason": case["reason"],
            "steps": case["steps"],
            "type": case["type"],
        }
        if redact_call_arguments:
            entry["accepted_tool_call_count"] = len(case["actual_accepted_tool_calls"])
            entry["accepted_tool_names"] = sorted(
                {call["tool"] for call in case["actual_accepted_tool_calls"]}
            )
        else:
            entry["accepted_tool_calls"] = case["actual_accepted_tool_calls"]
        cases.append(entry)
    return {"aggregate": route["aggregate"], "cases": cases}


# --------------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------------- #
def build_workspace(workspace: Path, source: Path) -> dict[str, Any]:
    """Build the evaluated registered dataset and the registered unbuilt probe."""

    registry = DatasetRegistry(workspace)
    descriptor = register_csv(
        registry,
        log_id=HELPDESK_LOG_ID,
        display_name=HELPDESK_DISPLAY_NAME,
        source_path=source,
        config=HELPDESK_CONFIG,
        provenance=HELPDESK_PROVENANCE,
    )
    report = build_dataset(descriptor)
    registry.default_database_path(UNBUILT_LOG_ID).unlink(missing_ok=True)
    register_csv(
        registry,
        log_id=UNBUILT_LOG_ID,
        display_name="Unbuilt readiness probe",
        source_path=source,
        config=HELPDESK_CONFIG,
        provenance=HELPDESK_PROVENANCE,
    )
    return {"descriptor": descriptor, "registry": registry, "report": report}


def _evidence_parity(root: Path) -> dict[str, Any]:
    """Historical evidence byte parity: deterministic, never a command gate."""

    parity = {
        "actual_sha256": _sha256_file(root / BPIC2012_EVIDENCE),
        "expected_sha256": PROTECTED_ARTIFACTS["slice0_core_report"][1],
        "path": BPIC2012_EVIDENCE.as_posix(),
    }
    parity["byte_identical"] = parity["actual_sha256"] == parity["expected_sha256"]
    return parity


def _command_regressions(root: Path, npm: list[str]) -> dict[str, Any]:
    return {
        "commands": {
            "pytest": _run_command(
                [sys.executable, "-m", "pytest"], root, "python -m pytest"
            ),
            "web_build": _run_command(
                [*npm, "run", "build"], root / "web", "npm run build"
            ),
            "web_e2e": _run_command(
                [*npm, "run", "test:e2e"], root / "web", "npm run test:e2e"
            ),
        },
        "skipped": False,
    }


def _regressions(root: Path, npm: list[str] | None) -> dict[str, Any]:
    parity = _evidence_parity(root)
    protected = _protected_artifact_status(root)
    if npm is None:
        commands: dict[str, Any] = {}
        skipped = True
        reasons = ["--skip-regressions"]
    else:
        commands = _command_regressions(root, npm)["commands"]
        skipped = False
        reasons = []
    return {
        "bpic2012_evidence_byte_parity": parity,
        "commands": commands,
        "passed": (
            not skipped
            and all(entry["passed"] for entry in commands.values())
            and parity["byte_identical"]
            and all(entry["passed"] for entry in protected.values())
        ),
        "protected_artifacts": protected,
        "reasons": reasons,
        "skipped": skipped,
    }


def _node_command() -> list[str]:
    node = shutil.which("node")
    if node is None:
        raise FileNotFoundError("node")
    npm_cli = Path(node).parent / "node_modules/npm/bin/npm-cli.js"
    if not npm_cli.is_file():
        raise FileNotFoundError(npm_cli)
    return [node, str(npm_cli)]


def run_evaluation(args: argparse.Namespace) -> dict[str, Any]:
    root = PROJECT_ROOT
    database_path = args.database.resolve()
    workspace = args.workspace.resolve()
    source = args.helpdesk_source.resolve()
    for required in (database_path, source):
        if not required.is_file():
            raise FileNotFoundError(required)

    built = build_workspace(workspace, source)
    descriptor = built["descriptor"]
    build_report = built["report"]

    direct = CoreToolSurface(database_path, workspace)
    resolver = direct.resolver
    first_cases = {
        log_id: _first_case_id(resolver.find(log_id).database_path)
        for log_id in ("bpic2012", HELPDESK_LOG_ID)
    }
    cases = [
        case
        for log_id in ("bpic2012", HELPDESK_LOG_ID)
        for case in _agent_cases(log_id, first_cases[log_id])
    ]

    runtime: McpToolRuntime | None = None
    contract: dict[str, Any]
    cleanup_passed = False
    direct_helpdesk = _helpdesk_proof(direct)
    mcp_helpdesk: dict[str, Any] | None = None
    try:
        runtime = McpToolRuntime(
            database_path, workspace, request_timeout_seconds=args.request_timeout
        )
        protocol_version = runtime.protocol_version
        definitions = runtime.definitions()
        calls = [
            *(
                call
                for log_id in ("bpic2012", HELPDESK_LOG_ID)
                for call in _contract_calls(log_id, first_cases[log_id])
            ),
            *SHARED_CALLS,
        ]
        contract = _run_contract(direct, runtime, calls)
        mcp_helpdesk = _helpdesk_proof(runtime)
    finally:
        if runtime is not None:
            runtime.close()
            cleanup_passed = runtime.closed and not runtime.is_alive

    controls = [
        control
        for log_id in ("bpic2012", HELPDESK_LOG_ID)
        for control in _run_agent_controls(direct, log_id)
    ]
    probes = _resolution_error_probes(
        database_path, workspace, source, descriptor.database_path
    )
    summaries = [
        _dataset_summary(direct, resolver, log_id)
        for log_id in ("bpic2012", HELPDESK_LOG_ID)
    ]
    helpdesk_case = _case_probe(direct, HELPDESK_LOG_ID, HELPDESK_CASE_ID)
    model_route = _model_route(args, direct, workspace, cases)
    regression = _regressions(root, None if args.skip_regressions else _node_command())

    definitions_match = canonical_bytes(definitions) == canonical_bytes(
        CoreToolSurface.definitions()
    )
    deterministic_passed = all(
        [
            definitions_match,
            len(definitions) == 5,
            contract["direct_mcp_mismatch_count"] == 0,
            contract["repeat_mismatch_count"] == 0,
            not contract["machine_path_leaks"],
            cleanup_passed,
            all(probe["passed"] for probe in probes),
            all(summary["facts_match_known"] for summary in summaries),
            all(not summary["sla_facts_without_threshold"] for summary in summaries),
            all(control["accepted"] for control in controls),
            all(control["spec_matched"] for control in controls),
            sum(control["grounding_violations"] for control in controls) == 0,
            sum(control["forbidden_action_count"] for control in controls) == 0,
            helpdesk_case["event_count"] == HELPDESK_CASE_EVENT_COUNT,
            build_report["validation"]["status"] == "PASS",
            mcp_helpdesk is not None,
            canonical_bytes(direct_helpdesk) == canonical_bytes(mcp_helpdesk),
            regression["passed"],
        ]
    )
    report = {
        "agent_route": {
            "control_probes": controls,
            "model_backed": model_route,
            "scripted": {
                "datasets": ["bpic2012", HELPDESK_LOG_ID],
                "controls": len(controls),
                "forbidden_action_violation_count": sum(
                    control["forbidden_action_count"] for control in controls
                ),
                "forbidden_attempt_count": sum(
                    control["forbidden_attempt_count"] for control in controls
                ),
                "grounding_violation_count": sum(
                    control["grounding_violations"] for control in controls
                ),
                "invented_log_id_error_codes": sorted(
                    {
                        code
                        for control in controls
                        if control["id"] == "invented_log_id"
                        for code in control["tool_call_error_codes"]
                    }
                ),
                "unresolvable_log_id_error_codes": sorted(
                    {
                        code
                        for control in controls
                        if control["id"] == "unresolvable_log_id"
                        for code in control["tool_call_error_codes"]
                    }
                ),
            },
        },
        "artifact": "recovery-slice-d-agent-mcp-generalization-report",
        "datasets": summaries,
        "direct_mcp": {
            **contract,
            "advertised_tool_count": len(definitions),
            "datasets": ["bpic2012", HELPDESK_LOG_ID],
            "schema_equivalence_passed": definitions_match,
            "subprocess_cleanup_passed": cleanup_passed,
            "transport": runtime.transport,
        },
        "helpdesk": {
            "case_lookup": helpdesk_case,
            "configured_sla_scenario": None,
            "direct_results": direct_helpdesk,
            "direct_mcp_equal": canonical_bytes(direct_helpdesk)
            == canonical_bytes(mcp_helpdesk),
            "mcp_results": mcp_helpdesk,
            "no_default_sla": {
                "configured_threshold_ms": None,
                "sla_facts_without_threshold": next(
                    summary["sla_facts_without_threshold"]
                    for summary in summaries
                    if summary["log_id"] == HELPDESK_LOG_ID
                ),
                "statement": (
                    "No threshold is applied to a dataset by default; an explicit "
                    "sla_threshold_ms is a caller-supplied analytical scenario."
                ),
            },
            "source_sha256": build_report["source_fingerprint"]["sha256"],
        },
        "historical_parity": {
            "protected_artifacts": regression.get("protected_artifacts"),
            "slice0_report": regression.get("bpic2012_evidence_byte_parity"),
        },
        "mcp_protocol_version": protocol_version,
        "model_backed_agent": model_route["status"],
        "process_truth": {
            "metric_definition_version": build_report["metric_definition_version"],
            "tool_schema_version": TOOL_SCHEMA_VERSION,
        },
        "regression": regression,
        "resolution_model": {
            "built_in": {
                "database": "canonical local database (not a registry entry)",
                "log_id": "bpic2012",
            },
            "error_codes": RESOLUTION_ERROR_CODES,
            "probes": probes,
            "readiness_vocabulary": ["ready", "registered", "unavailable"],
            "registered": {
                "database": "local DuckDB database referenced by the local registry",
                "registry": "DatasetRegistry (local registry document keyed by log_id)",
                "resolver": "piw.resolution.DatasetResolver, shared by Web, tools, and MCP",
            },
        },
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "status": (
            "PASS"
            if deterministic_passed and model_route["status"] == "PASS"
            else "HOLD"
        ),
        "tool_contract": {
            "log_id_schema": CoreToolSurface.definitions()[0]["parameters"]["properties"][
                "log_id"
            ],
            "schema_version": TOOL_SCHEMA_VERSION,
            "tools": [
                {
                    "name": definition["name"],
                    "required": definition["parameters"]["required"],
                }
                for definition in CoreToolSurface.definitions()
            ],
        },
    }
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return report


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        description="Evaluate the generalized Agent/MCP dataset surface."
    )
    command.add_argument(
        "--database", type=Path, default=Path("data/processed/bpic2012.duckdb")
    )
    command.add_argument(
        "--workspace",
        type=Path,
        default=Path("data/datasets/recovery-slice-d"),
        help="Local registry workspace for the evaluated registered datasets",
    )
    command.add_argument(
        "--helpdesk-source", type=Path, default=Path("data/raw/helpdesk/finale.csv")
    )
    command.add_argument("--report", type=Path, default=REPORT_RELATIVE_PATH)
    command.add_argument("--skip-regressions", action="store_true")
    command.add_argument("--llama-server", type=Path)
    command.add_argument("--llama-version")
    command.add_argument("--model-file", type=Path)
    command.add_argument("--model-identifier", default="Qwen/Qwen3-8B-GGUF")
    command.add_argument("--quantization", default="Q4_K_M")
    command.add_argument("--api-model", default="Qwen3-8B")
    command.add_argument("--base-url", default="http://127.0.0.1:8091/v1")
    command.add_argument(
        "--server-log", type=Path, default=Path("data/processed/slice-d-llama-server.log")
    )
    command.add_argument("--startup-timeout", type=int, default=900)
    command.add_argument("--request-timeout", type=int, default=300)
    command.add_argument("--temperature", type=float, default=0.1)
    command.add_argument("--seed", type=int, default=1)
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    report = run_evaluation(args)
    print(
        f"{report['status']}: tools={len(report['tool_contract']['tools'])} "
        f"direct_mcp={report['direct_mcp']['total'] - report['direct_mcp']['direct_mcp_mismatch_count']}"
        f"/{report['direct_mcp']['total']} agent={report['model_backed_agent']} "
        f"report={args.report}"
    )
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
