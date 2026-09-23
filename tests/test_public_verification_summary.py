from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


SUMMARY_PATH = Path(__file__).resolve().parents[1] / "evidence/public-verification-summary.json"
ABSOLUTE_PATH = re.compile(r"(?i)(?:[a-z]:[\\/]|file://|/users/|/home/)")
PRIVATE_KEY_PARTS = ("participant", "process_id", "pid", "raw_event_rows", "random_id")


def _summary() -> dict[str, Any]:
    return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))


def _strings(value: Any):
    if isinstance(value, dict):
        for key, child in value.items():
            yield str(key)
            yield from _strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _strings(child)
    elif isinstance(value, str):
        yield value


def test_public_summary_carries_the_bounded_recovery_contract() -> None:
    summary = _summary()
    assert summary["schema_version"] == "traceverity-public-verification-v2"
    assert summary["product"]["product_baseline_revision"] == (
        "f4da118f7f8f8df9beedf0c8b00e1f9bc14fb535"
    )
    assert summary["contracts"] == {
        "metric_definition_version": "slice0-metrics-v1",
        "import_contract_version": "dataset-import-v1",
        "tool_schema_version": "slice-d-tool-v2",
        "http_contract_version": "slice-c-http-v2",
        "analytics_export_version": "slice3-powerbi-export-v1",
    }
    assert summary["datasets"]["bpic2012"]["aggregates"]["case_count"] == 13_087
    assert summary["datasets"]["helpdesk"]["aggregates"]["case_count"] == 4_580
    assert summary["agent_mcp"]["advertised_read_only_tool_count"] == 5
    assert summary["agent_mcp"]["direct_mcp_contract"] == {
        "passed": 22,
        "total": 22,
        "mismatch_count": 0,
    }
    assert summary["failure_boundary"]["path_leak_scan"] == {
        "responses_scanned": 179,
        "leak_count": 0,
    }
    assert summary["usability"]["browser_onboarding_human_tested"] is False
    assert summary["usability"]["recovery_slice_f_cohort"] == "NOT_RUN"
    assert summary["power_bi"]["generic_power_bi"] is False


def test_public_summary_has_no_machine_path_or_private_record_material() -> None:
    summary = _summary()
    serialized = json.dumps(summary, ensure_ascii=False, sort_keys=True)
    assert not ABSOLUTE_PATH.search(serialized)
    assert "evidence/usability" not in serialized.lower()
    assert "duckdb_path" not in serialized.lower()
    assert "raw_rows" not in serialized.lower()
    for value in _strings(summary):
        lowered = value.lower()
        assert not any(part in lowered for part in PRIVATE_KEY_PARTS)
    assert all(summary["distribution_boundary"].values())
