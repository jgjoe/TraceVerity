from __future__ import annotations

import json
from pathlib import Path

import pytest

from piw.core import build_report

SOURCE = Path("data/raw/BPI_Challenge_2012.xes.gz")


@pytest.mark.integration
def test_actual_bpic2012_counts_and_determinism(tmp_path: Path) -> None:
    if not SOURCE.is_file():
        pytest.fail(f"canonical integration source is missing: {SOURCE}")
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    report = build_report(SOURCE, tmp_path / "first.duckdb", first, 604_800_000)
    build_report(SOURCE, tmp_path / "second.duckdb", second, 604_800_000)

    assert report["raw_summary"]["case_count"] == 13_087
    assert report["raw_summary"]["event_count"] == 262_200
    assert report["source_fingerprint"]["size_bytes"] == 3_342_406
    assert report["source_fingerprint"]["sha256"] == (
        "5cd9cc16b9bcb20bd4aae45666a5d87479ddbf47e6371618b6ad217174cecdf3"
    )
    assert report["validation"]["status"] == "PASS"
    assert first.read_bytes() == second.read_bytes()
    assert json.loads(first.read_text(encoding="utf-8"))["analysis_event_count"] > 0
