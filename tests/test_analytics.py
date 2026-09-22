from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from piw.analytics import CSV_COLUMNS, export_analytics
from piw.core import case_analytics, install_events
from piw.xes import Event

DATABASE = Path("data/processed/bpic2012.duckdb")


def _event(case_id: str, activity: str, timestamp: int, position: int) -> Event:
    return Event(case_id, activity, timestamp, position, "COMPLETE", None)


def _fixture_database(path: Path) -> Path:
    with duckdb.connect(str(path)) as connection:
        install_events(
            connection,
            [
                _event("equal", "A", 0, 0),
                _event("equal", "A", 100, 1),
                _event("over", "A", 0, 0),
                _event("over", "B", 101, 1),
            ],
            {
                "doi": "fixture",
                "filename": "fixture.xes",
                "sha256": "c" * 64,
                "size_bytes": 1,
                "source_url": "https://example.invalid/fixture",
            },
        )
    return path


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_case_analytics_preserves_strict_sla_and_rework_semantics(
    tmp_path: Path,
) -> None:
    database = _fixture_database(tmp_path / "fixture.duckdb")
    with duckdb.connect(str(database), read_only=True) as connection:
        rows = case_analytics(connection, 100)
    assert rows == [
        {
            "case_id": "equal",
            "cycle_time_ms": 100,
            "complete_event_count": 2,
            "has_rework": True,
            "rework_event_count": 1,
            "configured_sla_violation": False,
        },
        {
            "case_id": "over",
            "cycle_time_ms": 101,
            "complete_event_count": 2,
            "has_rework": False,
            "rework_event_count": 0,
            "configured_sla_violation": True,
        },
    ]


def test_export_schema_hashes_ordering_and_two_run_determinism(
    tmp_path: Path,
) -> None:
    database = _fixture_database(tmp_path / "fixture.duckdb")
    first_data = tmp_path / "first" / "data"
    second_data = tmp_path / "second" / "data"
    first_manifest_path = tmp_path / "first" / "manifest.json"
    second_manifest_path = tmp_path / "second" / "manifest.json"
    first = export_analytics(
        database,
        first_data,
        first_manifest_path,
        validate_expected_facts=False,
    )
    second = export_analytics(
        database,
        second_data,
        second_manifest_path,
        validate_expected_facts=False,
    )

    assert first == second
    assert first_manifest_path.read_bytes() == second_manifest_path.read_bytes()
    for dataset in first["datasets"]:
        filename = dataset["filename"]
        assert (first_data / filename).read_bytes() == (second_data / filename).read_bytes()
        assert dataset["sha256"] == _digest(first_data / filename)
        with (first_data / filename).open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            assert tuple(reader.fieldnames or ()) == CSV_COLUMNS[filename]

    with (first_data / "variants.csv").open(encoding="utf-8", newline="") as source:
        variants = list(csv.DictReader(source))
    assert [row["rank"] for row in variants] == ["1", "2"]
    assert variants[0]["activity_sequence"] == '["A","A"]'
    with (first_data / "cases.csv").open(encoding="utf-8", newline="") as source:
        cases = list(csv.DictReader(source))
    assert [row["case_id"] for row in cases] == ["equal", "over"]


@pytest.mark.integration
def test_actual_bpic2012_analytics_export(tmp_path: Path) -> None:
    if not DATABASE.is_file():
        pytest.fail(f"canonical integration database is missing: {DATABASE}")
    data_dir = tmp_path / "data"
    manifest_path = tmp_path / "export-manifest.json"
    manifest = export_analytics(DATABASE, data_dir, manifest_path)
    second_data_dir = tmp_path / "second-data"
    second_manifest_path = tmp_path / "second-export-manifest.json"
    second_manifest = export_analytics(
        DATABASE, second_data_dir, second_manifest_path
    )
    row_counts = {
        item["filename"]: item["row_count"] for item in manifest["datasets"]
    }
    assert row_counts == {
        "summary.csv": 1,
        "variants.csv": 4_336,
        "transitions.csv": 138,
        "activities.csv": 23,
        "cases.csv": 13_087,
    }
    with (data_dir / "summary.csv").open(encoding="utf-8", newline="") as source:
        summary = next(csv.DictReader(source))
    assert summary["case_count"] == "13087"
    assert summary["raw_event_count"] == "262200"
    assert summary["analysis_event_count"] == "164506"
    assert summary["direct_follow_count"] == "151419"
    assert json.loads(manifest_path.read_text(encoding="utf-8")) == manifest
    assert manifest == second_manifest
    assert manifest_path.read_bytes() == second_manifest_path.read_bytes()
    for dataset in manifest["datasets"]:
        filename = dataset["filename"]
        assert (data_dir / filename).read_bytes() == (
            second_data_dir / filename
        ).read_bytes()
