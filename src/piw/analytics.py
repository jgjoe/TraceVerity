from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import duckdb

from .core import (
    BPIC12_EXPECTED_SHA256,
    METRIC_DEFINITION_VERSION,
    activity_summary,
    case_analytics,
    complete_traces,
    cycle_time_summary,
    observed_gap_summary,
    raw_summary,
    rework_summary,
    sla_summary,
    transition_summary,
    variant_summary,
)

EXPORT_SCHEMA_VERSION = "slice3-powerbi-export-v1"
CONFIGURED_SLA_THRESHOLD_MS = 604_800_000
LOG_ID = "bpic2012"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "processed" / "bpic2012.duckdb"
DEFAULT_DATA_DIR = PROJECT_ROOT / "analytics" / "powerbi" / "data"
DEFAULT_MANIFEST = PROJECT_ROOT / "analytics" / "powerbi" / "export-manifest.json"

EXPECTED_CANONICAL_SOURCE_FACTS = {
    "analysis_event_count": 164_506,
    "case_count": 13_087,
    "direct_follow_count": 151_419,
    "raw_event_count": 262_200,
    "variant_count": 4_336,
}

CSV_COLUMNS = {
    "summary.csv": (
        "log_id",
        "log_fingerprint",
        "metric_definition_version",
        "case_count",
        "raw_event_count",
        "analysis_event_count",
        "variant_count",
        "direct_follow_count",
        "cycle_time_p50_ms",
        "cycle_time_p90_ms",
        "observed_gap_p50_ms",
        "observed_gap_p90_ms",
        "cases_with_rework",
        "aggregate_rework_event_count",
        "configured_sla_threshold_ms",
        "sla_violation_case_count",
        "sla_violation_case_share",
    ),
    "variants.csv": (
        "rank",
        "variant_id",
        "case_count",
        "case_share",
        "activity_count",
        "activity_sequence",
    ),
    "transitions.csv": (
        "rank",
        "from_activity",
        "to_activity",
        "transition_count",
    ),
    "activities.csv": (
        "rank",
        "activity",
        "event_count",
        "case_count",
        "rework_event_count",
    ),
    "cases.csv": (
        "case_id",
        "cycle_time_ms",
        "complete_event_count",
        "has_rework",
        "rework_event_count",
        "configured_sla_violation",
    ),
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv(
    path: Path, columns: tuple[str, ...], rows: Iterable[Mapping[str, Any]]
) -> int:
    row_count = 0
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=columns,
            extrasaction="raise",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            row_count += 1
    return row_count


def _log_fingerprint(connection: duckdb.DuckDBPyConnection) -> str:
    rows = connection.execute("SELECT sha256 FROM source_metadata").fetchall()
    if len(rows) != 1 or not rows[0][0]:
        raise ValueError("source_metadata must contain exactly one log fingerprint")
    return str(rows[0][0])


def _canonical_json_sequence(sequence: list[str]) -> str:
    return json.dumps(sequence, ensure_ascii=False, separators=(",", ":"))


def export_analytics(
    database_path: Path,
    data_dir: Path,
    manifest_path: Path,
    *,
    validate_expected_facts: bool = True,
) -> dict[str, Any]:
    """Export the fixed Slice 3 Power BI source contract from a read-only database."""

    database_path = Path(database_path)
    if not database_path.is_file():
        raise FileNotFoundError(database_path)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        fingerprint = _log_fingerprint(connection)
        raw = raw_summary(connection)
        analysis_event_count = connection.execute(
            "SELECT count(*) FROM analysis_events"
        ).fetchone()[0]
        traces = complete_traces(connection)
        variants = variant_summary(traces, len(traces))
        transitions = transition_summary(connection)
        activities = sorted(
            activity_summary(connection),
            key=lambda item: (-item["rework_event_count"], item["activity"]),
        )
        cycle_times = cycle_time_summary(connection)
        observed_gaps = observed_gap_summary(connection)
        rework = rework_summary(connection)
        sla = sla_summary(connection, CONFIGURED_SLA_THRESHOLD_MS)
        cases = case_analytics(connection, CONFIGURED_SLA_THRESHOLD_MS)

    source_facts = {
        "analysis_event_count": analysis_event_count,
        "case_count": raw["case_count"],
        "direct_follow_count": transitions["transition_count"],
        "raw_event_count": raw["event_count"],
        "variant_count": variants["variant_count"],
    }
    if validate_expected_facts:
        if fingerprint != BPIC12_EXPECTED_SHA256:
            raise ValueError("database log fingerprint does not match canonical BPIC12")
        mismatches = {
            name: {"actual": source_facts[name], "expected": expected}
            for name, expected in EXPECTED_CANONICAL_SOURCE_FACTS.items()
            if source_facts[name] != expected
        }
        if mismatches:
            raise ValueError(f"canonical source fact mismatch: {mismatches}")

    rows_by_file: dict[str, list[dict[str, Any]]] = {
        "summary.csv": [
            {
                "log_id": LOG_ID,
                "log_fingerprint": fingerprint,
                "metric_definition_version": METRIC_DEFINITION_VERSION,
                "case_count": raw["case_count"],
                "raw_event_count": raw["event_count"],
                "analysis_event_count": analysis_event_count,
                "variant_count": variants["variant_count"],
                "direct_follow_count": transitions["transition_count"],
                "cycle_time_p50_ms": cycle_times["p50_ms"],
                "cycle_time_p90_ms": cycle_times["p90_ms"],
                "observed_gap_p50_ms": observed_gaps["p50_ms"],
                "observed_gap_p90_ms": observed_gaps["p90_ms"],
                "cases_with_rework": rework["cases_with_rework"],
                "aggregate_rework_event_count": rework[
                    "aggregate_rework_event_count"
                ],
                "configured_sla_threshold_ms": CONFIGURED_SLA_THRESHOLD_MS,
                "sla_violation_case_count": sla["violation_case_count"],
                "sla_violation_case_share": sla["violation_case_share"],
            }
        ],
        "variants.csv": [
            {
                "rank": rank,
                "variant_id": item["variant_id"],
                "case_count": item["case_count"],
                "case_share": item["case_share"],
                "activity_count": len(item["activity_sequence"]),
                "activity_sequence": _canonical_json_sequence(
                    item["activity_sequence"]
                ),
            }
            for rank, item in enumerate(variants["variants"], 1)
        ],
        "transitions.csv": [
            {"rank": rank, **item}
            for rank, item in enumerate(transitions["transitions"], 1)
        ],
        "activities.csv": [
            {"rank": rank, **item}
            for rank, item in enumerate(activities, 1)
        ],
        "cases.csv": cases,
    }

    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    dataset_entries = []
    for filename, columns in CSV_COLUMNS.items():
        output = data_dir / filename
        row_count = _write_csv(output, columns, rows_by_file[filename])
        dataset_entries.append(
            {
                "filename": filename,
                "row_count": row_count,
                "sha256": _sha256(output),
            }
        )

    manifest = {
        "configured_sla_threshold_ms": CONFIGURED_SLA_THRESHOLD_MS,
        "datasets": dataset_entries,
        "expected_canonical_source_facts": EXPECTED_CANONICAL_SOURCE_FACTS,
        "export_schema_version": EXPORT_SCHEMA_VERSION,
        "log_fingerprint": fingerprint,
        "metric_definition_version": METRIC_DEFINITION_VERSION,
    }
    manifest_path = Path(manifest_path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(
        description="Export deterministic BPIC12 analytics files for Power BI Desktop."
    )
    command.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    command.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    command.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    manifest = export_analytics(args.database, args.data_dir, args.manifest)
    counts = ", ".join(
        f"{item['filename']}={item['row_count']}" for item in manifest["datasets"]
    )
    print(f"SOURCE EXPORT PASS: {counts}; manifest={args.manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
