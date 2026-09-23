from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from piw.core import (
    activity_summary,
    build_dataset,
    case_trace,
    complete_traces,
    cycle_time_summary,
    ingest_dataset,
    observed_gap_summary,
    raw_summary,
    rework_summary,
    sla_summary,
    transition_summary,
    variant_summary,
)
from piw.csv_ingest import CsvContractError, csv_trace_count, iter_csv_events
from piw.datasets import (
    CsvColumnMapping,
    CsvImportConfig,
    DatasetRegistry,
    TimestampConfig,
    register_csv,
    register_xes,
)
from piw.events import SourceContractError

HEADER = ("Work Item", "Task", "Timestamp", "Phase", "Performer")
ROWS = [
    ("C-1", "Register", "2024-03-01T08:00:00+01:00", "START", "Ann"),
    ("C-1", "Register", "2024-03-01T08:00:00+01:00", "COMPLETE", "Ann"),
    ("C-1", "Approve", "2024-03-01T09:30:00+01:00", "COMPLETE", "Bob"),
    ("C-2", "Register", "2024-03-01T08:15:00+01:00", "COMPLETE", "Ann"),
    ("C-2", "Register", "2024-03-01T10:15:00+01:00", "COMPLETE", "Cara"),
    ("C-2", "Reject", "2024-03-01T11:00:00+01:00", "", ""),
]
MAPPING = CsvColumnMapping(
    case_id="Work Item",
    activity="Task",
    timestamp="Timestamp",
    lifecycle="Phase",
    resource="Performer",
)
CONFIG = CsvImportConfig(mapping=MAPPING)
EQUIVALENT_XES = """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="C-1"/>
    <event>
      <string key="concept:name" value="Register"/>
      <string key="lifecycle:transition" value="START"/>
      <date key="time:timestamp" value="2024-03-01T08:00:00+01:00"/>
      <string key="org:resource" value="Ann"/>
    </event>
    <event>
      <string key="concept:name" value="Register"/>
      <string key="lifecycle:transition" value="COMPLETE"/>
      <date key="time:timestamp" value="2024-03-01T08:00:00+01:00"/>
      <string key="org:resource" value="Ann"/>
    </event>
    <event>
      <string key="concept:name" value="Approve"/>
      <string key="lifecycle:transition" value="COMPLETE"/>
      <date key="time:timestamp" value="2024-03-01T09:30:00+01:00"/>
      <string key="org:resource" value="Bob"/>
    </event>
  </trace>
  <trace>
    <string key="concept:name" value="C-2"/>
    <event>
      <string key="concept:name" value="Register"/>
      <string key="lifecycle:transition" value="COMPLETE"/>
      <date key="time:timestamp" value="2024-03-01T08:15:00+01:00"/>
      <string key="org:resource" value="Ann"/>
    </event>
    <event>
      <string key="concept:name" value="Register"/>
      <string key="lifecycle:transition" value="COMPLETE"/>
      <date key="time:timestamp" value="2024-03-01T10:15:00+01:00"/>
      <string key="org:resource" value="Cara"/>
    </event>
    <event>
      <string key="concept:name" value="Reject"/>
      <date key="time:timestamp" value="2024-03-01T11:00:00+01:00"/>
    </event>
  </trace>
</log>
"""


def write_csv(
    path: Path,
    rows: list[list[str]] | list[tuple[str, ...]],
    header: tuple[str, ...] = HEADER,
    delimiter: str = ",",
) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=delimiter, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    return path


def registry(tmp_path: Path) -> DatasetRegistry:
    return DatasetRegistry(tmp_path / "datasets")


def csv_dataset(tmp_path: Path, registry_root: DatasetRegistry, log_id: str = "orders"):
    return register_csv(
        registry_root,
        log_id=log_id,
        display_name="Order log",
        source_path=write_csv(tmp_path / f"{log_id}.csv", ROWS),
        config=CONFIG,
    )


def test_arbitrary_source_columns_enter_the_canonical_core_path(tmp_path: Path) -> None:
    datasets = registry(tmp_path)
    descriptor = csv_dataset(tmp_path, datasets)

    report = build_dataset(descriptor, report_path=tmp_path / "report.json")

    assert report["dataset"] == {
        "dataset_id": descriptor.dataset_id,
        "display_name": "Order log",
        "log_id": "orders",
        "source_format": "csv",
    }
    assert report["report_schema_version"] == "dataset-report-v1"
    assert report["metric_definition_version"] == "slice0-metrics-v1"
    assert set(report["source_fingerprint"]) == {"filename", "sha256", "size_bytes"}
    assert report["validation"]["status"] == "PASS"
    invariant_names = [item["name"] for item in report["validation"]["invariants"]]
    assert not any(name.startswith("expected_") for name in invariant_names)
    assert "sla_scenario_covers_raw_cases" not in invariant_names
    assert report["raw_summary"]["case_count"] == 2
    assert report["raw_summary"]["event_count"] == 6
    assert report["raw_summary"]["distinct_activity_count"] == 3
    assert report["analysis_event_count"] == 5
    assert report["variant_summary"]["variant_count"] == 2
    assert report["transition_summary"]["transition_count"] == 3
    assert report["rework_summary"]["aggregate_rework_event_count"] == 1
    assert report["cycle_time_summary"]["p90_ms"] == 9_900_000
    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8")) == report

    with duckdb.connect(str(descriptor.database_path), read_only=True) as connection:
        assert [row[0] for row in connection.execute("DESCRIBE events").fetchall()] == [
            "case_id",
            "activity",
            "event_ts_utc_ms",
            "event_pos",
            "lifecycle",
            "resource",
        ]
        assert connection.execute(
            "SELECT count(*) FROM analysis_events"
        ).fetchone()[0] == 5
        assert raw_summary(connection) == report["raw_summary"]
        assert complete_traces(connection) == [
            ("C-1", ["Register", "Approve"]),
            ("C-2", ["Register", "Register", "Reject"]),
        ]
        assert variant_summary(complete_traces(connection), 2) == report["variant_summary"]
        assert transition_summary(connection) == report["transition_summary"]
        assert cycle_time_summary(connection) == report["cycle_time_summary"]
        assert observed_gap_summary(connection) == report["observed_gap_summary"]
        assert rework_summary(connection) == report["rework_summary"]
        assert activity_summary(connection) == [
            {"activity": "Register", "case_count": 2, "event_count": 3, "rework_event_count": 1},
            {"activity": "Approve", "case_count": 1, "event_count": 1, "rework_event_count": 0},
            {"activity": "Reject", "case_count": 1, "event_count": 1, "rework_event_count": 0},
        ]
        assert case_trace(connection, "C-2", "raw") == [
            {
                "activity": "Register",
                "event_pos": 0,
                "event_ts_utc_ms": 1_709_277_300_000,
                "lifecycle": "COMPLETE",
                "resource": "Ann",
            },
            {
                "activity": "Register",
                "event_pos": 1,
                "event_ts_utc_ms": 1_709_284_500_000,
                "lifecycle": "COMPLETE",
                "resource": "Cara",
            },
            {
                "activity": "Reject",
                "event_pos": 2,
                "event_ts_utc_ms": 1_709_287_200_000,
                "lifecycle": None,
                "resource": None,
            },
        ]


def test_xes_and_csv_sources_produce_identical_core_facts(tmp_path: Path) -> None:
    datasets = registry(tmp_path)
    csv_descriptor = csv_dataset(tmp_path, datasets, log_id="orders")
    xes_path = tmp_path / "equivalent.xes"
    xes_path.write_text(EQUIVALENT_XES, encoding="utf-8")
    xes_descriptor = register_xes(
        datasets,
        log_id="equivalent-xes",
        display_name="Equivalent log",
        source_path=xes_path,
    )

    csv_report = build_dataset(csv_descriptor, sla_threshold_ms=7_200_000)
    xes_report = build_dataset(xes_descriptor, sla_threshold_ms=7_200_000)

    assert csv_descriptor.dataset_id != xes_descriptor.dataset_id
    shared_keys = [
        "analysis_event_count",
        "cycle_time_summary",
        "metric_definition_version",
        "observed_gap_summary",
        "process_summary",
        "raw_summary",
        "rework_summary",
        "sla_scenario",
        "transition_summary",
        "validation",
        "variant_summary",
    ]
    assert {key: csv_report[key] for key in shared_keys} == {
        key: xes_report[key] for key in shared_keys
    }
    assert csv_report["sla_scenario"]["violation_case_count"] == 1


def test_lifecycle_and_resource_mapping_are_optional(tmp_path: Path) -> None:
    datasets = registry(tmp_path)
    source = write_csv(
        tmp_path / "minimal.csv",
        [row[:3] for row in ROWS],
        header=HEADER[:3],
    )
    descriptor = register_csv(
        datasets,
        log_id="minimal",
        display_name="Minimal log",
        source_path=source,
        config=CsvImportConfig(
            mapping=CsvColumnMapping(
                case_id="Work Item", activity="Task", timestamp="Timestamp"
            )
        ),
    )

    report = build_dataset(descriptor)

    assert "sla_scenario" not in report
    assert report["analysis_event_count"] == report["raw_summary"]["event_count"] == 6
    with duckdb.connect(str(descriptor.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM events WHERE lifecycle IS NOT NULL OR resource IS NOT NULL"
        ).fetchone()[0] == 0


def test_ingest_result_reports_source_traces_and_inserted_events(tmp_path: Path) -> None:
    datasets = registry(tmp_path)
    descriptor = csv_dataset(tmp_path, datasets)
    assert csv_trace_count(descriptor.source_path, CONFIG) == 2

    with duckdb.connect(str(descriptor.database_path)) as connection:
        result = ingest_dataset(connection, descriptor)

    assert result.trace_count == 2
    assert result.inserted == 6
    assert result.fingerprint.sha256 == descriptor.sha256
    assert result.fingerprint.provenance.doi is None


@pytest.mark.parametrize("missing", ["case_id", "activity", "timestamp"])
def test_missing_required_mapping_fails_closed(missing: str) -> None:
    fields = {"case_id": "A", "activity": "B", "timestamp": "C"}
    fields[missing] = ""
    with pytest.raises(SourceContractError, match=missing):
        CsvColumnMapping(**fields)


def test_duplicate_canonical_mapping_fails_closed() -> None:
    with pytest.raises(SourceContractError, match="distinct source column"):
        CsvColumnMapping(case_id="A", activity="A", timestamp="C")
    with pytest.raises(SourceContractError, match="distinct source column"):
        CsvColumnMapping(case_id="A", activity="B", timestamp="C", lifecycle="C")


def test_mapped_column_absent_from_header_fails_closed(tmp_path: Path) -> None:
    source = write_csv(tmp_path / "absent.csv", ROWS, header=("Work Item", "Task", "When"))
    with pytest.raises(CsvContractError, match="absent from the CSV header"):
        list(iter_csv_events(source, CONFIG))


@pytest.mark.parametrize(
    ("header", "match"),
    [
        (("Work Item", "Task", "Timestamp", "Task", "Performer"), "duplicate CSV header"),
        (("Work Item", "Task", "Timestamp", "", "Performer"), "has no name"),
    ],
)
def test_malformed_header_fails_closed(
    tmp_path: Path, header: tuple[str, ...], match: str
) -> None:
    source = write_csv(tmp_path / "header.csv", ROWS, header=header)
    with pytest.raises(CsvContractError, match=match):
        list(iter_csv_events(source, CONFIG))


def test_duplicate_unmapped_header_columns_are_tolerated(tmp_path: Path) -> None:
    """Real-world exports repeat unmapped columns; only mapped columns must be unique."""

    header = ("Notes", "Work Item", "Notes", "Task", "Timestamp", "Phase", "Performer")
    padded = [("first", row[0], "second", *row[1:]) for row in ROWS]
    duplicated = write_csv(tmp_path / "duplicated-extra.csv", padded, header=header)
    clean = write_csv(tmp_path / "clean.csv", ROWS, header=HEADER)

    assert list(iter_csv_events(duplicated, CONFIG)) == list(iter_csv_events(clean, CONFIG))


def test_row_field_count_mismatch_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "ragged.csv"
    source.write_text(
        "Work Item,Task,Timestamp\nC-1,Register,2024-03-01T08:00:00+01:00,extra\n",
        encoding="utf-8",
        newline="",
    )
    with pytest.raises(CsvContractError, match="expected 3 columns, found 4"):
        list(
            iter_csv_events(
                source,
                CsvImportConfig(
                    mapping=CsvColumnMapping(
                        case_id="Work Item", activity="Task", timestamp="Timestamp"
                    )
                ),
            )
        )


def test_non_utf8_source_fails_closed(tmp_path: Path) -> None:
    source = tmp_path / "latin1.csv"
    source.write_bytes("Work Item,Task,Timestamp\nC-1,R\xe9gister,2024-03-01T08:00:00+01:00\n".encode("latin-1"))
    with pytest.raises(CsvContractError, match="must be UTF-8"):
        list(
            iter_csv_events(
                source,
                CsvImportConfig(
                    mapping=CsvColumnMapping(
                        case_id="Work Item", activity="Task", timestamp="Timestamp"
                    )
                ),
            )
        )


@pytest.mark.parametrize(
    ("column", "value"),
    [("Work Item", ""), ("Task", "")],
)
def test_empty_required_values_fail_closed(
    tmp_path: Path, column: str, value: str
) -> None:
    row = list(ROWS[0])
    row[HEADER.index(column)] = value
    source = write_csv(tmp_path / "empty.csv", [row])
    with pytest.raises(CsvContractError, match=f"column '{column}' must not be empty"):
        list(iter_csv_events(source, CONFIG))


def test_invalid_timestamp_fails_closed(tmp_path: Path) -> None:
    row = list(ROWS[0])
    row[HEADER.index("Timestamp")] = "01/03/2024 08:00"
    source = write_csv(tmp_path / "invalid.csv", [row])
    with pytest.raises(CsvContractError, match="invalid timestamp"):
        list(iter_csv_events(source, CONFIG))


def test_offset_less_timestamp_requires_explicit_timezone(tmp_path: Path) -> None:
    row = list(ROWS[1])
    row[HEADER.index("Timestamp")] = "2024-03-01T08:00:00"
    source = write_csv(tmp_path / "naive.csv", [row])

    with pytest.raises(CsvContractError, match="no explicit timezone interpretation"):
        list(iter_csv_events(source, CONFIG))

    configured = CsvImportConfig(
        mapping=MAPPING, timestamp=TimestampConfig(assume_timezone="+02:00")
    )
    events = list(iter_csv_events(source, configured))
    assert [event.event_ts_utc_ms for event in events] == [1_709_272_800_000]
    assert events[0].event_ts_utc_ms != 1_709_247_600_000


def test_configured_timestamp_format_parses_offset_less_values(tmp_path: Path) -> None:
    row = list(ROWS[1])
    row[HEADER.index("Timestamp")] = "01.03.2024 08:00:00"
    source = write_csv(tmp_path / "formatted.csv", [row])
    config = CsvImportConfig(
        mapping=MAPPING,
        timestamp=TimestampConfig(
            assume_timezone="UTC+02:00", timestamp_format="%d.%m.%Y %H:%M:%S"
        ),
    )

    assert [event.event_ts_utc_ms for event in iter_csv_events(source, config)] == [
        1_709_272_800_000
    ]


def test_configured_delimiter_is_honored(tmp_path: Path) -> None:
    source = write_csv(tmp_path / "semicolon.csv", ROWS, delimiter=";")
    config = CsvImportConfig(mapping=MAPPING, delimiter=";")

    assert len(list(iter_csv_events(source, config))) == 6
    with pytest.raises(CsvContractError, match="absent from the CSV header"):
        list(iter_csv_events(source, CONFIG))


def test_empty_source_fails_closed(tmp_path: Path) -> None:
    empty = write_csv(tmp_path / "empty.csv", [])
    with pytest.raises(CsvContractError, match="contains no data rows"):
        list(iter_csv_events(empty, CONFIG))


@pytest.mark.parametrize("phase", ["open", "closed", ""])
def test_unusable_lifecycle_mapping_fails_closed(tmp_path: Path, phase: str) -> None:
    source = write_csv(
        tmp_path / "unusable-lifecycle.csv",
        [[*row[:3], phase, row[4]] for row in ROWS],
    )
    with pytest.raises(CsvContractError, match="canonical completion rule"):
        list(iter_csv_events(source, CONFIG))


def test_lowercase_completion_lifecycle_is_included(tmp_path: Path) -> None:
    datasets = registry(tmp_path)
    source = write_csv(
        tmp_path / "case-lifecycle.csv",
        [
            ("C-1", "Register", "2024-03-01T08:00:00+01:00", "start", "Ann"),
            ("C-1", "Register", "2024-03-01T08:00:00+01:00", "complete", "Ann"),
            ("C-1", "Approve", "2024-03-01T09:00:00+01:00", "", ""),
        ],
    )
    descriptor = register_csv(
        datasets,
        log_id="case-lifecycle",
        display_name="Lifecycle casing",
        source_path=source,
        config=CONFIG,
    )

    report = build_dataset(descriptor)

    assert report["raw_summary"]["event_count"] == 3
    assert report["analysis_event_count"] == 2
