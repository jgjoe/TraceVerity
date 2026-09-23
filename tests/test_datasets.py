from __future__ import annotations

import csv
from pathlib import Path

import pytest

from piw.core import build_dataset, build_report
from piw.datasets import (
    CsvColumnMapping,
    CsvImportConfig,
    DatasetRegistry,
    DatasetRegistryError,
    SourceProvenance,
    register_csv,
    register_xes,
)
from piw.events import SourceContractError

HEADER = ("Work Item", "Task", "Timestamp")
ROWS = [
    ("C-1", "Register", "2024-03-01T08:00:00+01:00"),
    ("C-1", "Approve", "2024-03-01T09:00:00+01:00"),
    ("C-2", "Register", "2024-03-01T08:30:00+01:00"),
]
CONFIG = CsvImportConfig(
    mapping=CsvColumnMapping(case_id="Work Item", activity="Task", timestamp="Timestamp")
)
MINIMAL_XES = """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="C-1"/>
    <event>
      <string key="concept:name" value="Register"/>
      <date key="time:timestamp" value="2024-03-01T08:00:00+01:00"/>
    </event>
  </trace>
</log>
"""


def write_source(tmp_path: Path, name: str = "orders.csv") -> Path:
    path = tmp_path / name
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(HEADER)
        writer.writerows(ROWS)
    return path


def test_registration_is_idempotent_and_identity_uses_bytes_and_config(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    source = write_source(tmp_path)
    descriptor = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=source,
        config=CONFIG,
    )
    registry_bytes = datasets.registry_path.read_bytes()

    repeated = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=source,
        config=CONFIG,
    )
    assert datasets.registry_path.read_bytes() == registry_bytes

    alias = register_csv(
        datasets,
        log_id="orders-alias",
        display_name="Order log",
        source_path=source,
        config=CONFIG,
    )

    assert repeated.dataset_id == descriptor.dataset_id
    assert alias.dataset_id == descriptor.dataset_id


def test_reregistration_from_a_new_path_resolves_under_the_same_identity(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    first = write_source(tmp_path, "first.csv")
    descriptor = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=first,
        config=CONFIG,
    )

    second = tmp_path / "second.csv"
    second.write_bytes(first.read_bytes())
    first.unlink()
    reimported = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=second,
        config=CONFIG,
    )

    assert reimported.dataset_id == descriptor.dataset_id
    assert reimported.source_path == second.resolve()
    assert not descriptor.source_path.exists()

    registry_bytes = datasets.registry_path.read_bytes()
    repeated = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=second,
        config=CONFIG,
    )
    assert repeated.source_path == second.resolve()
    assert datasets.registry_path.read_bytes() == registry_bytes

    report = build_dataset(reimported, report_path=tmp_path / "report.json")

    assert report["dataset"]["dataset_id"] == descriptor.dataset_id
    assert report["validation"]["status"] == "PASS"
    assert report["raw_summary"]["event_count"] == 3
    reloaded = DatasetRegistry(tmp_path / "datasets").get("orders")
    assert reloaded.source_path == second.resolve()
    assert build_dataset(reloaded) == report


def test_changed_mapping_is_distinguishable_and_needs_a_new_log_id(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    source = write_source(tmp_path)
    descriptor = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=source,
        config=CONFIG,
    )
    remapped = CsvImportConfig(
        mapping=CsvColumnMapping(
            case_id="Work Item", activity="Task", timestamp="Timestamp"
        ),
        delimiter=";",
    )
    changed = register_csv(
        datasets,
        log_id="orders-semicolon",
        display_name="Order log",
        source_path=source,
        config=remapped,
    )

    assert changed.sha256 == descriptor.sha256
    assert changed.dataset_id != descriptor.dataset_id
    with pytest.raises(DatasetRegistryError, match="different dataset identity"):
        register_csv(
            datasets,
            log_id="orders",
            display_name="Order log",
            source_path=source,
            config=remapped,
        )


def test_registry_resolves_log_id_to_local_database(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    descriptor = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=write_source(tmp_path),
        config=CONFIG,
    )
    assert descriptor.build_state == "registered"

    build_dataset(descriptor)

    reloaded = DatasetRegistry(tmp_path / "datasets").get("orders")
    assert reloaded == descriptor
    assert reloaded.build_state == "built"
    assert reloaded.database_path == datasets.default_database_path("orders")
    assert reloaded.database_path.is_file()
    assert DatasetRegistry(tmp_path / "datasets").log_ids() == ["orders"]
    with pytest.raises(DatasetRegistryError, match="unknown log_id"):
        datasets.get("missing")


def test_rebuild_is_idempotent(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    descriptor = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=write_source(tmp_path),
        config=CONFIG,
    )

    first = build_dataset(descriptor, report_path=tmp_path / "first.json")
    second = build_dataset(descriptor, report_path=tmp_path / "second.json")

    assert first == second
    assert (tmp_path / "first.json").read_bytes() == (tmp_path / "second.json").read_bytes()
    assert first["validation"]["status"] == "PASS"
    assert first["dataset"]["dataset_id"] == descriptor.dataset_id


def test_changed_source_bytes_fail_closed(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    source = write_source(tmp_path)
    descriptor = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=source,
        config=CONFIG,
    )
    source.write_text(source.read_text(encoding="utf-8") + "C-3,Close,2024-03-01T10:00:00+01:00\n", encoding="utf-8")

    with pytest.raises(SourceContractError, match="re-register the dataset"):
        build_dataset(descriptor)


def test_provenance_is_optional_and_preserved(tmp_path: Path) -> None:
    datasets = DatasetRegistry(tmp_path / "datasets")
    plain = register_csv(
        datasets,
        log_id="orders",
        display_name="Order log",
        source_path=write_source(tmp_path),
        config=CONFIG,
    )
    plain_report = build_dataset(plain)
    assert set(plain_report["source_fingerprint"]) == {"filename", "sha256", "size_bytes"}

    xes_path = tmp_path / "attributed.xes"
    xes_path.write_text(MINIMAL_XES, encoding="utf-8")
    attributed = register_xes(
        datasets,
        log_id="attributed",
        display_name="Attributed log",
        source_path=xes_path,
        provenance=SourceProvenance(
            dataset_url="https://example.invalid/dataset",
            doi="10.1234/example",
            source_url="https://example.invalid/log",
        ),
    )
    attributed_report = build_dataset(attributed)

    assert {
        key: attributed_report["source_fingerprint"][key]
        for key in ("dataset_url", "doi", "source_url")
    } == {
        "dataset_url": "https://example.invalid/dataset",
        "doi": "10.1234/example",
        "source_url": "https://example.invalid/log",
    }
    assert DatasetRegistry(tmp_path / "datasets").get("attributed").provenance.doi == "10.1234/example"


def test_bpic2012_profile_gate_is_separate_from_structural_validation(tmp_path: Path) -> None:
    source = tmp_path / "not-bpic2012.xes"
    source.write_text(MINIMAL_XES, encoding="utf-8")

    report = build_report(source, tmp_path / "strict.duckdb", tmp_path / "strict.json", 604_800_000)

    invariants = {item["name"]: item for item in report["validation"]["invariants"]}
    profile_checks = [name for name in invariants if name.startswith("expected_bpic2012_")]
    assert profile_checks == [
        "expected_bpic2012_sha256",
        "expected_bpic2012_file_size",
        "expected_bpic2012_case_count",
        "expected_bpic2012_event_count",
    ]
    assert not any(invariants[name]["passed"] for name in profile_checks)
    assert all(
        item["passed"]
        for name, item in invariants.items()
        if not name.startswith("expected_bpic2012_")
    )
    assert report["validation"]["status"] == "HOLD"
    assert set(report["source_fingerprint"]) == {
        "dataset_url",
        "doi",
        "filename",
        "sha256",
        "size_bytes",
        "source_url",
    }
