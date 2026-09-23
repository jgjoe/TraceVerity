"""Local dataset onboarding through the Web HTTP contract.

Everything here runs against temporary local workspaces, so the ordinary local
`data/datasets/registry.json` is never touched. The real Help Desk source is
exercised only by the integration/E2E proof.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from piw.core import build_dataset
from piw.core_surface import (
    STATUS_READY,
    STATUS_REGISTERED,
    STATUS_UNAVAILABLE,
    database_health,
)
from piw.datasets import CsvColumnMapping, CsvImportConfig, register_csv
from piw.imports import ImportContractError, _import_id, resolve_import
from piw.web import API_SCHEMA_VERSION, create_app

HEADER = (
    "Case ID",
    "Activity",
    "Complete Timestamp",
    "Resource",
    "Variant",
    "Variant index",
    "Variant",
)
ROWS = [
    ("Case 1", "Assign seriousness", "2012/10/09 14:50:17.000", "Value 1", "V1", "1", "V1"),
    ("Case 1", "Take in charge ticket", "2012/10/09 14:50:17.500", "Value 1", "V1", "1", "V1"),
    ("Case 1", "Closed", "2012/10/10 08:00:00.000", "Value 3", "V1", "1", "V1"),
    ("Case 2", "Assign seriousness", "2012/10/09 15:00:00.000", "Value 2", "V2", "2", "V2"),
    ("Case 2", "Resolve ticket", "2012/10/09 16:00:00.000", "Value 2", "V2", "2", "V2"),
]
CSV_CONFIG = {
    "delimiter": ",",
    "mapping": {
        "activity": "Activity",
        "case_id": "Case ID",
        "lifecycle": None,
        "resource": "Resource",
        "timestamp": "Complete Timestamp",
    },
    "timestamp": {
        "assume_timezone": "UTC",
        "timestamp_format": "%Y/%m/%d %H:%M:%S.%f",
    },
}
MINIMAL_XES = """<?xml version="1.0" encoding="UTF-8"?>
<log xmlns="http://www.xes-standard.org/">
  <trace>
    <string key="concept:name" value="case-1"/>
    <event>
      <string key="concept:name" value="Register"/>
      <date key="time:timestamp" value="2024-03-01T08:00:00+01:00"/>
    </event>
  </trace>
  <trace>
    <string key="concept:name" value="case-2"/>
    <event>
      <string key="concept:name" value="Register"/>
      <date key="time:timestamp" value="2024-03-01T09:30:00+01:00"/>
    </event>
    <event>
      <string key="concept:name" value="Approve"/>
      <date key="time:timestamp" value="2024-03-01T10:00:00+01:00"/>
    </event>
  </trace>
</log>
"""


def csv_bytes(
    rows: list[tuple[str, ...]] | None = None, header: tuple[str, ...] = HEADER
) -> bytes:
    lines = [",".join(header)]
    lines.extend(",".join(row) for row in (ROWS if rows is None else rows))
    return ("\n".join(lines) + "\n").encode("utf-8")


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    return TestClient(
        create_app(
            database_path=tmp_path / "canonical.duckdb",
            registry_root=tmp_path / "registry",
            upload_root=tmp_path / "imports",
            web_dist=tmp_path / "dist",
        )
    )


def upload(
    client: TestClient,
    content: bytes,
    filename: str = "tickets.csv",
    delimiter: str = ",",
):
    return client.post(
        "/api/imports/preview",
        content=content,
        params={"delimiter": delimiter, "filename": filename},
    )


def build(
    client: TestClient,
    import_id: str,
    *,
    log_id: str = "tickets",
    display_name: str = "Ticket log",
    csv_config: dict | None = None,
):
    payload: dict = {"display_name": display_name, "import_id": import_id, "log_id": log_id}
    if csv_config is not None:
        payload["csv"] = csv_config
    return client.post("/api/imports/build", json=payload)


def onboard_csv(
    client: TestClient,
    *,
    content: bytes | None = None,
    filename: str = "tickets.csv",
    log_id: str = "tickets",
    display_name: str = "Ticket log",
    csv_config: dict | None = None,
):
    preview = upload(client, csv_bytes() if content is None else content, filename)
    assert preview.status_code == 200, preview.text
    return build(
        client,
        preview.json()["import"]["import_id"],
        log_id=log_id,
        display_name=display_name,
        csv_config=CSV_CONFIG if csv_config is None else csv_config,
    )


def test_application_health_lists_the_canonical_baseline_and_registered_datasets(
    client: TestClient,
) -> None:
    listing = client.get("/api/logs").json()
    assert listing["items"] == [
        {
            "built_in": True,
            "dataset_id": None,
            "display_name": "BPI Challenge 2012",
            "log_id": "bpic2012",
            "source_fingerprint": None,
            "source_format": "xes",
            "status": "unavailable",
        }
    ]

    response = onboard_csv(client)
    assert response.status_code == 200

    health = client.get("/api/health").json()
    assert health["status"] == "ok"
    assert health["api_schema_version"] == API_SCHEMA_VERSION
    assert health["canonical_dataset"]["status"] == "unavailable"
    assert health["datasets"] == {"ready": 1, "registered": 0, "total": 2, "unavailable": 1}

    listing = client.get("/api/logs").json()
    assert [item["log_id"] for item in listing["items"]] == ["bpic2012", "tickets"]
    imported = listing["items"][1]
    assert imported["built_in"] is False
    assert imported["display_name"] == "Ticket log"
    assert imported["source_format"] == "csv"
    assert imported["status"] == STATUS_READY
    assert imported["source_fingerprint"]["filename"] == "tickets.csv"
    assert imported["source_fingerprint"]["sha256"] == hashlib.sha256(csv_bytes()).hexdigest()
    assert imported["source_fingerprint"]["size_bytes"] == len(csv_bytes())
    assert imported["dataset_id"] is not None


def test_csv_preview_describes_the_source_without_exposing_records(client: TestClient) -> None:
    preview = upload(client, csv_bytes())
    assert preview.status_code == 200
    body = preview.json()
    assert body["api_schema_version"] == API_SCHEMA_VERSION
    assert body["import"]["filename"] == "tickets.csv"
    assert body["import"]["source_format"] == "csv"
    assert body["import"]["sha256"] == hashlib.sha256(csv_bytes()).hexdigest()
    assert body["import"]["size_bytes"] == len(csv_bytes())
    assert body["import"]["import_id"].startswith("imp_")
    assert body["csv_preview"] == {
        "data_row_count": 5,
        "delimiter": ",",
        "duplicate_columns": ["Variant"],
        "header": list(HEADER),
    }
    assert "Assign seriousness" not in preview.text
    assert "Case 1" not in preview.text


def test_xes_preview_carries_no_csv_mapping_surface(client: TestClient) -> None:
    preview = upload(client, MINIMAL_XES.encode("utf-8"), filename="orders.xes")
    assert preview.status_code == 200
    body = preview.json()
    assert body["csv_preview"] is None
    assert body["import"]["source_format"] == "xes"
    assert body["import"]["filename"] == "orders.xes"


@pytest.mark.parametrize(
    ("filename", "code"),
    [
        ("../outside.csv", "UNSAFE_FILENAME"),
        ("..\\outside.csv", "UNSAFE_FILENAME"),
        ("C:\\Users\\someone\\finale.csv", "UNSAFE_FILENAME"),
        ("nested/orders.csv", "UNSAFE_FILENAME"),
        ("..", "UNSAFE_FILENAME"),
        ("", "INVALID_FILENAME"),
        (" orders.csv", "INVALID_FILENAME"),
        ("orders.txt", "UNSUPPORTED_SOURCE_FORMAT"),
        ("orders.csv.exe", "UNSUPPORTED_SOURCE_FORMAT"),
        ("CON.csv", "UNSAFE_FILENAME"),
        ("nul.xes", "UNSAFE_FILENAME"),
        ("COM1.csv", "UNSAFE_FILENAME"),
        ("lpt9.xes.gz", "UNSAFE_FILENAME"),
        ("C:orders.csv", "UNSAFE_FILENAME"),
        ("orders?.csv", "UNSAFE_FILENAME"),
        ("orders*.csv", "UNSAFE_FILENAME"),
        ("orders|1.csv", "UNSAFE_FILENAME"),
        ("orders.csv.", "UNSAFE_FILENAME"),
    ],
)
def test_unsafe_filenames_and_unsupported_formats_are_rejected(
    client: TestClient, filename: str, code: str
) -> None:
    response = upload(client, b"a,b\n1,2\n", filename=filename)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == code
    assert not list(Path(client.app.state.upload_root).glob("*/imp_*"))


@pytest.mark.parametrize(
    "filename", ["orders.csv", "console-log.csv", "com10.csv", "my.con.notes.xes"]
)
def test_ordinary_filenames_are_not_over_rejected(
    client: TestClient, filename: str
) -> None:
    """The Windows hardening stays bounded: only reserved or invalid names fail."""

    preview = upload(client, csv_bytes(), filename=filename)
    assert preview.status_code == 200
    assert preview.json()["import"]["filename"] == filename


def test_empty_import_is_rejected(client: TestClient) -> None:
    response = upload(client, b"")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "EMPTY_IMPORT"


@pytest.mark.parametrize("filename", ["empty.csv", "empty.xes", "empty.xes.gz"])
def test_an_empty_upload_leaves_no_staging_behind(
    client: TestClient, filename: str
) -> None:
    """An empty source fails closed and its staging area is removed once.

    The CSV path seals the staged bytes for the preview parser and the XES path
    goes straight to promotion, so both reach the same empty-upload rejection;
    removing the staging area twice must stay harmless.
    """

    response = upload(client, b"", filename=filename)

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "EMPTY_IMPORT"
    upload_root = Path(client.app.state.upload_root)
    assert list(upload_root.rglob("*.partial")) == []
    assert list(upload_root.rglob("import.json")) == []
    assert [item["log_id"] for item in client.get("/api/logs").json()["items"]] == [
        "bpic2012"
    ]


def test_csv_delimiter_is_configurable_and_ragged_rows_fail_closed(client: TestClient) -> None:
    semicolon = (
        "\n".join([";".join(HEADER), *[";".join(row) for row in ROWS]]) + "\n"
    ).encode("utf-8")
    preview = upload(client, semicolon, filename="semicolon.csv", delimiter=";")
    assert preview.status_code == 200
    assert preview.json()["csv_preview"]["delimiter"] == ";"
    assert preview.json()["csv_preview"]["header"] == list(HEADER)
    built = build(
        client,
        preview.json()["import"]["import_id"],
        csv_config={**CSV_CONFIG, "delimiter": ";"},
    )
    assert built.status_code == 200, built.text
    assert client.get("/api/logs/tickets/summary").json()["result"]["raw_event_count"] == 5

    ragged = (",".join(HEADER) + "\n" + ",".join(ROWS[0]) + ",extra\n").encode("utf-8")
    rejected = upload(client, ragged, filename="ragged.csv")
    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "expected 7 columns, found 8" in rejected.json()["error"]["message"]


def test_duplicated_unmapped_header_is_tolerated_but_a_mapped_duplicate_is_not(
    client: TestClient,
) -> None:
    """A repeated unmapped column must not block onboarding; a mapped one must."""

    response = onboard_csv(client)
    assert response.status_code == 200, response.text
    assert response.json()["dataset"]["status"] == STATUS_READY

    ambiguous = dict(CSV_CONFIG)
    ambiguous["mapping"] = {**CSV_CONFIG["mapping"], "activity": "Variant"}
    response = onboard_csv(client, log_id="ambiguous", csv_config=ambiguous)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "duplicate CSV header" in response.json()["error"]["message"]
    assert [item["log_id"] for item in client.get("/api/logs").json()["items"]] == [
        "bpic2012",
        "tickets",
    ]


@pytest.mark.parametrize(
    ("mapping_overrides", "message"),
    [
        ({"case_id": None}, "case_id"),
        ({"activity": None}, "activity"),
        ({"timestamp": None}, "timestamp"),
        ({"case_id": "Case ID", "activity": "Case ID"}, "distinct source column"),
        ({"activity": "Absent"}, "absent from the CSV header"),
    ],
)
def test_required_mapping_errors_are_rejected_without_touching_the_registry(
    client: TestClient, mapping_overrides: dict, message: str
) -> None:
    config = {"delimiter": ",", "mapping": {**CSV_CONFIG["mapping"], **mapping_overrides}, "timestamp": CSV_CONFIG["timestamp"]}
    response = onboard_csv(client, csv_config=config)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert message in response.json()["error"]["message"]
    assert client.get("/api/logs").json()["items"][0]["log_id"] == "bpic2012"


def test_offset_less_timestamps_require_an_explicit_timezone(client: TestClient) -> None:
    without_timezone = {**CSV_CONFIG, "timestamp": {"timestamp_format": "%Y/%m/%d %H:%M:%S.%f"}}
    response = onboard_csv(client, csv_config=without_timezone)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "no explicit timezone interpretation" in response.json()["error"]["message"]

    response = onboard_csv(client)
    assert response.status_code == 200, response.text
    summary = client.get("/api/logs/tickets/summary").json()
    assert summary["result"]["case_count"] == 2
    assert "sla_threshold_ms" not in summary["result"]


def test_changed_source_bytes_never_destroy_an_existing_good_dataset(client: TestClient) -> None:
    preview = upload(client, csv_bytes())
    import_id = preview.json()["import"]["import_id"]
    assert build(client, import_id, csv_config=CSV_CONFIG).status_code == 200
    stored = Path(client.app.state.upload_root)
    source = next(stored.glob("*/imp_*/*.csv"))
    database = Path(client.app.state.registry.default_database_path("tickets"))
    before = database.read_bytes()
    summary_before = client.get("/api/logs/tickets/summary").json()

    source.write_bytes(
        source.read_bytes().replace(b"2012/10/09", b"2013/10/09", 1)
    )
    assert source.stat().st_size == len(csv_bytes())

    rejected = build(client, import_id, csv_config=CSV_CONFIG)
    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "re-register the dataset" in rejected.json()["error"]["message"]

    assert database.read_bytes() == before
    assert not list(database.parent.glob("*.building"))
    assert client.get("/api/logs/tickets/summary").json() == summary_before


def test_a_rejected_csv_preview_leaves_no_import_and_no_registry_change(
    client: TestClient,
) -> None:
    """The preview validates staged bytes, so a rejected source is never promoted.

    The good dataset keeps its database bytes, its registry entry, and its ready
    listing: a rejected preview neither retains an import nor touches a dataset.
    """

    good = onboard_csv(client)
    assert good.status_code == 200, good.text
    registry_path = Path(client.app.state.registry.registry_path)
    database = Path(client.app.state.registry.default_database_path("tickets"))
    before_database = database.read_bytes()
    before_registry = registry_path.read_bytes()
    before_listing = client.get("/api/logs").json()

    ragged = (",".join(HEADER) + "\n" + ",".join(ROWS[0]) + ",extra\n").encode("utf-8")
    rejected = upload(client, ragged, filename="ragged.csv")

    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "expected 7 columns, found 8" in rejected.json()["error"]["message"]

    upload_root = Path(client.app.state.upload_root)
    rejected_id = _import_id(
        "ragged.csv", hashlib.sha256(ragged).hexdigest(), len(ragged)
    )
    with pytest.raises(ImportContractError) as rejected_lookup:
        resolve_import(upload_root, rejected_id)
    assert rejected_lookup.value.code == "UNKNOWN_IMPORT"
    good_id = _import_id(
        "tickets.csv", hashlib.sha256(csv_bytes()).hexdigest(), len(csv_bytes())
    )
    assert sorted(path.parent.name for path in upload_root.rglob("import.json")) == [good_id]
    assert not list(upload_root.rglob("*.partial"))

    assert database.read_bytes() == before_database
    assert registry_path.read_bytes() == before_registry
    assert client.get("/api/logs").json() == before_listing


def test_an_unnamed_header_column_is_rejected_without_leaving_staging_behind(
    client: TestClient,
) -> None:
    """A nonempty CSV with an unnamed header column fails closed at the preview.

    The strict parser judges the header before the source is promoted, so the
    rejected bytes are discarded again: no staged source, no manifest, and no
    import of those bytes is retained, while the good dataset keeps its database
    bytes, its registry entry, its stored import, and its ready listing. The
    source is closed before the header error leaves the reader, so the staged
    bytes of a rejected source can be removed at all.
    """

    good = onboard_csv(client)
    assert good.status_code == 200, good.text
    registry_path = Path(client.app.state.registry.registry_path)
    database = Path(client.app.state.registry.default_database_path("tickets"))
    before_database = database.read_bytes()
    before_registry = registry_path.read_bytes()
    before_listing = client.get("/api/logs").json()

    header = ("Case ID", "Activity", "Complete Timestamp", "Resource", "", "Variant", "Variant")
    unnamed = csv_bytes(header=header)
    rejected = upload(client, unnamed, filename="unnamed-column.csv")

    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "header column 5 has no name" in rejected.json()["error"]["message"]

    upload_root = Path(client.app.state.upload_root)
    rejected_id = _import_id(
        "unnamed-column.csv", hashlib.sha256(unnamed).hexdigest(), len(unnamed)
    )
    with pytest.raises(ImportContractError) as rejected_lookup:
        resolve_import(upload_root, rejected_id)
    assert rejected_lookup.value.code == "UNKNOWN_IMPORT"
    good_id = _import_id(
        "tickets.csv", hashlib.sha256(csv_bytes()).hexdigest(), len(csv_bytes())
    )
    assert sorted(path.parent.name for path in upload_root.rglob("import.json")) == [good_id]
    assert not list(upload_root.rglob("*.partial"))
    staging = upload_root / ".staging"
    assert not (staging.is_dir() and any(staging.iterdir()))

    assert database.read_bytes() == before_database
    assert registry_path.read_bytes() == before_registry
    assert client.get("/api/logs").json() == before_listing


def test_an_invalid_delimiter_repreview_keeps_the_built_import_intact(
    client: TestClient,
) -> None:
    """A rejected re-preview cannot touch the import the built dataset owns.

    Import identity is content-addressed, so previewing the same file name and
    bytes again names the very import the built dataset already owns. The
    delimiter is rejected on the staged bytes before promotion, so the built
    source, its manifest, the database, the registry entry, and the ready
    listing all survive, and no import of the rejected request is left behind.
    """

    onboarded = onboard_csv(client)
    assert onboarded.status_code == 200, onboarded.text
    content = csv_bytes()
    upload_root = Path(client.app.state.upload_root)
    import_id = _import_id(
        "tickets.csv", hashlib.sha256(content).hexdigest(), len(content)
    )
    stored = resolve_import(upload_root, import_id)
    manifest = stored.path.parent / "import.json"
    before_source = stored.path.read_bytes()
    before_manifest = manifest.read_bytes()
    registry_path = Path(client.app.state.registry.registry_path)
    database = Path(client.app.state.registry.default_database_path("tickets"))
    before_database = database.read_bytes()
    before_registry = registry_path.read_bytes()
    before_listing = client.get("/api/logs").json()

    rejected = upload(client, content, filename="tickets.csv", delimiter=",,")

    assert rejected.status_code == 400
    assert rejected.json()["error"] == {
        "code": "SOURCE_CONTRACT_ERROR",
        "details": {"filename": "tickets.csv"},
        "message": "CSV delimiter must be exactly one character",
    }
    assert list(upload_root.rglob("*.partial")) == []
    assert sorted(path.parent.name for path in upload_root.rglob("import.json")) == [import_id]
    assert resolve_import(upload_root, import_id).path.read_bytes() == before_source
    assert manifest.read_bytes() == before_manifest
    assert database.read_bytes() == before_database
    assert registry_path.read_bytes() == before_registry
    assert client.get("/api/logs").json() == before_listing


def test_failed_build_leaves_no_ready_or_registered_state(client: TestClient) -> None:
    content = csv_bytes().replace(b"2012/10/09", b"not-a-timestamp")
    response = onboard_csv(client, content=content)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "SOURCE_CONTRACT_ERROR"
    assert "invalid timestamp" in response.json()["error"]["message"]

    registry_root = Path(client.app.state.registry.root)
    assert not (registry_root / "registry.json").exists()
    assert not list(registry_root.glob("*.duckdb"))
    assert not list(registry_root.glob("*.building"))
    assert [item["log_id"] for item in client.get("/api/logs").json()["items"]] == ["bpic2012"]


def test_reimporting_the_same_source_is_idempotent(client: TestClient) -> None:
    first = onboard_csv(client)
    assert first.status_code == 200
    registry_path = Path(client.app.state.registry.registry_path)
    registry_bytes = registry_path.read_bytes()
    summary = client.get("/api/logs/tickets/summary").json()

    second = onboard_csv(client)

    assert second.status_code == 200
    assert second.json()["dataset"]["dataset_id"] == first.json()["dataset"]["dataset_id"]
    assert registry_path.read_bytes() == registry_bytes
    assert client.get("/api/logs/tickets/summary").json() == summary
    assert [item["log_id"] for item in client.get("/api/logs").json()["items"]] == [
        "bpic2012",
        "tickets",
    ]


def test_conflicting_identity_under_one_log_id_is_rejected_before_any_overwrite(
    client: TestClient,
) -> None:
    assert onboard_csv(client).status_code == 200
    original = client.get("/api/logs/tickets/summary").json()
    database = Path(client.app.state.registry.default_database_path("tickets"))
    before = database.read_bytes()

    changed = csv_bytes(rows=[*ROWS, ("Case 3", "Closed", "2012/10/11 09:00:00.000", "Value 4", "V3", "3", "V3")])
    response = onboard_csv(client, content=changed)

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "CONFLICTING_DATASET_IDENTITY"
    assert database.read_bytes() == before
    assert client.get("/api/logs/tickets/summary").json() == original


def test_a_corrupt_or_missing_database_is_never_reported_ready(client: TestClient) -> None:
    assert onboard_csv(client).status_code == 200
    descriptor_database = Path(client.app.state.registry.default_database_path("tickets"))

    descriptor_database.write_bytes(b"not a duckdb database at all")
    assert client.get("/api/health").json()["datasets"] == {
        "ready": 0,
        "registered": 0,
        "total": 2,
        "unavailable": 2,
    }
    listing = {item["log_id"]: item for item in client.get("/api/logs").json()["items"]}
    assert listing["tickets"]["status"] == STATUS_UNAVAILABLE
    assert listing["tickets"]["source_fingerprint"]["sha256"] == hashlib.sha256(
        csv_bytes()
    ).hexdigest()
    rejected = client.get("/api/logs/tickets/summary")
    assert rejected.status_code == 503
    assert rejected.json()["error"]["code"] == "DATASET_UNAVAILABLE"

    descriptor_database.unlink()
    listing = {item["log_id"]: item for item in client.get("/api/logs").json()["items"]}
    assert listing["tickets"]["status"] == STATUS_REGISTERED
    assert client.get("/api/logs/tickets/summary").status_code == 503


def test_dataset_readiness_probes_the_database_not_the_file(client: TestClient, tmp_path: Path) -> None:
    assert onboard_csv(client).status_code == 200
    database = Path(client.app.state.registry.default_database_path("tickets"))
    assert database_health(database).is_ready

    missing = tmp_path / "missing.duckdb"
    assert database_health(missing).status == STATUS_UNAVAILABLE
    corrupted = tmp_path / "corrupted.duckdb"
    corrupted.write_bytes(b"not a duckdb database at all")
    assert database_health(corrupted).status == STATUS_UNAVAILABLE
    foreign = tmp_path / "foreign.duckdb"
    with duckdb.connect(str(foreign)) as connection:
        connection.execute("CREATE TABLE unrelated (value INTEGER)")
    assert database_health(foreign).status == STATUS_UNAVAILABLE

    truncated = tmp_path / "truncated.duckdb"
    truncated.write_bytes(database.read_bytes()[:4096])
    assert database_health(truncated).status == STATUS_UNAVAILABLE

    # Schema-shaped but not Core-servable: counting rows in `analysis_events`
    # would succeed, while the real activity/rework metric cannot run.
    partial = tmp_path / "partial.duckdb"
    with duckdb.connect(str(partial)) as connection:
        connection.execute(
            "CREATE TABLE events (case_id VARCHAR NOT NULL, activity VARCHAR NOT NULL, "
            "event_ts_utc_ms BIGINT NOT NULL, event_pos INTEGER NOT NULL, "
            "lifecycle VARCHAR, resource VARCHAR)"
        )
        connection.execute("CREATE TABLE analysis_events (case_id VARCHAR, event_ts_utc_ms BIGINT)")
        connection.execute(
            "CREATE TABLE source_metadata (doi VARCHAR, source_url VARCHAR, "
            "filename VARCHAR NOT NULL, sha256 VARCHAR NOT NULL, size_bytes BIGINT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO source_metadata VALUES (NULL, NULL, 'partial.csv', ?, 1)",
            ["b" * 64],
        )
    assert database_health(partial).status == STATUS_UNAVAILABLE


def test_generalized_routes_resolve_the_imported_log(client: TestClient) -> None:
    assert onboard_csv(client).status_code == 200

    summary = client.get("/api/logs/tickets/summary").json()
    assert summary["parameters"]["log_id"] == "tickets"
    assert summary["result"]["case_count"] == 2
    assert summary["result"]["raw_event_count"] == 5
    assert summary["result"]["analysis_event_count"] == 5
    assert summary["result"]["distinct_activity_count"] == 4
    assert summary["result"]["variant_count"] == 2
    assert summary["result"]["direct_follow_count"] == 3
    assert summary["result"]["cases_with_rework"] == 0
    assert summary["result"]["aggregate_rework_event_count"] == 0
    assert summary["supplemental"]["facts"][0]["name"] == "observed_gap_p50_ms"
    assert summary["supplemental"]["parameters"]["log_id"] == "tickets"

    variants = client.get("/api/logs/tickets/variants", params={"order_by": "case_count_desc", "limit": 10}).json()
    assert [item["case_count"] for item in variants["result"]["items"]] == [1, 1]
    transitions = client.get("/api/logs/tickets/transitions", params={"order_by": "transition_count_desc", "limit": 10}).json()
    assert {item["transition_count"] for item in transitions["result"]["items"]} == {1}
    activities = client.get("/api/logs/tickets/activities", params={"order_by": "rework_event_count_desc", "limit": 10}).json()
    assert {item["activity"] for item in activities["result"]["items"]} == {
        "Assign seriousness",
        "Closed",
        "Resolve ticket",
        "Take in charge ticket",
    }

    trace = client.get("/api/logs/tickets/cases/Case 1")
    assert trace.status_code == 200
    assert trace.json()["result"]["activities"] == [
        "Assign seriousness",
        "Take in charge ticket",
        "Closed",
    ]
    assert trace.json()["parameters"]["log_id"] == "tickets"
    assert client.get("/api/logs/tickets/cases/absent").status_code == 404
    assert client.get("/api/logs/tickets/cases/absent").json()["error"]["code"] == "NOT_FOUND"


def test_generic_xes_import_reaches_the_same_core_facts(client: TestClient) -> None:
    for filename, log_id, content in (
        ("orders.xes", "orders", MINIMAL_XES.encode("utf-8")),
        ("orders.xes.gz", "orders-gz", gzip.compress(MINIMAL_XES.encode("utf-8"))),
    ):
        preview = upload(client, content, filename=filename)
        assert preview.status_code == 200
        assert preview.json()["csv_preview"] is None
        import_id = preview.json()["import"]["import_id"]

        response = build(client, import_id, log_id=log_id, display_name="Order log")
        assert response.status_code == 200, response.text
        assert response.json()["dataset"]["source_format"] == "xes"
        assert response.json()["build"] == {
            "analysis_event_count": 3,
            "case_count": 2,
            "raw_event_count": 3,
            "validation_status": "PASS",
            "variant_count": 2,
        }
        summary = client.get(f"/api/logs/{log_id}/summary").json()
        assert summary["result"]["case_count"] == 2
        assert summary["result"]["raw_event_count"] == 3
        assert "sla_threshold_ms" not in summary["result"]


def test_xes_import_rejects_a_csv_configuration_and_unknown_fields(client: TestClient) -> None:
    preview = upload(client, MINIMAL_XES.encode("utf-8"), filename="orders.xes")
    import_id = preview.json()["import"]["import_id"]

    with_csv = build(client, import_id, csv_config=CSV_CONFIG)
    assert with_csv.status_code == 400
    assert with_csv.json()["error"]["code"] == "INVALID_IMPORT_REQUEST"

    unknown = client.post(
        "/api/imports/build",
        json={"display_name": "Order log", "import_id": import_id, "log_id": "orders", "path": "/etc/passwd"},
    )
    assert unknown.status_code == 400
    assert unknown.json()["error"]["code"] == "INVALID_IMPORT_REQUEST"
    assert unknown.json()["error"]["details"]["fields"] == ["path"]


def test_unknown_import_and_malformed_build_requests_are_structured(client: TestClient) -> None:
    assert client.post("/api/imports/build", json={"import_id": "imp_" + "0" * 64, "log_id": "x", "display_name": "X"}).status_code == 404
    assert client.post("/api/imports/build", content=b"not json").status_code == 400
    assert client.post("/api/imports/build", json={"import_id": "not-a-reference"}).status_code == 400
    response = client.post("/api/imports/build", json={"import_id": "imp_" + "0" * 64})
    assert response.json()["error"]["code"] == "UNKNOWN_IMPORT"


def test_csv_build_rejects_a_missing_or_malformed_csv_configuration(client: TestClient) -> None:
    preview = upload(client, csv_bytes())
    import_id = preview.json()["import"]["import_id"]

    without_csv = build(client, import_id, csv_config=None)
    assert without_csv.status_code == 400
    assert without_csv.json()["error"]["code"] == "INVALID_IMPORT_REQUEST"

    malformed = build(client, import_id, csv_config={"mapping": {"case_id": "Case ID"}})
    assert malformed.status_code == 400
    assert malformed.json()["error"]["code"] == "INVALID_IMPORT_REQUEST"
    assert malformed.json()["error"]["details"]["fields"] == ["activity", "timestamp"]

    unknown_field = build(client, import_id, csv_config={**CSV_CONFIG, "path": "x"})
    assert unknown_field.status_code == 400
    assert unknown_field.json()["error"]["code"] == "INVALID_IMPORT_REQUEST"


def test_import_area_is_deterministic_and_ignored_style_local(client: TestClient) -> None:
    first = upload(client, csv_bytes())
    second = upload(client, csv_bytes())
    assert first.json()["import"] == second.json()["import"]
    stored = list(Path(client.app.state.upload_root).glob("*/imp_*/*.csv"))
    assert len(stored) == 1
    assert stored[0].read_bytes() == csv_bytes()
    assert list(Path(client.app.state.upload_root).rglob("*.partial")) == []
    manifest = json.loads((stored[0].parent / "import.json").read_text(encoding="utf-8"))
    assert manifest["filename"] == "tickets.csv"
    assert manifest["import_id"] == first.json()["import"]["import_id"]


def test_the_reserved_builtin_log_id_cannot_be_imported(client: TestClient) -> None:
    """The built-in baseline owns its log_id; a browser import must never shadow it."""

    preview = upload(client, csv_bytes())
    import_id = preview.json()["import"]["import_id"]

    rejected = build(client, import_id, log_id="bpic2012", display_name="Shadow baseline")

    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "RESERVED_LOG_ID"
    assert rejected.json()["error"]["details"] == {"log_id": "bpic2012"}
    registry_root = Path(client.app.state.registry.root)
    assert not (registry_root / "registry.json").exists()
    assert not list(registry_root.glob("*.duckdb"))
    assert not list(registry_root.glob("*.building"))
    listing = client.get("/api/logs").json()
    assert [item["log_id"] for item in listing["items"]] == ["bpic2012"]
    assert listing["items"][0]["built_in"] is True
    assert client.get("/api/health").json()["datasets"]["total"] == 1


def test_a_registry_entry_under_the_reserved_log_id_fails_closed(
    client: TestClient,
) -> None:
    """Out-of-band registry state can never publish a duplicate identifier."""

    upload(client, csv_bytes())
    source = next(Path(client.app.state.upload_root).glob("*/imp_*/*.csv"))
    register_csv(
        client.app.state.registry,
        log_id="bpic2012",
        display_name="Shadow baseline",
        source_path=source,
        config=CsvImportConfig(
            mapping=CsvColumnMapping(
                case_id="Case ID", activity="Activity", timestamp="Complete Timestamp"
            )
        ),
    )

    for path in ("/api/logs", "/api/health"):
        response = client.get(path)
        assert response.status_code == 503
        assert response.json()["error"]["code"] == "REGISTRY_UNAVAILABLE"


def test_registry_rebuild_of_a_web_built_dataset_reproduces_the_same_facts(
    client: TestClient,
) -> None:
    """A browser-imported dataset resolves through the registry for later rebuilds."""

    assert onboard_csv(client).status_code == 200

    descriptor = client.app.state.registry.get("tickets")
    report = build_dataset(descriptor)
    summary = client.get("/api/logs/tickets/summary").json()

    assert report["validation"]["status"] == "PASS"
    assert report["raw_summary"]["case_count"] == summary["result"]["case_count"] == 2
    assert report["raw_summary"]["event_count"] == summary["result"]["raw_event_count"] == 5
    assert [item["log_id"] for item in client.get("/api/logs").json()["items"]] == [
        "bpic2012",
        "tickets",
    ]
    assert client.get("/api/logs").json()["items"][1]["status"] == STATUS_READY
