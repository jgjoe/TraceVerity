from __future__ import annotations

import ast
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from piw.core import observed_gap_summary
from piw.tools import CoreToolSurface
from piw.web import API_SCHEMA_VERSION, create_app

DATABASE = Path("data/processed/bpic2012.duckdb")
REAL_CASE_ID = "173688"


@pytest.fixture(scope="module")
def client() -> TestClient:
    if not DATABASE.is_file():
        pytest.fail(f"canonical integration database is missing: {DATABASE}")
    with TestClient(create_app(database_path=DATABASE)) as test_client:
        yield test_client


def test_health_proves_canonical_dataset_is_available(client: TestClient) -> None:
    response = client.get("/api/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["dataset"] == "BPI Challenge 2012"
    assert body["log_fingerprint"] == "5cd9cc16b9bcb20bd4aae45666a5d87479ddbf47e6371618b6ad217174cecdf3"
    assert "timestamp" not in body


def test_summary_preserves_tool_facts_and_adds_core_gap_p50(client: TestClient) -> None:
    response = client.get(
        "/api/logs/bpic2012/summary", params={"sla_threshold_ms": 604_800_000}
    )
    assert response.status_code == 200
    body = response.json()
    tool = CoreToolSurface(DATABASE).describe_log("bpic2012", 604_800_000)
    for key in (
        "facts",
        "log_fingerprint",
        "metric_definition_version",
        "parameters",
        "query_id",
        "result",
        "schema_version",
    ):
        assert body[key] == tool[key]
    assert body["result"]["case_count"] == 13_087
    assert body["result"]["raw_event_count"] == 262_200
    assert body["result"]["analysis_event_count"] == 164_506
    assert body["result"]["variant_count"] == 4_336
    assert body["supplemental"]["facts"][0]["name"] == "observed_gap_p50_ms"
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        assert body["supplemental"]["facts"][0]["value"] == observed_gap_summary(
            connection
        )["p50_ms"]
    assert body["supplemental"]["facts"][0]["fact_id"].startswith("f_")
    assert body["supplemental"]["schema_version"] == API_SCHEMA_VERSION


def test_list_endpoints_return_real_deterministic_facts(client: TestClient) -> None:
    cases = [
        ("/api/logs/bpic2012/variants", "case_count_desc", "variant_id"),
        ("/api/logs/bpic2012/transitions", "transition_count_desc", "from_activity"),
        ("/api/logs/bpic2012/activities", "rework_event_count_desc", "activity"),
    ]
    for path, order_by, required_field in cases:
        response = client.get(path, params={"order_by": order_by, "limit": 10})
        assert response.status_code == 200
        body = response.json()
        assert body["result"]["items"]
        assert required_field in body["result"]["items"][0]
        assert body["facts"][0]["value"] == body["result"]["items"][0]


def test_exact_complete_case_trace_defaults_and_is_grounded(client: TestClient) -> None:
    response = client.get(f"/api/logs/bpic2012/cases/{REAL_CASE_ID}")
    assert response.status_code == 200
    body = response.json()
    expected = CoreToolSurface(DATABASE).get_case_trace(
        "bpic2012", REAL_CASE_ID, "complete"
    )
    assert body == expected
    assert body["parameters"]["perspective"] == "complete"
    assert body["result"]["activities"]


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/api/logs/bpic2012/variants", {"limit": 0}),
        ("/api/logs/bpic2012/transitions", {"limit": 101}),
        ("/api/logs/bpic2012/activities", {"limit": "many"}),
        ("/api/logs/bpic2012/summary", {"sla_threshold_ms": -1}),
    ],
)
def test_invalid_bounded_parameters_are_structured_400(
    client: TestClient, path: str, params: dict[str, object]
) -> None:
    response = client.get(path, params=params)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_ARGUMENT"


def test_missing_case_is_structured_404(client: TestClient) -> None:
    response = client.get("/api/logs/bpic2012/cases/not-a-real-bpic12-case")
    assert response.status_code == 404
    assert response.json()["error"] == {
        "code": "NOT_FOUND",
        "details": {"case_id": "not-a-real-bpic12-case"},
        "message": "case_id does not exist",
    }


def test_missing_dataset_is_structured_503(tmp_path: Path) -> None:
    response = TestClient(create_app(database_path=tmp_path / "missing.duckdb")).get(
        "/api/health"
    )
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "DATASET_UNAVAILABLE"


def test_web_runtime_has_no_agent_dependency() -> None:
    tree = ast.parse(Path("src/piw/web.py").read_text(encoding="utf-8"))
    imported_modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module)
    assert all(not module.endswith("agent") for module in imported_modules)
    assert all(not module.endswith("evaluation") for module in imported_modules)
