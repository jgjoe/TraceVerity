from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Any

import duckdb
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .core import METRIC_DEFINITION_VERSION, observed_gap_summary
from .tools import TOOL_SCHEMA_VERSION, CoreToolSurface, canonical_json

API_SCHEMA_VERSION = "slice2-http-v1"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "processed" / "bpic2012.duckdb"
DEFAULT_WEB_DIST = PROJECT_ROOT / "web" / "dist"


def _error(
    status_code: int, code: str, message: str, details: dict[str, Any] | None = None
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "details": details or {},
                "message": message,
            },
            "schema_version": API_SCHEMA_VERSION,
        },
    )


def _tool_status(response: dict[str, Any]) -> int:
    code = response.get("error", {}).get("code")
    if code == "NOT_FOUND":
        return 404
    if code == "DATA_CONTRACT_ERROR":
        return 503
    return 400


def _tool_response(response: dict[str, Any]) -> dict[str, Any] | JSONResponse:
    if "error" in response:
        return JSONResponse(status_code=_tool_status(response), content=response)
    return response


def _integer_parameter(value: str) -> int | str:
    try:
        return int(value)
    except ValueError:
        return value


def _supplemental_gap_p50(
    database_path: Path, source_response: dict[str, Any]
) -> dict[str, Any]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = observed_gap_summary(connection)["p50_ms"]
    parameters = {"log_id": "bpic2012", "perspective": "complete"}
    query_document = {
        "api_schema_version": API_SCHEMA_VERSION,
        "log_fingerprint": source_response["log_fingerprint"],
        "metric_definition_version": METRIC_DEFINITION_VERSION,
        "parameters": parameters,
        "source_query_id": source_response["query_id"],
        "metric": "observed_gap_p50_ms",
    }
    query_id = "q_" + hashlib.sha256(
        canonical_json(query_document).encode("utf-8")
    ).hexdigest()
    fact_document = {
        "name": "observed_gap_p50_ms",
        "query_id": query_id,
        "value": value,
    }
    fact_id = "f_" + hashlib.sha256(
        canonical_json(fact_document).encode("utf-8")
    ).hexdigest()
    return {
        "facts": [{"fact_id": fact_id, "name": "observed_gap_p50_ms", "value": value}],
        "log_fingerprint": source_response["log_fingerprint"],
        "metric_definition_version": METRIC_DEFINITION_VERSION,
        "parameters": parameters,
        "query_id": query_id,
        "schema_version": API_SCHEMA_VERSION,
        "source_query_id": source_response["query_id"],
    }


def create_app(
    *, database_path: Path = DEFAULT_DATABASE, web_dist: Path = DEFAULT_WEB_DIST
) -> FastAPI:
    app = FastAPI(
        title="Process Intelligence Workbench",
        version="0.2.0",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )
    app.state.database_path = Path(database_path)
    app.state.web_dist = Path(web_dist)

    def call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any] | JSONResponse:
        try:
            surface = CoreToolSurface(app.state.database_path)
            return _tool_response(surface.dispatch(name, arguments))
        except (FileNotFoundError, OSError, duckdb.Error):
            return _error(
                503,
                "DATASET_UNAVAILABLE",
                "canonical BPIC12 local dataset is unavailable",
                {"log_id": "bpic2012"},
            )

    @app.get("/api/health")
    def health() -> Any:
        response = call_tool("describe_log", {"log_id": "bpic2012"})
        if isinstance(response, JSONResponse):
            return response
        return {
            "api_schema_version": API_SCHEMA_VERSION,
            "dataset": "BPI Challenge 2012",
            "log_fingerprint": response["log_fingerprint"],
            "metric_definition_version": response["metric_definition_version"],
            "status": "ok",
            "tool_schema_version": response["schema_version"],
        }

    @app.get("/api/logs/bpic2012/summary")
    def summary(
        sla_threshold_ms: str | None = Query(default=None),
    ) -> Any:
        arguments: dict[str, Any] = {"log_id": "bpic2012"}
        if sla_threshold_ms is not None:
            arguments["sla_threshold_ms"] = _integer_parameter(sla_threshold_ms)
        response = call_tool("describe_log", arguments)
        if isinstance(response, JSONResponse):
            return response
        try:
            supplement = _supplemental_gap_p50(app.state.database_path, response)
        except (FileNotFoundError, OSError, duckdb.Error):
            return _error(
                503,
                "DATASET_UNAVAILABLE",
                "canonical BPIC12 local dataset is unavailable",
                {"log_id": "bpic2012"},
            )
        return {**response, "api_schema_version": API_SCHEMA_VERSION, "supplemental": supplement}

    @app.get("/api/logs/bpic2012/variants")
    def variants(
        order_by: str = Query(default="case_count_desc"),
        limit: str = Query(default="10"),
    ) -> Any:
        return call_tool(
            "list_variants",
            {
                "limit": _integer_parameter(limit),
                "log_id": "bpic2012",
                "order_by": order_by,
            },
        )

    @app.get("/api/logs/bpic2012/transitions")
    def transitions(
        from_activity: str | None = Query(default=None),
        to_activity: str | None = Query(default=None),
        order_by: str = Query(default="transition_count_desc"),
        limit: str = Query(default="10"),
    ) -> Any:
        arguments: dict[str, Any] = {
            "limit": _integer_parameter(limit),
            "log_id": "bpic2012",
            "order_by": order_by,
        }
        if from_activity is not None:
            arguments["from_activity"] = from_activity
        if to_activity is not None:
            arguments["to_activity"] = to_activity
        return call_tool("list_transitions", arguments)

    @app.get("/api/logs/bpic2012/activities")
    def activities(
        order_by: str = Query(default="rework_event_count_desc"),
        limit: str = Query(default="10"),
    ) -> Any:
        return call_tool(
            "list_activities",
            {
                "limit": _integer_parameter(limit),
                "log_id": "bpic2012",
                "order_by": order_by,
            },
        )

    @app.get("/api/logs/bpic2012/cases/{case_id}")
    def case_trace(
        case_id: str, perspective: str = Query(default="complete")
    ) -> Any:
        return call_tool(
            "get_case_trace",
            {
                "case_id": case_id,
                "log_id": "bpic2012",
                "perspective": perspective,
            },
        )

    assets = app.state.web_dist / "assets"
    if assets.is_dir():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{path:path}", include_in_schema=False)
    def frontend(path: str) -> Any:
        if path.startswith("api/"):
            return _error(404, "NOT_FOUND", "API route does not exist")
        index = app.state.web_dist / "index.html"
        if not index.is_file():
            return _error(
                503,
                "FRONTEND_UNAVAILABLE",
                "built frontend is unavailable; run the documented frontend build",
            )
        return FileResponse(index)

    return app


app = create_app()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Slice 2 web workbench")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--web-dist", type=Path, default=DEFAULT_WEB_DIST)
    args = parser.parse_args()
    if not 1 <= args.port <= 65_535:
        parser.error("--port must be within 1..65535")
    uvicorn.run(
        create_app(database_path=args.database, web_dist=args.web_dist),
        host="127.0.0.1",
        port=args.port,
    )


if __name__ == "__main__":
    main()
