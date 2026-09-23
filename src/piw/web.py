from __future__ import annotations

import argparse
import hashlib
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import duckdb
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .core import (
    METRIC_DEFINITION_VERSION,
    build_dataset,
    discard_staged_database,
    observed_gap_summary,
    promote_staged_database,
    staging_database_path,
)
from .core_surface import (
    STATUS_READY,
    STATUS_REGISTERED,
    STATUS_UNAVAILABLE,
    TOOL_SCHEMA_VERSION,
    CoreReadSurface,
    CoreSurfaceError,
    canonical_json,
)
from .csv_ingest import preview_csv
from .datasets import (
    SOURCE_FORMAT_CSV,
    CsvColumnMapping,
    CsvImportConfig,
    DEFAULT_REGISTRY_ROOT,
    DatasetDescriptor,
    DatasetRegistryError,
    TimestampConfig,
)
from .events import SourceContractError
from .imports import (
    DEFAULT_UPLOAD_ROOT,
    ImportContractError,
    ImportSink,
    StoredImport,
    resolve_import,
)
from .profiles import BPIC2012
from .resolution import DatasetResolver, ResolvedDataset

API_SCHEMA_VERSION = "slice-c-http-v2"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATABASE = PROJECT_ROOT / "data" / "processed" / "bpic2012.duckdb"
DEFAULT_WEB_DIST = PROJECT_ROOT / "web" / "dist"
_BUILD_FIELDS = {"csv", "display_name", "import_id", "log_id"}
_CSV_FIELDS = {"delimiter", "mapping", "timestamp"}
_MAPPING_FIELDS = {"activity", "case_id", "lifecycle", "resource", "timestamp"}
_TIMESTAMP_FIELDS = {"assume_timezone", "timestamp_format"}


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


def _integer_parameter(value: str) -> int | str:
    try:
        return int(value)
    except ValueError:
        return value


def _supplemental_gap_p50(
    database_path: Path, log_id: str, source_response: dict[str, Any]
) -> dict[str, Any]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        value = observed_gap_summary(connection)["p50_ms"]
    parameters = {"log_id": log_id, "perspective": "complete"}
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


def _call_surface(
    resolved: ResolvedDataset, method: str, **arguments: Any
) -> dict[str, Any] | JSONResponse:
    """Read facts through the generic Core surface, mapping failures to HTTP."""

    try:
        surface = CoreReadSurface(resolved.database_path, resolved.log_id)
        return getattr(surface, method)(**arguments)
    except CoreSurfaceError as exc:
        response = exc.response()
        return JSONResponse(status_code=_tool_status(response), content=response)
    except (FileNotFoundError, OSError, duckdb.Error):
        return _error(
            503,
            "DATASET_UNAVAILABLE",
            f"log_id {resolved.log_id!r} cannot serve the Core read path",
            {"log_id": resolved.log_id},
        )


def _import_error_status(code: str) -> int:
    """HTTP status for one local-import contract violation."""

    if code == "UNKNOWN_IMPORT":
        return 404
    if code == "RESERVED_LOG_ID":
        return 409
    return 400


def _csv_import_config(value: Any) -> CsvImportConfig:
    """Build the explicit CSV import configuration carried by a build request."""

    if not isinstance(value, dict):
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            "a CSV import requires a csv object naming the mapping and the timestamp interpretation",
        )
    _reject_unknown_fields("csv", value, _CSV_FIELDS)
    mapping = value.get("mapping")
    if not isinstance(mapping, dict):
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            "csv.mapping must be an object of canonical field to source column",
            {"field": "mapping"},
        )
    _reject_unknown_fields("csv.mapping", mapping, _MAPPING_FIELDS)
    missing = sorted({"activity", "case_id", "timestamp"} - set(mapping))
    if missing:
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            f"csv.mapping must name the required source column(s) {missing}",
            {"fields": missing, "scope": "csv.mapping"},
        )
    timestamp = value.get("timestamp", {})
    if not isinstance(timestamp, dict):
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            "csv.timestamp must be an object of timestamp interpretation settings",
            {"field": "timestamp"},
        )
    _reject_unknown_fields("csv.timestamp", timestamp, _TIMESTAMP_FIELDS)
    return CsvImportConfig(
        delimiter=value.get("delimiter", ","),
        mapping=CsvColumnMapping(**mapping),
        timestamp=TimestampConfig(**timestamp),
    )


def _reject_unknown_fields(scope: str, value: dict[str, Any], allowed: set[str]) -> None:
    unsupported = sorted(set(value) - allowed)
    if unsupported:
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            f"unsupported {scope} field(s) {unsupported}",
            {"fields": unsupported, "scope": scope},
        )


def _import_descriptor(
    state: Any, stored: StoredImport, payload: dict[str, Any]
) -> DatasetDescriptor:
    """Describe the dataset a build request asks for, before anything is built."""

    _reject_unknown_fields("build request", payload, _BUILD_FIELDS)
    log_id = payload.get("log_id")
    display_name = payload.get("display_name")
    if not isinstance(log_id, str) or not log_id:
        raise ImportContractError("INVALID_IMPORT_REQUEST", "log_id is required")
    if log_id == BPIC2012.log_id:
        raise ImportContractError(
            "RESERVED_LOG_ID",
            f"log_id {log_id!r} is reserved for the built-in {BPIC2012.display_name} "
            "baseline; choose a different log_id for this source",
            {"log_id": log_id},
        )
    if not isinstance(display_name, str) or not display_name:
        raise ImportContractError("INVALID_IMPORT_REQUEST", "display_name is required")
    csv_import = None
    if stored.source_format == SOURCE_FORMAT_CSV:
        csv_import = _csv_import_config(payload.get("csv"))
    elif payload.get("csv") is not None:
        raise ImportContractError(
            "INVALID_IMPORT_REQUEST",
            f"{stored.source_format} sources are interpreted without a CSV configuration",
            {"source_format": stored.source_format},
        )
    return DatasetDescriptor(
        csv_import=csv_import,
        database_path=state.registry.default_database_path(log_id),
        display_name=display_name,
        log_id=log_id,
        sha256=stored.sha256,
        size_bytes=stored.size_bytes,
        source_format=stored.source_format,
        source_path=stored.path,
    )


def create_app(
    *,
    database_path: Path = DEFAULT_DATABASE,
    registry_root: Path = DEFAULT_REGISTRY_ROOT,
    upload_root: Path = DEFAULT_UPLOAD_ROOT,
    web_dist: Path = DEFAULT_WEB_DIST,
) -> FastAPI:
    app = FastAPI(
        title="Process Intelligence Workbench",
        version="0.3.0",
        docs_url="/api/docs",
        openapi_url="/api/openapi.json",
    )
    app.state.database_path = Path(database_path)
    app.state.resolver = DatasetResolver(database_path, registry_root)
    app.state.registry = app.state.resolver.registry
    app.state.upload_root = Path(upload_root)
    app.state.web_dist = Path(web_dist)

    def require_dataset(log_id: str) -> ResolvedDataset | JSONResponse:
        try:
            resolved = app.state.resolver.find(log_id)
        except DatasetRegistryError:
            return _error(
                503, "REGISTRY_UNAVAILABLE", "the local dataset registry cannot be read"
            )
        if resolved is None:
            return _error(
                404,
                "UNKNOWN_LOG",
                f"log_id {log_id!r} is not a known local dataset",
                {"log_id": log_id},
            )
        if not resolved.is_ready:
            return _error(
                503,
                "DATASET_UNAVAILABLE",
                f"log_id {log_id!r} is not a ready local dataset",
                {"log_id": log_id, "status": resolved.status},
            )
        return resolved

    def analyze(log_id: str, method: str, **arguments: Any) -> Any:
        resolved = require_dataset(log_id)
        if isinstance(resolved, JSONResponse):
            return resolved
        return _call_surface(resolved, method, **arguments)

    @app.get("/api/health")
    def health() -> Any:
        """Application health: it never fails because one optional dataset is not ready."""

        try:
            items = app.state.resolver.items()
        except DatasetRegistryError:
            return _error(
                503, "REGISTRY_UNAVAILABLE", "the local dataset registry cannot be read"
            )
        counts = Counter(item.status for item in items)
        canonical = next(item for item in items if item.built_in)
        return {
            "api_schema_version": API_SCHEMA_VERSION,
            "canonical_dataset": {
                "display_name": canonical.display_name,
                "log_fingerprint": canonical.sha256,
                "log_id": canonical.log_id,
                "status": canonical.status,
            },
            "datasets": {
                "ready": counts.get(STATUS_READY, 0),
                "registered": counts.get(STATUS_REGISTERED, 0),
                "total": len(items),
                "unavailable": counts.get(STATUS_UNAVAILABLE, 0),
            },
            "metric_definition_version": METRIC_DEFINITION_VERSION,
            "status": "ok",
            "tool_schema_version": TOOL_SCHEMA_VERSION,
        }

    @app.get("/api/logs")
    def list_logs() -> Any:
        try:
            items = app.state.resolver.items()
        except DatasetRegistryError:
            return _error(
                503, "REGISTRY_UNAVAILABLE", "the local dataset registry cannot be read"
            )
        return {
            "api_schema_version": API_SCHEMA_VERSION,
            "canonical_log_id": BPIC2012.log_id,
            "items": [item.as_item() for item in items],
        }

    @app.post("/api/imports/preview")
    async def preview_import(
        request: Request,
        filename: str = Query(description="Browser-provided source file name"),
        delimiter: str = Query(default=",", description="CSV delimiter; ignored for XES"),
    ) -> Any:
        """Store the selected bytes locally and describe the source for mapping.

        The strict CSV parser runs over the staged bytes before they are
        promoted, so a source the preview itself rejects is discarded from
        staging: no import of those bytes is created, and the promoted bytes of
        an import already stored under the same deterministic reference are
        never touched.
        """

        try:
            sink = ImportSink(app.state.upload_root, filename)
        except ImportContractError as exc:
            return _error(400, exc.code, exc.message, exc.details)
        try:
            async for chunk in request.stream():
                sink.write(chunk)
            csv_preview = None
            if sink.source_format == SOURCE_FORMAT_CSV:
                csv_preview = preview_csv(sink.seal(), delimiter).as_dict()
            stored = sink.finish()
        except SourceContractError as exc:
            sink.discard()
            return _error(
                400,
                "SOURCE_CONTRACT_ERROR",
                str(exc),
                {"filename": sink.filename},
            )
        except ImportContractError as exc:
            sink.discard()
            return _error(400, exc.code, exc.message, exc.details)
        except BaseException:
            sink.discard()
            raise
        return {
            "api_schema_version": API_SCHEMA_VERSION,
            "csv_preview": csv_preview,
            "import": stored.as_summary(),
        }

    @app.post("/api/imports/build")
    async def build_import(request: Request) -> Any:
        """Validate a stored import, and register it only after the build passes."""

        try:
            payload = await request.json()
        except Exception:
            return _error(
                400, "INVALID_IMPORT_REQUEST", "the build request must be a JSON object"
            )
        if not isinstance(payload, dict):
            return _error(
                400, "INVALID_IMPORT_REQUEST", "the build request must be a JSON object"
            )
        try:
            stored = resolve_import(app.state.upload_root, payload.get("import_id"))
        except ImportContractError as exc:
            return _error(
                _import_error_status(exc.code), exc.code, exc.message, exc.details
            )
        try:
            descriptor = _import_descriptor(app.state, stored, payload)
        except (SourceContractError, ImportContractError) as exc:
            code = getattr(exc, "code", None) or "SOURCE_CONTRACT_ERROR"
            return _error(
                _import_error_status(code),
                code,
                str(exc),
                getattr(exc, "details", None),
            )
        try:
            existing = app.state.registry.find(descriptor.log_id)
        except DatasetRegistryError:
            return _error(
                503, "REGISTRY_UNAVAILABLE", "the local dataset registry cannot be read"
            )
        if existing is not None and existing.dataset_id != descriptor.dataset_id:
            return _error(
                409,
                "CONFLICTING_DATASET_IDENTITY",
                f"log_id {descriptor.log_id!r} is already registered with a different "
                "dataset identity; choose a new log_id for this source and configuration",
                {"log_id": descriptor.log_id},
            )
        staging = staging_database_path(descriptor.database_path)
        discard_staged_database(staging)
        try:
            report = build_dataset(replace(descriptor, database_path=staging))
        except SourceContractError as exc:
            discard_staged_database(staging)
            return _error(
                400, "SOURCE_CONTRACT_ERROR", str(exc), {"log_id": descriptor.log_id}
            )
        except (duckdb.Error, OSError):
            discard_staged_database(staging)
            return _error(
                503,
                "DATASET_UNAVAILABLE",
                "the local staging build could not be written",
                {"log_id": descriptor.log_id},
            )
        if report["validation"]["status"] != "PASS":
            failed = [
                item["name"]
                for item in report["validation"]["invariants"]
                if not item["passed"]
            ]
            discard_staged_database(staging)
            return _error(
                422,
                "VALIDATION_FAILED",
                "the canonical structural invariants rejected this build, so no dataset "
                "was registered and no existing dataset was replaced",
                {"failed_invariants": failed, "log_id": descriptor.log_id},
            )
        promote_staged_database(staging, descriptor.database_path)
        try:
            app.state.registry.register(descriptor)
        except DatasetRegistryError as exc:
            return _error(
                409,
                "CONFLICTING_DATASET_IDENTITY",
                str(exc),
                {"log_id": descriptor.log_id},
            )
        resolved = app.state.resolver.registered(descriptor)
        return {
            "api_schema_version": API_SCHEMA_VERSION,
            "build": {
                "analysis_event_count": report["analysis_event_count"],
                "case_count": report["raw_summary"]["case_count"],
                "raw_event_count": report["raw_summary"]["event_count"],
                "validation_status": report["validation"]["status"],
                "variant_count": report["variant_summary"]["variant_count"],
            },
            "dataset": resolved.as_item(),
        }

    @app.get("/api/logs/{log_id}/summary")
    def summary(
        log_id: str, sla_threshold_ms: str | None = Query(default=None)
    ) -> Any:
        resolved = require_dataset(log_id)
        if isinstance(resolved, JSONResponse):
            return resolved
        arguments: dict[str, Any] = {}
        if sla_threshold_ms is not None:
            arguments["sla_threshold_ms"] = _integer_parameter(sla_threshold_ms)
        response = _call_surface(resolved, "describe_log", **arguments)
        if isinstance(response, JSONResponse):
            return response
        try:
            supplement = _supplemental_gap_p50(
                resolved.database_path, resolved.log_id, response
            )
        except (FileNotFoundError, OSError, duckdb.Error):
            return _error(
                503,
                "DATASET_UNAVAILABLE",
                f"log_id {resolved.log_id!r} cannot serve the Core gap supplement",
                {"log_id": resolved.log_id},
            )
        return {
            **response,
            "api_schema_version": API_SCHEMA_VERSION,
            "supplemental": supplement,
        }

    @app.get("/api/logs/{log_id}/variants")
    def variants(
        log_id: str,
        order_by: str = Query(default="case_count_desc"),
        limit: str = Query(default="10"),
    ) -> Any:
        return analyze(
            log_id,
            "list_variants",
            limit=_integer_parameter(limit),
            order_by=order_by,
        )

    @app.get("/api/logs/{log_id}/transitions")
    def transitions(
        log_id: str,
        from_activity: str | None = Query(default=None),
        to_activity: str | None = Query(default=None),
        order_by: str = Query(default="transition_count_desc"),
        limit: str = Query(default="10"),
    ) -> Any:
        arguments: dict[str, Any] = {
            "limit": _integer_parameter(limit),
            "order_by": order_by,
        }
        if from_activity is not None:
            arguments["from_activity"] = from_activity
        if to_activity is not None:
            arguments["to_activity"] = to_activity
        return analyze(log_id, "list_transitions", **arguments)

    @app.get("/api/logs/{log_id}/activities")
    def activities(
        log_id: str,
        order_by: str = Query(default="rework_event_count_desc"),
        limit: str = Query(default="10"),
    ) -> Any:
        return analyze(
            log_id,
            "list_activities",
            limit=_integer_parameter(limit),
            order_by=order_by,
        )

    @app.get("/api/logs/{log_id}/cases/{case_id}")
    def case_trace(
        log_id: str,
        case_id: str,
        perspective: str = Query(default="complete"),
    ) -> Any:
        return analyze(
            log_id, "get_case_trace", case_id=case_id, perspective=perspective
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
    parser = argparse.ArgumentParser(description="Run the local TraceVerity workbench")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument(
        "--registry-root",
        type=Path,
        default=DEFAULT_REGISTRY_ROOT,
        help="Local dataset registry workspace (gitignored by default)",
    )
    parser.add_argument(
        "--upload-root",
        type=Path,
        default=DEFAULT_UPLOAD_ROOT,
        help="Local browser-import workspace (gitignored by default)",
    )
    parser.add_argument("--web-dist", type=Path, default=DEFAULT_WEB_DIST)
    args = parser.parse_args()
    if not 1 <= args.port <= 65_535:
        parser.error("--port must be within 1..65535")
    uvicorn.run(
        create_app(
            database_path=args.database,
            registry_root=args.registry_root,
            upload_root=args.upload_root,
            web_dist=args.web_dist,
        ),
        host="127.0.0.1",
        port=args.port,
    )


if __name__ == "__main__":
    main()
