"""Generic read-only Core fact surface for one resolved local dataset.

`CoreReadSurface` is the internal, dataset-parameterized read path: it performs
exactly the deterministic `slice0-metrics-v1` queries the Slice 1 tool boundary
has always performed, but resolves them against any local DuckDB database that
satisfies the canonical event contract instead of one hardcoded log.

`piw.tools.CoreToolSurface` is the published external wrapper over this class:
Recovery Slice D generalized it to resolve any ready dataset by runtime
`log_id` (built-in BPIC12 or a registered dataset) under the `slice-d-tool-v2`
schema version, without adding a data access path of its own.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .core import (
    EVENT_COLUMNS,
    METRIC_DEFINITION_VERSION,
    activity_summary,
    case_trace,
    complete_traces,
    cycle_time_summary,
    observed_gap_summary,
    raw_summary,
    rework_summary,
    sla_summary,
    transition_summary,
    variant_summary,
)
from .datasets import canonical_json

# Recovery Slice D deliberately bumps the external tool schema version: every
# tool now accepts `log_id` as a runtime dataset identifier instead of the
# compiled-in BPIC12 constant. `schema_version` participates in `query_id` and
# `fact_id`, so this change intentionally moves every deterministic ID of the
# tool boundary, including BPIC12 answers whose facts themselves are unchanged.
TOOL_SCHEMA_VERSION = "slice-d-tool-v2"
STATUS_READY = "ready"
STATUS_REGISTERED = "registered"
STATUS_UNAVAILABLE = "unavailable"
"""The published dataset-readiness vocabulary: a database is `ready` only when
it serves the canonical read path, a registered dataset without a database is
`registered`, and anything else is `unavailable`."""


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def _stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{hashlib.sha256(canonical_bytes(value)).hexdigest()}"


class CoreSurfaceError(ValueError):
    """Structured read-contract violation, published in the tool error shape."""

    def __init__(
        self, code: str, message: str, details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def response(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "details": self.details,
                "message": self.message,
            },
            "schema_version": TOOL_SCHEMA_VERSION,
        }


def require_string(name: str, value: Any, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise CoreSurfaceError(
            "INVALID_ARGUMENT", f"{name} must be a non-empty string", {"parameter": name}
        )
    return value


def require_integer(name: str, value: Any, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CoreSurfaceError(
            "INVALID_ARGUMENT", f"{name} must be an integer", {"parameter": name}
        )
    if minimum is not None and value < minimum:
        raise CoreSurfaceError(
            "INVALID_ARGUMENT",
            f"{name} must be at least {minimum}",
            {"parameter": name},
        )
    return value


@dataclass(frozen=True, slots=True)
class DatabaseHealth:
    """Readiness of one local DuckDB dataset, proven by actual reads.

    `status` is `ready` only when the canonical event columns, the
    `analysis_events` view, exactly one source fingerprint row, and a read over
    both relations were served by the local database. `detail` is local
    diagnostics and is never published over HTTP.
    """

    status: str
    detail: str | None = None
    filename: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None

    @property
    def is_ready(self) -> bool:
        return self.status == STATUS_READY

    def as_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "status": self.status,
        }


def database_health(database_path: Path) -> DatabaseHealth:
    """Prove that a database can serve the canonical Core read path.

    A file that merely exists is not ready: a truncated, foreign, or
    inconsistent database fails every check here instead of reaching a query.
    """

    path = Path(database_path)
    if not path.is_file():
        return DatabaseHealth(
            status=STATUS_UNAVAILABLE, detail="database file does not exist"
        )
    try:
        with duckdb.connect(str(path), read_only=True) as connection:
            columns = {
                row[0] for row in connection.execute("DESCRIBE events").fetchall()
            }
            missing = [name for name, _ in EVENT_COLUMNS if name not in columns]
            if missing:
                return DatabaseHealth(
                    status=STATUS_UNAVAILABLE,
                    detail=f"events is missing required columns {missing}",
                )
            rows = connection.execute(
                "SELECT filename, sha256, size_bytes FROM source_metadata"
            ).fetchall()
            if len(rows) != 1:
                return DatabaseHealth(
                    status=STATUS_UNAVAILABLE,
                    detail="source_metadata must contain exactly one fingerprint row",
                )
            filename, sha256, size_bytes = rows[0]
            if not isinstance(sha256, str) or not sha256:
                return DatabaseHealth(
                    status=STATUS_UNAVAILABLE, detail="source_metadata has no fingerprint"
                )
            # Real deterministic Core reads, not just schema/count probes: the
            # raw fact summary runs over `events`, and the activity/rework
            # metric runs over the `analysis_events` control-flow perspective.
            raw_summary(connection)
            activity_summary(connection)
    except (duckdb.Error, OSError, ValueError) as exc:
        return DatabaseHealth(
            status=STATUS_UNAVAILABLE,
            detail=f"{type(exc).__name__}: database cannot serve the canonical read path",
        )
    return DatabaseHealth(
        status=STATUS_READY,
        filename=None if filename is None else str(filename),
        sha256=str(sha256),
        size_bytes=None if size_bytes is None else int(size_bytes),
    )


class CoreReadSurface:
    """The generic read-only fact surface for one resolved local dataset.

    The implementation is the established deterministic Core query path; only
    the resolution of `database_path` and `log_id` is parameterized, so every
    consumer of this class derives the same facts from the same Core functions.
    """

    def __init__(self, database_path: Path, log_id: str) -> None:
        self._database_path = Path(database_path)
        self._log_id = require_string("log_id", log_id)
        if not self._database_path.is_file():
            raise FileNotFoundError(self._database_path)

    @property
    def log_id(self) -> str:
        return self._log_id

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self._database_path), read_only=True)

    def _fingerprint(self, connection: duckdb.DuckDBPyConnection) -> str:
        rows = connection.execute("SELECT sha256 FROM source_metadata").fetchall()
        if len(rows) != 1 or not rows[0][0]:
            raise CoreSurfaceError(
                "DATA_CONTRACT_ERROR", "source_metadata must contain one fingerprint"
            )
        return str(rows[0][0])

    def _success(
        self,
        *,
        tool: str,
        parameters: dict[str, Any],
        result: Any,
        fact_values: list[tuple[str, Any]],
        log_fingerprint: str,
    ) -> dict[str, Any]:
        query_id = _stable_id(
            "q",
            {
                "log_fingerprint": log_fingerprint,
                "metric_definition_version": METRIC_DEFINITION_VERSION,
                "parameters": parameters,
                "schema_version": TOOL_SCHEMA_VERSION,
                "tool": tool,
            },
        )
        facts = [
            {
                "fact_id": _stable_id(
                    "f", {"name": name, "query_id": query_id, "value": value}
                ),
                "name": name,
                "value": value,
            }
            for name, value in fact_values
        ]
        return {
            "facts": facts,
            "log_fingerprint": log_fingerprint,
            "metric_definition_version": METRIC_DEFINITION_VERSION,
            "parameters": parameters,
            "query_id": query_id,
            "result": result,
            "schema_version": TOOL_SCHEMA_VERSION,
        }

    def describe_log(self, sla_threshold_ms: Any | None = None) -> dict[str, Any]:
        threshold = (
            None
            if sla_threshold_ms is None
            else require_integer("sla_threshold_ms", sla_threshold_ms, minimum=0)
        )
        parameters = {"log_id": self._log_id, "sla_threshold_ms": threshold}
        with self._connect() as connection:
            fingerprint = self._fingerprint(connection)
            raw = raw_summary(connection)
            analysis_event_count = connection.execute(
                "SELECT count(*) FROM analysis_events"
            ).fetchone()[0]
            traces = complete_traces(connection)
            variants = variant_summary(traces, len(traces))
            transitions = transition_summary(connection)
            cycle = cycle_time_summary(connection)
            gaps = observed_gap_summary(connection)
            rework = rework_summary(connection)
            values: dict[str, Any] = {
                "aggregate_rework_event_count": rework[
                    "aggregate_rework_event_count"
                ],
                "analysis_event_count": analysis_event_count,
                "case_count": raw["case_count"],
                "cases_with_rework": rework["cases_with_rework"],
                "cycle_time_max_ms": cycle["max_ms"],
                "cycle_time_p50_ms": cycle["p50_ms"],
                "cycle_time_p90_ms": cycle["p90_ms"],
                "direct_follow_count": transitions["transition_count"],
                "distinct_activity_count": raw["distinct_activity_count"],
                "observed_gap_p90_ms": gaps["p90_ms"],
                "raw_event_count": raw["event_count"],
                "variant_count": variants["variant_count"],
            }
            if threshold is not None:
                sla = sla_summary(connection, threshold)
                values.update(
                    {
                        "sla_threshold_ms": sla["threshold_ms"],
                        "sla_violation_case_count": sla["violation_case_count"],
                        "sla_violation_case_share": sla["violation_case_share"],
                    }
                )
        return self._success(
            tool="describe_log",
            parameters=parameters,
            result=values,
            fact_values=list(values.items()),
            log_fingerprint=fingerprint,
        )

    def list_variants(self, order_by: Any, limit: Any) -> dict[str, Any]:
        order = require_string("order_by", order_by)
        if order not in {"case_count_desc", "variant_id_asc"}:
            raise CoreSurfaceError("INVALID_ARGUMENT", "unsupported variant order_by")
        bounded_limit = require_integer("limit", limit, minimum=1)
        if bounded_limit > 100:
            raise CoreSurfaceError("INVALID_ARGUMENT", "limit must be at most 100")
        parameters = {
            "limit": bounded_limit,
            "log_id": self._log_id,
            "order_by": order,
        }
        with self._connect() as connection:
            fingerprint = self._fingerprint(connection)
            traces = complete_traces(connection)
            items = variant_summary(traces, len(traces))["variants"]
        if order == "variant_id_asc":
            items = sorted(items, key=lambda item: item["variant_id"])
        items = items[:bounded_limit]
        return self._success(
            tool="list_variants",
            parameters=parameters,
            result={"items": items},
            fact_values=[(f"variant_{index}", item) for index, item in enumerate(items, 1)],
            log_fingerprint=fingerprint,
        )

    def list_transitions(
        self,
        order_by: Any,
        limit: Any,
        from_activity: Any | None = None,
        to_activity: Any | None = None,
    ) -> dict[str, Any]:
        order = require_string("order_by", order_by)
        if order not in {"transition_count_desc", "activities_asc"}:
            raise CoreSurfaceError(
                "INVALID_ARGUMENT", "unsupported transition order_by"
            )
        bounded_limit = require_integer("limit", limit, minimum=1)
        if bounded_limit > 100:
            raise CoreSurfaceError("INVALID_ARGUMENT", "limit must be at most 100")
        source = (
            None
            if from_activity is None
            else require_string("from_activity", from_activity)
        )
        target = (
            None if to_activity is None else require_string("to_activity", to_activity)
        )
        parameters = {
            "from_activity": source,
            "limit": bounded_limit,
            "log_id": self._log_id,
            "order_by": order,
            "to_activity": target,
        }
        with self._connect() as connection:
            fingerprint = self._fingerprint(connection)
            items = transition_summary(connection)["transitions"]
        items = [
            item
            for item in items
            if (source is None or item["from_activity"] == source)
            and (target is None or item["to_activity"] == target)
        ]
        if order == "activities_asc":
            items.sort(key=lambda item: (item["from_activity"], item["to_activity"]))
        items = items[:bounded_limit]
        return self._success(
            tool="list_transitions",
            parameters=parameters,
            result={"items": items},
            fact_values=[
                (f"transition_{index}", item) for index, item in enumerate(items, 1)
            ],
            log_fingerprint=fingerprint,
        )

    def list_activities(self, order_by: Any, limit: Any) -> dict[str, Any]:
        order = require_string("order_by", order_by)
        allowed = {
            "event_count_desc",
            "case_count_desc",
            "rework_event_count_desc",
            "activity_asc",
        }
        if order not in allowed:
            raise CoreSurfaceError("INVALID_ARGUMENT", "unsupported activity order_by")
        bounded_limit = require_integer("limit", limit, minimum=1)
        if bounded_limit > 100:
            raise CoreSurfaceError("INVALID_ARGUMENT", "limit must be at most 100")
        parameters = {
            "limit": bounded_limit,
            "log_id": self._log_id,
            "order_by": order,
        }
        with self._connect() as connection:
            fingerprint = self._fingerprint(connection)
            items = activity_summary(connection)
        if order == "activity_asc":
            items.sort(key=lambda item: item["activity"])
        elif order != "event_count_desc":
            field = order.removesuffix("_desc")
            items.sort(key=lambda item: (-item[field], item["activity"]))
        items = items[:bounded_limit]
        return self._success(
            tool="list_activities",
            parameters=parameters,
            result={"items": items},
            fact_values=[
                (f"activity_{index}", item) for index, item in enumerate(items, 1)
            ],
            log_fingerprint=fingerprint,
        )

    def get_case_trace(self, case_id: Any, perspective: Any) -> dict[str, Any]:
        normalized_case = require_string("case_id", case_id)
        normalized_perspective = require_string("perspective", perspective)
        if normalized_perspective not in {"complete", "raw"}:
            raise CoreSurfaceError("INVALID_ARGUMENT", "unsupported perspective")
        parameters = {
            "case_id": normalized_case,
            "log_id": self._log_id,
            "perspective": normalized_perspective,
        }
        with self._connect() as connection:
            fingerprint = self._fingerprint(connection)
            exists = connection.execute(
                "SELECT count(*) FROM events WHERE case_id = ?", [normalized_case]
            ).fetchone()[0]
            if not exists:
                raise CoreSurfaceError(
                    "NOT_FOUND", "case_id does not exist", {"case_id": normalized_case}
                )
            events = case_trace(connection, normalized_case, normalized_perspective)
        value = {
            "activities": [event["activity"] for event in events],
            "case_id": normalized_case,
            "perspective": normalized_perspective,
        }
        return self._success(
            tool="get_case_trace",
            parameters=parameters,
            result=value,
            fact_values=[("case_trace", value)],
            log_fingerprint=fingerprint,
        )
