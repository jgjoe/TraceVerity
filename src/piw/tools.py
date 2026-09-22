from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb

from .core import (
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

TOOL_SCHEMA_VERSION = "slice1-tool-v1"
SUPPORTED_LOG_ID = "bpic2012"


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def canonical_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def _stable_id(prefix: str, value: Any) -> str:
    return f"{prefix}_{hashlib.sha256(canonical_bytes(value)).hexdigest()}"


class ToolContractError(ValueError):
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


def _require_string(name: str, value: Any, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ToolContractError(
            "INVALID_ARGUMENT", f"{name} must be a non-empty string", {"parameter": name}
        )
    return value


def _require_integer(name: str, value: Any, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ToolContractError(
            "INVALID_ARGUMENT", f"{name} must be an integer", {"parameter": name}
        )
    if minimum is not None and value < minimum:
        raise ToolContractError(
            "INVALID_ARGUMENT",
            f"{name} must be at least {minimum}",
            {"parameter": name},
        )
    return value


class CoreToolSurface:
    """The complete read-only capability boundary exposed to the Slice 1 Agent."""

    _PARAMETERS = {
        "describe_log": {"log_id", "sla_threshold_ms"},
        "list_variants": {"log_id", "order_by", "limit"},
        "list_transitions": {
            "log_id",
            "from_activity",
            "to_activity",
            "order_by",
            "limit",
        },
        "list_activities": {"log_id", "order_by", "limit"},
        "get_case_trace": {"log_id", "case_id", "perspective"},
    }

    def __init__(self, database_path: Path) -> None:
        self._database_path = Path(database_path)
        if not self._database_path.is_file():
            raise FileNotFoundError(self._database_path)

    @staticmethod
    def definitions() -> list[dict[str, Any]]:
        return [
            {
                "name": "describe_log",
                "description": "Return deterministic log scalars and optional configured SLA facts.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "log_id": {"const": SUPPORTED_LOG_ID},
                        "sla_threshold_ms": {"type": "integer", "minimum": 0},
                    },
                    "required": ["log_id"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "list_variants",
                "description": "List deterministic COMPLETE-perspective variants.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "log_id": {"const": SUPPORTED_LOG_ID},
                        "order_by": {
                            "enum": ["case_count_desc", "variant_id_asc"]
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    },
                    "required": ["log_id", "order_by", "limit"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "list_transitions",
                "description": "List deterministic COMPLETE-perspective direct-follow transitions.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "log_id": {"const": SUPPORTED_LOG_ID},
                        "from_activity": {"type": ["string", "null"]},
                        "to_activity": {"type": ["string", "null"]},
                        "order_by": {
                            "enum": ["transition_count_desc", "activities_asc"]
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    },
                    "required": ["log_id", "order_by", "limit"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "list_activities",
                "description": "List deterministic COMPLETE-perspective activity facts.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "log_id": {"const": SUPPORTED_LOG_ID},
                        "order_by": {
                            "enum": [
                                "event_count_desc",
                                "case_count_desc",
                                "rework_event_count_desc",
                                "activity_asc",
                            ]
                        },
                        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                    },
                    "required": ["log_id", "order_by", "limit"],
                    "additionalProperties": False,
                },
            },
            {
                "name": "get_case_trace",
                "description": "Return one deterministic raw or COMPLETE-perspective case trace.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "log_id": {"const": SUPPORTED_LOG_ID},
                        "case_id": {"type": "string", "minLength": 1},
                        "perspective": {"enum": ["complete", "raw"]},
                    },
                    "required": ["log_id", "case_id", "perspective"],
                    "additionalProperties": False,
                },
            },
        ]

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self._database_path), read_only=True)

    def _fingerprint(self, connection: duckdb.DuckDBPyConnection) -> str:
        rows = connection.execute("SELECT sha256 FROM source_metadata").fetchall()
        if len(rows) != 1 or not rows[0][0]:
            raise ToolContractError(
                "DATA_CONTRACT_ERROR", "source_metadata must contain one fingerprint"
            )
        return str(rows[0][0])

    def _validate_log_id(self, log_id: Any) -> str:
        value = _require_string("log_id", log_id)
        if value != SUPPORTED_LOG_ID:
            raise ToolContractError(
                "UNKNOWN_LOG", "unsupported log_id", {"log_id": value}
            )
        return value

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

    def dispatch(self, tool: Any, arguments: Any) -> dict[str, Any]:
        try:
            name = _require_string("tool", tool)
            if name not in self._PARAMETERS:
                raise ToolContractError(
                    "FORBIDDEN_TOOL", "tool is not in the read-only allowlist", {"tool": name}
                )
            if not isinstance(arguments, dict):
                raise ToolContractError(
                    "INVALID_ARGUMENT", "tool arguments must be an object"
                )
            unsupported = sorted(set(arguments) - self._PARAMETERS[name])
            if unsupported:
                raise ToolContractError(
                    "UNSUPPORTED_PARAMETER",
                    "unsupported tool parameter",
                    {"parameters": unsupported, "tool": name},
                )
            return getattr(self, name)(**arguments)
        except ToolContractError as exc:
            return exc.response()
        except TypeError as exc:
            return ToolContractError(
                "INVALID_ARGUMENT", "missing or invalid tool parameters", {"reason": str(exc)}
            ).response()

    def describe_log(
        self, log_id: Any, sla_threshold_ms: Any | None = None
    ) -> dict[str, Any]:
        normalized_log = self._validate_log_id(log_id)
        threshold = (
            None
            if sla_threshold_ms is None
            else _require_integer("sla_threshold_ms", sla_threshold_ms, minimum=0)
        )
        parameters = {"log_id": normalized_log, "sla_threshold_ms": threshold}
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

    def list_variants(self, log_id: Any, order_by: Any, limit: Any) -> dict[str, Any]:
        normalized_log = self._validate_log_id(log_id)
        order = _require_string("order_by", order_by)
        if order not in {"case_count_desc", "variant_id_asc"}:
            raise ToolContractError("INVALID_ARGUMENT", "unsupported variant order_by")
        bounded_limit = _require_integer("limit", limit, minimum=1)
        if bounded_limit > 100:
            raise ToolContractError("INVALID_ARGUMENT", "limit must be at most 100")
        parameters = {
            "limit": bounded_limit,
            "log_id": normalized_log,
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
        log_id: Any,
        order_by: Any,
        limit: Any,
        from_activity: Any | None = None,
        to_activity: Any | None = None,
    ) -> dict[str, Any]:
        normalized_log = self._validate_log_id(log_id)
        order = _require_string("order_by", order_by)
        if order not in {"transition_count_desc", "activities_asc"}:
            raise ToolContractError("INVALID_ARGUMENT", "unsupported transition order_by")
        bounded_limit = _require_integer("limit", limit, minimum=1)
        if bounded_limit > 100:
            raise ToolContractError("INVALID_ARGUMENT", "limit must be at most 100")
        source = None if from_activity is None else _require_string("from_activity", from_activity)
        target = None if to_activity is None else _require_string("to_activity", to_activity)
        parameters = {
            "from_activity": source,
            "limit": bounded_limit,
            "log_id": normalized_log,
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

    def list_activities(
        self, log_id: Any, order_by: Any, limit: Any
    ) -> dict[str, Any]:
        normalized_log = self._validate_log_id(log_id)
        order = _require_string("order_by", order_by)
        allowed = {
            "event_count_desc",
            "case_count_desc",
            "rework_event_count_desc",
            "activity_asc",
        }
        if order not in allowed:
            raise ToolContractError("INVALID_ARGUMENT", "unsupported activity order_by")
        bounded_limit = _require_integer("limit", limit, minimum=1)
        if bounded_limit > 100:
            raise ToolContractError("INVALID_ARGUMENT", "limit must be at most 100")
        parameters = {
            "limit": bounded_limit,
            "log_id": normalized_log,
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

    def get_case_trace(
        self, log_id: Any, case_id: Any, perspective: Any
    ) -> dict[str, Any]:
        normalized_log = self._validate_log_id(log_id)
        normalized_case = _require_string("case_id", case_id)
        normalized_perspective = _require_string("perspective", perspective)
        if normalized_perspective not in {"complete", "raw"}:
            raise ToolContractError("INVALID_ARGUMENT", "unsupported perspective")
        parameters = {
            "case_id": normalized_case,
            "log_id": normalized_log,
            "perspective": normalized_perspective,
        }
        with self._connect() as connection:
            fingerprint = self._fingerprint(connection)
            exists = connection.execute(
                "SELECT count(*) FROM events WHERE case_id = ?", [normalized_case]
            ).fetchone()[0]
            if not exists:
                raise ToolContractError(
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
