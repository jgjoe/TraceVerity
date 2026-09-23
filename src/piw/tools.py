from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from .core_surface import (
    TOOL_SCHEMA_VERSION,
    CoreReadSurface,
    CoreSurfaceError,
    canonical_bytes,
    canonical_json,
    require_string,
)
from .datasets import DEFAULT_REGISTRY_ROOT, DatasetRegistryError
from .resolution import DatasetResolver

# The published module API keeps the historical name of the read-contract error
# that every structured tool failure is raised and returned as.
ToolContractError = CoreSurfaceError

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

# `log_id` is a runtime dataset identifier, never a compiled-in dataset name:
# the same five tools serve the built-in baseline and any registered dataset.
_LOG_ID_SCHEMA = {"type": "string", "minLength": 1}


class CoreToolSurface:
    """The complete read-only capability boundary exposed to the local Agent.

    The five tools, their arguments, ordering enums, limits, response
    semantics, and deterministic IDs are the published `slice-d-tool-v2`
    contract. Every call resolves its `log_id` through the shared dataset
    resolver and then reads through the generic `CoreReadSurface`, so this
    boundary — and every consumer of it — contains no metric implementation.
    """

    def __init__(
        self,
        database_path: Path,
        registry_root: Path = DEFAULT_REGISTRY_ROOT,
    ) -> None:
        self._resolver = DatasetResolver(database_path, registry_root)

    @property
    def database_path(self) -> Path:
        return self._resolver.database_path

    @property
    def resolver(self) -> DatasetResolver:
        return self._resolver

    @staticmethod
    def definitions() -> list[dict[str, Any]]:
        return [
            {
                "name": "describe_log",
                "description": "Return deterministic log scalars and optional configured SLA facts.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "log_id": dict(_LOG_ID_SCHEMA),
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
                        "log_id": dict(_LOG_ID_SCHEMA),
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
                        "log_id": dict(_LOG_ID_SCHEMA),
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
                        "log_id": dict(_LOG_ID_SCHEMA),
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
                        "log_id": dict(_LOG_ID_SCHEMA),
                        "case_id": {"type": "string", "minLength": 1},
                        "perspective": {"enum": ["complete", "raw"]},
                    },
                    "required": ["log_id", "case_id", "perspective"],
                    "additionalProperties": False,
                },
            },
        ]

    def _surface(self, log_id: Any) -> CoreReadSurface:
        """Resolve one ready dataset, or fail closed with a structured error.

        Error details carry the requested identifier and the published dataset
        status only — never a local machine path.
        """

        value = require_string("log_id", log_id)
        try:
            resolved = self._resolver.find(value)
        except DatasetRegistryError:
            raise CoreSurfaceError(
                "DATA_CONTRACT_ERROR",
                "the local dataset registry cannot be resolved",
                {"log_id": value},
            ) from None
        if resolved is None:
            raise CoreSurfaceError(
                "UNKNOWN_LOG", "log_id is not a known local dataset", {"log_id": value}
            )
        if not resolved.is_ready:
            raise CoreSurfaceError(
                "DATASET_UNAVAILABLE",
                "log_id is not a ready local dataset",
                {"log_id": value, "status": resolved.status},
            )
        return CoreReadSurface(resolved.database_path, resolved.log_id)

    def _call(self, method: str, log_id: Any, **arguments: Any) -> dict[str, Any]:
        surface = self._surface(log_id)
        try:
            return getattr(surface, method)(**arguments)
        except (OSError, duckdb.Error):
            raise CoreSurfaceError(
                "DATASET_UNAVAILABLE",
                "the resolved dataset cannot serve the canonical read path",
                {"log_id": surface.log_id},
            ) from None

    def dispatch(self, tool: Any, arguments: Any) -> dict[str, Any]:
        try:
            name = require_string("tool", tool)
            if name not in _PARAMETERS:
                raise CoreSurfaceError(
                    "FORBIDDEN_TOOL", "tool is not in the read-only allowlist", {"tool": name}
                )
            if not isinstance(arguments, dict):
                raise CoreSurfaceError(
                    "INVALID_ARGUMENT", "tool arguments must be an object"
                )
            unsupported = sorted(set(arguments) - _PARAMETERS[name])
            if unsupported:
                raise CoreSurfaceError(
                    "UNSUPPORTED_PARAMETER",
                    "unsupported tool parameter",
                    {"parameters": unsupported, "tool": name},
                )
            return getattr(self, name)(**arguments)
        except CoreSurfaceError as exc:
            return exc.response()
        except TypeError as exc:
            return CoreSurfaceError(
                "INVALID_ARGUMENT", "missing or invalid tool parameters", {"reason": str(exc)}
            ).response()

    def describe_log(
        self, log_id: Any, sla_threshold_ms: Any | None = None
    ) -> dict[str, Any]:
        return self._call("describe_log", log_id, sla_threshold_ms=sla_threshold_ms)

    def list_variants(self, log_id: Any, order_by: Any, limit: Any) -> dict[str, Any]:
        return self._call("list_variants", log_id, order_by=order_by, limit=limit)

    def list_transitions(
        self,
        log_id: Any,
        order_by: Any,
        limit: Any,
        from_activity: Any | None = None,
        to_activity: Any | None = None,
    ) -> dict[str, Any]:
        return self._call(
            "list_transitions",
            log_id,
            order_by=order_by,
            limit=limit,
            from_activity=from_activity,
            to_activity=to_activity,
        )

    def list_activities(self, log_id: Any, order_by: Any, limit: Any) -> dict[str, Any]:
        return self._call("list_activities", log_id, order_by=order_by, limit=limit)

    def get_case_trace(
        self, log_id: Any, case_id: Any, perspective: Any
    ) -> dict[str, Any]:
        return self._call(
            "get_case_trace", log_id, case_id=case_id, perspective=perspective
        )
