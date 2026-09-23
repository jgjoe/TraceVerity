from __future__ import annotations

import hashlib
import json
import csv
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .csv_ingest import csv_trace_count, iter_csv_events
from .datasets import (
    SOURCE_FORMAT_CSV,
    SOURCE_FORMAT_XES,
    DatasetDescriptor,
    SourceFingerprint,
    source_fingerprint,
)
from .events import COMPLETE_LIFECYCLE, Event, SourceContractError
from .profiles import BPIC2012
from .xes import iter_xes_events, source_trace_count

METRIC_DEFINITION_VERSION = "slice0-metrics-v1"
REPORT_SCHEMA_VERSION = "slice0-report-v1"
DATASET_REPORT_SCHEMA_VERSION = "dataset-report-v1"
EVENT_COLUMNS = (
    ("case_id", "VARCHAR NOT NULL"),
    ("activity", "VARCHAR NOT NULL"),
    ("event_ts_utc_ms", "BIGINT NOT NULL"),
    ("event_pos", "INTEGER NOT NULL"),
    ("lifecycle", "VARCHAR"),
    ("resource", "VARCHAR"),
)
"""The canonical `events` schema in order: the single source of truth for both
the created table and every readiness check over an existing database."""

_NULL_SENTINEL = "__PIW_NULL_8f5706c86f8d4db9__"
_ANALYSIS_VIEW_SQL = (
    "CREATE VIEW analysis_events AS\n"
    "SELECT * FROM events\n"
    f"WHERE lifecycle IS NULL OR UPPER(lifecycle) = '{COMPLETE_LIFECYCLE}'"
)


def _create_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("DROP VIEW IF EXISTS analysis_events")
    connection.execute("DROP TABLE IF EXISTS events")
    connection.execute("DROP TABLE IF EXISTS source_metadata")
    connection.execute(
        "CREATE TABLE events ("
        + ", ".join(f"{name} {sql_type}" for name, sql_type in EVENT_COLUMNS)
        + ")"
    )
    connection.execute(
        """
        CREATE TABLE source_metadata (
            doi VARCHAR,
            source_url VARCHAR,
            filename VARCHAR NOT NULL,
            sha256 VARCHAR NOT NULL,
            size_bytes BIGINT NOT NULL
        )
        """
    )


def _insert_metadata(
    connection: duckdb.DuckDBPyConnection, fingerprint: SourceFingerprint
) -> None:
    """Attribution metadata is optional; generic datasets carry only the fingerprint."""

    connection.execute(
        "INSERT INTO source_metadata VALUES (?, ?, ?, ?, ?)",
        [
            fingerprint.provenance.doi,
            fingerprint.provenance.source_url,
            fingerprint.filename,
            fingerprint.sha256,
            fingerprint.size_bytes,
        ],
    )


def install_events(
    connection: duckdb.DuckDBPyConnection,
    events: Iterable[Event],
    fingerprint: SourceFingerprint | None = None,
) -> int:
    """Replace the event table transactionally; duplicates are intentionally retained."""

    connection.execute("BEGIN TRANSACTION")
    inserted = 0
    try:
        _create_schema(connection)
        batch: list[tuple[Any, ...]] = []
        for event in events:
            batch.append(
                (
                    event.case_id,
                    event.activity,
                    event.event_ts_utc_ms,
                    event.event_pos,
                    event.lifecycle,
                    event.resource,
                )
            )
            if len(batch) == 10_000:
                connection.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)", batch)
                inserted += len(batch)
                batch.clear()
        if batch:
            connection.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)", batch)
            inserted += len(batch)
        if fingerprint:
            _insert_metadata(connection, fingerprint)
        connection.execute(_ANALYSIS_VIEW_SQL)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return inserted


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Canonical ingest facts for one registered dataset."""

    fingerprint: SourceFingerprint
    trace_count: int
    inserted: int


def _stage_events(
    descriptor: DatasetDescriptor, events: Iterable[Event]
) -> tuple[Path, int]:
    """Stage canonical events beside the dataset database, transactionally safe."""

    staging_dir = descriptor.database_path.parent
    staging_dir.mkdir(parents=True, exist_ok=True)
    staging_path: Path | None = None
    inserted = 0
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".tsv",
            prefix="piw-events-",
            dir=staging_dir,
            delete=False,
        ) as staging:
            staging_path = Path(staging.name)
            writer = csv.writer(
                staging,
                delimiter="\t",
                quotechar='"',
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
            )
            for event in events:
                optional_values = (event.lifecycle, event.resource)
                if _NULL_SENTINEL in optional_values:
                    raise ValueError("source value collides with the internal null sentinel")
                writer.writerow(
                    (
                        event.case_id,
                        event.activity,
                        event.event_ts_utc_ms,
                        event.event_pos,
                        event.lifecycle if event.lifecycle is not None else _NULL_SENTINEL,
                        event.resource if event.resource is not None else _NULL_SENTINEL,
                    )
                )
                inserted += 1
    except BaseException:
        # The handle must be closed before the staged file can be removed on
        # Windows, so the cleanup runs after the context manager exits.
        if staging_path is not None:
            staging_path.unlink(missing_ok=True)
        raise
    assert staging_path is not None
    return staging_path, inserted


def _copy_staged(
    connection: duckdb.DuckDBPyConnection,
    staging_path: Path,
    fingerprint: SourceFingerprint,
) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        _create_schema(connection)
        sql_path = staging_path.resolve().as_posix().replace("'", "''")
        connection.execute(
            f"""
            COPY events FROM '{sql_path}' (
                FORMAT CSV,
                HEADER false,
                DELIMITER '\t',
                NULLSTR '{_NULL_SENTINEL}',
                QUOTE '"',
                ESCAPE '"'
            )
            """
        )
        _insert_metadata(connection, fingerprint)
        connection.execute(_ANALYSIS_VIEW_SQL)
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def ingest_dataset(
    connection: duckdb.DuckDBPyConnection, descriptor: DatasetDescriptor
) -> IngestResult:
    """Ingest a registered dataset into the canonical schema.

    Every source format produces the same canonical events, schema, and
    `analysis_events` view. Changed source bytes fail closed instead of silently
    rebuilding a different dataset under the registered identity.
    """

    source_path = descriptor.source_path
    fingerprint = source_fingerprint(source_path, descriptor.provenance)
    if fingerprint.sha256 != descriptor.sha256 or fingerprint.size_bytes != descriptor.size_bytes:
        raise SourceContractError(
            f"{descriptor.log_id}: source bytes differ from the registered dataset "
            f"(registered sha256 {descriptor.sha256}, found {fingerprint.sha256}); "
            "re-register the dataset before rebuilding"
        )
    if descriptor.source_format == SOURCE_FORMAT_XES:
        trace_count = source_trace_count(source_path)
        events: Iterable[Event] = iter_xes_events(source_path)
    else:
        config = descriptor.csv_config()
        trace_count = csv_trace_count(source_path, config)
        events = iter_csv_events(source_path, config)
    staging_path, inserted = _stage_events(descriptor, events)
    try:
        _copy_staged(connection, staging_path, fingerprint)
    finally:
        staging_path.unlink(missing_ok=True)
    return IngestResult(fingerprint=fingerprint, trace_count=trace_count, inserted=inserted)


def raw_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    case_count, event_count, activity_count, start_ms, end_ms = connection.execute(
        """
        SELECT count(DISTINCT case_id), count(*), count(DISTINCT activity),
               min(event_ts_utc_ms), max(event_ts_utc_ms)
        FROM events
        """
    ).fetchone()
    lifecycle_counts = [
        {"lifecycle": lifecycle, "event_count": count}
        for lifecycle, count in connection.execute(
            """
            SELECT lifecycle, count(*)
            FROM events
            GROUP BY lifecycle
            ORDER BY lifecycle IS NOT NULL, lifecycle
            """
        ).fetchall()
    ]
    return {
        "case_count": case_count,
        "distinct_activity_count": activity_count,
        "event_count": event_count,
        "lifecycle_counts": lifecycle_counts,
        "time_range_utc_ms": {"end": end_ms, "start": start_ms},
    }


def complete_traces(connection: duckdb.DuckDBPyConnection) -> list[tuple[str, list[str]]]:
    rows = connection.execute(
        """
        SELECT case_id, activity
        FROM analysis_events
        ORDER BY case_id ASC, event_ts_utc_ms ASC, event_pos ASC
        """
    ).fetchall()
    traces: list[tuple[str, list[str]]] = []
    current_case: str | None = None
    activities: list[str] = []
    for case_id, activity in rows:
        if current_case is not None and case_id != current_case:
            traces.append((current_case, activities))
            activities = []
        current_case = case_id
        activities.append(activity)
    if current_case is not None:
        traces.append((current_case, activities))
    return traces


def case_trace(
    connection: duckdb.DuckDBPyConnection, case_id: str, perspective: str
) -> list[dict[str, Any]]:
    """Return one canonically ordered trace without changing established semantics."""

    if perspective not in {"complete", "raw"}:
        raise ValueError("perspective must be 'complete' or 'raw'")
    relation = "analysis_events" if perspective == "complete" else "events"
    rows = connection.execute(
        f"""
        SELECT activity, event_ts_utc_ms, event_pos, lifecycle, resource
        FROM {relation}
        WHERE case_id = ?
        ORDER BY event_ts_utc_ms ASC, event_pos ASC
        """,
        [case_id],
    ).fetchall()
    return [
        {
            "activity": activity,
            "event_pos": event_pos,
            "event_ts_utc_ms": event_ts_utc_ms,
            "lifecycle": lifecycle,
            "resource": resource,
        }
        for activity, event_ts_utc_ms, event_pos, lifecycle, resource in rows
    ]


def variant_id(sequence: list[str] | tuple[str, ...]) -> str:
    canonical = json.dumps(
        list(sequence), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def variant_summary(
    traces: list[tuple[str, list[str]]], case_count: int
) -> dict[str, Any]:
    counts: dict[tuple[str, ...], int] = defaultdict(int)
    for _, sequence in traces:
        counts[tuple(sequence)] += 1
    variants = [
        {
            "activity_sequence": list(sequence),
            "case_count": count,
            "case_share": round(count / case_count, 12) if case_count else 0,
            "variant_id": variant_id(sequence),
        }
        for sequence, count in counts.items()
    ]
    variants.sort(key=lambda item: (-item["case_count"], item["variant_id"]))
    return {"variant_count": len(variants), "variants": variants}


def transition_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = connection.execute(
        """
        WITH ordered AS (
            SELECT case_id, activity AS from_activity,
                   lead(activity) OVER (
                       PARTITION BY case_id ORDER BY event_ts_utc_ms, event_pos
                   ) AS to_activity
            FROM analysis_events
        )
        SELECT from_activity, to_activity, count(*) AS transition_count
        FROM ordered
        WHERE to_activity IS NOT NULL
        GROUP BY from_activity, to_activity
        ORDER BY transition_count DESC, from_activity ASC, to_activity ASC
        """
    ).fetchall()
    transitions = [
        {
            "from_activity": source,
            "to_activity": target,
            "transition_count": count,
        }
        for source, target, count in rows
    ]
    return {
        "distinct_transition_count": len(transitions),
        "transition_count": sum(item["transition_count"] for item in transitions),
        "transitions": transitions,
    }


def _discrete_summary(
    connection: duckdb.DuckDBPyConnection, relation_sql: str, column: str
) -> dict[str, Any]:
    count, minimum, p50, p90, maximum = connection.execute(
        f"""
        SELECT count(*), min({column}), quantile_disc({column}, 0.5),
               quantile_disc({column}, 0.9), max({column})
        FROM ({relation_sql}) values_to_summarize
        """
    ).fetchone()
    return {
        "count": count,
        "max_ms": maximum,
        "min_ms": minimum,
        "p50_ms": p50,
        "p90_ms": p90,
    }


def cycle_time_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    return _discrete_summary(
        connection,
        """
        SELECT case_id, max(event_ts_utc_ms) - min(event_ts_utc_ms) AS cycle_time_ms
        FROM events GROUP BY case_id
        """,
        "cycle_time_ms",
    )


def observed_gap_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    return _discrete_summary(
        connection,
        """
        WITH ordered AS (
            SELECT case_id, event_ts_utc_ms,
                   lag(event_ts_utc_ms) OVER (
                       PARTITION BY case_id ORDER BY event_ts_utc_ms, event_pos
                   ) AS prior_ts
            FROM analysis_events
        )
        SELECT event_ts_utc_ms - prior_ts AS observed_gap_ms
        FROM ordered WHERE prior_ts IS NOT NULL
        """,
        "observed_gap_ms",
    )


def rework_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = connection.execute(
        """
        WITH case_activity AS (
            SELECT case_id, activity, count(*) - 1 AS rework_event_count
            FROM analysis_events
            GROUP BY case_id, activity
            HAVING count(*) > 1
        )
        SELECT activity, sum(rework_event_count) AS rework_event_count,
               count(*) AS cases_with_rework
        FROM case_activity
        GROUP BY activity
        ORDER BY rework_event_count DESC, activity ASC
        """
    ).fetchall()
    by_activity = [
        {
            "activity": activity,
            "cases_with_rework": case_count,
            "rework_event_count": count,
        }
        for activity, count, case_count in rows
    ]
    cases_with_rework = connection.execute(
        """
        SELECT count(*) FROM (
            SELECT case_id FROM analysis_events
            GROUP BY case_id
            HAVING count(*) > count(DISTINCT activity)
        )
        """
    ).fetchone()[0]
    return {
        "activities": by_activity,
        "aggregate_rework_event_count": sum(
            item["rework_event_count"] for item in by_activity
        ),
        "cases_with_rework": cases_with_rework,
    }


def activity_summary(connection: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Summarize COMPLETE-perspective activity counts using the rework contract."""

    rows = connection.execute(
        """
        WITH case_activity AS (
            SELECT case_id, activity, count(*) AS event_count
            FROM analysis_events
            GROUP BY case_id, activity
        )
        SELECT activity,
               sum(event_count) AS event_count,
               count(*) AS case_count,
               sum(event_count - 1) AS rework_event_count
        FROM case_activity
        GROUP BY activity
        ORDER BY event_count DESC, activity ASC
        """
    ).fetchall()
    return [
        {
            "activity": activity,
            "case_count": case_count,
            "event_count": event_count,
            "rework_event_count": rework_event_count,
        }
        for activity, event_count, case_count, rework_event_count in rows
    ]


def sla_summary(
    connection: duckdb.DuckDBPyConnection, threshold_ms: int
) -> dict[str, Any]:
    if threshold_ms < 0:
        raise ValueError("SLA threshold must be non-negative")
    total, violations = connection.execute(
        """
        WITH cycle_times AS (
            SELECT case_id, max(event_ts_utc_ms) - min(event_ts_utc_ms) AS cycle_time_ms
            FROM events GROUP BY case_id
        )
        SELECT count(*), count(*) FILTER (WHERE cycle_time_ms > ?)
        FROM cycle_times
        """,
        [threshold_ms],
    ).fetchone()
    return {
        "case_count": total,
        "label": "configured test threshold; not a claimed BPIC12 business SLA",
        "strict_operator": ">",
        "threshold_ms": threshold_ms,
        "violation_case_count": violations,
        "violation_case_share": round(violations / total, 12) if total else 0,
    }


def case_analytics(
    connection: duckdb.DuckDBPyConnection, threshold_ms: int
) -> list[dict[str, Any]]:
    """Return deterministic per-case facts using the established metric contract."""

    if threshold_ms < 0:
        raise ValueError("SLA threshold must be non-negative")
    rows = connection.execute(
        """
        WITH cycle_times AS (
            SELECT case_id,
                   max(event_ts_utc_ms) - min(event_ts_utc_ms) AS cycle_time_ms
            FROM events
            GROUP BY case_id
        ),
        complete_counts AS (
            SELECT case_id, count(*) AS complete_event_count
            FROM analysis_events
            GROUP BY case_id
        ),
        case_activity AS (
            SELECT case_id, count(*) - 1 AS rework_event_count
            FROM analysis_events
            GROUP BY case_id, activity
        ),
        rework AS (
            SELECT case_id, sum(rework_event_count) AS rework_event_count
            FROM case_activity
            GROUP BY case_id
        )
        SELECT cycle_times.case_id,
               cycle_times.cycle_time_ms,
               coalesce(complete_counts.complete_event_count, 0),
               coalesce(rework.rework_event_count, 0) > 0 AS has_rework,
               coalesce(rework.rework_event_count, 0),
               cycle_times.cycle_time_ms > ? AS configured_sla_violation
        FROM cycle_times
        LEFT JOIN complete_counts USING (case_id)
        LEFT JOIN rework USING (case_id)
        ORDER BY cycle_times.case_id ASC
        """,
        [threshold_ms],
    ).fetchall()
    return [
        {
            "case_id": case_id,
            "cycle_time_ms": cycle_time_ms,
            "complete_event_count": complete_event_count,
            "has_rework": has_rework,
            "rework_event_count": rework_event_count,
            "configured_sla_violation": configured_sla_violation,
        }
        for (
            case_id,
            cycle_time_ms,
            complete_event_count,
            has_rework,
            rework_event_count,
            configured_sla_violation,
        ) in rows
    ]


@dataclass(frozen=True, slots=True)
class _CoreFacts:
    raw: dict[str, Any]
    analysis_event_count: int
    traces: list[tuple[str, list[str]]]
    variants: dict[str, Any]
    transitions: dict[str, Any]
    cycle_times: dict[str, Any]
    observed_gaps: dict[str, Any]
    rework: dict[str, Any]
    process_summary: dict[str, Any]
    sla: dict[str, Any] | None


def _measure(
    connection: duckdb.DuckDBPyConnection, sla_threshold_ms: int | None
) -> _CoreFacts:
    """Compute the established metric contract once, for every dataset."""

    raw = raw_summary(connection)
    analysis_event_count = connection.execute("SELECT count(*) FROM analysis_events").fetchone()[0]
    traces = complete_traces(connection)
    variants = variant_summary(traces, len(traces))
    transitions = transition_summary(connection)
    cycle_times = cycle_time_summary(connection)
    observed_gaps = observed_gap_summary(connection)
    rework = rework_summary(connection)
    sla = None if sla_threshold_ms is None else sla_summary(connection, sla_threshold_ms)
    process_summary = {
        "analysis_case_count": len(traces),
        "analysis_distinct_activity_count": connection.execute(
            "SELECT count(DISTINCT activity) FROM analysis_events"
        ).fetchone()[0],
        "raw_cases_without_analysis_events": raw["case_count"] - len(traces),
    }
    return _CoreFacts(
        analysis_event_count=analysis_event_count,
        cycle_times=cycle_times,
        observed_gaps=observed_gaps,
        process_summary=process_summary,
        raw=raw,
        rework=rework,
        sla=sla,
        traces=traces,
        transitions=transitions,
        variants=variants,
    )


def _structural_invariants(
    connection: duckdb.DuckDBPyConnection,
    trace_count: int,
    inserted_count: int,
    facts: _CoreFacts,
) -> list[tuple[str, bool, Any]]:
    """Generic invariants that hold for any dataset and any source format."""

    raw = facts.raw
    duplicate_positions = connection.execute(
        """
        SELECT count(*) FROM (
            SELECT case_id, event_pos FROM events
            GROUP BY case_id, event_pos HAVING count(*) > 1
        )
        """
    ).fetchone()[0]
    required_nulls = connection.execute(
        """
        SELECT count(*) FROM events
        WHERE case_id IS NULL OR case_id = '' OR activity IS NULL OR activity = ''
              OR event_ts_utc_ms IS NULL OR event_pos IS NULL
        """
    ).fetchone()[0]
    negative_gaps = connection.execute(
        """
        WITH ordered AS (
            SELECT case_id, event_ts_utc_ms,
                   lag(event_ts_utc_ms) OVER (
                       PARTITION BY case_id ORDER BY event_ts_utc_ms, event_pos
                   ) AS prior_ts
            FROM analysis_events
        )
        SELECT count(*) FROM ordered
        WHERE prior_ts IS NOT NULL AND event_ts_utc_ms - prior_ts < 0
        """
    ).fetchone()[0]
    analysis_case_count = connection.execute(
        "SELECT count(DISTINCT case_id) FROM analysis_events"
    ).fetchone()[0]
    traces = facts.traces
    variants = facts.variants
    transitions = facts.transitions
    expected_transition_count = sum(max(len(sequence) - 1, 0) for _, sequence in traces)
    expected_rework_count = connection.execute(
        """
        SELECT coalesce(sum(rework_event_count), 0) FROM (
            SELECT count(*) - 1 AS rework_event_count
            FROM analysis_events GROUP BY case_id, activity
        )
        """
    ).fetchone()[0]
    checks: list[tuple[str, bool, Any]] = [
        ("source_trace_count_matches_distinct_cases", trace_count == raw["case_count"], trace_count),
        ("all_parsed_events_inserted", inserted_count == raw["event_count"], inserted_count),
        ("required_fields_present", required_nulls == 0, required_nulls),
        ("case_event_positions_unambiguous", duplicate_positions == 0, duplicate_positions),
        ("analysis_is_raw_subset", facts.analysis_event_count <= raw["event_count"], facts.analysis_event_count),
        ("analysis_cases_match_trace_count", len(traces) == analysis_case_count, len(traces)),
        ("variant_case_counts_cover_analysis_cases", sum(v["case_count"] for v in variants["variants"]) == len(traces), sum(v["case_count"] for v in variants["variants"])),
        ("transition_count_matches_traces", transitions["transition_count"] == expected_transition_count, transitions["transition_count"]),
        ("cycle_distribution_covers_raw_cases", facts.cycle_times["count"] == raw["case_count"], facts.cycle_times["count"]),
        ("gap_distribution_covers_transitions", facts.observed_gaps["count"] == transitions["transition_count"], facts.observed_gaps["count"]),
        ("rework_aggregate_matches_case_activity_counts", facts.rework["aggregate_rework_event_count"] == expected_rework_count, facts.rework["aggregate_rework_event_count"]),
    ]
    if facts.sla is not None:
        checks.append(
            ("sla_scenario_covers_raw_cases", facts.sla["case_count"] == raw["case_count"], facts.sla["case_count"])
        )
    checks.append(("observed_gaps_non_negative", negative_gaps == 0, negative_gaps))
    return checks


def _validation(checks: list[tuple[str, bool, Any]]) -> dict[str, Any]:
    invariants = [
        {"actual": actual, "name": name, "passed": passed}
        for name, passed, actual in checks
    ]
    return {
        "invariants": invariants,
        "status": "PASS" if all(item["passed"] for item in invariants) else "HOLD",
    }


def _metric_payload(facts: _CoreFacts) -> dict[str, Any]:
    payload = {
        "analysis_event_count": facts.analysis_event_count,
        "cycle_time_summary": facts.cycle_times,
        "metric_definition_version": METRIC_DEFINITION_VERSION,
        "observed_gap_summary": facts.observed_gaps,
        "process_summary": facts.process_summary,
        "raw_summary": facts.raw,
        "rework_summary": facts.rework,
        "transition_summary": facts.transitions,
        "variant_summary": facts.variants,
    }
    if facts.sla is not None:
        payload["sla_scenario"] = facts.sla
    return payload


def _run_pipeline(
    descriptor: DatasetDescriptor, sla_threshold_ms: int | None
) -> tuple[IngestResult, _CoreFacts, list[tuple[str, bool, Any]]]:
    database_path = descriptor.database_path
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        ingest = ingest_dataset(connection, descriptor)
        facts = _measure(connection, sla_threshold_ms)
        checks = _structural_invariants(
            connection, ingest.trace_count, ingest.inserted, facts
        )
    finally:
        connection.close()
    return ingest, facts, checks


def _write_report(report_path: Path, report: dict[str, Any]) -> None:
    canonical = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(canonical, encoding="utf-8", newline="\n")


def build_report(
    source_path: Path,
    database_path: Path,
    report_path: Path,
    sla_threshold_ms: int,
) -> dict[str, Any]:
    """Canonical BPIC12 Slice 0 workflow; its report stays byte-stable."""

    report_path = Path(report_path)
    descriptor = BPIC2012.descriptor(Path(source_path).resolve(), Path(database_path))
    ingest, facts, checks = _run_pipeline(descriptor, sla_threshold_ms)
    report = _metric_payload(facts) | {
        "report_schema_version": REPORT_SCHEMA_VERSION,
        "source_fingerprint": ingest.fingerprint.as_dict(),
        "validation": _validation(
            checks + BPIC2012.expectations(facts.raw, ingest.fingerprint)
        ),
    }
    _write_report(report_path, report)
    return report


def build_dataset(
    descriptor: DatasetDescriptor,
    *,
    report_path: Path | None = None,
    sla_threshold_ms: int | None = None,
) -> dict[str, Any]:
    """Ingest a registered dataset and return its deterministic Core facts.

    No configured SLA threshold is applied unless the caller supplies one, and
    the report carries only generic structural invariants.
    """

    ingest, facts, checks = _run_pipeline(descriptor, sla_threshold_ms)
    report = _metric_payload(facts) | {
        "dataset": {
            "dataset_id": descriptor.dataset_id,
            "display_name": descriptor.display_name,
            "log_id": descriptor.log_id,
            "source_format": descriptor.source_format,
        },
        "report_schema_version": DATASET_REPORT_SCHEMA_VERSION,
        "source_fingerprint": ingest.fingerprint.as_dict(),
        "validation": _validation(checks),
    }
    if report_path is not None:
        _write_report(Path(report_path), report)
    return report


def staging_database_path(database_path: Path) -> Path:
    """Deterministic local staging path for a build that must validate first."""

    path = Path(database_path)
    return path.with_name(f"{path.name}.building")


def discard_staged_database(staging_path: Path) -> None:
    """Remove an unvalidated staging build and any write-ahead sidecar."""

    staging = Path(staging_path)
    for candidate in (staging, staging.with_name(f"{staging.name}.wal")):
        candidate.unlink(missing_ok=True)


def promote_staged_database(staging_path: Path, database_path: Path) -> None:
    """Atomically replace a dataset database with a validated staging build.

    The staging build is written and validated beside the final database, so
    the replacement is a same-filesystem rename: a caller that never reaches
    promotion cannot damage an existing good database. The replaced database's
    write-ahead sidecar is removed here and only here, because it belongs to
    the file being replaced.
    """

    staging = Path(staging_path)
    target = Path(database_path)
    if not staging.is_file():
        raise FileNotFoundError(staging)
    if staging.with_name(f"{staging.name}.wal").exists():
        raise RuntimeError(
            f"{staging.name} was not closed cleanly; refusing to promote an unmerged build"
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.with_name(f"{target.name}.wal").unlink(missing_ok=True)
    staging.replace(target)
