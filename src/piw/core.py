from __future__ import annotations

import hashlib
import json
import csv
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import duckdb

from .xes import Event, iter_xes_events, source_trace_count

METRIC_DEFINITION_VERSION = "slice0-metrics-v1"
REPORT_SCHEMA_VERSION = "slice0-report-v1"
BPIC12_DOI = "10.4121/uuid:3926db30-f712-4394-aebc-75976070e91f"
BPIC12_SOURCE_URL = "https://ndownloader.figshare.com/files/24027287"
BPIC12_DATASET_URL = "https://data.4tu.nl/articles/dataset/BPI_Challenge_2012/12689204"
BPIC12_EXPECTED_CASES = 13_087
BPIC12_EXPECTED_EVENTS = 262_200
BPIC12_EXPECTED_SHA256 = "5cd9cc16b9bcb20bd4aae45666a5d87479ddbf47e6371618b6ad217174cecdf3"
BPIC12_EXPECTED_SIZE_BYTES = 3_342_406
_NULL_SENTINEL = "__PIW_NULL_8f5706c86f8d4db9__"


def source_fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return {
        "dataset_url": BPIC12_DATASET_URL,
        "doi": BPIC12_DOI,
        "filename": path.name,
        "sha256": digest.hexdigest(),
        "size_bytes": path.stat().st_size,
        "source_url": BPIC12_SOURCE_URL,
    }


def _create_schema(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("DROP VIEW IF EXISTS analysis_events")
    connection.execute("DROP TABLE IF EXISTS events")
    connection.execute("DROP TABLE IF EXISTS source_metadata")
    connection.execute(
        """
        CREATE TABLE events (
            case_id VARCHAR NOT NULL,
            activity VARCHAR NOT NULL,
            event_ts_utc_ms BIGINT NOT NULL,
            event_pos INTEGER NOT NULL,
            lifecycle VARCHAR,
            resource VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE source_metadata (
            doi VARCHAR NOT NULL,
            source_url VARCHAR NOT NULL,
            filename VARCHAR NOT NULL,
            sha256 VARCHAR NOT NULL,
            size_bytes BIGINT NOT NULL
        )
        """
    )


def install_events(
    connection: duckdb.DuckDBPyConnection,
    events: Iterable[Event],
    fingerprint: dict[str, Any] | None = None,
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
            connection.execute(
                "INSERT INTO source_metadata VALUES (?, ?, ?, ?, ?)",
                [
                    fingerprint["doi"],
                    fingerprint["source_url"],
                    fingerprint["filename"],
                    fingerprint["sha256"],
                    fingerprint["size_bytes"],
                ],
            )
        connection.execute(
            """
            CREATE VIEW analysis_events AS
            SELECT *
            FROM events
            WHERE lifecycle IS NULL OR UPPER(lifecycle) = 'COMPLETE'
            """
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return inserted


def ingest_xes(
    connection: duckdb.DuckDBPyConnection, source_path: Path
) -> tuple[dict[str, Any], int, int]:
    fingerprint = source_fingerprint(source_path)
    trace_count = source_trace_count(source_path)
    staging_path: Path | None = None
    inserted = 0
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".tsv",
            prefix="piw-events-",
            dir=source_path.parent.parent / "processed",
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
            for event in iter_xes_events(source_path):
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
            connection.execute(
                "INSERT INTO source_metadata VALUES (?, ?, ?, ?, ?)",
                [
                    fingerprint["doi"],
                    fingerprint["source_url"],
                    fingerprint["filename"],
                    fingerprint["sha256"],
                    fingerprint["size_bytes"],
                ],
            )
            connection.execute(
                """
                CREATE VIEW analysis_events AS
                SELECT * FROM events
                WHERE lifecycle IS NULL OR UPPER(lifecycle) = 'COMPLETE'
                """
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    finally:
        if staging_path is not None:
            staging_path.unlink(missing_ok=True)
    return fingerprint, trace_count, inserted


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


def _validations(
    connection: duckdb.DuckDBPyConnection,
    trace_count: int,
    inserted_count: int,
    raw: dict[str, Any],
    analysis_count: int,
    traces: list[tuple[str, list[str]]],
    variants: dict[str, Any],
    transitions: dict[str, Any],
    cycle_times: dict[str, Any],
    observed_gaps: dict[str, Any],
    rework: dict[str, Any],
    sla: dict[str, Any],
    fingerprint: dict[str, Any],
) -> dict[str, Any]:
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
    expected_transition_count = sum(max(len(sequence) - 1, 0) for _, sequence in traces)
    expected_rework_count = connection.execute(
        """
        SELECT coalesce(sum(rework_event_count), 0) FROM (
            SELECT count(*) - 1 AS rework_event_count
            FROM analysis_events GROUP BY case_id, activity
        )
        """
    ).fetchone()[0]
    checks = [
        ("source_trace_count_matches_distinct_cases", trace_count == raw["case_count"], trace_count),
        ("all_parsed_events_inserted", inserted_count == raw["event_count"], inserted_count),
        ("required_fields_present", required_nulls == 0, required_nulls),
        ("case_event_positions_unambiguous", duplicate_positions == 0, duplicate_positions),
        ("analysis_is_raw_subset", analysis_count <= raw["event_count"], analysis_count),
        ("analysis_cases_match_trace_count", len(traces) == analysis_case_count, len(traces)),
        ("variant_case_counts_cover_analysis_cases", sum(v["case_count"] for v in variants["variants"]) == len(traces), sum(v["case_count"] for v in variants["variants"])),
        ("transition_count_matches_traces", transitions["transition_count"] == expected_transition_count, transitions["transition_count"]),
        ("cycle_distribution_covers_raw_cases", cycle_times["count"] == raw["case_count"], cycle_times["count"]),
        ("gap_distribution_covers_transitions", observed_gaps["count"] == transitions["transition_count"], observed_gaps["count"]),
        ("rework_aggregate_matches_case_activity_counts", rework["aggregate_rework_event_count"] == expected_rework_count, rework["aggregate_rework_event_count"]),
        ("sla_scenario_covers_raw_cases", sla["case_count"] == raw["case_count"], sla["case_count"]),
        ("observed_gaps_non_negative", negative_gaps == 0, negative_gaps),
        ("expected_bpic2012_sha256", fingerprint["sha256"] == BPIC12_EXPECTED_SHA256, fingerprint["sha256"]),
        ("expected_bpic2012_file_size", fingerprint["size_bytes"] == BPIC12_EXPECTED_SIZE_BYTES, fingerprint["size_bytes"]),
        ("expected_bpic2012_case_count", raw["case_count"] == BPIC12_EXPECTED_CASES, raw["case_count"]),
        ("expected_bpic2012_event_count", raw["event_count"] == BPIC12_EXPECTED_EVENTS, raw["event_count"]),
    ]
    results = [
        {"actual": actual, "name": name, "passed": passed}
        for name, passed, actual in checks
    ]
    return {
        "invariants": results,
        "status": "PASS" if all(item["passed"] for item in results) else "HOLD",
    }


def build_report(
    source_path: Path,
    database_path: Path,
    report_path: Path,
    sla_threshold_ms: int,
) -> dict[str, Any]:
    source_path = source_path.resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        fingerprint, trace_count, inserted_count = ingest_xes(connection, source_path)
        raw = raw_summary(connection)
        analysis_count = connection.execute("SELECT count(*) FROM analysis_events").fetchone()[0]
        traces = complete_traces(connection)
        variants = variant_summary(traces, len(traces))
        transitions = transition_summary(connection)
        cycle_times = cycle_time_summary(connection)
        observed_gaps = observed_gap_summary(connection)
        rework = rework_summary(connection)
        sla = sla_summary(connection, sla_threshold_ms)
        process_summary = {
            "analysis_case_count": len(traces),
            "analysis_distinct_activity_count": connection.execute(
                "SELECT count(DISTINCT activity) FROM analysis_events"
            ).fetchone()[0],
            "raw_cases_without_analysis_events": raw["case_count"] - len(traces),
        }
        report = {
            "analysis_event_count": analysis_count,
            "cycle_time_summary": cycle_times,
            "metric_definition_version": METRIC_DEFINITION_VERSION,
            "observed_gap_summary": observed_gaps,
            "process_summary": process_summary,
            "raw_summary": raw,
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "rework_summary": rework,
            "sla_scenario": sla,
            "source_fingerprint": fingerprint,
            "transition_summary": transitions,
            "validation": _validations(
                connection,
                trace_count,
                inserted_count,
                raw,
                analysis_count,
                traces,
                variants,
                transitions,
                cycle_times,
                observed_gaps,
                rework,
                sla,
                fingerprint,
            ),
            "variant_summary": variants,
        }
    finally:
        connection.close()
    canonical = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    report_path.write_text(canonical, encoding="utf-8", newline="\n")
    return report
