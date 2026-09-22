# Deterministic Contracts

## Canonical event schema

| Field | Type | Rule |
|---|---|---|
| `case_id` | string | required, non-empty |
| `activity` | string | required, non-empty |
| `event_ts_utc_ms` | integer | required UTC epoch milliseconds |
| `event_pos` | integer | required, zero-based original position within the source trace |
| `lifecycle` | nullable string | source value preserved |
| `resource` | nullable string | source value preserved |

Offset-aware timestamps are normalized to UTC before duration calculations. Missing/invalid required values fail the transaction. Source events, including duplicates, are preserved. No event is silently dropped or deduplicated. Canonical within-case order is `event_ts_utc_ms ASC, event_pos ASC`.

The default control-flow perspective contains events whose lifecycle is missing or whose lifecycle uppercases to `COMPLETE`. It is used for traces, direct-follow transitions, variants, rework, and observed gap/wait. Raw events remain the basis for source counts and case cycle time. Slice 0 does not reconstruct lifecycle queue waiting.

## Metric definitions (`slice0-metrics-v1`)

- **Raw summary:** distinct case count, raw event count, distinct activity count, lifecycle counts, and raw UTC time range.
- **Canonical process trace:** ordered COMPLETE-perspective activity sequence for each case.
- **Direct-follow transition:** each consecutive pair in a canonical trace; aggregate count ranks by count descending, then `from_activity` and `to_activity` ascending.
- **Variant:** exact canonical trace sequence. `variant_id` is SHA-256 of UTF-8 JSON produced with no insignificant whitespace and literal Unicode (`["A","B"]` form). Aggregate count ranks by case count descending, then `variant_id` ascending. Case share is rounded to 12 decimal places.
- **Case cycle time:** maximum raw timestamp minus minimum raw timestamp for a case, in integer milliseconds.
- **Observed gap/wait:** timestamp delta between consecutive COMPLETE-perspective events, in integer milliseconds. This is not true queue waiting time.
- **Rework:** for each case/activity, `max(completion_count - 1, 0)`. Aggregate rework event count is the sum; a case has rework if any case/activity value is positive.
- **Configured SLA violation:** `cycle_time_ms > threshold_ms`. Equality is not a violation. A supplied threshold is a test/configuration scenario, never an assertion about BPIC12's real business SLA.
- **Distributions:** p50 and p90 use DuckDB `quantile_disc`; no interpolated floating percentile is used.

## Slice 1 Agent read-only tool contract (`slice1-tool-v1`)

The single local Agent receives only:

- `describe_log(log_id, sla_threshold_ms?)`
- `list_variants(log_id, order_by, limit)`
- `list_transitions(log_id, from_activity?, to_activity?, order_by, limit)`
- `list_activities(log_id, order_by, limit)`
- `get_case_trace(log_id, case_id, perspective)`

List limits are 1–100. Supported ordering values are explicit enums. Case traces accept only `complete` and `raw` perspectives. Unknown tools, unknown logs, missing cases, unsupported parameters, invalid enum values, and out-of-bound limits fail closed with structured errors.

There is no arbitrary SQL, DuckDB connection, shell, filesystem, network, Python execution, or write/update/delete tool. `llama-server` is contacted only through its localhost chat endpoint and is started without built-in agent, filesystem, or MCP tools.

Each successful response exposes `schema_version`, `metric_definition_version`, `log_fingerprint`, normalized `parameters`, `query_id`, `result`, and deterministic facts. Canonical JSON is UTF-8, literal Unicode, sorted by object key, and contains no insignificant whitespace. `query_id` is SHA-256 over the tool name, normalized parameters, tool/metric versions, and source fingerprint. Each `fact_id` is SHA-256 over its query ID, fact name, and exact value. IDs never contain timestamps, randomness, Python `hash()`, UUIDs, paths, or other machine state.

The bounded Agent requests one allowed tool at a time for at most six steps. A final `ANSWERED` response contains exact returned fact objects and matching `supporting_fact_ids`; the runtime rejects ungrounded or transformed values. A request outside the tool surface must end as `UNAVAILABLE` when no deterministic fact can answer it.

## Slice 2 local HTTP contract (`slice2-http-v1`)

The localhost-only FastAPI application exposes:

- `GET /api/health`
- `GET /api/logs/bpic2012/summary?sla_threshold_ms=`
- `GET /api/logs/bpic2012/variants?order_by=&limit=`
- `GET /api/logs/bpic2012/transitions?from_activity=&to_activity=&order_by=&limit=`
- `GET /api/logs/bpic2012/activities?order_by=&limit=`
- `GET /api/logs/bpic2012/cases/{case_id}?perspective=`

Product defaults are `case_count_desc`/10 variants, `transition_count_desc`/10 transitions, `rework_event_count_desc`/10 activities, and `complete` case-trace perspective. Bounds, enums, filters, and facts delegate to `CoreToolSurface`; its success envelope and provenance remain intact. The summary adds `observed_gap_p50_ms` in a separate `supplemental` envelope because that already-defined Core metric is not part of `slice1-tool-v1`. The supplement is calculated by the existing deterministic Core over a read-only connection and carries deterministic query/fact IDs plus the source tool query ID.

Structured tool errors remain structured over HTTP: invalid parameters map to 400, missing cases to 404, and unavailable/corrupt local data to 503. Responses contain no timestamp or random identifier. The frontend may format values for display but does not calculate process metrics.

## Slice 3 analytics export contract (`slice3-powerbi-export-v1`)

`piw-analytics-export` opens the canonical BPIC12 DuckDB database read-only and writes `summary.csv`, `variants.csv`, `transitions.csv`, `activities.csv`, and `cases.csv`. Files use UTF-8, LF newlines, fixed column order, deterministic row order, and locale-independent serialization. `analytics/powerbi/export-manifest.json` records the source fingerprint, metric version, fixed configured SLA threshold, expected canonical facts, row counts, and SHA-256 for every CSV without timestamps or paths.

Variant sequences are compact literal-Unicode JSON text. Variant and transition orders reuse the established Core orders. Activities rank by rework event count descending and activity ascending. Cases rank by case ID ascending and expose Core-derived raw cycle time, COMPLETE event count, rework count/flag, and the configured strict SLA flag. The threshold is fixed at 604,800,000 ms as a test scenario, not a claimed business SLA.

Power Query is restricted to loading these files, promoting headers, and assigning types through one configurable local `DataRoot`. DAX is a thin display layer over exported facts. Neither layer may reconstruct lifecycle filtering, event order, variants, direct-follow transitions, cycle time, observed gap, rework, or SLA semantics.

## Slice 5 MCP integration contract

Slice 5 uses the official MCP Python SDK over stdio only. It preserves the direct Agent path and adds this route:

```
Qwen Agent -> MCP client -> stdio MCP server -> same CoreToolSurface
```

The MCP server exposes exactly the existing five `slice1-tool-v1` read-only tools: `describe_log`, `list_variants`, `list_transitions`, `list_activities`, and `get_case_trace`. Their schemas, validation, provenance envelopes, `query_id`/`fact_id` behavior, metric definitions, and fail-closed semantics remain unchanged. The MCP layer may neither calculate new process facts nor introduce or reinterpret metric semantics; it is a transport/integration proof, not a second truth system.

The MCP route reuses the exact ten cases in `evaluation/slice1-cases.json` and passes all ten with zero accepted tool-call mismatches, grounding violations, and forbidden actions. The preserved direct Agent route is re-run and must also pass 10/10 with the same zero-violation expectations. Existing contracts may not be weakened to obtain a pass, and Core, Web, and Power BI behavior and evidence may not regress semantically or operationally. Historical Slice 0-4 evidence is immutable for this proof.

## Zero-additional-cost contract

No new paid API, hosted model, paid SaaS, cloud database/server, paid dataset, paid hosting, or paid license is allowed. Existing local hardware, OSS Python/JavaScript packages (including the official MCP Python SDK), DuckDB, local llama.cpp, the zero-cost official `Qwen/Qwen3-8B-GGUF` model, and local Power BI Desktop use are allowed. Power BI Service/Fabric publishing is excluded. Any future billing, subscription, credit-card, or external-account mutation requires explicit user approval.
