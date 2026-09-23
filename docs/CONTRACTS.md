# Public contract reference

This document names the current public contracts and their observable boundaries. The implementation and tests are authoritative for field-level details.

## `slice0-metrics-v1`

The deterministic Core owns canonical ordering, lifecycle-aware analysis perspective, case/variant/activity/direct-follow counts, durations, rework, and configured-threshold results.

- Source events and duplicates are preserved.
- COMPLETE is an analysis perspective, not deletion from evidence.
- Observed event gap is an adjacent-analysis-event timestamp delta, not true queue/waiting time.
- A threshold is included only when the caller configures an analytical scenario; it is not a claimed business SLA.

## `dataset-import-v1`

Supported source formats: CSV, XES, XES.GZ.

CSV configuration requires mappings for `case_id`, `activity`, and `timestamp`. `resource` and `lifecycle` are nullable explicit mappings. Timestamp format and `assume_timezone` are explicit interpretation inputs. A timezone such as UTC is a deterministic normalization convention unless the source documentation separately establishes real-world timezone semantics.

Import stages are preview, configuration, deterministic build/validation, then ready registration. Malformed input, changed source bytes, invalid mapping, invalid timestamps, or inconsistent registry/database state fails closed.

## `slice-c-http-v2`

Localhost HTTP exposes:

- dataset listing and readiness
- source preview and validated build
- dataset-scoped summary
- variants
- transitions
- activities
- case trace

Every analytical response wraps the same Core facts and provenance. Unknown or unavailable datasets return stable errors rather than fallback data.

## `slice-d-tool-v2`

Exactly five read-only tools are advertised:

1. `describe_log`
2. `list_variants`
3. `list_transitions`
4. `list_activities`
5. `get_case_trace`

Each accepts a `log_id` and resolves through the shared `DatasetResolver` / `CoreReadSurface`. Direct Agent and stdio MCP are two clients of this contract. MCP does not define a second schema or metric path.

Unsupported tools or parameters fail closed. Agent answers must be grounded in returned facts; the model is not allowed to calculate authoritative metrics.

## `slice3-powerbi-export-v1`

The historical analytics export produces deterministic BPIC12 CSV inputs for Power Query and thin DAX measures. Generated CSVs and PBIX files are local-only. This contract is not a generic Power BI import/export promise for registered datasets.

## Distribution contract

The public repository includes project-authored code, public-safe tests/docs, aggregate evidence, and product screenshots. It excludes raw datasets, generated DuckDB databases, generated analytics CSVs, PBIX, local models, private participant evidence, local paths, process IDs, secrets, and canonical/internal recovery artifacts.
