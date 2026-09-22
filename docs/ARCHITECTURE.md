# Architecture

TraceVerity keeps one deterministic source of process truth and treats every
other surface as a consumer.

```text
BPI Challenge 2012 XES
        |
        v
Python / DuckDB deterministic Core
  - canonical event model
  - slice0-metrics-v1
  - source fingerprint + 17 invariants
        |
        +--> five typed read-only tools (slice1-tool-v1)
        |       +--> bounded local Agent
        |       +--> stdio MCP transport --> same tools
        |
        +--> localhost FastAPI --> React workbench
        |
        +--> deterministic CSV export
                --> Power Query / thin DAX display layer
                --> local Power BI Desktop report
```

## Central trust rule

**AI never defines or calculates authoritative process facts.** Metric semantics
live in the Python/DuckDB Core. The Agent can call only five typed read-only
tools and accepted answers must point back to exact returned facts. A request
that cannot be answered from the tool surface must end as `UNAVAILABLE`.

The Web workbench formats returned values but does not reimplement process
metrics. Power BI loads deterministic exports and uses a thin display layer.
The MCP server is transport only: it exposes the same five tools and does not
create a second truth system.

## Determinism and fail-closed behavior

- The BPIC12 source archive is pinned by SHA-256 and expected source metadata.
- The Core validates 17 invariants. A mismatch produces `HOLD` instead of a
  warning-only result.
- Canonical process order is deterministic and duplicates are preserved.
- `query_id` and `fact_id` values are hashes of normalized contract content,
  not timestamps or random IDs.
- Invalid tool names, bounds, enums, missing cases, and unsupported parameters
  fail closed with structured errors.

## Consumer boundaries

| Surface | Responsibility | Does not do |
|---|---|---|
| Core | ingest, ordering, metrics, validation, DuckDB persistence | delegate process truth to AI |
| Agent | choose bounded tools and return grounded facts | SQL, writes, filesystem/network tools, independent metric calculation |
| MCP | stdio transport for the same Core tools | introduce new metrics or semantics |
| Web | local API + analyst workbench | recalculate authoritative process facts |
| Power BI | consume deterministic exports for analysis/display | reconstruct lifecycle/order/metric logic |

See [CONTRACTS.md](CONTRACTS.md) for the exact event, metric, tool, HTTP,
analytics, and MCP contracts.
