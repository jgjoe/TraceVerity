# Architecture

## Trust rule

The deterministic Python/DuckDB Core is the only component allowed to define authoritative process metrics. Web, Agent, MCP, and the historical Power BI export consume Core facts; they do not independently calculate process truth.

## Current local data path

```text
local CSV / XES / XES.GZ
        |
        v
import preview -> explicit mapping/interpretation -> validation
        |
        v
DatasetRegistry -> DatasetResolver -> CoreReadSurface
        |                         |
        |                         v
        |               Python / DuckDB Core
        |                         |
        +--> localhost HTTP ------+--> React workbench
        +--> Direct Agent --------+--> five read-only tools
        +--> stdio MCP -----------+
        \--> BPIC12 export ----------> historical Power BI proof
```

`DatasetRegistry` stores local descriptors for imported sources. `DatasetResolver` resolves the built-in BPIC12 baseline and registered datasets into one readiness vocabulary. `CoreReadSurface` performs all supported reads and supplies the same facts to HTTP, Direct Agent, and MCP.

## Import and readiness

Contract `dataset-import-v1` supports CSV, XES, and XES.GZ.

- Source preview records filename, format, size, and SHA-256.
- CSV requires explicit case ID, activity, and timestamp mappings; resource and lifecycle are optional explicit mappings.
- Timestamp format and timezone interpretation are explicit inputs.
- Validation occurs before a dataset becomes ready.
- Unknown, unbuilt, corrupt, or inconsistent dataset state fails closed.
- Source bytes, descriptors, and generated DuckDB files stay in ignored local storage.

## Core contract

Contract `slice0-metrics-v1` owns event ordering, lifecycle perspective, variants, direct-follow transitions, cycle time, observed event gaps, rework, and configured-threshold semantics. Duplicate source events are preserved. Observed event gap is not true queue/waiting time. Configured thresholds are analytical scenarios, not actual business SLAs.

## HTTP and Web

Contract `slice-c-http-v2` adds dataset listing, source preview/build, and dataset-scoped read routes. The React workbench has a Dataset Workspace for import and switching. It renders returned facts and does not reimplement metrics.

## Agent and MCP

Contract `slice-d-tool-v2` advertises exactly five read-only tools: `describe_log`, `list_variants`, `list_transitions`, `list_activities`, and `get_case_trace`. Direct Agent and stdio MCP share schemas, resolution, and Core reads. SQL, shell, filesystem, network, and write capabilities are not part of this tool surface.

## Power BI boundary

Contract `slice3-powerbi-export-v1` is the historical BPIC12 analytics export. Power Query and thin DAX measures consume deterministic exported facts. Power BI is not generalized to arbitrary registered datasets.

## Distribution boundary

Public distribution excludes raw datasets, generated DuckDB databases, generated analytics CSVs, PBIX, local model files, private usability evidence, and machine-specific state. The application is local-first and makes no cloud/SaaS, multi-user/auth, or enterprise-scale claim.
