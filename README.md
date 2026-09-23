# TraceVerity — Process Intelligence Workbench

TraceVerity is a local-first Process Intelligence product. In the browser, a user can import a local CSV, XES, or XES.GZ event log, map and validate it, then immediately analyze it in the same deterministic Python/DuckDB-backed workbench proven across two real business processes.

**AI never defines authoritative process metrics.** The browser, Direct Agent, and stdio MCP resolve datasets through the same Core-backed read path. The historical Power BI proof consumes a deterministic BPIC12 export rather than independently defining process metrics.

**Stack / value:** Python · DuckDB · FastAPI · React · Playwright · MCP — one deterministic process-metric core shared by browser and agent surfaces, with reproducible checks against two public event logs.

![Browser import controls open above the same deterministic workbench analyzing the imported Italian Help Desk dataset](docs/images/traceverity-overview.png)

## One process-truth boundary

Event-log results drift when each UI, BI layer, or model calculates them independently. TraceVerity keeps ordering, lifecycle perspective, variants, direct-follow counts, durations, rework, and configured-threshold semantics in one versioned Core. Invalid sources and inconsistent local state fail closed.

```text
local CSV / XES / XES.GZ
        |
        v
explicit preview, mapping, timestamp interpretation, validation
        |
        v
DatasetRegistry -> DatasetResolver -> CoreReadSurface
        |                         deterministic Python / DuckDB Core
        +--> localhost FastAPI --> React workbench
        +--> 5 read-only tools --> Direct Agent
        |                       \-> stdio MCP
        \--> BPIC12 export ------> historical Power BI proof
```

Current contracts:

- metrics: `slice0-metrics-v1`
- generic import: `dataset-import-v1`
- HTTP: `slice-c-http-v2`
- tool schema: `slice-d-tool-v2`
- analytics export: `slice3-powerbi-export-v1`

## Browser onboarding

The Dataset Workspace supports local `.csv`, `.xes`, and `.xes.gz` files. A user previews source identity, maps CSV fields explicitly, states timestamp format/timezone interpretation, validates the source, and only then registers a ready dataset. The same workbench then immediately analyzes the selected dataset without changing Core semantics.

![The real Help Desk CSV imported and selected with 4,580 cases and no configured SLA card](docs/images/traceverity-helpdesk-onboarding.png)

For CSV, case ID, activity, and timestamp mappings are required. Resource and lifecycle are explicit optional mappings. A supplied timezone is a deterministic normalization convention, not automatically a claim about the source's real-world timezone.

## Two real-world dataset proof

The same unchanged metric engine was independently re-derived against two real event logs.

| Verified aggregate | BPI Challenge 2012 | Italian Help Desk |
|---|---:|---:|
| Cases | 13,087 | 4,580 |
| Raw events | 262,200 | 21,348 |
| Analysis events | 164,506 | 21,348 |
| Activities | 24 | 14 |
| Variants | 4,336 | 226 |
| Direct-follow occurrences | 151,419 | 16,768 |
| Cases with rework | 7,019 | 1,240 |
| Aggregate rework events | 57,556 | 1,905 |

Neither raw dataset is bundled. Source identity, fingerprints, attribution, and aggregate facts are in [`evidence/public-verification-summary.json`](evidence/public-verification-summary.json).

![Current aggregate variants, transitions, and activities](docs/images/traceverity-process-patterns.png)

## Shared Web, Agent, and MCP facts

The public tool surface has exactly five read-only operations:

- `describe_log`
- `list_variants`
- `list_transitions`
- `list_activities`
- `get_case_trace`

Direct Agent and stdio MCP use the same schemas, `DatasetResolver`, and `CoreReadSurface`; MCP is a transport, not a second metric engine. Agent answers are accepted only when values trace to returned facts. Unsupported requests remain unavailable rather than being estimated.

## Verification

Independent end-to-end verification recorded:

| Gate | Result |
|---|---:|
| Python regression | 190 / 190 PASS |
| Web production build | PASS |
| Playwright | 2 / 2 PASS |
| Direct Agent | 12 / 12 PASS |
| MCP Agent | 12 / 12 PASS |
| Direct/MCP comparisons | 22 / 22, mismatch 0 |
| Grounding / forbidden-action violations | 0 / 0 |
| Machine-path scan | 179 responses, 0 leaks |

The public tree passes **168 / 168 Python tests**. The larger independent run also covered local evidence integrations and model-backed Agent/MCP re-derivation that are intentionally absent from the public package. The aggregate public record is in [`evidence/public-verification-summary.json`](evidence/public-verification-summary.json); details are in [`docs/REPRODUCTION.md`](docs/REPRODUCTION.md).

## Local reproduction

### Data prerequisites

- Raw datasets are **not bundled**. Obtain BPI Challenge 2012 and/or the Italian Help Desk log from their original public providers and keep them under the ignored local `data/` tree.
- Exact expected filenames, fingerprints, attribution, and the Help Desk CSV mapping are documented in [`docs/REPRODUCTION.md`](docs/REPRODUCTION.md).
- Model-backed Agent/MCP evaluation is optional and additionally requires a local `llama.cpp` runtime and model file.

```powershell
uv sync --extra test
uv run piw-slice0
uv run pytest -q

Set-Location web
npm ci
npm run build
npm run test:e2e
```

Full BPIC12 baseline, generic browser onboarding, optional real Help Desk reproduction, Agent/MCP checks, and the historical Power BI path are separated in [`docs/REPRODUCTION.md`](docs/REPRODUCTION.md).

## Boundaries

- Local-first single-user workbench; no cloud/SaaS, authentication, multi-user, or enterprise-scale claim.
- Browser/Core/Agent/MCP flows are implemented for registered CSV/XES/XES.GZ datasets that satisfy the documented import contract. The two published real-data validations demonstrate portability across those datasets; they are not a claim of universal compatibility with every event-log variant. Power BI remains a historical BPIC12-specific proof.
- No predictive process mining or BPMN/Petri-net discovery claim.
- Observed event gap is **not** true queue or waiting time.
- Configured thresholds are analytical scenarios, **not** actual business SLAs. Help Desk has no default configured threshold.
- Historical analysis-UI usability passed with three valid participants. Current browser onboarding has **not** completed a human usability cohort.

![Current trust caveats for observed event gap and configured thresholds](docs/images/traceverity-trust-caveats.png)

## Data rights and licence

Project-authored source and documentation in this distribution are licensed under the [MIT License](LICENSE).

BPI Challenge 2012 and the Italian Help Desk dataset remain under their own source terms. They are not bundled and are not relicensed by this repository. See [NOTICE](NOTICE) and [`docs/REPRODUCTION.md`](docs/REPRODUCTION.md).
