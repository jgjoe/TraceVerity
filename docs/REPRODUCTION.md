# Public reproduction

TraceVerity never auto-downloads or redistributes the real datasets. Obtain each source separately, verify its fingerprint, and keep it under the ignored local `data/` tree.

## Install dependencies

```powershell
uv sync --extra test
Set-Location web
npm ci
Set-Location ..
```

## A. Generic browser onboarding

Build and start the current product:

```powershell
Set-Location web
npm run build
Set-Location ..
uv run piw-web
```

Open `http://127.0.0.1:8000`, then use **Import event log**:

1. Select a local `.csv`, `.xes`, or `.xes.gz` file.
2. Preview source identity and structure.
3. For CSV, map case ID, activity, timestamp, and optional resource/lifecycle fields explicitly.
4. State timestamp format and timezone interpretation.
5. Validate and build. The dataset is not registered as ready before validation succeeds.
6. Switch between ready datasets in the Dataset Workspace.

The selected file is sent only to the localhost application and stored in the ignored local workspace. Nothing is uploaded to a hosted service.

The focused browser proof uses the real Help Desk CSV when it is present:

```powershell
Set-Location web
npm run test:e2e
Set-Location ..
```

## B. Optional real Help Desk reproduction

Obtain the source separately from its public data provider.

- Dataset: `Dataset belonging to the help desk log of an Italian Company`
- Author: Mirko Polato
- DOI: `10.4121/uuid:0c60edf1-6f83-4e75-9367-4c63b3e9d5bb`
- Licence label: `4TU General Terms of Use`
- Expected filename: `finale.csv`
- Expected local path: `data/raw/helpdesk/finale.csv`
- Expected SHA-256: `31024fa6da0a35578643d50f2bea6e90d5ce97628b64851e271b521f005eef1c`

Canonical CSV mapping:

- `case_id <- Case ID`
- `activity <- Activity`
- `timestamp <- Complete Timestamp`
- `resource <- Resource`
- lifecycle unmapped
- `timestamp_format: %Y/%m/%d %H:%M:%S.%f`
- `assume_timezone: UTC`

`UTC` is a deterministic normalization convention. TraceVerity does not claim the source timezone was established by the dataset documentation.

Expected aggregates include 4,580 cases, 21,348 raw and analysis events, 14 activities, 226 variants, 16,768 direct-follow occurrences, 1,240 cases with rework, and 1,905 aggregate rework events. No default configured SLA scenario exists.

## C. BPIC12 baseline reproduction

- Dataset: BPI Challenge 2012
- Author: Boudewijn van Dongen
- DOI: `10.4121/uuid:3926db30-f712-4394-aebc-75976070e91f`
- Licence label: `4TU General Terms of Use`
- Expected file: `data/raw/BPI_Challenge_2012.xes.gz`
- Expected SHA-256: `5cd9cc16b9bcb20bd4aae45666a5d87479ddbf47e6371618b6ad217174cecdf3`

```powershell
uv run piw-slice0
```

Expected baseline: 13,087 cases, 262,200 raw events, 164,506 analysis events, 24 activities, 4,336 variants, and 151,419 direct-follow occurrences. A wrong source fingerprint or failed invariant produces `HOLD`.

## D. Agent and MCP verification

The same five read-only tools back Direct Agent and stdio MCP:

```text
describe_log
list_variants
list_transitions
list_activities
get_case_trace
```

Run the public Python suite after the BPIC12 baseline is built:

```powershell
uv run pytest -q
```

Public-tree Slice H result: **168 / 168 PASS**.

The 22-test difference from the canonical Slice E count is intentional. This sanitized tree excludes canonical-only Help Desk evidence integration, model-backed Agent/MCP re-derivation, protected-artifact/PBIX regression, and historical evidence checks. Its 168 tests still cover the deterministic Core, imports, dataset resolution, HTTP behavior, tools, Agent protocol, MCP transport, analytics export, and browser onboarding. Canonical Slice E independently recorded Python 190/190, Direct Agent 12/12, MCP 12/12, and 22/22 Direct/MCP comparisons with mismatch 0; those remain published aggregate evidence, not the public-tree count.

Model-backed evaluation remains local and optional. It requires a separately installed `llama.cpp` runtime and local model file; neither is bundled.

## E. Historical BPIC12-only Power BI path

```powershell
uv run piw-analytics-export
```

The command writes ignored deterministic CSVs under `analytics/powerbi/data/`. Power Query/DAX and the local PBIX were historically verified for BPIC12. The PBIX is excluded from this repository, and Power BI is not claimed as a generic imported-dataset surface.

## Public checks

```powershell
uv sync --extra test
uv run piw-slice0
uv run pytest -q

Set-Location web
npm ci
npm run build
npm run test:e2e
Set-Location ..
```

The summary file is aggregate-only: [`../evidence/public-verification-summary.json`](../evidence/public-verification-summary.json). Raw event rows, participant records, timestamps, local paths, process IDs, and random run identifiers are intentionally absent.
