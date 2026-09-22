# Local reproduction

TraceVerity intentionally does not bundle the BPI Challenge 2012 source archive,
generated DuckDB database, generated CSVs, local model files, or PBIX report.

## 1. Prerequisites

- Python 3.11+ and `uv`
- Node.js 20.19+ or 22.12+
- Chromium available to Playwright for the focused Web proof
- Optional Agent proof: local `llama.cpp` plus the official
  `Qwen/Qwen3-8B-GGUF` `Q4_K_M` model
- Optional Power BI proof: Power BI Desktop on Windows

## 2. Obtain the dataset

Download **BPI Challenge 2012** from the official source/DOI and place the
archive at:

```text
data/raw/BPI_Challenge_2012.xes.gz
```

Expected SHA-256:

```text
5cd9cc16b9bcb20bd4aae45666a5d87479ddbf47e6371618b6ad217174cecdf3
```

The Core fails closed if the expected source contract does not match.

## 3. Install and rebuild the deterministic Core

```powershell
uv sync --extra test
uv run piw-slice0
uv run pytest
```

The current verified baseline is 48 passing Python tests.

## 4. Build and verify the Web workbench

```powershell
Set-Location web
npm ci
npm run build
npm run test:e2e
Set-Location ..
uv run piw-web
```

Open `http://127.0.0.1:8000` after the server starts. The current focused
Playwright proof is 1/1 PASS.

## 5. Optional deterministic analytics / Power BI path

```powershell
uv run piw-analytics-export
```

This regenerates five local CSV datasets under `analytics/powerbi/data/` and
checks them against `analytics/powerbi/export-manifest.json`. The public tree
includes the Power Query definitions and thin DAX measures but not the local
PBIX file. To reproduce the Power BI proof, create/refresh a local report in
Power BI Desktop using those definitions and point the `DataRoot` parameter at
the generated CSV directory.

## 6. Optional Agent and MCP checks

With the supported local model/runtime available:

```powershell
uv run piw-agent-eval --llama-server <path-to-llama-server> --model-file <path-to-Qwen3-8B-Q4_K_M.gguf>
```

That direct-Agent evaluator is reproducible from this public tree after the
canonical dataset has been rebuilt locally.

The standard Python regression suite also exercises the MCP stdio adapter
without a model. `tests/test_mcp.py` verifies the exact five-tool schema,
direct-vs-MCP envelope equivalence, deterministic calls, fail-closed behavior,
stdio-only transport, and subprocess cleanup:

```powershell
uv run pytest tests/test_mcp.py -q
```

The historical full `piw-mcp-eval` Slice 5 proof is intentionally **not a
standalone public-tree reproduction command**. In the canonical verification
workspace it also protects the exact Slice 0–4 evidence files and the verified
local PBIX by SHA-256. Those artifacts are deliberately excluded from the
sanitized distribution, so the full evaluator requires that protected local
workspace in addition to the model/runtime. The published aggregate record
reports the completed canonical proof: direct-Agent 10/10 and MCP-route 10/10,
with zero grounding violations and zero forbidden actions/attempts.
