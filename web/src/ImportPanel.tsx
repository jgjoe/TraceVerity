import { ChangeEvent, FormEvent, useState } from "react";

export type DatasetItem = {
  built_in: boolean;
  dataset_id: string | null;
  display_name: string;
  log_id: string;
  source_fingerprint: { filename: string | null; sha256: string; size_bytes: number } | null;
  source_format: "csv" | "xes";
  status: "ready" | "registered" | "unavailable";
};

type ImportSummary = {
  filename: string;
  import_id: string;
  sha256: string;
  size_bytes: number;
  source_format: "csv" | "xes";
};

type CsvPreview = {
  data_row_count: number;
  delimiter: string;
  duplicate_columns: string[];
  header: string[];
};

type PreviewResponse = {
  api_schema_version: string;
  csv_preview: CsvPreview | null;
  import: ImportSummary;
};

type BuildResponse = {
  api_schema_version: string;
  build: {
    analysis_event_count: number;
    case_count: number;
    raw_event_count: number;
    validation_status: string;
    variant_count: number;
  };
  dataset: DatasetItem;
};

type Mapping = {
  activity: string;
  case_id: string;
  lifecycle: string;
  resource: string;
  timestamp: string;
};

const UNMAPPED = "";
const integer = new Intl.NumberFormat("en-US");

function defaultLogId(name: string): string {
  const slug = name
    .replace(/\.[^.]+$/i, "")
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^[^a-z0-9]+/, "")
    .slice(0, 64);
  return slug || "imported-log";
}

async function fetchJson(url: string, init: RequestInit, fallback: string): Promise<unknown> {
  const response = await fetch(url, init);
  const payload = await response.json();
  if (!response.ok) {
    const message = (payload as { error?: { message?: unknown } } | null)?.error?.message;
    throw new Error(typeof message === "string" && message ? message : fallback);
  }
  return payload;
}

export default function ImportPanel({
  onImported,
}: {
  onImported: (logId: string) => void;
}) {
  const [file, setFile] = useState<File | null>(null);
  const [sourceFormat, setSourceFormat] = useState<"csv" | "xes">("csv");
  const [delimiter, setDelimiter] = useState(",");
  const [preview, setPreview] = useState<PreviewResponse | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const [mapping, setMapping] = useState<Mapping>({
    activity: UNMAPPED,
    case_id: UNMAPPED,
    lifecycle: UNMAPPED,
    resource: UNMAPPED,
    timestamp: UNMAPPED,
  });
  const [timestampFormat, setTimestampFormat] = useState("");
  const [assumeTimezone, setAssumeTimezone] = useState("");
  const [logId, setLogId] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [building, setBuilding] = useState(false);
  const [buildError, setBuildError] = useState("");
  const [buildSuccess, setBuildSuccess] = useState("");

  const header = preview?.csv_preview?.header ?? [];

  function chooseFile(event: ChangeEvent<HTMLInputElement>) {
    const chosen = event.target.files?.[0] ?? null;
    setFile(chosen);
    setPreview(null);
    setPreviewError("");
    setBuildError("");
    setBuildSuccess("");
    if (!chosen) return;
    const lowered = chosen.name.toLowerCase();
    if (lowered.endsWith(".csv")) setSourceFormat("csv");
    if (lowered.endsWith(".xes") || lowered.endsWith(".xes.gz")) setSourceFormat("xes");
    setLogId(defaultLogId(chosen.name));
    setDisplayName(chosen.name.replace(/\.[^.]+$/i, ""));
  }

  function setField(field: keyof Mapping, value: string) {
    setMapping((current) => ({ ...current, [field]: value }));
  }

  async function requestPreview() {
    if (!file) {
      setPreviewError("Choose an event-log file first.");
      return;
    }
    setPreviewing(true);
    setPreviewError("");
    setBuildError("");
    setBuildSuccess("");
    setPreview(null);
    try {
      const query = new URLSearchParams({ filename: file.name });
      if (sourceFormat === "csv") query.set("delimiter", delimiter || ",");
      const body = await fetchJson(
        `/api/imports/preview?${query.toString()}`,
        { body: file, method: "POST" },
        "The source could not be previewed.",
      );
      setPreview(body as PreviewResponse);
      setMapping({ activity: UNMAPPED, case_id: UNMAPPED, lifecycle: UNMAPPED, resource: UNMAPPED, timestamp: UNMAPPED });
    } catch (error) {
      setPreviewError(error instanceof Error ? error.message : "Preview failed");
    } finally {
      setPreviewing(false);
    }
  }

  async function requestBuild() {
    if (!preview) {
      setBuildError("Preview the source before importing it.");
      return;
    }
    if (sourceFormat === "csv") {
      const required: Array<[string, string]> = [
        ["Case ID column", mapping.case_id],
        ["Activity column", mapping.activity],
        ["Timestamp column", mapping.timestamp],
      ];
      const missing = required.filter(([, value]) => !value).map(([label]) => label);
      if (missing.length) {
        setBuildError(`Map the required source columns first: ${missing.join(", ")}.`);
        return;
      }
    }
    if (!logId.trim() || !displayName.trim()) {
      setBuildError("log_id and display name are both required.");
      return;
    }
    setBuilding(true);
    setBuildError("");
    setBuildSuccess("");
    try {
      const timestamp: Record<string, string> = {};
      if (timestampFormat) timestamp.timestamp_format = timestampFormat;
      if (assumeTimezone) timestamp.assume_timezone = assumeTimezone;
      const payload: Record<string, unknown> = {
        display_name: displayName.trim(),
        import_id: preview.import.import_id,
        log_id: logId.trim(),
      };
      if (sourceFormat === "csv") {
        payload.csv = {
          delimiter: delimiter || ",",
          mapping: {
            activity: mapping.activity,
            case_id: mapping.case_id,
            lifecycle: mapping.lifecycle || null,
            resource: mapping.resource || null,
            timestamp: mapping.timestamp,
          },
          timestamp,
        };
      }
      const body = await fetchJson(
        "/api/imports/build",
        {
          body: JSON.stringify(payload),
          headers: { "Content-Type": "application/json" },
          method: "POST",
        },
        "The dataset could not be validated.",
      );
      const result = body as BuildResponse;
      setBuildSuccess(
        `Validated and imported ${result.dataset.display_name} — ` +
          `${integer.format(result.build.case_count)} cases, ` +
          `${integer.format(result.build.raw_event_count)} raw events, ` +
          `${integer.format(result.build.variant_count)} variants. Selected in the workbench.`,
      );
      onImported(result.dataset.log_id);
    } catch (error) {
      setBuildError(error instanceof Error ? error.message : "The dataset could not be validated.");
    } finally {
      setBuilding(false);
    }
  }

  return (
    <div className="import-panel" data-testid="import-panel">
      <div className="section-heading compact">
        <div>
          <h3>Import event log</h3>
        </div>
        <span className="data-tag">localhost · local filesystem only</span>
      </div>
      <p className="field-hint">
        The selected file's bytes are sent to this local server only
        (127.0.0.1) and written to the local workspace on this machine. Nothing is sent to a cloud
        service or an external API.
      </p>
      <form
        onSubmit={(event: FormEvent) => {
          event.preventDefault();
          void requestPreview();
        }}
      >
        <div className="field-row">
          <div className="field">
            <label htmlFor="import-file">Event-log file</label>
            <input
              accept=".csv,.xes,.gz"
              data-testid="import-file"
              id="import-file"
              onChange={chooseFile}
              type="file"
            />
          </div>
          <div className="field narrow">
            <label htmlFor="import-format">Source format</label>
            <select
              data-testid="import-format"
              id="import-format"
              onChange={(event) => setSourceFormat(event.target.value as "csv" | "xes")}
              value={sourceFormat}
            >
              <option value="csv">CSV</option>
              <option value="xes">XES / XES.GZ</option>
            </select>
          </div>
          {sourceFormat === "csv" && (
            <div className="field narrow">
              <label htmlFor="import-delimiter">CSV delimiter</label>
              <input
                data-testid="import-delimiter"
                id="import-delimiter"
                maxLength={1}
                onChange={(event) => setDelimiter(event.target.value)}
                value={delimiter}
              />
            </div>
          )}
          <button
            className="standalone"
            data-testid="import-preview-button"
            disabled={previewing}
            type="submit"
          >
            {previewing ? "Reading source…" : "Preview source"}
          </button>
        </div>
      </form>

      {previewError && (
        <div className="import-error" data-testid="import-error" role="alert">
          {previewError}
        </div>
      )}

      {preview && (
        <div className="preview-block" data-testid="import-preview">
          <dl className="preview-meta">
            <div>
              <dt>File</dt>
              <dd>{preview.import.filename}</dd>
            </div>
            <div>
              <dt>Format</dt>
              <dd>{preview.import.source_format.toUpperCase()}</dd>
            </div>
            <div>
              <dt>Size</dt>
              <dd>{integer.format(preview.import.size_bytes)} bytes</dd>
            </div>
            <div>
              <dt>SHA-256</dt>
              <dd className="mono">{preview.import.sha256}</dd>
            </div>
          </dl>
          {preview.csv_preview && (
            <div className="preview-columns">
              <p>
                <strong>{integer.format(preview.csv_preview.data_row_count)}</strong> data rows ·{" "}
                <strong>{preview.csv_preview.header.length}</strong> columns · delimiter{" "}
                <code>{preview.csv_preview.delimiter}</code>
              </p>
              <p className="mono">{preview.csv_preview.header.join(" | ")}</p>
              {preview.csv_preview.duplicate_columns.length > 0 && (
                <p className="duplicate-note">
                  Duplicated header names (unmapped only):{" "}
                  <strong>{preview.csv_preview.duplicate_columns.join(", ")}</strong>. A column that
                  is mapped must be unique.
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {preview && sourceFormat === "csv" && (
        <div className="mapping-grid">
          <div className="field">
            <label htmlFor="mapping-case-id">Case ID column</label>
            <select
              data-testid="mapping-case_id"
              id="mapping-case-id"
              onChange={(event) => setField("case_id", event.target.value)}
              value={mapping.case_id}
            >
              <option value={UNMAPPED}>Select a column…</option>
              {header.map((name) => (
                <option key={`case-${name}`} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="mapping-activity">Activity column</label>
            <select
              data-testid="mapping-activity"
              id="mapping-activity"
              onChange={(event) => setField("activity", event.target.value)}
              value={mapping.activity}
            >
              <option value={UNMAPPED}>Select a column…</option>
              {header.map((name) => (
                <option key={`activity-${name}`} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="mapping-timestamp">Timestamp column</label>
            <select
              data-testid="mapping-timestamp"
              id="mapping-timestamp"
              onChange={(event) => setField("timestamp", event.target.value)}
              value={mapping.timestamp}
            >
              <option value={UNMAPPED}>Select a column…</option>
              {header.map((name) => (
                <option key={`timestamp-${name}`} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="mapping-lifecycle">Lifecycle column (optional)</label>
            <select
              data-testid="mapping-lifecycle"
              id="mapping-lifecycle"
              onChange={(event) => setField("lifecycle", event.target.value)}
              value={mapping.lifecycle}
            >
              <option value={UNMAPPED}>Leave unmapped</option>
              {header.map((name) => (
                <option key={`lifecycle-${name}`} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="mapping-resource">Resource column (optional)</label>
            <select
              data-testid="mapping-resource"
              id="mapping-resource"
              onChange={(event) => setField("resource", event.target.value)}
              value={mapping.resource}
            >
              <option value={UNMAPPED}>Leave unmapped</option>
              {header.map((name) => (
                <option key={`resource-${name}`} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </div>
          <div className="field">
            <label htmlFor="timestamp-format">Timestamp format (strptime, optional)</label>
            <input
              data-testid="timestamp-format"
              id="timestamp-format"
              onChange={(event) => setTimestampFormat(event.target.value)}
              placeholder="%Y/%m/%d %H:%M:%S.%f"
              value={timestampFormat}
            />
          </div>
          <div className="field">
            <label htmlFor="assume-timezone">Assumed timezone for offset-less timestamps</label>
            <input
              data-testid="assume-timezone"
              id="assume-timezone"
              onChange={(event) => setAssumeTimezone(event.target.value)}
              placeholder="UTC, +02:00, or Europe/Rome"
              value={assumeTimezone}
            />
          </div>
        </div>
      )}

      {preview && sourceFormat === "csv" && (
        <p className="timezone-caveat" data-testid="timezone-caveat">
          An assumed timezone is a <strong>normalization convention</strong> for timestamps that
          carry no offset of their own — it is not automatically a claim about the original event
          timezone. Leave it empty when the source timestamps carry their own offsets: offset-less
          timestamps then fail validation instead of being silently assumed to be UTC.
        </p>
      )}

      {preview && (
        <div className="field-row">
          <div className="field">
            <label htmlFor="import-log-id">log_id</label>
            <input
              data-testid="import-log-id"
              id="import-log-id"
              onChange={(event) => setLogId(event.target.value)}
              placeholder="lowercase, e.g. helpdesk"
              value={logId}
            />
          </div>
          <div className="field">
            <label htmlFor="import-display-name">Display name</label>
            <input
              data-testid="import-display-name"
              id="import-display-name"
              onChange={(event) => setDisplayName(event.target.value)}
              placeholder="Shown in the workbench"
              value={displayName}
            />
          </div>
          <button
            className="standalone"
            data-testid="import-build-button"
            disabled={building}
            onClick={() => void requestBuild()}
            type="button"
          >
            {building ? "Validating and building…" : "Validate & import"}
          </button>
        </div>
      )}

      <div className="import-status" data-testid="import-status" role="status">
        {buildSuccess ||
          (building
            ? "Validating the mapping and building the canonical DuckDB…"
            : "Validate the mapping before the dataset is registered: a failed build registers nothing.")}
      </div>
      {buildError && (
        <div className="import-error" data-testid="import-error" role="alert">
          {buildError}
        </div>
      )}
    </div>
  );
}
