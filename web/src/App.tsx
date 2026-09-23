import { FormEvent, useCallback, useEffect, useRef, useState } from "react";

import ImportPanel, { DatasetItem } from "./ImportPanel";

type Fact<T = unknown> = {
  fact_id: string;
  name: string;
  value: T;
};

type Envelope<T> = {
  facts: Fact[];
  log_fingerprint: string;
  metric_definition_version: string;
  parameters: Record<string, unknown>;
  query_id: string;
  result: T;
  schema_version: string;
};

type Summary = {
  aggregate_rework_event_count: number;
  analysis_event_count: number;
  case_count: number;
  cases_with_rework: number;
  cycle_time_p50_ms: number;
  cycle_time_p90_ms: number;
  direct_follow_count: number;
  observed_gap_p90_ms: number;
  raw_event_count: number;
  sla_threshold_ms?: number;
  sla_violation_case_count?: number;
  sla_violation_case_share?: number;
  variant_count: number;
};

type SummaryEnvelope = Envelope<Summary> & {
  supplemental: {
    facts: Fact[];
    log_fingerprint: string;
    metric_definition_version: string;
    parameters: Record<string, unknown>;
    query_id: string;
    schema_version: string;
    source_query_id: string;
  };
};

type Variant = {
  activity_sequence: string[];
  case_count: number;
  case_share: number;
  rank?: number;
  variant_id: string;
};

type Transition = {
  from_activity: string;
  to_activity: string;
  transition_count: number;
};

type Activity = {
  activity: string;
  event_count: number;
  case_count: number;
  rework_event_count: number;
};

type Trace = {
  activities: string[];
  case_id: string;
  perspective: "complete" | "raw";
};

type DatasetListing = {
  api_schema_version: string;
  canonical_log_id: string;
  items: DatasetItem[];
};

/**
 * Configured test scenarios per dataset. Only a dataset that actually carries
 * one is analyzed with an SLA threshold; an imported dataset receives none.
 */
const CONFIGURED_SLA_SCENARIOS: Record<string, number> = { bpic2012: 604_800_000 };

class ApiError extends Error {
  code: string;

  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  const body = await response.json();
  if (!response.ok) {
    throw new ApiError(body.error?.code ?? "REQUEST_FAILED", body.error?.message ?? "Request failed");
  }
  return body as T;
}

const integer = new Intl.NumberFormat("en-US");
const percentage = new Intl.NumberFormat("en-US", {
  style: "percent",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatInteger(value: number): string {
  return integer.format(value);
}

function formatShare(value: number): string {
  return percentage.format(value);
}

function formatDuration(ms: number): string {
  const hours = ms / 3_600_000;
  if (hours >= 48) return `${(hours / 24).toFixed(1)} days`;
  if (hours >= 1) return `${hours.toFixed(1)} hours`;
  const minutes = ms / 60_000;
  return `${minutes.toFixed(1)} min`;
}

function MetricCard({
  label,
  value,
  detail,
  testId,
}: {
  label: string;
  value: string;
  detail?: string;
  testId?: string;
}) {
  return (
    <div className="metric-card" data-testid={testId}>
      <span>{label}</span>
      <strong>{value}</strong>
      {detail && <small>{detail}</small>}
    </div>
  );
}

export default function App() {
  const [datasets, setDatasets] = useState<DatasetItem[]>([]);
  const [selectedLogId, setSelectedLogId] = useState("");
  const selectedLogRef = useRef(selectedLogId);
  selectedLogRef.current = selectedLogId;
  const [listingError, setListingError] = useState("");
  const [panelOpen, setPanelOpen] = useState(false);
  const [summary, setSummary] = useState<SummaryEnvelope | null>(null);
  const [variants, setVariants] = useState<Variant[]>([]);
  const [transitions, setTransitions] = useState<Transition[]>([]);
  const [activities, setActivities] = useState<Activity[]>([]);
  const [loadingError, setLoadingError] = useState("");
  const [caseId, setCaseId] = useState("");
  const [trace, setTrace] = useState<Trace | null>(null);
  const [traceError, setTraceError] = useState("");
  const [traceLoading, setTraceLoading] = useState(false);

  const refreshDatasets = useCallback(async (preferred?: string) => {
    try {
      const listing = await getJson<DatasetListing>("/api/logs");
      setDatasets(listing.items);
      setListingError("");
      setSelectedLogId((current) => {
        const wanted = preferred ?? current;
        if (wanted && listing.items.some((item) => item.log_id === wanted && item.status === "ready")) {
          return wanted;
        }
        const fallback =
          listing.items.find((item) => item.built_in && item.status === "ready") ??
          listing.items.find((item) => item.status === "ready");
        return fallback?.log_id ?? "";
      });
    } catch (error) {
      setListingError(error instanceof Error ? error.message : "Unable to list local datasets");
    }
  }, []);

  useEffect(() => {
    void refreshDatasets();
  }, [refreshDatasets]);

  useEffect(() => {
    if (!selectedLogId) return;
    let active = true;
    setLoadingError("");
    setSummary(null);
    setVariants([]);
    setTransitions([]);
    setActivities([]);
    setTrace(null);
    setTraceError("");
    setTraceLoading(false);
    setCaseId("");
    const threshold = CONFIGURED_SLA_SCENARIOS[selectedLogId];
    const summaryQuery = threshold === undefined ? "" : `?sla_threshold_ms=${threshold}`;
    Promise.all([
      getJson<SummaryEnvelope>(`/api/logs/${selectedLogId}/summary${summaryQuery}`),
      getJson<Envelope<{ items: Variant[] }>>(
        `/api/logs/${selectedLogId}/variants?order_by=case_count_desc&limit=10`,
      ),
      getJson<Envelope<{ items: Transition[] }>>(
        `/api/logs/${selectedLogId}/transitions?order_by=transition_count_desc&limit=10`,
      ),
      getJson<Envelope<{ items: Activity[] }>>(
        `/api/logs/${selectedLogId}/activities?order_by=rework_event_count_desc&limit=10`,
      ),
    ])
      .then(([summaryResult, variantResult, transitionResult, activityResult]) => {
        if (!active) return;
        setSummary(summaryResult);
        setVariants(variantResult.result.items);
        setTransitions(transitionResult.result.items);
        setActivities(activityResult.result.items);
      })
      .catch((error: unknown) => {
        if (active) setLoadingError(error instanceof Error ? error.message : "Unable to load analysis");
      });
    return () => {
      active = false;
    };
  }, [selectedLogId]);

  const selected = datasets.find((item) => item.log_id === selectedLogId) ?? null;
  const selectedName = selected?.display_name ?? selectedLogId;

  async function lookupTrace(event: FormEvent) {
    event.preventDefault();
    const requestedCase = caseId.trim();
    if (!requestedCase || !selectedLogId) return;
    const requestedLogId = selectedLogId;
    setTraceLoading(true);
    setTraceError("");
    setTrace(null);
    try {
      const response = await getJson<Envelope<Trace>>(
        `/api/logs/${requestedLogId}/cases/${encodeURIComponent(requestedCase)}?perspective=complete`,
      );
      if (selectedLogRef.current !== requestedLogId) return;
      setTrace(response.result);
    } catch (error) {
      if (selectedLogRef.current !== requestedLogId) return;
      if (error instanceof ApiError && error.code === "NOT_FOUND") {
        setTraceError(`No ${selectedName} case found for “${requestedCase}”.`);
      } else {
        setTraceError(error instanceof Error ? error.message : "Case lookup failed");
      }
    } finally {
      if (selectedLogRef.current === requestedLogId) setTraceLoading(false);
    }
  }

  const gapP50 = summary?.supplemental.facts.find(
    (fact) => fact.name === "observed_gap_p50_ms",
  )?.value as number | undefined;

  return (
    <main>
      <header className="hero">
        <div className="hero-copy">
          <div className="eyebrow">Process Intelligence Workbench</div>
          <h1>TraceVerity</h1>
          <p>
            <span className="status-dot" aria-hidden="true" /> Dataset:{" "}
            <strong data-testid="selected-dataset-name">{selectedName || "none available"}</strong>
            {selected && <span data-testid="selected-dataset-status"> · {selected.status}</span>}
          </p>
        </div>
        <div className="principle">
          <span>Deterministic Core</span>
          <p>Process facts are computed by the validated deterministic Core—not by AI.</p>
        </div>
      </header>

      <section aria-labelledby="workspace-heading">
        <div className="section-heading">
          <div>
            <h2 id="workspace-heading">Dataset workspace</h2>
          </div>
          <span className="data-tag">{datasets.length} local datasets</span>
        </div>
        <div className="workspace-bar">
          <div className="field">
            <label htmlFor="dataset-select">Dataset</label>
            <select
              data-testid="dataset-select"
              id="dataset-select"
              onChange={(event) => setSelectedLogId(event.target.value)}
              value={selectedLogId}
            >
              {datasets.map((item) => (
                <option
                  disabled={item.status !== "ready"}
                  key={item.log_id}
                  value={item.log_id}
                >
                  {item.display_name} ({item.log_id}) · {item.status}
                </option>
              ))}
            </select>
          </div>
          <button
            className="standalone"
            data-testid="import-toggle"
            onClick={() => setPanelOpen((open) => !open)}
            type="button"
          >
            {panelOpen ? "Close import" : "Import event log"}
          </button>
        </div>
        {listingError && <div className="error-inline" role="alert">{listingError}</div>}
        {panelOpen && <ImportPanel onImported={(logId) => void refreshDatasets(logId)} />}
      </section>

      {loadingError && <div className="error-banner">Unable to load the workbench: {loadingError}</div>}
      {!selectedLogId && !loadingError && (
        <div className="loading">No ready local dataset is available yet.</div>
      )}
      {selectedLogId && !summary && !loadingError && (
        <div className="loading">Reading the selected local dataset…</div>
      )}

      {selectedLogId && summary && (
        <>
          <section aria-labelledby="summary-heading">
            <div className="section-heading">
              <div>
                <h2 id="summary-heading">Core summary</h2>
              </div>
              <span className="data-tag">COMPLETE perspective</span>
            </div>
            <div className="metrics-grid">
              <MetricCard label="Cases" value={formatInteger(summary.result.case_count)} testId="case-count" />
              <MetricCard label="Raw events" value={formatInteger(summary.result.raw_event_count)} testId="raw-events" />
              <MetricCard label="Analysis events" value={formatInteger(summary.result.analysis_event_count)} testId="analysis-events" />
              <MetricCard label="Variants" value={formatInteger(summary.result.variant_count)} testId="variant-count" />
              <MetricCard label="Direct-follow occurrences" value={formatInteger(summary.result.direct_follow_count)} />
              <MetricCard label="Cycle time · p50" value={formatDuration(summary.result.cycle_time_p50_ms)} detail={`${summary.result.cycle_time_p50_ms} ms`} />
              <MetricCard label="Cycle time · p90" value={formatDuration(summary.result.cycle_time_p90_ms)} detail={`${summary.result.cycle_time_p90_ms} ms`} />
              <MetricCard label="Observed gap · p50" value={formatDuration(gapP50 ?? 0)} detail={`${gapP50 ?? 0} ms · not true queue waiting`} />
              <MetricCard label="Observed gap · p90" value={formatDuration(summary.result.observed_gap_p90_ms)} detail={`${summary.result.observed_gap_p90_ms} ms · not true queue waiting`} />
              <MetricCard label="Cases with rework" value={formatInteger(summary.result.cases_with_rework)} />
              <MetricCard label="Aggregate rework events" value={formatInteger(summary.result.aggregate_rework_event_count)} />
              {summary.result.sla_threshold_ms !== undefined && (
                <MetricCard
                  label="Configured SLA violations"
                  value={formatInteger(summary.result.sla_violation_case_count ?? 0)}
                  detail={`raw share ${formatShare(summary.result.sla_violation_case_share ?? 0)} · configured test threshold: ${summary.result.sla_threshold_ms} ms`}
                  testId="sla-card"
                />
              )}
            </div>
          </section>

          <div className="two-column">
            <section aria-labelledby="variants-heading">
              <div className="section-heading compact">
                <div><h2 id="variants-heading">Top variants</h2></div>
                <span className="data-tag">by case count</span>
              </div>
              <div className="table-shell">
                <table data-testid="variants-table">
                  <thead><tr><th>Rank</th><th>Activity sequence</th><th>Cases</th><th>Share</th></tr></thead>
                  <tbody>
                    {variants.map((variant, index) => (
                      <tr key={variant.variant_id}>
                        <td className="rank">{variant.rank ?? index + 1}</td>
                        <td><div className="sequence">{variant.activity_sequence.join(" → ")}</div></td>
                        <td>{formatInteger(variant.case_count)}</td>
                        <td>{formatShare(variant.case_share)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>

            <section aria-labelledby="transitions-heading">
              <div className="section-heading compact">
                <div><h2 id="transitions-heading">Top direct-follow transitions</h2></div>
                <span className="data-tag">by frequency</span>
              </div>
              <div className="transition-list" data-testid="transitions-list">
                {transitions.map((transition, index) => (
                  <div className="transition-row" key={`${transition.from_activity}-${transition.to_activity}`}>
                    <span className="rank">{String(index + 1).padStart(2, "0")}</span>
                    <div><strong>{transition.from_activity}</strong><span className="arrow">→</span><strong>{transition.to_activity}</strong></div>
                    <span className="count-pill">{formatInteger(transition.transition_count)}</span>
                  </div>
                ))}
              </div>
            </section>
          </div>

          <section aria-labelledby="activities-heading">
            <div className="section-heading compact">
              <div><h2 id="activities-heading">Activity &amp; rework</h2></div>
              <span className="data-tag">by rework event count</span>
            </div>
            <div className="table-shell">
              <table data-testid="activities-table">
                <thead><tr><th>Activity</th><th>Events</th><th>Cases</th><th>Rework events</th></tr></thead>
                <tbody>
                  {activities.map((activity) => (
                    <tr key={activity.activity}>
                      <td><strong>{activity.activity}</strong></td>
                      <td>{formatInteger(activity.event_count)}</td>
                      <td>{formatInteger(activity.case_count)}</td>
                      <td><span className="rework-value">{formatInteger(activity.rework_event_count)}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>

          <section className="trace-panel" aria-labelledby="trace-heading">
            <div>
              <h2 id="trace-heading">Case trace lookup</h2>
              <p>
                Retrieve the canonical ordered activity trace for{" "}
                <strong>{selectedName}</strong>. Default perspective: <strong>COMPLETE</strong>.
              </p>
              <form onSubmit={lookupTrace}>
                <label htmlFor="case-id">Case ID</label>
                <div className="input-row">
                  <input id="case-id" value={caseId} onChange={(event) => setCaseId(event.target.value)} placeholder="Enter a case ID" />
                  <button type="submit" disabled={traceLoading}>{traceLoading ? "Looking up…" : "Look up case"}</button>
                </div>
              </form>
            </div>
            <div className="trace-result" aria-live="polite">
              {!trace && !traceError && <div className="empty-trace">Enter a case ID to inspect its COMPLETE-perspective path.</div>}
              {traceError && <div className="trace-error" data-testid="trace-error" role="alert">{traceError}</div>}
              {trace && (
                <div data-testid="case-trace">
                  <div className="trace-meta"><span>CASE {trace.case_id}</span><span>{trace.activities.length} events</span></div>
                  <ol>{trace.activities.map((activity, index) => <li key={`${activity}-${index}`}><span>{index + 1}</span>{activity}</li>)}</ol>
                </div>
              )}
            </div>
          </section>

          <footer>
            <span>Metric contract: {summary.metric_definition_version}</span>
            <span>Log fingerprint: {summary.log_fingerprint.slice(0, 12)}…</span>
            <span>Query: {summary.query_id.slice(0, 14)}…</span>
          </footer>
        </>
      )}
    </main>
  );
}
