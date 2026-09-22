let
    Source = Csv.Document(
        File.Contents(DataRoot & "\summary.csv"),
        [Delimiter = ",", Columns = 17, Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Headers,
        {
            {"log_id", type text},
            {"log_fingerprint", type text},
            {"metric_definition_version", type text},
            {"case_count", Int64.Type},
            {"raw_event_count", Int64.Type},
            {"analysis_event_count", Int64.Type},
            {"variant_count", Int64.Type},
            {"direct_follow_count", Int64.Type},
            {"cycle_time_p50_ms", Int64.Type},
            {"cycle_time_p90_ms", Int64.Type},
            {"observed_gap_p50_ms", Int64.Type},
            {"observed_gap_p90_ms", Int64.Type},
            {"cases_with_rework", Int64.Type},
            {"aggregate_rework_event_count", Int64.Type},
            {"configured_sla_threshold_ms", Int64.Type},
            {"sla_violation_case_count", Int64.Type},
            {"sla_violation_case_share", type number}
        },
        "en-US"
    )
in
    Typed
