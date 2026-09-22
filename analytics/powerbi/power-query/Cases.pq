let
    Source = Csv.Document(
        File.Contents(DataRoot & "\cases.csv"),
        [Delimiter = ",", Columns = 6, Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Headers,
        {
            {"case_id", type text},
            {"cycle_time_ms", Int64.Type},
            {"complete_event_count", Int64.Type},
            {"has_rework", type logical},
            {"rework_event_count", Int64.Type},
            {"configured_sla_violation", type logical}
        },
        "en-US"
    )
in
    Typed
