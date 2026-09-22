let
    Source = Csv.Document(
        File.Contents(DataRoot & "\activities.csv"),
        [Delimiter = ",", Columns = 5, Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Headers,
        {
            {"rank", Int64.Type},
            {"activity", type text},
            {"event_count", Int64.Type},
            {"case_count", Int64.Type},
            {"rework_event_count", Int64.Type}
        },
        "en-US"
    )
in
    Typed
