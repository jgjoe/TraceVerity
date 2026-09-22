let
    Source = Csv.Document(
        File.Contents(DataRoot & "\transitions.csv"),
        [Delimiter = ",", Columns = 4, Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Headers,
        {
            {"rank", Int64.Type},
            {"from_activity", type text},
            {"to_activity", type text},
            {"transition_count", Int64.Type}
        },
        "en-US"
    )
in
    Typed
