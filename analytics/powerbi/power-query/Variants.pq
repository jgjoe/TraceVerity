let
    Source = Csv.Document(
        File.Contents(DataRoot & "\variants.csv"),
        [Delimiter = ",", Columns = 6, Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(
        Headers,
        {
            {"rank", Int64.Type},
            {"variant_id", type text},
            {"case_count", Int64.Type},
            {"case_share", type number},
            {"activity_count", Int64.Type},
            {"activity_sequence", type text}
        },
        "en-US"
    )
in
    Typed
