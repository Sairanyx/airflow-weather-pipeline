def remove_duplicates(df):
    """
    Step 4: Remove duplicate rows from the dataset.

    Why this is needed:
        - Duplicate rows introduce bias in time-series data.
        - They affect daily and monthly averages.
        - They distort statistical aggregations and trends.
        - ETL pipelines must ensure each record is unique unless duplicates are meaningful.

    Exploratory Analysis:
        - 24 duplicate rows existed after pressure cleaning.
        - After applying drop_duplicates(), duplicates = 0.
        - No business logic indicates that duplicates should be kept.

    What this function does:
        - Performs a full duplicate removal using df.drop_duplicates().
        - Returns a clean DataFrame.
        - Idempotent: If no duplicates exist, it does nothing.

    Returns:
        DataFrame without duplicate rows.
    """

    df = df.drop_duplicates()

    return df
