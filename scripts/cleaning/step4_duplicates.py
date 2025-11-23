def remove_duplicates(df):
    """
    Step 4: Remove duplicate rows 

    Why:
        - Duplicate rows distort time-series analysis.
        - They affect daily/monthly averages and aggregations.
        - ETL pipelines require unique records unless duplicates hold meaning.

    Exploratory Analysis:
        - 24 duplicate rows existed 
        

    Returns:
        DataFrame without duplicate rows.
    """

    df = df.drop_duplicates()

    return df
