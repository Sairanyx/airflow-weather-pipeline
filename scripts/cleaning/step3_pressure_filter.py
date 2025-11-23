def filter_invalid_pressure(df):
    """
    Step 3: Remove rows with invalid pressure values.

    Valid range:
        870 <= Pressure (millibars) <= 1100

    Based on the exploratory analysis, 1288 rows fall outside this
    physical range. These rows are removed to ensure reliable
    daily and monthly aggregations in later steps.

    Returns:
        Cleaned DataFrame containing only valid pressure values.
    """
    df = df[
        (df["Pressure (millibars)"] >= 870) &
        (df["Pressure (millibars)"] <= 1100)
    ]
    return df
