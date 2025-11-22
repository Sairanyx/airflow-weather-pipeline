def filter_invalid_pressure(df):
    """
    Step 3: Filter out rows with invalid pressure values.

    define valid range:
        870 <= Pressure (millibars) <= 1100

    Range filter  is applied 

    Based on output:
        - 1288 rows contained invalid pressure values.
        - These rows must be removed to ensure accurate analysis
          in later Feature Engineering steps (daily/monthly aggregates).

    

    Returns:
        DataFrame with valid pressure values only.
    """

    df = df[
        (df["Pressure (millibars)"] >= 870) &
        (df["Pressure (millibars)"] <= 1100)
    ]

    return df
