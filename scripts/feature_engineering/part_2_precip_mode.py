import pandas as pd
from scripts.feature_engineering.part_1_daily_features import set_datetime_index

def monthly_precip_mode(df):

    """
    Compute the most frequent precipitation type (mode) for each month.
    """

    df = set_datetime_index(df)

    # Resample monthly and compute mode 
    monthly_precip = df["Precip Type"].resample("M").agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else None
    )

    # Convert to DataFrame
    monthly_precip = monthly_precip.to_frame(name="Mode Precip Type")

     # Extract YYYY-MM for merging and database loading
    monthly_precip["YearMonth"] = monthly_precip.index.to_period("M").astype(str)

    # Reset index  for storage/CSV/XCom
    monthly_precip = monthly_precip.reset_index(drop=True)

    return monthly_precip
