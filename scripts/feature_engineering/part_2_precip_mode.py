import pandas as pd
from scripts.feature_engineering.part_1_daily_features import set_datetime_index

def monthly_precip_mode(df):
    """
    Compute the most frequent precipitation type (mode) for each month.
    """
    df = set_datetime_index(df)

    # Resample monthly and compute mode safely
    monthly_precip = df["Precip Type"].resample("ME").agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else None
    )

    # Convert to DataFrame
    monthly_precip = monthly_precip.to_frame(name="Mode Precip Type")

    # Add YearMonth column for merging
    monthly_precip["YearMonth"] = monthly_precip.index.to_period("M").astype(str)

    # Reset index to make merging easy
    monthly_precip = monthly_precip.reset_index(drop=True)

    return monthly_precip
