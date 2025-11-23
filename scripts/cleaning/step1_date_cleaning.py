import pandas as pd

def clean_date_column(df):
    """
    Clean 'Formatted Date' column:
    1) Remove timezone offset (+0200)
    2) Convert string to datetime
    3) Ensure it's timezone-naive for consistent grouping
    """

    # 1 — Remove timezone manually by splitting on last space
    # Example: "2006-04-01 00:00:00.000 +0200" → "2006-04-01 00:00:00.000"
    df["Formatted Date"] = df["Formatted Date"].str.rsplit(" ", n=1).str[0]

    # 2 — Convert to datetime 
    df["Formatted Date"] = pd.to_datetime(
        df["Formatted Date"],
        errors="coerce"
    )



    return df
