import pandas as pd


def set_datetime_index(df):
    """
    Convert 'Formatted Date' to datetime and set it as index.
    Required for resampling and period operations. 
    """
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], errors="coerce")
    df = df.set_index("Formatted Date")
    df = df.sort_index()
    return df


def compute_daily_averages(df):
    # Ensure index is datetime
    df = set_datetime_index(df)

    # Select ONLY numeric columns for averaging
    numeric_cols = [
        "Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)"
    ]

    # Resample daily
    daily = df[numeric_cols].resample("D").mean().reset_index()

    # Rename columns
    daily = daily.rename(columns={
        "Temperature (C)": "avg_temp",
        "Humidity": "avg_humidity",
        "Wind Speed (km/h)": "avg_wind_speed"
    })

    return daily


if __name__ == "__main__":
    print("Import OK, daily features module loaded!")