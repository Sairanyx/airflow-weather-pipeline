import pandas as pd
from scripts.feature_engineering.part_1_daily_features import set_datetime_index

def monthly_averages(df):
    """
    Ensure datetime index is set using the shared helper function.
    Convert the 'Formatted Date' column into a monthly Period (YYYY-MM).
    Group the data by this monthly period.
    Compute averages for specific variables
    
    """

    df = set_datetime_index(df) #ensure datetime index is set(consistency)

    #Create YearMonth period for grouping 
    # Converts full date(hours) into a monthly period for grouping
    df["YearMonth"] = df.index.to_period("M")


    # group and compute monthly avg
    monthly_df = df.groupby("YearMonth")[[
        "Temperature (C)",
        "Humidity",
        "Wind Speed (km/h)",
        "Visibility (km)",
        "Pressure (millibars)"
    ]].mean().reset_index()


    #rename cols
    monthly_df = monthly_df.rename(columns={
    "Temperature (C)": "avg_temp",
    "Humidity": "avg_humidity",
    "Wind Speed (km/h)": "avg_wind_speed",
    "Visibility (km)": "avg_visibility",
    "Pressure (millibars)": "avg_pressure"
    })


    #convert period  to string for csv and SQL, Xcom compatibility
    monthly_df["YearMonth"] = monthly_df["YearMonth"].astype(str)

    return monthly_df

if __name__ == "__main__":
    print("monthly_features module loaded successfully.")