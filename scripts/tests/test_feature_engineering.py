import pandas as pd
from scripts.feature_engineering.part_1_daily_features import compute_daily_averages
from scripts.feature_engineering.part_2_precip_mode import monthly_precip_mode
from scripts.feature_engineering.part_3_wind_strength import wind_categorization
from scripts.feature_engineering.part_4_monthly_features import monthly_averages


# -------------------------------------------------------------
# 1) TEST FOR DAILY AVERAGES
# -------------------------------------------------------------
def test_daily_averages():
    data = {
        "Formatted Date": [
            "2006-01-01 00:00:00",
            "2006-01-01 01:00:00",
            "2006-01-02 00:00:00",
        ],
        "Temperature (C)": [10, 14, 12],
        "Humidity": [0.8, 0.6, 0.7],
        "Wind Speed (km/h)": [5, 7, 6]
    }

    
    df = pd.DataFrame(data)

    # no cleaning - convert again to datetime so resample works
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"])


    # run the function under test
    daily_df = compute_daily_averages(df)

    # Check correct number of rows 
    assert len(daily_df) == 2 #resampling per day - 2 days

    # Check output column names exist -renaming
    expected_cols = {
        "Formatted Date",
        "avg_temp", 
        "avg_humidity", 
        "avg_wind_speed"}
    
    assert expected_cols.issubset(daily_df.columns)

    # Check correct average values
    # Row for 2006-01-01
    day1 = daily_df.iloc[0]
    assert day1["avg_temp"] == 12        
    assert day1["avg_humidity"] == 0.7   
    assert day1["avg_wind_speed"] == 6   

    # Row for 2006-01-02
    day2 = daily_df.iloc[1]
    assert day2["avg_temp"] == 12
    assert day2["avg_humidity"] == 0.7
    assert day2["avg_wind_speed"] == 6

# -------------------------------------------------------------
# 2) TEST FOR PRECIPITATION MODE
# -------------------------------------------------------------
def test_precipitation_mode():
    data = {
        "Formatted Date": [
            "2006-01-01 00:00:00",
            "2006-01-02 00:00:00",
            "2006-01-03 00:00:00"
        ],
        "Precip Type": ["rain", "snow", "rain"]
    }

    df = pd.DataFrame(data)

    # no cleaning - convert to datetime
    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"])

    monthly_df = monthly_precip_mode(df)

    # Should have 1 row
    assert len(monthly_df) == 1

    # should contain the correct col
    assert "Mode Precip Type" in monthly_df.columns

    # Mode should be rain
    assert monthly_df.loc[0, "Mode Precip Type"] == "rain"

    # YearMonth should exist - value is correct
    assert "YearMonth" in monthly_df.columns
    assert monthly_df.loc[0, "YearMonth"] == "2006-01"

    # index should be reset to start at 0
    assert monthly_df.index[0] == 0


# -------------------------------------------------------------
# 3) TEST FOR WIND CATEGORIZATION
# -------------------------------------------------------------
def test_wind_categorization():
    data = {
        "Formatted Date": [
            "2006-01-01 00:00:00",
            "2006-01-02 00:00:00",
            "2006-01-03 00:00:00",
            "2006-01-04 00:00:00"
        ],
        "Wind Speed (km/h)": [0.5, 3.0, 12, 40]
    }

    df = pd.DataFrame(data)

    df = wind_categorization(df)

    # Check column exists
    assert "wind_strength" in df.columns

    # Output size unchanged
    assert len(df) == 4

    # Correct categorization based on bins
    expected = ["Calm", "Light Air", "Fresh Breeze", "Violent Storm"]


    for i, label in enumerate(expected):
        assert df.loc[i, "wind_strength"] == label

    # type should be categorical 
    assert df["wind_strength"].dtype.name == "category"


# -------------------------------------------------------------
# 4) TEST FOR MONTHLY AVERAGES
# -------------------------------------------------------------
def test_monthly_averages():
    data = {
        "Formatted Date": [
            "2006-02-01 00:00:00",
            "2006-02-15 00:00:00"
        ],
        "Temperature (C)": [10, 20],
        "Humidity": [0.5, 0.9],
        "Wind Speed (km/h)": [5, 15],
        "Visibility (km)": [10, 20],
        "Pressure (millibars)": [1000, 1020]
    }

    df = pd.DataFrame(data)

    monthly_df = monthly_averages(df)

    # Should produce 1 row
    assert len(monthly_df) == 1

    row = monthly_df.iloc[0]

    # Check correctness of means
    assert row["avg_temp"] == 15          # (10+20)/2
    assert row["avg_humidity"] == 0.7     # (0.5+0.9)/2
    assert row["avg_wind_speed"] == 10    # (5+15)/2
    assert row["avg_visibility"] == 15    # (10+20)/2
    assert row["avg_pressure"] == 1010    # (1000+1020)/2

    # Check YearMonth exists
    assert row["YearMonth"] == "2006-02"
