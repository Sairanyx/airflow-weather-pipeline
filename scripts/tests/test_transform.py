import pandas as pd
from scripts.transform import transform_weather_data
import os 
def test_transform_basic_functionality():

    # sample dataset
    data = {
        "Formatted Date": [
            "2006-04-01 00:00:00.000 +0200",
            "2006-04-01 01:00:00.000 +0200",
            "2006-04-02 00:00:00.000 +0200",
        ],
        "Temperature (C)": [10, 12, 13],
        "Humidity": [0.8, 0.7, 0.75],
        "Wind Speed (km/h)": [5, 6, 4],
        "Visibility (km)": [10, 10, 10],
        "Pressure (millibars)": [1012, 1013, 1011],
        "Precip Type": ["rain", "rain", "snow"],
    }
        
    df = pd.DataFrame(data)

    #run transorm
    daily_path, monthly_path = transform_weather_data(df)

    # basic checks - function returns expected output types

    assert isinstance(daily_path, str)
    assert isinstance(monthly_path, str)

    # the function creates the output files
    assert os.path.exists(daily_path)
    assert os.path.exists(monthly_path)

    # load CSVs to verify content
    daily_df = pd.read_csv(daily_path)
    monthly_df = pd.read_csv(monthly_path)

    #key functionality checks- real data existence
    assert len(daily_df) > 0
    assert len(monthly_df) > 0