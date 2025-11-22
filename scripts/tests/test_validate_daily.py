import pandas as pd
import pytest
from scripts.validate import validate_daily


def test_validate_daily_success(tmp_path):
    # create temporary CSV
    fp = tmp_path / "daily_weather.csv"
    df = pd.DataFrame({
        "Formatted Date": ["2006-01-01", "2006-01-02"],
        "avg_temp": [10, 15],
        "avg_humidity": [0.5, 0.7],
        "avg_wind_speed": [3, 6],
    })
    df.to_csv(fp, index=False)

    # should NOT raise error
    assert validate_daily(fp) is True


def test_validate_daily_missing_column(tmp_path):
    fp = tmp_path / "daily_weather.csv"
    df = pd.DataFrame({
        "avg_temp": [10, 20],  # Missing Formatted Date !!!
        "avg_humidity": [0.5, 0.7],
        "avg_wind_speed": [5, 6],
    })
    df.to_csv(fp, index=False)

    with pytest.raises(ValueError):
        validate_daily(fp)


def test_validate_daily_out_of_range(tmp_path):
    fp = tmp_path / "daily_weather.csv"
    df = pd.DataFrame({
        "Formatted Date": ["2006-01-01"],
        "avg_temp": [100],  # Out of range!
        "avg_humidity": [0.5],
        "avg_wind_speed": [5],
    })
    df.to_csv(fp, index=False)

    with pytest.raises(ValueError):
        validate_daily(fp)
