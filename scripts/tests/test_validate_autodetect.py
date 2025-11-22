import pandas as pd
import pytest
from scripts.validate import validate_weather


def test_autodetect_daily(tmp_path):
    fp = tmp_path / "daily_weather.csv"
    df = pd.DataFrame({
        "Formatted Date": ["2006-01-01"],
        "avg_temp": [10],
        "avg_humidity": [0.5],
        "avg_wind_speed": [2],
    })
    df.to_csv(fp, index=False)

    assert validate_weather(fp) is True


def test_autodetect_monthly(tmp_path):
    fp = tmp_path / "monthly_weather.csv"
    df = pd.DataFrame({
        "YearMonth": ["2006-01"],
        "avg_temp": [10],
        "avg_humidity": [0.5],
        "avg_wind_speed": [3],
        "avg_visibility": [10],
        "avg_pressure": [1000],
        "Mode Precip Type": ["rain"],
    })
    df.to_csv(fp, index=False)

    assert validate_weather(fp) is True


def test_autodetect_unknown(tmp_path):
    fp = tmp_path / "weird.csv"
    df = pd.DataFrame({
        "something_else": [1],
        "unknown": [2]
    })
    df.to_csv(fp, index=False)

    with pytest.raises(ValueError):
        validate_weather(fp)
