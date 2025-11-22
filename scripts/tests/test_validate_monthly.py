import pandas as pd
import pytest
from scripts.validate import validate_monthly


def test_monthly_success(tmp_path):
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

    assert validate_monthly(fp) is True


def test_monthly_missing_column(tmp_path):
    fp = tmp_path / "monthly_weather.csv"
    df = pd.DataFrame({
        "YearMonth": ["2006-01"],
        # missing avg_temp
        "avg_humidity": [0.5],
        "avg_wind_speed": [3],
        "avg_visibility": [10],
        "avg_pressure": [1000],
        "Mode Precip Type": ["rain"],
    })
    df.to_csv(fp, index=False)

    with pytest.raises(ValueError):
        validate_monthly(fp)


def test_monthly_invalid_range(tmp_path):
    fp = tmp_path / "monthly_weather.csv"
    df = pd.DataFrame({
        "YearMonth": ["2006-01"],
        "avg_temp": [200],  # unrealistic
        "avg_humidity": [0.5],
        "avg_wind_speed": [3],
        "avg_visibility": [10],
        "avg_pressure": [1000],
        "Mode Precip Type": ["rain"],
    })
    df.to_csv(fp, index=False)

    with pytest.raises(ValueError):
        validate_monthly(fp)
