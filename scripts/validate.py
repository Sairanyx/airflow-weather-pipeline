# ============================================================
# VALIDATION MODULE for Weather ETL Pipeline
# ------------------------------------------------------------
# Performs:
# - Column existence checks
# - Missing value checks
# - Range consistency checks on engineered features
# - Separate validation for daily_weather.csv & monthly_weather.csv
# - Automatic detection of dataset type (daily/monthly)
# ============================================================

import pandas as pd


# ------------------------------------------------------------
# DAILY VALIDATION
# ------------------------------------------------------------
def validate_daily(csv_path):

    print(f"\n[VALIDATION - DAILY] Validating: {csv_path}")
    df = pd.read_csv(csv_path)

    # 1. Required columns
    required_cols = [
        "Formatted Date",
        "avg_temp",
        "avg_humidity",
        "avg_wind_speed"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"[DAILY ERROR] Missing required column: {col}")

    print("[DAILY] All required columns exist.")

    # 2. Missing values
    for col in required_cols:
        if df[col].isnull().any():
            raise ValueError(f"[DAILY ERROR] Missing values in: {col}")

    print("[DAILY] No missing values.")

    # 3. Range checks
    if not df["avg_temp"].between(-50, 50).all():
        raise ValueError("[DAILY ERROR] avg_temp contains unrealistic values (-50 to 50°C).")

    if not df["avg_humidity"].between(0, 1).all():
        raise ValueError("[DAILY ERROR] avg_humidity outside valid range (0–1).")

    if not (df["avg_wind_speed"] >= 0).all():
        raise ValueError("[DAILY ERROR] avg_wind_speed contains negative values.")

    print("[DAILY] Range checks passed successfully.")
    print("[DAILY] DAILY DATASET VALID ✔")
    return True


# ------------------------------------------------------------
# MONTHLY VALIDATION
# ------------------------------------------------------------
def validate_monthly(csv_path):

    print(f"\n[VALIDATION - MONTHLY] Validating: {csv_path}")
    df = pd.read_csv(csv_path)

    # 1. Required columns in monthly_weather.csv
    required_cols = [
        "YearMonth",
        "avg_temp",
        "avg_humidity",
        "avg_wind_speed",
        "avg_visibility",
        "avg_pressure",
        "Mode Precip Type"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"[MONTHLY ERROR] Missing required column: {col}")

    print("[MONTHLY] All required columns exist.")

    # 2. Missing values
    for col in required_cols:
        if df[col].isnull().any():
            if col == "Mode Precip Type":   # Allowed
                continue
            raise ValueError(f"[MONTHLY ERROR] Missing values in: {col}")

    print("[MONTHLY] No missing values (except allowed NaN for Mode Precip Type).")

    # 3. Range checks
    if not df["avg_temp"].between(-50, 50).all():
        raise ValueError("[MONTHLY ERROR] avg_temp contains unrealistic values (-50 to 50°C).")

    if not df["avg_humidity"].between(0, 1).all():
        raise ValueError("[MONTHLY ERROR] avg_humidity outside valid range (0–1).")

    if not (df["avg_wind_speed"] >= 0).all():
        raise ValueError("[MONTHLY ERROR] avg_wind_speed contains negative values.")

    if not (df["avg_visibility"] >= 0).all():
        raise ValueError("[MONTHLY ERROR] avg_visibility contains negative values.")

    if not df["avg_pressure"].between(870, 1100).all():
        raise ValueError("[MONTHLY ERROR] avg_pressure outside expected range (870–1100 mbar).")

    print("[MONTHLY] Range checks passed successfully.")
    print("[MONTHLY] MONTHLY DATASET VALID ✔")
    return True

# ------------------------------------------------------------
# AUTO-DETECT VALIDATION (required for test_validate_autodetect.py)
# ------------------------------------------------------------

def validate_weather(csv_path):
    """
    Automatically detect whether the dataset is daily or monthly
    and run the corresponding validation function.
    """

    df = pd.read_csv(csv_path)

    # Detect DAILY
    if {"Formatted Date", "avg_temp", "avg_humidity", "avg_wind_speed"}.issubset(df.columns):
        return validate_daily(csv_path)

    # Detect MONTHLY
    if {"YearMonth", "avg_temp", "avg_humidity", "avg_wind_speed",
        "avg_visibility", "avg_pressure", "Mode Precip Type"}.issubset(df.columns):
        return validate_monthly(csv_path)

    # Unknown dataset type
    raise ValueError("Cannot autodetect dataset type (daily/monthly).")



