import os
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))

if script_dir not in sys.path:
    sys.path.insert(0, script_dir)

def transform_weather_data(df_raw):
    from cleaning.step1_date_cleaning import clean_date_column
    from cleaning.step2_missing_erroneous import handle_missing_and_erroneous
    from cleaning.step3_pressure_filter import filter_invalid_pressure
    from cleaning.step4_duplicates import remove_duplicates

    from feature_engineering.part_1_daily_features import compute_daily_averages
    from feature_engineering.part_2_precip_mode import monthly_precip_mode
    from feature_engineering.part_3_wind_strength import wind_categorization
    from feature_engineering.part_4_monthly_features import monthly_averages

    # ---------- 1. DATA CLEANING ----------
    df_clean = clean_date_column(df_raw)
    df_clean = handle_missing_and_erroneous(df_clean)
    df_clean = filter_invalid_pressure(df_clean)
    df_clean = remove_duplicates(df_clean)

    # ---------- 2. FEATURE ENGINEERING ----------
    df_daily = compute_daily_averages(df_clean)
    precip_df = monthly_precip_mode(df_clean)
    df_wind = wind_categorization(df_clean)
    df_monthly = monthly_averages(df_clean)

    # Merge precip mode into monthly averages
    df_monthly = df_monthly.merge(
        precip_df,
        on="YearMonth",
        how="left"
    )

    # ---------- 3. SAVE CSV FILES ----------
    output_folder = "/home/zoi/airflow-weather-pipeline/reports"
    os.makedirs(output_folder, exist_ok=True)

    daily_path = f"{output_folder}/daily_weather.csv"
    monthly_path = f"{output_folder}/monthly_weather.csv"

    df_daily.to_csv(daily_path, index=False)
    df_monthly.to_csv(monthly_path, index=False)

    # return paths
    return daily_path, monthly_path

    


if __name__ == "__main__":
    print("Running transform manually...")
    import pandas as pd
    
    df = pd.read_csv("/home/zoi/airflow/datasets/weatherHistory.csv")  # OR your path
    daily, monthly = transform_weather_data(df)
    print("Daily saved at:", daily)
    print("Monthly saved at:", monthly)
    print("Transform completed successfully")