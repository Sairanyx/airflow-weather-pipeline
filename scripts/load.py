import sqlite3
import pandas as pd

DB_PATH = "/home/iuliia/airflow/dags/airflow-weather-pipeline/database/weather.db"


DAILY_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS daily_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    formatted_date TEXT, 
    avg_temperature_c REAL, 
    avg_humidity REAL, 
    avg_wind_speed_kmh REAL
);
"""

MONTHLY_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS monthly_weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month TEXT, 
    avg_temperature_c REAL, 
    avg_wind_speed_kmh REAL, 
    avg_humidity REAL, 
    avg_visibility_km REAL, 
    avg_pressure_millibars REAL, 
    mode_precip_type TEXT
);
"""

daily_mapping =  {

    'Formatted Date': 'formatted_date',
    'avg_temp': 'avg_temperature_c',
    'avg_humidity': 'avg_humidity',
    'avg_wind_speed': 'avg_wind_speed_kmh',

}


monthly_mapping = {
        'YearMonth': 'month',
        'avg_temp': 'avg_temperature_c',
        'avg_humidity': 'avg_humidity',
        'avg_wind_speed': 'avg_wind_speed_kmh',
        'avg_visibility': 'avg_visibility_km',
        'avg_pressure': 'avg_pressure_millibars',
        'Mode Precip Type': 'mode_precip_type'
    }

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(DAILY_WEATHER_TABLE)
    cursor.execute(MONTHLY_WEATHER_TABLE)
    conn.commit()
    conn.close()



def load_data(table_name, xcom_key, source_task_id, rename_mapping_type, **kwargs):
    fp = kwargs['ti'].xcom_pull(task_ids=source_task_id, key= xcom_key)
    df = pd.read_csv(fp)

    if rename_mapping_type == "Daily":
        rename_mapping = daily_mapping
    else:
        rename_mapping = monthly_mapping
    df.rename(columns=rename_mapping, inplace=True)

    conn = sqlite3.connect(DB_PATH)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    print("Test")