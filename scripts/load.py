
import sqlite3
import pandas as pd

DB_PATH = None
key_tr_d = None
key_tr_m = None
dbase_name = None
source_task_id = None
xcom_key = None
table_name = None

DAILY_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS "daily_weather"(
    "id" TEXT, "formatted_date" TEXT, "precip type" TEXT, 
    "temperature_c" REAL, "apparent_temperature_c" REAL, 
    "humidity" REAL, "wind_speed_kmh" REAL, "visibility_km" REAL, 
    "pressure_millibars" REAL, "wind_strength" TEXT, 
    "avg_temperature_c" REAL, "avg_humidity" REAL, "avg_wind_speed_kmh" REAL
);
"""

MONTHLY_WEATHER_TABLE = """
CREATE TABLE IF NOT EXISTS "monthly_weather"(
"id" TEXT, "month" TEXT, "avg_temperature_c" REAL, "avg_apparent_temperature_c" REAL, 
"avg_humidity" REAL, "avg_visibility_km" REAL, "avg_pressure_millibars" REAL, 
"mode_precip_type" TEXT
);
"""

def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(DAILY_WEATHER_TABLE)
    cursor.execute(MONTHLY_WEATHER_TABLE)
    conn.commit()
    conn.close()



def load_data(table_name, xcom_key, source_task_id, **kwargs):
    fp = kwargs['ti'].xcom_pull(task_ids=source_task_id, key= xcom_key)
    df = pd.read_csv(fp)

    conn = sqlite3.connect(DB_PATH)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
