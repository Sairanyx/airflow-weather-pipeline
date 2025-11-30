# The DAG automates the ETL process for the Weather History dataset.

# Steps:
# 1. Extracts, downloads and unzips the dataset from Kaggle using the Kaggle API.
# 2. Transforms 
# 3. Validates, checks that the daily and monthly files exist and meet requirements.
# 4. Loads


# Importing Libraries
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)

if project_root not in sys.path:
    sys.path.append(project_root)

from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator
from scripts.validate import validate_daily, validate_monthly
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


# Setting Default arguements

default_args = {
    'owner': 'Group 7',
    'start_date': datetime(2025, 11, 9),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False
}

# Setting the DAG
dag = DAG(
    "weather_history_etl",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description="ETL pipeline for Weather History dataset"
)


# Defining the Extracting task

def extract_task(ti):

    # Connecting the extract script to this script
    from scripts.extract import download_weather_dataset

    # Extracting

    final_path = download_weather_dataset()

    # Passing the file path to the next task through XCom

    ti.xcom_push(
        key="extracted_path",
        value=final_path
        )

    print(f"Successfully pushed downstream the extracted path at: {final_path}")

# Defining the Transform task

    """
    
    Pulls the extracted csv path from Xcom
    Loads raw dataset into a DataFrame
    runs the full transformation piplein (cleanig -featuring engineering)
    saves daily and monthly CSVs into /reports
    Pushes theif paths to Xcom for Validate + Load

    """

def transform_task(ti):

    from scripts.transform import transform_weather_data

    # 1. Pulling the dataset from Extract(Xcom)

    extracted_path = ti.xcom_pull(
        key="extracted_path",
        task_ids ="Extract"
    )


    # 2. Safety checking the path must exist

    if not extracted_path:
        raise ValueError("[Transform] ERROR: No extracted_path found in XCom.")
    
    print(f"[Transform] Received extracted dataset at: {extracted_path}")


    # 3. Loading the raw CSV

    df = pd.read_csv(extracted_path)


    # 4. Running the full Transform pipeline

    daily_path, monthly_path = transform_weather_data(df)

    # 5 Pushing the output for Validate + Load

    ti.xcom_push(key= "daily_weather", value=daily_path)
    ti.xcom_push(key= "monthly_weather", value= monthly_path)


    print(f"[Transform] Daily weather CSV saved at:   {daily_path}")
    print(f"[Transform] Monthly weather CSV saved at: {monthly_path}")
    print("[Transform] Transform step completed successfully!")



# Defining the Validation task (they run in parallel)

def validate_daily_task(ti):
    daily_path = ti.xcom_pull(key="daily_weather", task_ids="Transform")
    print(f"[DAG] Validating DAILY CSV at: {daily_path}")
    validate_daily(daily_path)
    print("[DAG] DAILY validation completed ✔")


def validate_monthly_task(ti):
    monthly_path = ti.xcom_pull(key="monthly_weather", task_ids="Transform")
    print(f"[DAG] Validating MONTHLY CSV at: {monthly_path}")
    validate_monthly(monthly_path)
    print("[DAG] MONTHLY validation completed ✔")


# Setting task for outlier detection, daily or monthly dataset
# Tries daily then monthly

def detect_outliers_task(ti):

    daily_path = ti.xcom_pull(key="daily_weather", task_ids="Transform")
    monthly_path = ti.xcom_pull(key="monthly_weather", task_ids="Transform")
    csv_path = daily_path or monthly_path

    if not csv_path:
        raise ValueError("No CSV available for outlier detection.")
   
    print(f"Outlier Detection: Using file: {csv_path}")
    df = pd.read_csv(csv_path)

    # Choosing the numeric columns only (almost like another check)

    chosen_cols = []

    for col in df.columns:
        name = col.lower()

        if any(key in name for key in ["temp", "humid", "wind", "press"]):

            if pd.api.types.is_numeric_dtype(df[col]):
                chosen_cols.append(col)

    if not chosen_cols:
        print("Outlier Detection: No suitable numeric columns found. Skipping.")
        return True

    print(f"Outlier Detection: Checking columns: {chosen_cols}")

    # Calculating the IQR-based outliers for every numeric column

    outlier_mask = pd.Series(False, index=df.index)

    for col in chosen_cols:
        series = df[col].dropna()
        if series.empty:
            continue

        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1

        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        col_mask = (df[col] < lower) | (df[col] > upper)
        num_out = col_mask.sum()
        print(f"Outlier Detection: {col}: {num_out} outliers (bounds {lower:.2f} .. {upper:.2f})")

        outlier_mask = outlier_mask | col_mask

    total_outliers = outlier_mask.sum()
    print(f"Outlier Detection: Total rows with at least one outlier: {total_outliers}")

    # Saving to CSV

    if total_outliers > 0:
        outliers_df = df[outlier_mask].copy()
        outlier_path = csv_path.replace(".csv", "_outliers.csv")
        outliers_df.to_csv(outlier_path, index=False)
        print(f"Outlier Detection: Outlier rows saved to: {outlier_path}")

    return True

"""

Transform
   ↓
 ┌───────────────┬──────────────────┬────────────────┐
 │               │                  │
validate_daily   validate_monthly   detect_outliers  (parallel)
 │               │                  │
 └───────────────┴──────────────────┴───────────────┘
            join_validation
                    ↓
                  Load

"""



# Stream

# Defining the Load task
def create_tables_wrapper():
    from scripts.load import create_tables
    create_tables()

def load_data_wrapper(**kwargs):
    from scripts.load import load_data
    load_data(**kwargs)

# Tasks definition

extract = PythonOperator(
    task_id="Extract",
    python_callable=extract_task,
    dag=dag,
)

transform = PythonOperator(
    task_id="Transform",
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)


validate_daily_op = PythonOperator(
    task_id="Validate_daily",
    python_callable=validate_daily_task,
    dag=dag,
)

validate_monthly_op = PythonOperator(
    task_id="Validate_monthly",
    python_callable=validate_monthly_task,
    dag=dag,
)

detect_outliers_op = PythonOperator(
    task_id="Detect_outliers",
    python_callable=detect_outliers_task,
    dag=dag
)

join_validation = EmptyOperator(
    task_id="Join_validation",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

create_table = PythonOperator(
    task_id="Create_tables",
    python_callable=create_tables_wrapper,
    dag=dag,
)

load_daily_data = PythonOperator(
    task_id="Load_daily_data",
    python_callable=load_data_wrapper,
    op_kwargs={
        "table_name" : "daily_weather",
        "xcom_key" : "daily_weather",
        "source_task_id" : "Transform",
        "rename_mapping_type": "Daily"
    },
    provide_context=True,
    dag=dag,
)

load_monthly_data = PythonOperator(
    task_id="Load_monthly_data",
    python_callable=load_data_wrapper,
    op_kwargs={
        "table_name" : "monthly_weather",
        "xcom_key" : "monthly_weather",
        "source_task_id" : "Transform",
        "rename_mapping_type" : "Monthly"
    },
    provide_context=True,
    dag=dag,
)


extract >> transform >> [validate_daily_op, validate_monthly_op, detect_outliers_op] >> join_validation >> create_table >> [load_daily_data, load_monthly_data]
