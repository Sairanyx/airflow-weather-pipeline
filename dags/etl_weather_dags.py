# The DAG automates the ETL process for the Weather History dataset.

# Steps:
# 1. Extracts, downloads and unzips the dataset from Kaggle using the Kaggle API.
# 3. Validates, checks that the daily and monthly files exist and meet requirements.



# Importing Libraries



#---------------------------------------------------------
# TEST LOCALLY

import sys
sys.path.append("/home/zoi/airflow-weather-pipeline")


#---------------------------------------------------------
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.validate import validate_daily, validate_monthly
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import sys, pathlib

# Making sure importing from root is possible by all here

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

# Connecting the extract script to this script

from scripts.extract import download_weather_dataset

# Setting Default arguements
default_args ={
    "owner":"zoi, eduard, julia",
    "start_date": datetime(2024,12,1),
    "retries": 2, #kaggle API may fail
    "retry_delay": timedelta(minutes=2)
}


# Setting the DAG

dag = DAG(
    dag_id="weather_history_etl",
    default_args=default_args,
    schedule_interval=None,          # Manual run 
    catchup=False,                   # Do NOT backfill old runs
    description="ETL pipeline for Weather History dataset: Extract -> Transform -> Validate -> Load"
)

# Defining the Extracting task

def extract_task(ti):

    # Extracting

    final_path = download_weather_dataset()

    # Passing the file path to the next task through XCom

    ti.xcom_push(key="extracted_path", value=final_path)
    print(f"Successfully pushed downstream the extracted path at: {final_path}")

# ---------------------- Transform task ----------------------

def transform_task(ti):
    """
    Pulls the extracted csv path from Xcom
    Loads raw dataset into a DataFrame
    runs the full transformation piplein (cleanig -featuring engineering)
    saves daily and monthly CSVs into /reports
    Pushes theif paths to Xcom for Validate + Load
    """

    import pandas as pd
    from scripts.transform import transform_weather_data

    # 1. pull the dataset from Extract(Xcom)
    extracted_path = ti.xcom_pull(
        key="extracted_path",
        task_ids = "Extract"
    )


    # 2. safety check the path must exist
    if not extracted_path:
        raise ValueError("[Transform] ERROR: No extracted_path found in XCom.")
    
    print(f"[Transform] Received extracted dataset at: {extracted_path}")


    # 3. Load raw CSV
    df = pd.read_csv(extracted_path)


    # 4. run full Transform pipeline
    daily_path, monthly_path = transform_weather_data(df)

    # 5 push the output for Validate + Load
    ti.xcom_push(key= "daily_weather", value=daily_path)
    ti.xcom_push(key= "monthly_weather", value= monthly_path)


    print(f"[Transform] Daily weather CSV saved at:   {daily_path}")
    print(f"[Transform] Monthly weather CSV saved at: {monthly_path}")
    print("[Transform] Transform step completed successfully!")



# Defining the Validation task (run in parallel)

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







# Defining the Load task







# Task objects

extract = PythonOperator(
    task_id= "Extract",
    python_callable=extract_task,
    dag=dag
)

transform = PythonOperator(
    task_id="Transform",
    python_callable=transform_task,
    dag=dag
)

validate_daily_op = PythonOperator(
    task_id="validate_daily",
    python_callable=validate_daily_task,
    dag=dag,
)

validate_monthly_op = PythonOperator(
    task_id="validate_monthly",
    python_callable=validate_monthly_task,
    dag=dag,
)
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# TEMPORARY placeholders until Edward & Iulia finish
dummy_outliers = EmptyOperator(
    task_id="detect_outliers",
    dag=dag
)

dummy_load = EmptyOperator(
    task_id="Load",
    dag=dag
)


#>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>









"""
Eduard 's part  just:  
1. create detect_outliers(csv_path)

2. create detect_outliers_op = PythonOperator(...)

3. make sure it pulls the XCom path “daily_weather” or “monthly_weather” 



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
# ----------------------------------------
# Join validation (waits until all succeed)
# ----------------------------------------

from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

join_validation = EmptyOperator(
    task_id="join_validation",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)








# Stream

extract >> transform >> [validate_daily_op, validate_monthly_op, dummy_outliers] >> join_validation >> dummy_load

