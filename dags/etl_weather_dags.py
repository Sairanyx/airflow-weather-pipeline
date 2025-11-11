# The DAG automates the ETL process for the Weather History dataset.

# Steps:
# 1. Extracts, Downloads and Unzips the dataset from Kaggle using the Kaggle API.



# Importing Libraries

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, pathlib

# Making sure importing from root is possible by all here

sys.pth.append(str(pathlib.Path(__file__).resolve().parents[1]))

# Connecting the extract script to this one

from scripts.extract import download_weather_dataset

# Setting Default arguements

DEFAULT_ARGS = {
    "owner": "-",
    "retries": 0,
}

# Setting the DAG

dag = DAG(
    dag_id="weather_etl",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    cacthup=False,
    tags=["weather", "etl"],
)

# Defining the Extracting task

def extract_task(ti):

    # Extracting

    final_path = download_weather_dataset()

    # Passing the file path to the next task through XCom

    ti.xcom_push(key="extracted_path", task_ids="extract_weather")
    print(f"Successfully pushed downstream the extracted path at: {final_path}")

# Defining the Transforming task

def transform_task(ti):

    # Pulling the file path from XCom

    extracted_path = ti.xcom_pull(key="extracted_path", task_ids="extract_weather")
    processed_path, daily_path, monthly_path = transform_weather(extracted_path)

    # Pushing the two file paths ti XCom

    ti.xcom_push(key="daily_weather", value=daily_path)
    ti.xcom_push(key="monthly_weather", value=monthly_path)
    print(f" Successfully pulled the path and got the file from extract: {extracted_path}")

# Defining the Validation task

def validate_task(ti):
    daily_path = ti.xcom_pull(key="daily_weather", task_ids="Transform")
    monthly_path = ti.xcom_pull(key="monthly_weather", task_ids="Transform")

    validate_weather(daily_path)
    validate_weather(monthly_path)
    print("Successfull. Daily and monthly weather files passed validation.")


# Defining the Load task



# Defining the Tasks with Operator




extract >> transform >> validate >> load