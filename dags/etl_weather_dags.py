# The DAG automates the ETL process for the Weather History dataset.

# Steps:
# 1. Extracts, downloads and unzips the dataset from Kaggle using the Kaggle API.
# 3. Validates, checks that the daily and monthly files exist and meet requirements.



# Importing Libraries

import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, pathlib



transform = None
load_data = None
create_tables = None
d_path = None
m_path = None

default_args = {
    'owner': 'Group 7',
    'start_date': datetime(2025, 11, 9),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False
}

dag = DAG(
    "etl_weather_dags",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Making sure importing from root is possible by all here

sys.pth.append(str(pathlib.Path(__file__).resolve().parents[1]))

# Connecting the extract script to this script

from scripts.extract import download_weather_dataset

# Setting Default arguements


# Setting the DAG


# Defining the Extracting task

def extract_task(ti):

    # Extracting

    final_path = download_weather_dataset()

    # Passing the file path to the next task through XCom

    ti.xcom_push(key="extracted_path", task_ids="extract_weather")
    print(f"Successfully pushed downstream the extracted path at: {final_path}")

# Defining the Transforming task


# Defining the Validation task

def validate_task(ti):
    daily_path = ti.xcom_pull(key="daily_weather", task_ids="Transform")
    monthly_path = ti.xcom_pull(key="monthly_weather", task_ids="Transform")

    validate_weather(daily_path)
    validate_weather(monthly_path)
    print("Successfull. Daily and monthly weather files passed validation.")


# Defining the Load task



# Defining the Tasks with Operator



# Stream






# Tasks definition

Extract = PythonOperator(
    task_id="Extract",
    python_callable=extract_task,
    dag=dag,
)

Transform = PythonOperator(
    task_id="Transform",
    python_callable=transform,
    dag=dag,
)

Validation = PythonOperator(
    task_id="Validation",
    python_callable=validate_task,
    trigger_rule="all_success",
    dag=dag,
)

Create_table = PythonOperator(
    task_id="Create_tables",
    python_callable=create_tables,
    dag=dag,
)

Load_daily_data = PythonOperator(
    task_id="Load_daily_data",
    python_callable=load_data,
    op_kwargs={
        "table_name" : "daily_weather",
        "xcom_key" : d_path,
        "source_task_id" : "Transform"
    },
    dag=dag,
)

Load_monthly_data = PythonOperator(
    task_id="Load_monthly_data",
    python_callable=load_data,
    op_kwargs={
        "table_name" : "monthly_weather",
        "xcom_key" : m_path,
        "source_task_id" : "Transform"
    },
    dag=dag,
)


Extract >> Transform >> Validation >> Create_table >> Load_daily_data >> Load_monthly_data 
