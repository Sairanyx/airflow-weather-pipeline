# Airflow ETL Pipeline for Historical Weather Data

## Team
- Iuliia Radionova  
- Zoi Theofilakou  
- Eduard Rednic  

---

## Project Overview
This Introduction to Data Engineering group project focuses on building an automated ETL pipeline using **Apache Airflow** to process historical weather data collected from Kaggle.

The pipeline:
- extracts the source dataset,
- transforms and cleans the data,
- validates data quality,
- and loads the processed results into a structured SQLite database.

It also demonstrates core Airflow features including **XCom**, **task dependencies**, and **trigger rules** to control execution flow.

---

## Dataset
**Source:** Kaggle – “Weather History”  
**File:** `weatherHistory.csv.zip`

---

## ETL Steps

### 1️⃣ Extract
- Authenticates to Kaggle API using config credentials  
- Downloads the dataset and unzips the source file  
- Extracted CSV path is pushed forward using **XCom**  

### 2️⃣ Transform
- Cleans the dataset (removes duplicates, invalid rows, and missing values)
- Normalizes and formats the date column
- Aggregates data into **daily** and **monthly** datasets
- Performs basic feature engineering (e.g. precipitation calculations and relative wind strength)
- Stores results under `data/processed`
- Returns processed file paths via XCom

### 3️⃣ Validate
- Checks data quality (missing values, required columns, and statistical ranges)
- Detects potential outliers
- Uses Airflow trigger rules to continue only when validation succeeds

### 4️⃣ Load
- Creates an SQLite database (if not existing)
- Loads both daily and monthly processed datasets into dedicated database tables

### 5️⃣ Orchestration (Airflow DAG)
- Defines ETL tasks, dependencies, and execution order
- Uses XCom for internal file path communication
- DAG execution is confirmed in the Airflow UI

---

## Team Roles and Contributions

### Eduard Rednic
- Set up the GitHub repository and base folder structure
- Installed and configured dependencies (Kaggle API, Airflow, SQLite3, Pandas)
- Implemented the **Extract** step (Kaggle API, ZIP handling, XCom messaging)
- Contributed to the validation logic
- Authored this README.md

### Zoi Theofilakou
- Implemented core transformation logic (cleaning, aggregation, feature engineering)
- Calculated daily and monthly averages and precipitation metrics
- Assisted in early validation logic

### Iuliia Radionova
- Implemented database creation and loading of processed data
- Built the Airflow DAG with task dependency structure and trigger rules
- Managed XCom pass-through between steps and verified DAG execution

---

## Submission Files
- `etl_weather_dags.py` (Airflow DAG and ETL logic)
- SQLite database output (`weather_data.db`)
- Airflow UI screenshot showing a successful DAG run
- Individual short reports (process, contributions, issues, solutions)

---

## Tools
**Python • Pandas • Apache Airflow • SQLite • Kaggle API • Git • Ubuntu/Linux • Visual Studio Code**

---

## Folder Structure
```
airflow-weather-pipeline
├── dags
│   └── etl_weather_dags.py
│
├── data
│   ├── downloads
│   ├── processed
│   └── raw
│
├── database
│   └── weather_data.db
│
├── diagram
│   └── etl_diagram.py
│
├── reports_pdf
│   ├── The_Final_Report_Template.pdf
│
├── screenshots
│   ├── daily_weather_screenshot.jpg
│   ├── monthly_weather_screenshot.jpg
│   ├── successfull_run_screenshot.jpg
│ 
├── scripts
│   ├── __init__.py
│   │
│   ├── cleaning
│   │   ├── __init__.py
│   │   ├── step1_date_cleaning.py
│   │   ├── step2_missing_erroneous.py
│   │   ├── step3_pressure_filter.py
│   │   └── step4_duplicates.py
│   │
│   ├── exploratory
│   │   └── exploratory_analysis.py
│   │
│   ├── feature_engineering
│   │   ├── __init__.py
│   │   ├── part_1_daily_features.py
│   │   ├── part_2_precip_mode.py
│   │   ├── part_3_wind_strength.py
│   │   └── part_4_monthly_features.py
│   │
│   ├── tests
│   │   ├── test_feature_engineering.py
│   │   ├── test_transform.py
│   │   ├── test_validate_autodetect.py
│   │   ├── test_validate_daily.py
│   │   └── test_validate_monthly.py
│   │
│   ├── extract.py
│   ├── load.py
│   ├── transform.py
│   └── validate.py
│
├── .airflowignore
├── .gitignore
├── README.md
└── requirements.txt
```