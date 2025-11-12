# Airflow ETL Pipeline for Historical Weather Data

**Team:**  

- Iuliia Radionova  
- Zoi Theofilakou  
- Eduard Rednic  

---

## Project Overview

This Introduction to Data Engineering group work project focuses on an automated **ETL pipeline** built with **Apache Airflow** for processing historical weather data from Kaggle.  
The pipeline extracts, transforms, validates, and loads data into a structured database and demonstrates Airflow features such as **XCom** and **trigger rules**.

---

## Dataset

**Source:** [Kaggle - Weather History](https://www.kaggle.com/datasets)  
**File:** `weatherHistory.csv.zip`  

---

## ETL Steps

### 1️⃣ Extract  

- Downloads the dataset via Kaggle API  
- The ZIP file is being extracted with Python `zipfile`  
- The XCom is used to pass dataset path to next task  

### 2️⃣ Transform  

- Cleans and formats the date column  
- Removes the missing values and duplicates  
- Computes the daily & monthly columns 
- Feature engineering includes: precipitation and wind strength  
- Saves the transformed CSVs and passes via XCom  

### 3️⃣ Validate  

- Checks the missing values and ranges  
- Detects the outliers and applies trigger rules  
- Continues only when the result is success

### 4️⃣ Load  

- Creates an SQLite database
- Loads the daily & monthly data into tables  

### 5️⃣ Orchestration (Airflow DAG)  

- Defines all the ETL tasks and dependencies  
- Uses XCom for the task communication  
- Verifies the full DAG execution in Airflow UI  

---

## Team Roles And Contributions

**Eduard Rednic**  

- Set up the GitHub repository and project structure  
- Installed & configured required libraries (Kaggle API, Pandas, SQLite3, Airflow)  
- Implemented the Extract step (Kaggle API, ZIP handling, XCom)  
- Contributed to late Validation
- Created and wrote this README.md

**Zoi Theofilakou** 

- Implemented the Transform step (cleaning, aggregation, feature engineering)  
- Added the daily and monthly averages and precipitation 
- Contributed to the early Validation

**Iuliia Radionova**  

- Built the SQLite database and handled data loading  
- Defined the Airflow DAG structure, dependencies, and trigger rules  
- Managed the XCom connections and verified the DAG execution  

---

## Submission files

- Python script (ETL and Airflow DAG)  
- Database screenshots (daily & monthly tables)  
- Airflow UI screenshot (successful DAG run)  
- Final individual short report (process, issues, solutions, roles)

---

## Tools

Python • Pandas • Airflow • SQLite • Kaggle API • Git • Ubuntu/Linux • Visual Studio Code

---

## Folder Structure

## 📂 Folder Structure

```text
airflow-weather-pipeline
├── dags
│   └── etl_weather_dags.py
│
├── scripts
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── validate.py
│   └── load.py
│
├── data
│   ├── raw
│   └── processed
│
├── database
│   └── weather_data.db
│
├── screenshots
│
├── reports
│   ├── Final_Report_DE_Eduard_Rednic.pdf
│   ├── Final_Report_DE_Zoi_Theofilakou.pdf
│   ├── Final_Report_DE_Iuliia_Radionova.pdf
│   └── DE_presentation.pptx
│
├── requirements.txt
├── README.md
└── .gitignore
