import pandas as pd

data_path = "/home/zoi/airflow/datasets/weatherHistory.csv"

df = pd.read_csv(data_path)

print("Before cleaning") 
print(df.head(), df.columns) #checking first rows and col names

# 2.1.1 convert formatted date to a proper format

#verify formatted date data type

print(df["Formatted Date"].info()) 

 
# Convert raw timestamp strings into timezone-aware datetime objects.
# - utc=True normalizes all timestamps to UTC for consistency
# - errors="coerce" safely handles malformed dates by converting them to NaT
df["Formatted Date"] = pd.to_datetime(
    df["Formatted Date"],
    utc= True, 
    errors = "coerce") 

print("\nAfter conversion:")
print(df["Formatted Date"].head())
print(df.info())

# 2.1.2 handling missing - erronous data
print(df.isnull().sum())

# Precip Type has 517 missing values but is non-critical, so we leave them as NaN.

#identify erronous values
# All critical numeric columns are float64 (verified with df.info()),
# so there are no type errors or garbage strings to handle.

#check range for humidity
print("\n Check for invalid humidity values:")
print(df[(df["Humidity"] < 0) | (df["Humidity"] > 1)])

# check wind speed values
print("\n Check for negative wind speed values:")
print(df[(df["Wind Speed (km/h)"] < 0)])

# check invalid pressure values 
print("\n Check for invalid pressure values")
print(df[(df["Pressure (millibars)"] < 870) | (df["Pressure (millibars)"] > 1100)])

#humidity and wind speed no erronous values
#pressure 1288 erronous rows 

# Filter out invalid pressure values (outside 870–1100 mbar)
df = df[(df["Pressure (millibars)"] >= 870) & 
        (df["Pressure (millibars)"] <= 1100)]


# 2.1.3 check duplicates and remove
print("\nChecking for duplicate rows:")
print(df.duplicated().sum())

df = df.drop_duplicates()
print("Duplicates after cleaning:", df.duplicated().sum())