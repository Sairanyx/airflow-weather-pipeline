import pandas as pd

data_path = "/home/zoi/airflow/datasets/weatherHistory.csv"

df = pd.read_csv(data_path)

#general check
print("Before cleaning") 
print(df.head(), df.columns) #checking first rows and col names
print(df.info())


# Targeted check to date column
print("\nDate column info BEFORE conversion:")
print(df["Formatted Date"].info()) 
print(df["Formatted Date"].head())

print(df.isnull().sum()) # missing values

#identify erronous values range validation

#check range for humidity
print("\n Check for invalid humidity values:")
print(df[(df["Humidity"] < 0) | (df["Humidity"] > 1)])

# check wind speed values
print("\n Check for negative wind speed values:")
print(df[(df["Wind Speed (km/h)"] < 0)])

# check invalid pressure values 
print("\n Check for invalid pressure values")
print(df[(df["Pressure (millibars)"] < 870) | (df["Pressure (millibars)"] > 1100)])



# check duplicates 
print("\nChecking for duplicate rows:")
print(df.duplicated().sum())


print("\nEXPLORATORY COMPLETE")