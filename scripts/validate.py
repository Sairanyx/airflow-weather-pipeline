# Validation for weatherHistory.csv
# 
# - Missing values check and if requires columns exist
# - Range check
# - Outlier Detection and log file
# - Trigger rules - returns Successfull on success and Error on failure

from pathlib import Path
import pandas as pd

