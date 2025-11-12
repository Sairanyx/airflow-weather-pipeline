# - Download a Kaggle dataset (ZIP)
# - Unzip it to a temporary folder
# - Picking a CSV
# - Move it to data/raw
# - Return the final path

# Libraries for extract.py script

from pathlib import Path
import os, shutil, zipfile
from kaggle.api.kaggle_api_extended import KaggleApi # Kaggle API

# Dynamic Absolute path (no need to change)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# Basic paths

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
DOWNLOADS_DIR = DATA_DIR / "downloads"
TEMPORARY_DIR = DOWNLOADS_DIR / "temporary"

# Defining Dataset and file name

DATASET_REF = "muthuj7/weather-dataset"
CSV_NAME = "weatherHistory.csv"

# Adding some checking in case the directories don't exist or get deleted by accident

def check_dirs():
    for i in [DOWNLOADS_DIR, RAW_DIR]:
        i.mkdir(parents=True, exist_ok=True)
    if TEMPORARY_DIR.exists():
        shutil.rmtree(TEMPORARY_DIR, ignore_errors=True)
    TEMPORARY_DIR.mkdir(parents=True, exist_ok=True)

# Cleaning old zip files first if there are any

def clean_old_zips():
    old_zips = list(DOWNLOADS_DIR.glob("*.zip"))
    if old_zips:
        print("Cleaning old ZIP files ...")

        for old_zip in old_zips:
            try:
                old_zip.unlink()
                print(f"Succesfully removed: {old_zip.name}")
            except Exception as e:
                print(f"Error. Could not remove {old_zip.name}: {e}")
    else:
        print("No old ZIP files were found")


# Downloading the file

def download_unzip():
    api = KaggleApi()
    api.authenticate()

    print(f"Downloading '{DATASET_REF}' to {DOWNLOADS_DIR} ...")
    api.dataset_download_files(DATASET_REF, path=str(DOWNLOADS_DIR), force=True, quiet=False)

    # Finding and unzipping the file

    zips = list(DOWNLOADS_DIR.glob("*.zip"))
    if not zips:
        raise FileNotFoundError("Error. There is no ZIP file found after the download!")
    zip_path = zips[0]

    print(f"Unzipping {zip_path} ...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(TEMPORARY_DIR)

    # Removing the ZIP after the extraction ends

    zip_path.unlink(missing_ok=True)
    print(f" Successfully unzipped and cleaned up {zip_path.name}")

# Moving the csv to raw folder

def move_csv():
    src = TEMPORARY_DIR / CSV_NAME
    if not src.exists():
        raise FileNotFoundError(f"Error. {CSV_NAME} not found in {TEMPORARY_DIR_DIR}")

    destination = RAW_DIR / CSV_NAME
    if destination.exists():
        destination.unlink()
    
    shutil.move(str(src), str(destination))
    print(f"Moved CSV to {destination}")
    return destination

# Putting it all together

def download_weather_dataset():

    check_dirs()
    clean_old_zips()
    download_unzip()
    final_csv = move_csv()
    return str(final_csv)

def main():
    final_csv = download_weather_dataset()
    print(f"Successful extraction and file is ready at: \n{final_csv}")

if __name__ == "__main__":
    main()
