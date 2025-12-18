import sqlite3
import pathlib 
import sys

from datetime import datetime, timedelta

# --- Define Project Root and Add to Path for Imports ---
PROJECT_ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent.parent  
sys.path.append(str(PROJECT_ROOT_DIR))

from src.logger import init_logger, logger
 
ETL_DIR: pathlib.Path = PROJECT_ROOT_DIR / "src" / "data_pipeline" 
PACKAGE_DIR: pathlib.Path = ETL_DIR.parent
DATA_BACKUPS_DIR = pathlib.Path("C:/Repos/data_backups") # path for external location
DB_PATH = DATA_BACKUPS_DIR / "maven_factory.db"

init_logger()  # Initialize the logger before any logging

logger.info(f"PROJECT_ROOT_DIR:    {PROJECT_ROOT_DIR}")
logger.info(f"ETL_DIR:             {ETL_DIR}")
logger.info(f"PACKAGE_DIR:         {PACKAGE_DIR}")
logger.info(f"DATA_BACKUPS_DIR:    {DATA_BACKUPS_DIR}")
logger.info(f"DB_PATH:             {DB_PATH}")

# 1. Initialize conn and cursor to None
conn = None
cursor = None

def populate_dim_date():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Define the range (covering your 2012-2015 data)
    start_date = datetime(2012, 1, 1)
    end_date = datetime(2015, 12, 31)
    
    current_date = start_date
    date_rows = []

    print(f"Generating dates from {start_date.date()} to {end_date.date()}...")

    while current_date <= end_date:
        date_key = current_date.strftime('%Y-%m-%d')
        full_date = current_date.strftime('%B %d, %Y')
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        day_of_week = current_date.strftime('%A') # e.g., 'Monday'
        is_weekend = 1 if current_date.weekday() >= 5 else 0 # 5=Sat, 6=Sun
        
        date_rows.append((date_key, full_date, year, quarter, month, day_of_week, is_weekend))
        current_date += timedelta(days=1)

    # Insert data into the table
    cursor.executemany("""
        INSERT OR REPLACE INTO dim_date 
        (date_key, full_date, year, quarter, month, day_of_week, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, date_rows)

    conn.commit()
    conn.close()
    print("Successfully populated dim_date table!")

if __name__ == "__main__":
    populate_dim_date()