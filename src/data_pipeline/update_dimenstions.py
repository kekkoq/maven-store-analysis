import sqlite3
import pathlib 
import sys

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

try:
    print("Attempting to connect to the database...")
    
    # Connect and create cursor inside the try block
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print("Starting dimension update: Adding 'channel_group' to dim_session_activity...")
    
    # 2. ADD COLUMN (Handles existing column without crashing)
    try:
        # SQL to add the new column to the dimension table
        cursor.execute("ALTER TABLE dim_session_activity ADD COLUMN channel_group TEXT;")
        print("-> Column 'channel_group' added successfully.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("-> Column 'channel_group' already exists. Skipping ALTER TABLE.")
        else:
            raise  # Re-raise unexpected errors 
    
    # 3. POPULATE COLUMN (The core business logic)
    cursor.execute("""
        -- Step 2: Populate the new column using your logic
        UPDATE dim_session_activity
        SET channel_group = 
            CASE 
                -- ORGANIC
                WHEN traffic_campaign IS NULL AND traffic_source IN ('https://www.gsearch.com', 'https://www.bsearch.com') THEN 'organic'
                -- PAID BRAND
                WHEN traffic_campaign = 'brand' THEN 'paid_brand'
                -- PAID NONBRAND
                WHEN traffic_campaign = 'nonbrand' THEN 'paid_nonbrand'
                -- DIRECT (traffic_source is NULL)
                WHEN traffic_source IS NULL THEN 'direct_type_in'
                -- PAID SOCIAL
                WHEN traffic_source = 'socialbook' THEN 'paid_social'
                -- Everything else (e.g., untracked referral)
                ELSE 'other' 
            END
        WHERE channel_group IS NULL OR channel_group = ''; -- Only update rows that haven't been grouped yet
    """)
    
    print(f"-> Successfully grouped {conn.total_changes} rows.")
    conn.commit()

except sqlite3.Error as e:
    print(f"An error occurred during dimension update: {e}")
    if conn:
        conn.rollback() # Only rollback if the connection was successful
    
finally:
    # 4. Safely close the connection ONLY if it was successfully opened
    if conn:
        conn.close()
    print("Dimension update script finished.")