"""ELT script to load prepared data into the data warehouse (SQLite database).

File: src/maven-store-analysis/src/data_pipeline/elt_core_load.py

Project Structure:

maven-store-analysis (project_root)/
├── src/data_pipeline/
│   ├── el_core_load.py  # This file 
"""

import sqlite3
import pathlib
import sys

# --- Define Project Root and Add to Path for Imports ---
PROJECT_ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent.parent  
sys.path.append(str(PROJECT_ROOT_DIR))

from src.logger import init_logger, logger
 
ELT_DIR: pathlib.Path = PROJECT_ROOT_DIR / "src" / "data_pipeline" 
PACKAGE_DIR: pathlib.Path = ELT_DIR.parent
DATA_BACKUPS_DIR = pathlib.Path("C:/Repos/data_backups") # path for external location
DB_PATH = DATA_BACKUPS_DIR / "maven_factory.db"

init_logger()  # Initialize the logger before any logging

logger.info(f"PROJECT_ROOT_DIR:    {PROJECT_ROOT_DIR}")
logger.info(f"ELT_DIR:             {ELT_DIR}")
logger.info(f"PACKAGE_DIR:         {PACKAGE_DIR}")
logger.info(f"DATA_BACKUPS_DIR:    {DATA_BACKUPS_DIR}")
logger.info(f"DB_PATH:             {DB_PATH}")

# --- 1a. SQL Transformation Logic for Bounce Rate Calculation ---
TRANSFORMATION_SQL = """
INSERT INTO dim_session_activity (
    session_id, 
    session_date, 
    landing_page, 
    total_pageviews, 
    is_bounced, 
    traffic_source, 
    traffic_campaign, 
    device_type, 
    referrer_domain, 
    is_returning
)
WITH session_pageview_summary AS (
    -- CTE 1: Calculate total pageviews and find the first pageview ID for every session
    SELECT 
        website_session_id,
        COUNT(website_pageview_id) AS total_pageviews,
        MIN(website_pageview_id) AS first_pageview_id
    FROM website_pageviews  -- Raw table loaded from .sql script
    GROUP BY website_session_id
)
SELECT 
    wss.website_session_id,
    DATE(wss.created_at),
    wps_min.pageview_url,
    sps.total_pageviews,
    -- Calculate the Bounce Flag
    CASE 
        WHEN sps.total_pageviews = 1 THEN 1 
        ELSE 0 
    END AS is_bounced,
    -- Segmentation Attributes from website_sessions
    wss.utm_source,
    wss.utm_campaign,
    wss.device_type,
    SUBSTR(wss.http_referer, INSTR(wss.http_referer, '://') + 3), 
    wss.is_repeat_session 
FROM website_sessions AS wss  -- Raw table loaded from .sql script
INNER JOIN session_pageview_summary AS sps
    ON wss.website_session_id = sps.website_session_id
INNER JOIN website_pageviews AS wps_min
    -- Join back to get the actual URL of the landing page
    ON sps.first_pageview_id = wps_min.website_pageview_id;
"""

# --- 1b. SQL Transformation Logic for Fact Orders ---
FACT_ORDERS_SQL = """
INSERT INTO fact_orders (
    order_id,
    website_session_id,
    user_id,
    price_usd,
    cogs_usd,
    total_items_purchased,
    primary_product_id,
    created_at
)
SELECT
    order_id,
    website_session_id,
    user_id,
    price_usd,
    cogs_usd,
    items_purchased,
    primary_product_id,
    created_at
FROM 
    orders; 
"""
# --- 2. Schema Creation Function ---   

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    logger.info("Defining and creating dim_session_activity schema...")
   
    try:
        # 1. Create dim_session_activity (For Bounce Rate, Funnels)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_session_activity (
                session_id          INTEGER PRIMARY KEY,
                session_date        TEXT,
                landing_page        TEXT,
                total_pageviews     INTEGER,
                is_bounced          INTEGER, -- Flag: 1 if total_pageviews = 1
                traffic_source      TEXT,
                traffic_campaign    TEXT,
                device_type         TEXT,
                referrer_domain     TEXT,
                is_returning        INTEGER         
            );
        """)

        # 2a. FACT TABLE: fact_orders (For Revenue, AOV, and CVR)
        # This stores one row per order and links to website_sessions and dim_date.
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS fact_orders (
                order_id                INTEGER PRIMARY KEY,
                website_session_id      INTEGER, 
                user_id                 INTEGER,           -- For LTV/Customer Analysis
                price_usd               REAL,              -- For Revenue and AOV Calculations
                cogs_usd                REAL,              -- For Profit Analysis
                total_items_purchased   INTEGER,           -- For Items-per-Order Metric
                primary_product_id      INTEGER,           -- For Entry Product Analysis
                created_at              TEXT               
        );
        """)
        # 2b. FACT TABLE: fact_order_items (for Cross-Sell Analysis)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_order_items (
                order_item_id           INTEGER PRIMARY KEY,
                order_id                INTEGER,           -- Links to fact_orders
                product_id              INTEGER,           -- The core item dimension
                is_primary_item         INTEGER,           -- Flag for item type
                item_revenue_usd        REAL,              -- Item-level financial metric
                item_cogs_usd           REAL,              -- Item-level financial metric
                created_at              TEXT
        );
        """)

        # Cleanup existing data before fresh insert (Idempotency)
        logger.info("Clearing existing data from dim_session_activity...")
        cursor.execute("DELETE FROM dim_session_activity;")

        logger.info("Clearing existing data from fact_orders...")
        cursor.execute("DELETE FROM fact_orders;")

        # 3. DIMENSION TABLE: dim_date (For Time-based Grouping and Trend Analysis)
            # This is critical for reporting daily, weekly, or monthly AOV/CVR trends.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key            TEXT PRIMARY KEY,  -- Format YYYY-MM-DD
                full_date           TEXT,
                year                INTEGER,
                quarter             INTEGER,
                month               INTEGER,
                day_of_week         TEXT,
                is_weekend          INTEGER
            );
        """)
        
        logger.info("Database schema creation complete for dim_session_activity, fact_orders, and dim_date.")
   
        # 3b. Future Transformation Logic Placeholders
        # logger.info("Executing transformation for fact_orders...")
        # cursor.execute(FACT_ORDERS_SQL)
        # logger.info("Executing transformation for dim_date...")
        # cursor.execute(DIM_DATE_SQL)

    except sqlite3.Error as e:
        logger.error(f"Error creating tables: {e}")
        # Re-raise the error or handle it to ensure the main ELT function knows the schema failed
        raise e

# --- 3. Main ELT Function

def run_bounce_rate_elt():
    """
    Main function to execute the Extract, Load, and Transform (ELT) process
    for the Bounce Rate metric.
    """
    logger.info("Starting Bounce Rate ELT process...")
    conn = None # Initialize connection to None for the finally block

    # Create the connection to the external SQLite database file
    try:
        # 1. Establish Database Connection and Cursor
        conn = sqlite3.connect(DB_PATH)
        logger.info(f"Successfully connected to database: {DB_PATH}")
        cursor = conn.cursor()
        logger.info("SQL cursor created successfully.")   

        # 2. Execute Schema Creation 
        create_schema(cursor)
        
       # 3. ELT STEP 1: DIMENSION TABLE (dim_session_activity)
        logger.info("Executing transformation to populate dim_session_activity...")
        # (Optional cleanup here: DELETE FROM dim_session_activity;)
        cursor.execute(TRANSFORMATION_SQL)
        logger.info(f"SUCCESS: Inserted {cursor.rowcount:,} records into dim_session_activity.") 
        
        # 4. ELT STEP 2: FACT TABLE (fact_orders) -- ADD THIS HERE!
        logger.info("Executing transformation to populate fact_orders...")
        # (Optional cleanup here: DELETE FROM fact_orders;)
        cursor.execute(FACT_ORDERS_SQL)
        logger.info(f"SUCCESS: Inserted {cursor.rowcount:,} records into fact_orders.")
        

        # 5. Commit Changes
        conn.commit() 
        
    except sqlite3.Error as e:
        logger.error(f"Database error occurred: {e}")
        if conn:
            conn.rollback()
        raise e # Re-raise to ensure the overall program stops cleanly

    finally:
        # --- 5. Close Connection ---
        if conn:
            conn.close()
            logger.info("Database connection closed.")
            
# --- Execution Block ---
if __name__ == "__main__":
    # --- 1: CALL THE INITIALIZATION FUNCTION ---
    init_logger() 
    logger.info("Application starting up and logger initialized.")
    # --- 2: START THE MAIN ELT PROCESS ---
    # This runs the connection, schema creation, and transformation
    run_bounce_rate_elt()