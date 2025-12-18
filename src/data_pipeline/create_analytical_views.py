import pathlib
import sqlite3
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

# 1. Initialize conn and cursor safely 
conn = None
cursor = None

"""
-- View Name: v_daily_website_performance
-- Description: This view aggregates daily website performance metrics including sessions, bounces, bounce rate, orders, revenue, and conversion rate (CVR).
"""
try:

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    print("Starting creation of v_daily_analytics_summary view...")

    # DROP/CREATE THE SINGLE COMPREHENSIVE VIEW ---
    cursor.execute("DROP VIEW IF EXISTS v_daily_analytics_summary;")
    
    cursor.execute("""
        CREATE VIEW v_daily_analytics_summary AS

        WITH daily_sessions AS (
            -- CTE 1: Calculates daily traffic metrics (Sessions, Bounces, Bounce Rate)
            SELECT
                session_date AS report_date,
                channel_group,
                COUNT(DISTINCT session_id) AS total_sessions,
                SUM(is_bounced) AS total_bounced_sessions,
                CAST(SUM(is_bounced) AS REAL) * 100.0 / COUNT(session_id) AS bounce_rate_percentage
            FROM 
                dim_session_activity
            WHERE   session_date < '2015-03-01'
            GROUP BY 
                session_date, channel_group   
        ),

        daily_orders AS (
            -- CTE 2: Calculates daily sales metrics (Orders, Revenue)
            SELECT
                STRFTIME('%Y-%m-%d', o.created_at) AS report_date,
                s.channel_group,
                COUNT(DISTINCT o.order_id) AS total_orders,
                SUM(o.price_usd) AS total_revenue_usd
            FROM 
                fact_orders as o
            JOIN 
                dim_session_activity AS s ON o.website_session_id = s.session_id -- Link to get the channel
            GROUP BY 
                STRFTIME('%Y-%m-%d', o.created_at), s.channel_group
        )

        -- Joins all data and calculates Conversion Rate (CVR)
        SELECT
            ds.report_date,
            ds.channel_group,
            dd.day_of_week,
            dd.is_weekend,
            
            -- Traffic Metrics
            ds.total_sessions,
            ds.total_bounced_sessions,
            ds.bounce_rate_percentage,
            
            -- Sales Metrics
            COALESCE(do.total_orders, 0) AS total_orders, 
            COALESCE(do.total_revenue_usd, 0.0) AS total_revenue_usd,
            
            -- Calculated CVR
            CAST(COALESCE(do.total_orders, 0) AS REAL) * 100.0 / ds.total_sessions AS conversion_rate_percentage

        FROM
            daily_sessions AS ds
        LEFT JOIN
            daily_orders AS do
            ON ds.report_date = do.report_date
            AND ds.channel_group = do.channel_group
        LEFT JOIN
            dim_date AS dd
            ON ds.report_date = dd.date_key
        ORDER BY
            ds.report_date, ds.channel_group;
    """)

    conn.commit()
    print("Successfully created the comprehensive analytical view: v_daily_analytics_summary")

except sqlite3.Error as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()
    print("View creation script finished.")



                     