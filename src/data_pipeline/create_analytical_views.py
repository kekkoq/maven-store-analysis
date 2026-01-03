import pathlib
import sqlite3
import sys
from venv import create

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

    print("Starting creation of v_customer_loyalty_metrics view...")

    cursor.execute("DROP VIEW IF EXISTS v_customer_loyalty_metrics;")
    
    cursor.execute("""
        CREATE VIEW v_customer_loyalty_metrics AS 
        
        WITH user_order_sequence AS (
        SELECT 
            fo.user_id,
            fo.order_id,
            fo.created_at,
            STRFTIME('%Y-%m-01', fo.created_at) as order_month,
            ds.channel_group,
            -- Calculate the order rank for each user to identify 1st, 2nd, 3rd+ purchases
            ROW_NUMBER() OVER(PARTITION BY fo.user_id ORDER BY fo.created_at) as order_number,
            -- Get the date of the previous order to calculate days between purchases
            LAG(fo.created_at) OVER(PARTITION BY fo.user_id ORDER BY fo.created_at) as previous_order_date
        FROM fact_orders AS fo
        LEFT JOIN dim_session_activity AS ds ON fo.website_session_id = ds.session_id
        )
        SELECT 
            order_id,
            user_id,
            order_month,
            channel_group,
            order_number,
            CASE 
                WHEN order_number = 1 THEN 'First-Time Buyer'
                ELSE 'Repeat Buyer'
            END AS buyer_type,
            -- Calculate the "Days to Repurchase" (Churn/Retention metric)
            CASE 
                WHEN previous_order_date IS NOT NULL 
                THEN (julianday(created_at) - julianday(previous_order_date))
                ELSE 0 
            END AS days_since_last_purchase
        FROM user_order_sequence;
        """)    

    print("Starting creation of v_customer_cohort_analysis...")

    cursor.execute("DROP VIEW IF EXISTS v_customer_cohort_analysis;")

    cursor.execute("""
        CREATE VIEW v_customer_cohort_analysis AS
        WITH user_first_order AS (
            SELECT
                user_id,
                STRFTIME('%Y-%m-01', MIN(created_at)) AS cohort_month
            FROM fact_orders
            GROUP BY user_id
        ),
        order_date AS (
            SELECT
                fo.user_id,
                fo.order_id,
                STRFTIME('%Y-%m-01', fo.created_at) AS order_month,
                ufo.cohort_month
            FROM fact_orders AS fo
            LEFT JOIN user_first_order AS ufo ON fo.user_id = ufo.user_id
        )
        SELECT
            order_id,
            user_id,
            STRFTIME('%Y-%m-01', cohort_month) AS cohort_month,
            order_month,
            ((CAST(STRFTIME('%Y', order_month) AS INT) - CAST(STRFTIME('%Y', cohort_month) AS INT)) * 12 + 
            (CAST(STRFTIME('%m', order_month) AS INT) - CAST(STRFTIME('%m', cohort_month) AS INT))) AS cohort_index            
        FROM order_date;
        """)

    print("Starting creation of v_dim_session_landing_pages_analysis...")

    cursor.execute("DROP VIEW IF EXISTS v_dim_session_landing_pages_analysis;")

    cursor.execute("""
        CREATE VIEW v_dim_session_landing_pages_analysis AS
        -- This view brings in the first pageview URL for each session, and whether the session converted into an order
        SELECT 
            ws.website_session_id,
            ws.created_at,
            ws.utm_source,
            ws.utm_campaign,
            wp.pageview_url AS landing_page, -- First pageview URL for each session
            CASE WHEN ws.is_repeat_session = 1 THEN 'Repeat' ELSE 'New' END AS user_type,
            CASE WHEN fo.order_id IS NOT NULL THEN 1 ELSE 0 END AS is_converted
        FROM website_sessions AS ws
        LEFT JOIN website_pageviews AS wp 
            ON ws.website_session_id = wp.website_session_id
            AND wp.website_pageview_id = (
                SELECT MIN(wpv.website_pageview_id) 
                FROM website_pageviews AS wpv
                WHERE wpv.website_session_id = ws.website_session_id
            )
        LEFT JOIN fact_orders AS fo 
            ON ws.website_session_id = fo.website_session_id;
        """)

    print("Starting creation of v_billing_performance_analysis...")

    cursor.execute("DROP VIEW IF EXISTS v_billing_performance_analysis;")

    cursor.execute("""
    CREATE VIEW v_billing_performance_analysis AS
        SELECT 
            ws.website_session_id,
            ws.created_at AS session_date,
            wp.pageview_url AS billing_version_seen,
            CASE WHEN ws.is_repeat_session = 1 THEN 'Repeat' ELSE 'New' END AS user_type,
            CASE WHEN o.order_id IS NOT NULL THEN 1 ELSE 0 END AS is_ordered
        FROM website_sessions ws
        INNER JOIN website_pageviews wp 
            ON ws.website_session_id = wp.website_session_id
        LEFT JOIN orders o 
            ON ws.website_session_id = o.website_session_id
        WHERE wp.pageview_url IN ('/billing', '/billing-2');
    """)        

    print("Successfully created the comprehensive analytical view: v_daily_analytics_summary")

except sqlite3.Error as e:
    print(f"An error occurred: {e}")

finally:
    if conn:
        conn.close()
    print("View creation script finished.")



                     