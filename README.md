# Project: E-Commerce Data Warehouse and Business Intelligence Pipeline

## 1. Project Overview and Objective
This project establishes a comprehensive Extract, Transform, Load (ETL) and Online Analytical Processing (OLAP) pipeline to transform raw transactional data into an optimized dimensional model.

The primary objective is to create a robust data foundation within a SQLite data warehouse that supports deep analytical querying and serves as the single source of truth for Business Intelligence (BI) reporting in Power BI.

Key Deliverables:
ETL Pipeline Design: Scripts for data cleaning, splitting, and robust loading.

Dimensional Model: Creation of Fact Tables (fact_sales) and Dimension Tables (e.g., dim_date, dim_customer) optimized for reporting performance (star schema).

BI Readiness: Creation of analytical SQL Views or Materialized Tables for direct consumption by Power BI dashboards.

## 2. Data Source and Compatibility Note
The raw dataset for this project originates from the LinkedIn Learning: Advanced MySQL Data Analysis course. The original SQL file was structured for MySQL environments, which posed significant compatibility challenges for bulk loading into SQLite.

The ETL sequence below specifically addresses these challenges, particularly the use of multi-line INSERT INTO ... VALUES statements and incompatible control commands (SET AUTOCOMMIT=0;), ensuring the data is parsed and loaded reliably into the SQLite environment.

## 3. Data Ingestion Sequence (The ETL 'Extract' and 'Load' Phases)
This sequence outlines the steps required to prepare the raw data and successfully load the transactional tables into the maven_factory.db database.

| Step # | File Used / Action | Purpose | Description |
| :---: | :--- | :--- | :--- |
| **0.0** | **Manual Source Cleanup** | **Schema Separation** | Before starting, the original raw file (`rd_mavenfuzzyfactory.sql`) is manually edited to **remove all `CREATE TABLE` and `CREATE INDEX` statements**. This leaves only the raw `INSERT INTO` data blocks for subsequent processing. |
| **1.0** | `mavenfactory_split.py` | **Data Modularization** | Executes the first transformation. It reads the single, massive raw data file and systematically splits the data into **six smaller, table-specific SQL files**. This prevents memory overload and enables a faster, more granular loading process. |
| **2.0** | `clean_sql_data.py` | **SQLite Compatibility** | Executes the second critical transformation. It processes the 6 split data files (from Step 1.0) to **remove all non-SQLite commands** (e.g., `SET AUTOCOMMIT=0;`, `COMMIT;`) that cause the Python loader to fail silently. |
| **3.0** | **Terminal Command** | **Database Schema Load** | Executes the clean schema file (`mavenfactory_schema.sql`) to **create all seven empty tables** (`website_sessions`, `orders`, etc.) within the `maven_factory.db` file. |
| **4.0** | `db_loader.py` | **Robust Data Loading** | The core loading step. This Python script uses a **robust, line-by-line parsing logic** designed to specifically overcome SQLite's difficulty with large, multi-line `INSERT INTO ... VALUES` blocks, finally writing the data to the database. |
| **5.0** | `verify_data.py` | **Data Integrity Check** | A quality assurance step. This script connects to the database and runs `SELECT COUNT(*)` queries on all tables to **confirm non-zero record counts**, verifying the successful and complete data load. |

## 4. Analytical Pipeline (Transform Phase)
<<<<<<< HEAD
Upon successful data ingestion, the ETL pipeline shifts focus to building a dimensional model optimized for key business analysis and reporting.

**Dimensional Modeling (The Data Warehouse)**
The pipeline transforms the raw operational data into a Star Schema by creating three central fact and dimension tables. This structure is designed to isolate metrics and attributes for high-speed BI reporting.

- dim_session_activity **(Dimension Table)**:

  - Purpose: The central dimension table for Web Metrics (e.g., Bounce Rate, Pageviews, Traffic Source).
  - Content: Contains one row per website session, pre-calculated with the is_bounced flag and all key segmentation attributes (device_type, traffic_source, landing_page).

- fact_orders **(Fact Table)**:

  - Purpose: The central table for Order-Level Financial Metrics (e.g., Conversion Rate, Average Order Value, Total Revenue, Total Margin).
  - Content: Contains one row per completed order with aggregated financials (price_usd, cogs_usd) and links to the user and session.

- fact_order_items **(Fact Table - New)**:

  - Purpose: Enables detailed Product and Cross-Sell Analysis.
  - Content: Contains one row for every individual item purchased in an order, allowing for analysis of product bundles, refunds, and item-level profitability.

 - dim_date **(Dimension Table)**:

  - Purpose: Enables time-series analysis (Daily, Weekly, Monthly) for all metrics.

**Analytical Views** 
  - Goal: SQL Views will be created on top of the dimensional model to pre-aggregate high-level metrics (e.g., monthly sales summary, top traffic sources) into simplified, flat tables.
  - Benefit: This provides the BI tool with fast, ready-to-consume data sources, eliminating the need for complex DAX or M code and speeding up report load times.
=======
Upon successful data ingestion, the focus shifts to building the dimensional model, which is optimized for BI reporting.

Dimensional Modeling: Creation of fact_sales (the central table containing metrics like revenue and COGS) and dimension tables (dim_date, dim_customer, dim_product) to create a Star Schema.

Analytical Views: SQL Views will be created to pre-aggregate high-level metrics (e.g., monthly sales summary, top traffic sources) to provide Power BI users with fast, simplified data sources, eliminating the need for complex DAX logic.
>>>>>>> a1257a781c387dab4cb96166aa45075ca5f02373
