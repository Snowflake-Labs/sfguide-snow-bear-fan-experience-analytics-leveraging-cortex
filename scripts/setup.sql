-- Snow Bear Fan Experience Analytics - Initial Setup Script
-- Run this script BEFORE using the notebook
-- This creates the role, grants privileges, then creates databases, schemas, warehouse, and stage

USE ROLE accountadmin;

-- Step 1: Create role for Snow Bear data scientists
CREATE OR REPLACE ROLE snow_bear_data_scientist;

-- Step 2: Grant system-level privileges to the role
-- Grant Cortex AI privileges (required for AI functions)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE snow_bear_data_scientist;

-- Grant database creation privilege to the role
GRANT CREATE DATABASE ON ACCOUNT TO ROLE snow_bear_data_scientist;

-- Grant additional necessary privileges
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE snow_bear_data_scientist;
GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE snow_bear_data_scientist;
GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE snow_bear_data_scientist;

-- Grant role to current user
SET my_user_var = (SELECT '"' || CURRENT_USER() || '"');
GRANT ROLE snow_bear_data_scientist TO USER identifier($my_user_var);

-- Step 3: Create warehouse and grant privileges (keep as ACCOUNTADMIN for stability)
CREATE OR REPLACE WAREHOUSE snow_bear_wh
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Analytics warehouse for Snow Bear fan experience analytics';

-- Grant warehouse privileges to snow_bear_data_scientist
GRANT USAGE ON WAREHOUSE snow_bear_wh TO ROLE snow_bear_data_scientist;
GRANT OPERATE ON WAREHOUSE snow_bear_wh TO ROLE snow_bear_data_scientist;
GRANT MONITOR ON WAREHOUSE snow_bear_wh TO ROLE snow_bear_data_scientist;

-- Step 4: Switch to snow_bear_data_scientist role to create databases as owner
USE ROLE snow_bear_data_scientist;
USE WAREHOUSE snow_bear_wh;

-- Verify role switch was successful
SELECT CURRENT_ROLE() AS active_role, CURRENT_USER() AS current_user;

-- Create database (now owned by snow_bear_data_scientist)
CREATE OR REPLACE DATABASE SNOW_BEAR_DB;

-- Create all schemas in SNOW_BEAR_DB
USE DATABASE SNOW_BEAR_DB;
CREATE OR REPLACE SCHEMA BRONZE_LAYER;
CREATE OR REPLACE SCHEMA GOLD_LAYER;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- Create stages in SNOW_BEAR_DB.ANALYTICS
CREATE OR REPLACE STAGE snow_bear_data_stage
    COMMENT = 'Stage for Snow Bear fan survey data files';

CREATE OR REPLACE STAGE semantic_models
    COMMENT = 'Stage for Cortex Analyst semantic model files';

-- Switch to data context for table creation
USE SCHEMA BRONZE_LAYER;

-- Create the raw data table for basketball fan survey responses
CREATE OR REPLACE TABLE SNOW_BEAR_DB.BRONZE_LAYER.GENERATED_DATA_MAJOR_LEAGUE_BASKETBALL_STRUCTURED (
	ID VARCHAR(16777216),
	FOOD_OFFERING_COMMENT VARCHAR(16777216),
	FOOD_OFFERING_SCORE VARCHAR(16777216),
	GAME_EXPERIENCE_COMMENT VARCHAR(16777216),
	GAME_EXPERIENCE_SCORE VARCHAR(16777216),
	MERCHANDISE_OFFERING_COMMENT VARCHAR(16777216),
	MERCHANDISE_OFFERING_SCORE VARCHAR(16777216),
	MERCHANDISE_PRICING_COMMENT VARCHAR(16777216),
	MERCHANDISE_PRICING_SCORE VARCHAR(16777216),
	OVERALL_EVENT_COMMENT VARCHAR(16777216),
	OVERALL_EVENT_SCORE VARCHAR(16777216),
	PARKING_COMMENT VARCHAR(16777216),
	PARKING_SCORE VARCHAR(16777216),
	SEAT_LOCATION_COMMENT VARCHAR(16777216),
	SEAT_LOCATION_SCORE VARCHAR(16777216),
	STADIUM_ACCESS_SCORE VARCHAR(16777216),
	STADIUM_COMMENT VARCHAR(16777216),
	TICKET_PRICE_COMMENT VARCHAR(16777216),
	TICKET_PRICE_SCORE VARCHAR(16777216),
	COMPANY_NAME VARCHAR(16777216),
	TOPIC VARCHAR(16777216),
	CREATED_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

-- Create file format for CSV loading
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE_UNENCLOSED_FIELD = '\134'
    COMMENT = 'File format for Snow Bear fan survey CSV data';

-- Final verification and status
SELECT CURRENT_ROLE() AS current_role, CURRENT_DATABASE() AS current_database;
SELECT 'Snow Bear setup complete! Now upload basketball_fan_survey_data.csv.gz to the snow_bear_data_stage and run the notebook.' AS status;

-- Verify databases are owned by snow_bear_data_scientist
SHOW DATABASES LIKE 'CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB';
SHOW DATABASES LIKE 'SNOW_BEAR_DB';

-- Instructions for next steps:
-- 1. Upload basketball_fan_survey_data.csv.gz and snow_bear.py to the snow_bear_data_stage
-- 2. Download and import snow_bear_complete_setup.ipynb using Snowsight's Import .ipynb file feature
-- 3. Run the imported Snow Bear notebook to process analytics and create Streamlit app

-- ============================================================================
-- TEARDOWN SCRIPT (Uncomment lines below to clean up all resources)
-- ============================================================================

/*
-- Snow Bear Analytics Teardown Script
-- Uncomment and run these lines to remove all objects created during the quickstart

-- Switch to ACCOUNTADMIN role for cleanup
USE ROLE ACCOUNTADMIN;

-- Reset session context to avoid "database does not exist" errors
-- This prevents context conflicts when dropping the database
USE DATABASE SNOWFLAKE;
USE SCHEMA INFORMATION_SCHEMA;
USE WAREHOUSE COMPUTE_WH;

-- Drop the database created during setup (this will cascade to remove all contained objects)
-- Note: This automatically removes all schemas, tables, stages, notebooks, and Streamlit apps
DROP DATABASE IF EXISTS SNOW_BEAR_DB;

-- Drop the warehouse created during setup
DROP WAREHOUSE IF EXISTS snow_bear_wh;

-- Drop the role created during setup
DROP ROLE IF EXISTS snow_bear_data_scientist;

-- Verification: Check that objects have been removed
SHOW DATABASES LIKE 'SNOW_BEAR_DB';
SHOW WAREHOUSES LIKE 'snow_bear_wh';
SHOW ROLES LIKE 'snow_bear_data_scientist';
*/
