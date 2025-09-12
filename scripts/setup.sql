-- Snow Bear Fan Experience Analytics - Initial Setup Script
-- Run this script BEFORE using the notebook
-- This creates the role, grants privileges, then creates databases, schemas, warehouse, and stage

USE ROLE accountadmin;

-- Step 1: Create role for Snow Bear data scientists
CREATE OR REPLACE ROLE snow_bear_data_scientist;

-- Step 2: Grant system-level privileges to the role
-- Grant Cortex AI privileges (required for AI functions)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE snow_bear_data_scientist;

-- Grant role to current user
SET my_user_var = (SELECT '"' || CURRENT_USER() || '"');
GRANT ROLE snow_bear_data_scientist TO USER identifier($my_user_var);

-- Step 3: Create Snowflake objects (databases, schemas, warehouse)
-- Create Snow Bear databases and schemas
CREATE OR REPLACE DATABASE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB;
CREATE OR REPLACE DATABASE SNOW_BEAR_DB;

USE DATABASE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB;
CREATE OR REPLACE SCHEMA BRONZE_LAYER;
CREATE OR REPLACE SCHEMA GOLD_LAYER;

USE DATABASE SNOW_BEAR_DB;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- Create warehouse for analytics
CREATE OR REPLACE WAREHOUSE snow_bear_analytics_wh
    WAREHOUSE_SIZE = 'small'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Analytics warehouse for Snow Bear fan experience analytics';

-- Step 4: Grant object-level privileges to the role
GRANT USAGE ON WAREHOUSE snow_bear_analytics_wh TO ROLE snow_bear_data_scientist;
GRANT OPERATE ON WAREHOUSE snow_bear_analytics_wh TO ROLE snow_bear_data_scientist;
GRANT ALL ON DATABASE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB TO ROLE snow_bear_data_scientist;
GRANT ALL ON DATABASE SNOW_BEAR_DB TO ROLE snow_bear_data_scientist;
GRANT ALL ON SCHEMA CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.BRONZE_LAYER TO ROLE snow_bear_data_scientist;
GRANT ALL ON SCHEMA CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.GOLD_LAYER TO ROLE snow_bear_data_scientist;
GRANT ALL ON SCHEMA SNOW_BEAR_DB.ANALYTICS TO ROLE snow_bear_data_scientist;

-- Switch to Snow Bear role and create stage in SNOW_BEAR_DB
USE ROLE snow_bear_data_scientist;
USE WAREHOUSE snow_bear_analytics_wh;
USE DATABASE SNOW_BEAR_DB;
USE SCHEMA ANALYTICS;

-- Create stage for CSV file upload in SNOW_BEAR_DB.ANALYTICS
CREATE OR REPLACE STAGE snow_bear_data_stage
    COMMENT = 'Stage for Snow Bear fan survey data files';

-- Switch back to data schema for table creation
USE SCHEMA CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.BRONZE_LAYER;

-- Create the raw data table for basketball fan survey responses
CREATE OR REPLACE TABLE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.BRONZE_LAYER.GENERATED_DATA_MAJOR_LEAGUE_BASKETBALL_STRUCTURED (
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

SELECT 'Snow Bear setup complete! Now upload basketball_fan_survey_data.csv.gz to the snow_bear_data_stage and run the notebook.' AS status;

-- Instructions for next steps:
-- 1. Upload basketball_fan_survey_data.csv.gz to the snow_bear_data_stage
-- 2. Run the Snow Bear notebook (snow_bear_complete_setup.ipynb)

-- ============================================================================
-- OPTIONAL: CREATE NOTEBOOK FROM STAGE (Uncomment to auto-create notebook)
-- ============================================================================

/*
-- Alternative: Create the notebook directly from the uploaded file
-- Uncomment these lines if you want to auto-create the notebook instead of manually importing

USE DATABASE SNOW_BEAR_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE NOTEBOOK "Snow Bear Complete Setup"
    FROM '@SNOW_BEAR_DB.ANALYTICS.SNOW_BEAR_DATA_STAGE'
    MAIN_FILE = 'snow_bear_complete_setup.ipynb'
    QUERY_WAREHOUSE = 'SNOW_BEAR_ANALYTICS_WH'
    COMMENT = 'Snow Bear Fan Experience Analytics - Complete Setup and Processing Notebook';

-- Grant usage to the Snow Bear role
GRANT USAGE ON NOTEBOOK "Snow Bear Complete Setup" TO ROLE SNOW_BEAR_DATA_SCIENTIST;

SELECT 'Snow Bear notebook created successfully! Navigate to Projects → Notebooks → Snow Bear Complete Setup to run the analytics workflow.' AS status;
*/

-- ============================================================================
-- TEARDOWN SCRIPT (Uncomment lines below to clean up all resources)
-- ============================================================================

/*
-- Snow Bear Analytics Teardown Script
-- Uncomment and run these lines to remove all objects created during the quickstart

-- Switch to ACCOUNTADMIN role for cleanup
USE ROLE ACCOUNTADMIN;

-- Drop the databases created during setup (this will cascade to remove all contained objects)
-- Note: This automatically removes all schemas, tables, stages, notebooks, and Streamlit apps
DROP DATABASE IF EXISTS CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB;
DROP DATABASE IF EXISTS SNOW_BEAR_DB;

-- Drop the warehouse created during setup
DROP WAREHOUSE IF EXISTS snow_bear_analytics_wh;

-- Drop the role created during setup
DROP ROLE IF EXISTS snow_bear_data_scientist;

-- Note: Dropping the SNOW_BEAR_DB database automatically removes:
-- - All notebooks (including "Snow Bear Complete Setup")
-- - All Streamlit apps (including "Snow Bear Fan Analytics") 
-- - All stages and file formats
-- No manual cleanup of individual apps/notebooks needed
*/
