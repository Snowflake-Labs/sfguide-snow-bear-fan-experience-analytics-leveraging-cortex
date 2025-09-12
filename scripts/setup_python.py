# Snow Bear Fan Experience Analytics - Complete Python Setup
# Run this Python worksheet in Snowflake to automatically set up everything
# This script does everything setup.sql does PLUS creates the notebook from GitHub

import urllib.request
import io
from snowflake.snowpark.context import get_active_session

# Get the active session
session = get_active_session()

print("🐻❄️ Starting Snow Bear Fan Experience Analytics Setup...")

# =============================================================================
# STEP 1: Create Role and Grant Privileges
# =============================================================================

print("\n📋 Step 1: Creating role and granting privileges...")

setup_sqls = [
    "USE ROLE accountadmin",
    
    # Create role for Snow Bear data scientists
    "CREATE OR REPLACE ROLE snow_bear_data_scientist",
    
    # Grant Cortex AI privileges (required for AI functions)
    "GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE snow_bear_data_scientist",
    
    # Grant role to current user
    "SET my_user_var = (SELECT '\"' || CURRENT_USER() || '\"')",
    "GRANT ROLE snow_bear_data_scientist TO USER identifier($my_user_var)"
]

for sql in setup_sqls:
    session.sql(sql).collect()
    print(f"  ✅ {sql}")

# =============================================================================
# STEP 2: Create Databases, Schemas, and Warehouse
# =============================================================================

print("\n🏗️ Step 2: Creating databases, schemas, and warehouse...")

object_sqls = [
    # Create Snow Bear databases and schemas
    "CREATE OR REPLACE DATABASE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB",
    "CREATE OR REPLACE DATABASE SNOW_BEAR_DB",
    
    "USE DATABASE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB",
    "CREATE OR REPLACE SCHEMA BRONZE_LAYER",
    "CREATE OR REPLACE SCHEMA GOLD_LAYER",
    
    "USE DATABASE SNOW_BEAR_DB",
    "CREATE OR REPLACE SCHEMA ANALYTICS",
    
    # Create warehouse for analytics
    """CREATE OR REPLACE WAREHOUSE snow_bear_analytics_wh
        WAREHOUSE_SIZE = 'small'
        WAREHOUSE_TYPE = 'standard'
        AUTO_SUSPEND = 60
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics warehouse for Snow Bear fan experience analytics'"""
]

for sql in object_sqls:
    session.sql(sql).collect()
    print(f"  ✅ {sql.split()[0:4]} {'...' if len(sql.split()) > 4 else ''}")

# =============================================================================
# STEP 3: Grant Object-Level Privileges
# =============================================================================

print("\n🔐 Step 3: Granting object-level privileges...")

privilege_sqls = [
    "GRANT USAGE ON WAREHOUSE snow_bear_analytics_wh TO ROLE snow_bear_data_scientist",
    "GRANT OPERATE ON WAREHOUSE snow_bear_analytics_wh TO ROLE snow_bear_data_scientist",
    "GRANT ALL ON DATABASE CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB TO ROLE snow_bear_data_scientist",
    "GRANT ALL ON DATABASE SNOW_BEAR_DB TO ROLE snow_bear_data_scientist",
    "GRANT ALL ON SCHEMA CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.BRONZE_LAYER TO ROLE snow_bear_data_scientist",
    "GRANT ALL ON SCHEMA CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.GOLD_LAYER TO ROLE snow_bear_data_scientist",
    "GRANT ALL ON SCHEMA SNOW_BEAR_DB.ANALYTICS TO ROLE snow_bear_data_scientist"
]

for sql in privilege_sqls:
    session.sql(sql).collect()
    print(f"  ✅ {sql}")

# =============================================================================
# STEP 4: Switch Context and Create Stage
# =============================================================================

print("\n📁 Step 4: Creating stage and data objects...")

stage_sqls = [
    "USE ROLE snow_bear_data_scientist",
    "USE WAREHOUSE snow_bear_analytics_wh",
    "USE DATABASE SNOW_BEAR_DB",
    "USE SCHEMA ANALYTICS",
    
    # Create stage for CSV file upload in SNOW_BEAR_DB.ANALYTICS
    """CREATE OR REPLACE STAGE snow_bear_data_stage
        COMMENT = 'Stage for Snow Bear fan survey data files'""",
        
    # Switch back to data schema for table creation
    "USE SCHEMA CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB.BRONZE_LAYER"
]

for sql in stage_sqls:
    session.sql(sql).collect()
    print(f"  ✅ {sql}")

# =============================================================================
# STEP 5: Create Data Table and File Format
# =============================================================================

print("\n🗄️ Step 5: Creating data table and file format...")

table_sql = """
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
)
"""

file_format_sql = """
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE_UNENCLOSED_FIELD = '\134'
    COMMENT = 'File format for Snow Bear fan survey CSV data'
"""

session.sql(table_sql).collect()
print("  ✅ Created GENERATED_DATA_MAJOR_LEAGUE_BASKETBALL_STRUCTURED table")

session.sql(file_format_sql).collect()
print("  ✅ Created csv_format file format")

# =============================================================================
# STEP 6: Download Files from GitHub and Upload to Stage
# =============================================================================

print("\n🌐 Step 6: Downloading files from GitHub and uploading to stage...")

# GitHub repository base URL
github_base = "https://raw.githubusercontent.com/Snowflake-Labs/sfguide-snow-bear-fan-experience-analytics-leveraging-cortex/main"

# Files to download
files_to_download = {
    "basketball_fan_survey_data.csv.gz": f"{github_base}/scripts/basketball_fan_survey_data.csv.gz",
    "snow_bear_complete_setup.ipynb": f"{github_base}/notebooks/snow_bear_complete_setup.ipynb", 
    "snow_bear.py": f"{github_base}/scripts/snow_bear.py"
}

# Switch to the stage context
session.sql("USE DATABASE SNOW_BEAR_DB").collect()
session.sql("USE SCHEMA ANALYTICS").collect()

for filename, url in files_to_download.items():
    try:
        print(f"  📥 Downloading {filename}...")
        
        # Use urllib instead of requests
        with urllib.request.urlopen(url) as response:
            if response.status != 200:
                raise Exception(f"HTTP {response.status}: {response.reason}")
            
            file_content = response.read()
        
        # Upload file to stage using put_stream
        file_stream = io.BytesIO(file_content)
        session.file.put_stream(
            input_stream=file_stream,
            stage_location=f"@snow_bear_data_stage/{filename}",
            auto_compress=False
        )
        print(f"  ✅ Uploaded {filename} to stage")
        
    except Exception as e:
        print(f"  ❌ Error with {filename}: {str(e)}")

# =============================================================================
# STEP 7: Create Notebook from Uploaded File
# =============================================================================

print("\n📓 Step 7: Creating notebook from uploaded file...")

try:
    notebook_sql = """
    CREATE OR REPLACE NOTEBOOK "Snow Bear Complete Setup"
        FROM '@SNOW_BEAR_DB.ANALYTICS.SNOW_BEAR_DATA_STAGE'
        MAIN_FILE = 'snow_bear_complete_setup.ipynb'
        QUERY_WAREHOUSE = 'SNOW_BEAR_ANALYTICS_WH'
        COMMENT = 'Snow Bear Fan Experience Analytics - Complete Setup and Processing Notebook'
    """
    
    session.sql(notebook_sql).collect()
    print("  ✅ Created notebook: Snow Bear Complete Setup")
    
    # Grant usage to the Snow Bear role
    session.sql('GRANT USAGE ON NOTEBOOK "Snow Bear Complete Setup" TO ROLE SNOW_BEAR_DATA_SCIENTIST').collect()
    print("  ✅ Granted notebook usage to snow_bear_data_scientist role")
    
except Exception as e:
    print(f"  ❌ Error creating notebook: {str(e)}")

# =============================================================================
# STEP 8: Create Streamlit App from Uploaded File
# =============================================================================

print("\n🎯 Step 8: Creating Streamlit app from uploaded file...")

try:
    streamlit_sql = """
    CREATE OR REPLACE STREAMLIT "Snow Bear Fan Analytics"
        ROOT_LOCATION = '@SNOW_BEAR_DB.ANALYTICS.SNOW_BEAR_DATA_STAGE'
        MAIN_FILE = 'snow_bear.py'
        QUERY_WAREHOUSE = 'SNOW_BEAR_ANALYTICS_WH'
        COMMENT = 'Snow Bear Fan Experience Analytics Dashboard - Complete 7-module platform'
    """
    
    session.sql(streamlit_sql).collect()
    print("  ✅ Created Streamlit app: Snow Bear Fan Analytics")
    
    # Grant usage to the Snow Bear role
    session.sql('GRANT USAGE ON STREAMLIT "Snow Bear Fan Analytics" TO ROLE SNOW_BEAR_DATA_SCIENTIST').collect()
    print("  ✅ Granted Streamlit app usage to snow_bear_data_scientist role")
    
except Exception as e:
    print(f"  ❌ Error creating Streamlit app: {str(e)}")

# =============================================================================
# FINAL STATUS
# =============================================================================

print("\n🎉 Snow Bear Fan Experience Analytics Setup Complete!")
print("\n📋 Summary of Created Objects:")
print("  ✅ Role: snow_bear_data_scientist (with Cortex AI privileges)")
print("  ✅ Databases: CUSTOMER_MAJOR_LEAGUE_BASKETBALL_DB, SNOW_BEAR_DB") 
print("  ✅ Schemas: BRONZE_LAYER, GOLD_LAYER, ANALYTICS")
print("  ✅ Warehouse: snow_bear_analytics_wh")
print("  ✅ Stage: snow_bear_data_stage (with all files)")
print("  ✅ Table: GENERATED_DATA_MAJOR_LEAGUE_BASKETBALL_STRUCTURED")
print("  ✅ File Format: csv_format")
print("  ✅ Notebook: Snow Bear Complete Setup")
print("  ✅ Streamlit App: Snow Bear Fan Analytics")

print("\n🚀 Next Steps:")
print("  1. Navigate to Projects → Notebooks → 'Snow Bear Complete Setup'")
print("  2. Run all notebook cells to process the analytics")
print("  3. Navigate to Projects → Streamlit → 'Snow Bear Fan Analytics'")
print("  4. Explore your complete 7-module analytics platform!")

print("\n📊 Your Snow Bear analytics platform is ready with real basketball fan data!")
