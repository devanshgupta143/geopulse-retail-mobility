-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Snowflake Infrastructure Setup
-- ============================================================================
-- Architecture: Medallion Multi-Tier Layering (Bronze -> Silver -> Gold)
-- Description : Initializes compute warehouse, analytical database, role-based
--               access control foundations, and multi-tier schemas.
-- ============================================================================

-- Switch to administrative role for infrastructure provisioning
USE ROLE ACCOUNTADMIN;

-- ----------------------------------------------------------------------------
-- 1. COMPUTE WAREHOUSE PROVISIONING
-- ----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated compute warehouse for GeoPulse geospatial transformations and queries';

-- Set session warehouse context
USE WAREHOUSE COMPUTE_WH;

-- ----------------------------------------------------------------------------
-- 2. ANALYTICAL DATABASE INITIALIZATION
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS GEOPULSE_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Core analytical database for GeoPulse retail mobility intelligence';

-- Set database context
USE DATABASE GEOPULSE_DB;

-- ----------------------------------------------------------------------------
-- 3. MEDALLION SCHEMA INITIALIZATION
-- ----------------------------------------------------------------------------

-- Bronze Layer: Raw immutable ingestion staging zone for telematics pings & geofence configs
CREATE SCHEMA IF NOT EXISTS GEOPULSE_DB.BRONZE
    DATA_RETENTION_TIME_IN_DAYS = 3
    COMMENT = 'Raw, unparsed telematics payloads, staging tables, and external file stages';

-- Silver Layer: Cleansed, validated, and spatially indexed mobility fact & dimension tables
CREATE SCHEMA IF NOT EXISTS GEOPULSE_DB.SILVER
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Cleansed tabular entities with H3 indices, GEOGRAPHY points, and validated catchments';

-- Gold Layer: Analytical marts, cannibalization metrics, and Kepler.gl visualization views
CREATE SCHEMA IF NOT EXISTS GEOPULSE_DB.GOLD
    DATA_RETENTION_TIME_IN_DAYS = 14
    COMMENT = 'Aggregated foot-traffic marts, store cannibalization matrices, and BI/viz models';

-- ----------------------------------------------------------------------------
-- 4. ROLE-BASED ACCESS CONTROL (RBAC) FOUNDATION
-- ----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS GEOPULSE_ENGINEER
    COMMENT = 'Data engineering role with full write access to GEOPULSE_DB schemas';

CREATE ROLE IF NOT EXISTS GEOPULSE_ANALYST
    COMMENT = 'BI & Analytics role with read-only access to Silver and Gold layers';

-- Grant warehouse privileges
GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE GEOPULSE_ENGINEER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE GEOPULSE_ANALYST;

-- Grant database privileges
GRANT ALL PRIVILEGES ON DATABASE GEOPULSE_DB TO ROLE GEOPULSE_ENGINEER;
GRANT USAGE ON DATABASE GEOPULSE_DB TO ROLE GEOPULSE_ANALYST;

-- Grant schema privileges for Engineer
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE GEOPULSE_DB TO ROLE GEOPULSE_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE GEOPULSE_DB TO ROLE GEOPULSE_ENGINEER;

-- Grant schema privileges for Analyst
GRANT USAGE ON SCHEMA GEOPULSE_DB.SILVER TO ROLE GEOPULSE_ANALYST;
GRANT USAGE ON SCHEMA GEOPULSE_DB.GOLD TO ROLE GEOPULSE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA GEOPULSE_DB.SILVER TO ROLE GEOPULSE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA GEOPULSE_DB.GOLD TO ROLE GEOPULSE_ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA GEOPULSE_DB.GOLD TO ROLE GEOPULSE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GEOPULSE_DB.SILVER TO ROLE GEOPULSE_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA GEOPULSE_DB.GOLD TO ROLE GEOPULSE_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA GEOPULSE_DB.GOLD TO ROLE GEOPULSE_ANALYST;

-- Establish role hierarchy
GRANT ROLE GEOPULSE_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE GEOPULSE_ANALYST TO ROLE SYSADMIN;

-- ----------------------------------------------------------------------------
-- 5. VERIFICATION & CONTEXT ASSERTIONS
-- ----------------------------------------------------------------------------
SHOW SCHEMAS IN DATABASE GEOPULSE_DB;
