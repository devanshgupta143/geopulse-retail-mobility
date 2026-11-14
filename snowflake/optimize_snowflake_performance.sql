-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Performance Optimization & Clustering
-- ============================================================================
-- Schema     : GEOPULSE_DB.SILVER
-- Target     : Micro-Partition Clustering, Search Optimization Service, Pruning
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA SILVER;

-- ----------------------------------------------------------------------------
-- 1. CLUSTERING STRATEGY: SPATIAL & TEMPORAL CO-LOCATION
-- ----------------------------------------------------------------------------
-- Co-locates mobile pings by Date and H3 Hexagon Cell (Res 9) to maximize
-- micro-partition pruning for spatiotemporal range queries.
ALTER TABLE GEOPULSE_DB.SILVER.SPATIAL_PINGS 
    CLUSTER BY (PING_DATE, H3_INDEX_RES9);

-- Enable Automatic Clustering on the Fact Table
ALTER TABLE GEOPULSE_DB.SILVER.SPATIAL_PINGS RESUME RECLUSTER;

-- Diagnostics: Check current clustering depth and overlap metrics
SELECT SYSTEM$CLUSTERING_INFORMATION('GEOPULSE_DB.SILVER.SPATIAL_PINGS', '(PING_DATE, H3_INDEX_RES9)') 
    AS CLUSTERING_METRICS;

-- Diagnostic Query: Clustered partition pruning efficiency verification
SELECT 
    PING_DATE,
    H3_INDEX_RES9,
    COUNT(1) AS PING_COUNT,
    COUNT(DISTINCT DEVICE_ID) AS UNIQUE_DEVICES
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE PING_DATE = '2026-11-01'
  AND H3_INDEX_RES9 = '892a100d66bffff'
GROUP BY PING_DATE, H3_INDEX_RES9;


-- ----------------------------------------------------------------------------
-- 2. SEARCH OPTIMIZATION SERVICE (SOS) CONFIGURATION
-- ----------------------------------------------------------------------------
-- Accelerates point-lookup queries on high-cardinality device UUIDs,
-- discrete H3 hexagonal indices, and native GEOGRAPHY spatial predicates (ST_INTERSECTS, ST_DWITHIN).
ALTER TABLE GEOPULSE_DB.SILVER.SPATIAL_PINGS 
    ADD SEARCH OPTIMIZATION ON 
        EQUALITY(DEVICE_ID, H3_INDEX_RES9, NEAREST_STORE_ID),
        GEO(PING_LOCATION);

-- Verify Search Optimization configuration and indexing progress
DESCRIBE SEARCH OPTIMIZATION ON GEOPULSE_DB.SILVER.SPATIAL_PINGS;

-- Diagnostic Query: Optimized Point Lookup by Device ID
SELECT 
    DEVICE_ID,
    PING_TIMESTAMP,
    LATITUDE,
    LONGITUDE,
    H3_INDEX_RES9,
    NEAREST_STORE_ID,
    MIN_DIST_TO_STORE_METERS
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE DEVICE_ID = '8826d916-cdfb-41c6-81ff-91a761565a70'
ORDER BY PING_TIMESTAMP ASC;


-- ----------------------------------------------------------------------------
-- 3. WAREHOUSE ACCELERATION & CACHING POLICIES
-- ----------------------------------------------------------------------------
-- Tune COMPUTE_WH for geospatial analytical queries
ALTER WAREHOUSE COMPUTE_WH SET
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 4
    STATEMENT_TIMEOUT_IN_SECONDS = 1800
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Tuned warehouse with Query Acceleration Service for heavy spatial analytics';

-- Enable result caching verification
ALTER SESSION SET USE_CACHED_RESULT = TRUE;


-- ----------------------------------------------------------------------------
-- 4. MICRO-PARTITION PRUNING & RECLUSTER COST AUDIT
-- ----------------------------------------------------------------------------
-- Audits the clustering depth over time
CREATE OR REPLACE VIEW GEOPULSE_DB.SILVER.VW_CLUSTERING_AUDIT AS
SELECT 
    'SILVER.SPATIAL_PINGS' AS TABLE_NAME,
    PARSE_JSON(SYSTEM$CLUSTERING_INFORMATION('GEOPULSE_DB.SILVER.SPATIAL_PINGS', '(PING_DATE, H3_INDEX_RES9)')) AS CLUSTERING_DATA,
    CURRENT_TIMESTAMP() AS AUDIT_TIMESTAMP;

COMMENT ON VIEW GEOPULSE_DB.SILVER.VW_CLUSTERING_AUDIT IS 
'Operational audit view monitoring micro-partition depth and spatial clustering efficiency';
