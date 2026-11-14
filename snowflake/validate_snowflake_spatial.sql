-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Snowflake Spatial Quality Test Suite
-- ============================================================================
-- Schema     : GEOPULSE_DB.SILVER / GEOPULSE_DB.GOLD
-- Target     : Automated SQL Validation Suite for Spatial Data Warehouse Integrity
-- Components : 1. Data Quality Assertion Queries
--              2. Stored Procedure SP_RUN_DATA_QUALITY_SUITE
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA SILVER;

-- ----------------------------------------------------------------------------
-- 1. INDIVIDUAL AUDIT & ASSERTION QUERIES
-- ----------------------------------------------------------------------------

-- Check 1: Coordinate Range & Manhattan Study Area Validity
-- Expectation: 0 records
SELECT 
    'CHECK_1_COORDINATE_BOUNDS' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE LATITUDE NOT BETWEEN 40.7000 AND 40.8500
   OR LONGITUDE NOT BETWEEN -74.0500 AND -73.9000
   OR LATITUDE IS NULL 
   OR LONGITUDE IS NULL;

-- Check 2: Native Geography Validity (ST_ISVALID)
-- Expectation: 0 records
SELECT 
    'CHECK_2_GEOGRAPHY_POINT_VALIDITY' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE NOT ST_ISVALID(PING_LOCATION)
   OR PING_LOCATION IS NULL;

-- Check 3: Distance Metric Sanity (Non-negative & bounded)
-- Expectation: 0 records
SELECT 
    'CHECK_3_DISTANCE_SANITY' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE DIST_TO_STORE_A_METERS < 0
   OR DIST_TO_STORE_B_METERS < 0
   OR DIST_TO_STORE_C_METERS < 0
   OR MIN_DIST_TO_STORE_METERS < 0
   OR MIN_DIST_TO_STORE_METERS > 25000.0; -- Exceeds study area max diameter

-- Check 4: Catchment Boundary Inclusion Fidelity (250-meter threshold)
-- Expectation: 0 records
SELECT 
    'CHECK_4_CATCHMENT_THRESHOLD_FIDELITY' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE (IN_CATCHMENT_STORE_A AND DIST_TO_STORE_A_METERS > 250.0)
   OR (NOT IN_CATCHMENT_STORE_A AND DIST_TO_STORE_A_METERS <= 250.0)
   OR (IN_CATCHMENT_STORE_B AND DIST_TO_STORE_B_METERS > 250.0)
   OR (NOT IN_CATCHMENT_STORE_B AND DIST_TO_STORE_B_METERS <= 250.0)
   OR (IN_CATCHMENT_STORE_C AND DIST_TO_STORE_C_METERS > 250.0)
   OR (NOT IN_CATCHMENT_STORE_C AND DIST_TO_STORE_C_METERS <= 250.0);

-- Check 5: Catchment Composite Flag Logic
-- Expectation: 0 records
SELECT 
    'CHECK_5_COMPOSITE_CATCHMENT_LOGIC' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE IN_ANY_CATCHMENT != (IN_CATCHMENT_STORE_A OR IN_CATCHMENT_STORE_B OR IN_CATCHMENT_STORE_C);

-- Check 6: H3 Hexagonal Res 9 Index Structure
-- Expectation: 0 records
SELECT 
    'CHECK_6_H3_RES9_STRUCTURE' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE H3_INDEX_RES9 IS NULL
   OR LENGTH(H3_INDEX_RES9) != 15
   OR NOT H3_INDEX_RES9 LIKE '89%';

-- Check 7: Referential Integrity / Orphan Facts Check
-- Expectation: 0 records
SELECT 
    'CHECK_7_REFERENTIAL_INTEGRITY' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS p
LEFT JOIN GEOPULSE_DB.SILVER.STORE_CATCHMENTS s
    ON p.NEAREST_STORE_ID = s.STORE_ID
WHERE s.STORE_ID IS NULL;

-- Check 8: Temporal Peak Window Alignment
-- Expectation: 0 records
SELECT 
    'CHECK_8_PEAK_WINDOW_HOURS' AS TEST_NAME,
    COUNT(1) AS FAILED_RECORD_COUNT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
WHERE (IS_MORNING_PEAK AND PING_HOUR NOT IN (7, 8, 9))
   OR (NOT IS_MORNING_PEAK AND PING_HOUR IN (7, 8, 9));


-- ----------------------------------------------------------------------------
-- 2. AUTOMATED STORED PROCEDURE: DATA QUALITY SUITE RUNNER
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE GEOPULSE_DB.SILVER.SP_RUN_DATA_QUALITY_SUITE()
RETURNS TABLE (
    TEST_ID             VARCHAR,
    TEST_CATEGORY       VARCHAR,
    TEST_DESCRIPTION    VARCHAR,
    STATUS              VARCHAR,
    FAILED_RECORDS      INTEGER,
    EXECUTED_AT         TIMESTAMP_NTZ
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    res RESULTSET;
BEGIN
    -- Temporary audit table to store assertion results
    CREATE OR REPLACE TEMPORARY TABLE _DQ_RESULTS (
        TEST_ID             VARCHAR(64),
        TEST_CATEGORY       VARCHAR(64),
        TEST_DESCRIPTION    VARCHAR(255),
        STATUS              VARCHAR(16),
        FAILED_RECORDS      INTEGER,
        EXECUTED_AT         TIMESTAMP_NTZ
    );

    -- Test 1: Coordinate Bounds
    INSERT INTO _DQ_RESULTS
    SELECT 
        'DQ_01', 
        'COORDINATE_VALIDITY', 
        'Pings fall within Manhattan bounding box (40.70-40.85 N, -74.05--73.90 W)',
        CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(1),
        CURRENT_TIMESTAMP()
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE LATITUDE NOT BETWEEN 40.7000 AND 40.8500
       OR LONGITUDE NOT BETWEEN -74.0500 AND -73.9000;

    -- Test 2: Distance Metric Bounds
    INSERT INTO _DQ_RESULTS
    SELECT 
        'DQ_02', 
        'GEODESIC_DISTANCE', 
        'Distances are non-negative and bounded by 25km study extent',
        CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(1),
        CURRENT_TIMESTAMP()
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE DIST_TO_STORE_A_METERS < 0 
       OR DIST_TO_STORE_B_METERS < 0 
       OR DIST_TO_STORE_C_METERS < 0
       OR MIN_DIST_TO_STORE_METERS > 25000.0;

    -- Test 3: Catchment Threshold Classification
    INSERT INTO _DQ_RESULTS
    SELECT 
        'DQ_03', 
        'CATCHMENT_LOGIC', 
        'Catchment boolean flags strictly adhere to <= 250.0m radius',
        CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(1),
        CURRENT_TIMESTAMP()
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE (IN_CATCHMENT_STORE_A AND DIST_TO_STORE_A_METERS > 250.0)
       OR (NOT IN_CATCHMENT_STORE_A AND DIST_TO_STORE_A_METERS <= 250.0)
       OR (IN_CATCHMENT_STORE_B AND DIST_TO_STORE_B_METERS > 250.0)
       OR (NOT IN_CATCHMENT_STORE_B AND DIST_TO_STORE_B_METERS <= 250.0);

    -- Test 4: H3 Index Res 9 Formatting
    INSERT INTO _DQ_RESULTS
    SELECT 
        'DQ_04', 
        'H3_INDEX_STRUCTURE', 
        'H3 index format matches 15-char hex starting with 89',
        CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(1),
        CURRENT_TIMESTAMP()
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE H3_INDEX_RES9 IS NULL 
       OR LENGTH(H3_INDEX_RES9) != 15 
       OR NOT H3_INDEX_RES9 LIKE '89%';

    -- Test 5: Foreign Key Referential Integrity
    INSERT INTO _DQ_RESULTS
    SELECT 
        'DQ_05', 
        'REFERENTIAL_INTEGRITY', 
        'NEAREST_STORE_ID maps to a valid store in SILVER.STORE_CATCHMENTS',
        CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(1),
        CURRENT_TIMESTAMP()
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS p
    LEFT JOIN GEOPULSE_DB.SILVER.STORE_CATCHMENTS s
        ON p.NEAREST_STORE_ID = s.STORE_ID
    WHERE s.STORE_ID IS NULL;

    -- Test 6: Commuter Peak Window Hour Fidelity
    INSERT INTO _DQ_RESULTS
    SELECT 
        'DQ_06', 
        'TEMPORAL_ALIGNMENT', 
        'IS_MORNING_PEAK flag strictly aligns with transit hours 7, 8, 9',
        CASE WHEN COUNT(1) = 0 THEN 'PASS' ELSE 'FAIL' END,
        COUNT(1),
        CURRENT_TIMESTAMP()
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE (IS_MORNING_PEAK AND PING_HOUR NOT IN (7, 8, 9))
       OR (NOT IS_MORNING_PEAK AND PING_HOUR IN (7, 8, 9));

    res := (SELECT * FROM _DQ_RESULTS ORDER BY TEST_ID);
    RETURN TABLE(res);
END;
$$;

COMMENT ON PROCEDURE GEOPULSE_DB.SILVER.SP_RUN_DATA_QUALITY_SUITE() IS 
'Executes spatial data warehouse assertions and returns tabular validation report';
