-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Bronze to Silver Spatial ELT
-- ============================================================================
-- Schema     : GEOPULSE_DB.SILVER
-- Target     : Automated Stored Procedure for Spatial Ping Transformation
-- Components : Stored Procedure SP_TRANSFORM_BRONZE_TO_SILVER, Merge Pipeline,
--              Automated Snowflake Task Definition
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA SILVER;

-- ----------------------------------------------------------------------------
-- 1. STORED PROCEDURE DEFINITION
-- ----------------------------------------------------------------------------
-- Performs idempotent batch ingestion and geospatial transformation from
-- BRONZE.RAW_GPS_PINGS into SILVER.SPATIAL_PINGS.
CREATE OR REPLACE PROCEDURE GEOPULSE_DB.SILVER.SP_TRANSFORM_BRONZE_TO_SILVER(
    TARGET_DATE VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_updated    INTEGER DEFAULT 0;
    start_time      TIMESTAMP_NTZ;
    end_time        TIMESTAMP_NTZ;
    execution_msg   VARCHAR;
BEGIN
    start_time := CURRENT_TIMESTAMP();

    -- Execute Idempotent Merge into Silver Spatial Fact Table
    MERGE INTO GEOPULSE_DB.SILVER.SPATIAL_PINGS AS target
    USING (
        WITH raw_pings AS (
            SELECT
                DEVICE_ID,
                LATITUDE,
                LONGITUDE,
                PING_TIMESTAMP,
                PING_TIMESTAMP::DATE AS P_DATE,
                HOUR(PING_TIMESTAMP) AS P_HOUR,
                (HOUR(PING_TIMESTAMP) BETWEEN 7 AND 9) AS IS_PEAK,
                ST_POINT(LONGITUDE, LATITUDE) AS PING_LOC,
                -- Native Snowflake H3 Indexing
                H3_POINT_TO_CELL_STRING(ST_POINT(LONGITUDE, LATITUDE), 9) AS H3_RES9,
                H3_POINT_TO_CELL_STRING(ST_POINT(LONGITUDE, LATITUDE), 8) AS H3_RES8
            FROM GEOPULSE_DB.BRONZE.RAW_GPS_PINGS
            WHERE (:TARGET_DATE IS NULL OR PING_TIMESTAMP::DATE = :TARGET_DATE::DATE)
        ),
        store_anchors AS (
            SELECT
                MAX(CASE WHEN STORE_ID = 'STORE_A_EXISTING' THEN STORE_LOCATION END) AS LOC_A,
                MAX(CASE WHEN STORE_ID = 'STORE_B_PROPOSED_5TH' THEN STORE_LOCATION END) AS LOC_B,
                MAX(CASE WHEN STORE_ID = 'STORE_C_ALT_WEST' THEN STORE_LOCATION END) AS LOC_C
            FROM GEOPULSE_DB.SILVER.STORE_CATCHMENTS
        ),
        geodesic_enrichment AS (
            SELECT
                r.DEVICE_ID,
                r.LATITUDE,
                r.LONGITUDE,
                r.PING_LOC,
                r.H3_RES9,
                r.H3_RES8,
                r.PING_TIMESTAMP,
                r.P_DATE,
                r.P_HOUR,
                r.IS_PEAK,
                -- Native Snowflake Great-Circle Geodesic Distances (Meters)
                ST_DISTANCE(r.PING_LOC, a.LOC_A) AS DIST_A,
                ST_DISTANCE(r.PING_LOC, a.LOC_B) AS DIST_B,
                ST_DISTANCE(r.PING_LOC, a.LOC_C) AS DIST_C
            FROM raw_pings r
            CROSS JOIN store_anchors a
        )
        SELECT
            -- Deterministic MD5 Primary Key
            MD5(CONCAT(DEVICE_ID, ':', TO_VARCHAR(PING_TIMESTAMP, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'))) AS PING_ID,
            DEVICE_ID,
            LATITUDE,
            LONGITUDE,
            PING_LOC AS PING_LOCATION,
            H3_RES9 AS H3_INDEX_RES9,
            H3_RES8 AS H3_INDEX_RES8,
            PING_TIMESTAMP,
            P_DATE AS PING_DATE,
            P_HOUR AS PING_HOUR,
            IS_PEAK AS IS_MORNING_PEAK,
            
            -- Store Distance Metrics
            ROUND(DIST_A, 2) AS DIST_TO_STORE_A_METERS,
            ROUND(DIST_B, 2) AS DIST_TO_STORE_B_METERS,
            ROUND(DIST_C, 2) AS DIST_TO_STORE_C_METERS,
            
            -- Catchment Inclusions (250m threshold)
            (DIST_A <= 250.0) AS IN_CATCHMENT_STORE_A,
            (DIST_B <= 250.0) AS IN_CATCHMENT_STORE_B,
            (DIST_C <= 250.0) AS IN_CATCHMENT_STORE_C,
            (DIST_A <= 250.0 OR DIST_B <= 250.0 OR DIST_C <= 250.0) AS IN_ANY_CATCHMENT,
            
            -- Nearest Store Association
            CASE 
                WHEN DIST_A <= LEAST(DIST_B, DIST_C) THEN 'STORE_A_EXISTING'
                WHEN DIST_B <= LEAST(DIST_A, DIST_C) THEN 'STORE_B_PROPOSED_5TH'
                ELSE 'STORE_C_ALT_WEST'
            END AS NEAREST_STORE_ID,
            ROUND(LEAST(DIST_A, DIST_B, DIST_C), 2) AS MIN_DIST_TO_STORE_METERS,
            CURRENT_TIMESTAMP() AS INGESTION_TIMESTAMP
        FROM geodesic_enrichment
    ) AS source
    ON target.PING_ID = source.PING_ID
    WHEN MATCHED THEN
        UPDATE SET
            target.H3_INDEX_RES9          = source.H3_INDEX_RES9,
            target.DIST_TO_STORE_A_METERS = source.DIST_TO_STORE_A_METERS,
            target.DIST_TO_STORE_B_METERS = source.DIST_TO_STORE_B_METERS,
            target.DIST_TO_STORE_C_METERS = source.DIST_TO_STORE_C_METERS,
            target.IN_CATCHMENT_STORE_A   = source.IN_CATCHMENT_STORE_A,
            target.IN_CATCHMENT_STORE_B   = source.IN_CATCHMENT_STORE_B,
            target.IN_CATCHMENT_STORE_C   = source.IN_CATCHMENT_STORE_C,
            target.IN_ANY_CATCHMENT       = source.IN_ANY_CATCHMENT,
            target.NEAREST_STORE_ID       = source.NEAREST_STORE_ID,
            target.MIN_DIST_TO_STORE_METERS = source.MIN_DIST_TO_STORE_METERS,
            target.INGESTION_TIMESTAMP    = source.INGESTION_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            PING_ID,
            DEVICE_ID,
            LATITUDE,
            LONGITUDE,
            PING_LOCATION,
            H3_INDEX_RES9,
            H3_INDEX_RES8,
            PING_TIMESTAMP,
            PING_DATE,
            PING_HOUR,
            IS_MORNING_PEAK,
            DIST_TO_STORE_A_METERS,
            DIST_TO_STORE_B_METERS,
            DIST_TO_STORE_C_METERS,
            IN_CATCHMENT_STORE_A,
            IN_CATCHMENT_STORE_B,
            IN_CATCHMENT_STORE_C,
            IN_ANY_CATCHMENT,
            NEAREST_STORE_ID,
            MIN_DIST_TO_STORE_METERS,
            INGESTION_TIMESTAMP
        )
        VALUES (
            source.PING_ID,
            source.DEVICE_ID,
            source.LATITUDE,
            source.LONGITUDE,
            source.PING_LOCATION,
            source.H3_INDEX_RES9,
            source.H3_INDEX_RES8,
            source.PING_TIMESTAMP,
            source.PING_DATE,
            source.PING_HOUR,
            source.IS_MORNING_PEAK,
            source.DIST_TO_STORE_A_METERS,
            source.DIST_TO_STORE_B_METERS,
            source.DIST_TO_STORE_C_METERS,
            source.IN_CATCHMENT_STORE_A,
            source.IN_CATCHMENT_STORE_B,
            source.IN_CATCHMENT_STORE_C,
            source.IN_ANY_CATCHMENT,
            source.NEAREST_STORE_ID,
            source.MIN_DIST_TO_STORE_METERS,
            source.INGESTION_TIMESTAMP
        );

    rows_inserted := SQLROWCOUNT;
    end_time := CURRENT_TIMESTAMP();

    execution_msg := OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'target_date', NVL(:TARGET_DATE, 'ALL_AVAILABLE'),
        'rows_merged', rows_inserted,
        'start_time', start_time,
        'end_time', end_time,
        'duration_seconds', DATEDIFF('millisecond', start_time, end_time) / 1000.0
    )::VARCHAR;

    RETURN execution_msg;
END;
$$;

COMMENT ON PROCEDURE GEOPULSE_DB.SILVER.SP_TRANSFORM_BRONZE_TO_SILVER(VARCHAR) IS 
'Transforms raw GPS pings into Silver spatial pings using native H3 indexing and geodesic distance calculations';


-- ----------------------------------------------------------------------------
-- 2. AUTOMATED SNOWFLAKE TASK ORCHESTRATION
-- ----------------------------------------------------------------------------
-- Hourly automated trigger executing the spatial ELT transformation
CREATE OR REPLACE TASK GEOPULSE_DB.SILVER.TASK_HOURLY_SPATIAL_ELT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 MINUTE'
    COMMENT = 'Hourly orchestration task triggering bronze-to-silver spatial transformation'
AS
    CALL GEOPULSE_DB.SILVER.SP_TRANSFORM_BRONZE_TO_SILVER(NULL);

-- Tasks are created in SUSPENDED state by default. Enable when production pipeline is active:
-- ALTER TASK GEOPULSE_DB.SILVER.TASK_HOURLY_SPATIAL_ELT RESUME;
