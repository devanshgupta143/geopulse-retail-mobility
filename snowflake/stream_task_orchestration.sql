-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Real-Time Stream & Task CDC Pipeline
-- ============================================================================
-- Schema     : GEOPULSE_DB.BRONZE / GEOPULSE_DB.SILVER / GEOPULSE_DB.GOLD
-- Target     : Change Data Capture (CDC) Streams & Automated Triggered Tasks
-- Components : 1. Append-Only Stream on Raw Pings
--              2. Automated Incremental Transformation Task (Silver)
--              3. Downstream Gold Aggregation DAG Task
--              4. Monitoring & Task History Views
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;

-- ----------------------------------------------------------------------------
-- 1. CHANGE DATA CAPTURE (CDC) STREAM DEFINITION
-- ----------------------------------------------------------------------------
-- Append-only stream tracking freshly ingested mobility pings in the Bronze layer.
-- APPEND_ONLY = TRUE optimizes metadata overhead for high-volume append streams.
CREATE OR REPLACE STREAM GEOPULSE_DB.BRONZE.RAW_PINGS_STREAM
    ON TABLE GEOPULSE_DB.BRONZE.RAW_GPS_PINGS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = FALSE
    COMMENT = 'CDC stream tracking new telematics ping batches landed in Bronze';


-- ----------------------------------------------------------------------------
-- 2. INCREMENTAL SPATIAL ELT TASK (SILVER LAYER)
-- ----------------------------------------------------------------------------
-- Triggered every 5 minutes ONLY when fresh ping records exist in the stream.
-- Ingests and spatially enriches stream deltas into SILVER.SPATIAL_PINGS.
CREATE OR REPLACE TASK GEOPULSE_DB.SILVER.TASK_PROCESS_CDC_PINGS
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('GEOPULSE_DB.BRONZE.RAW_PINGS_STREAM')
    COMMENT = 'Automated CDC task processing stream delta records into Silver spatial fact'
AS
MERGE INTO GEOPULSE_DB.SILVER.SPATIAL_PINGS AS target
USING (
    WITH stream_delta AS (
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
        FROM GEOPULSE_DB.BRONZE.RAW_PINGS_STREAM
        WHERE METADATA$ACTION = 'INSERT'
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
            -- Geodesic Great-Circle Distances (Meters)
            ST_DISTANCE(r.PING_LOC, a.LOC_A) AS DIST_A,
            ST_DISTANCE(r.PING_LOC, a.LOC_B) AS DIST_B,
            ST_DISTANCE(r.PING_LOC, a.LOC_C) AS DIST_C
        FROM stream_delta r
        CROSS JOIN store_anchors a
    )
    SELECT
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
        ROUND(DIST_A, 2) AS DIST_TO_STORE_A_METERS,
        ROUND(DIST_B, 2) AS DIST_TO_STORE_B_METERS,
        ROUND(DIST_C, 2) AS DIST_TO_STORE_C_METERS,
        (DIST_A <= 250.0) AS IN_CATCHMENT_STORE_A,
        (DIST_B <= 250.0) AS IN_CATCHMENT_STORE_B,
        (DIST_C <= 250.0) AS IN_CATCHMENT_STORE_C,
        (DIST_A <= 250.0 OR DIST_B <= 250.0 OR DIST_C <= 250.0) AS IN_ANY_CATCHMENT,
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
        target.H3_INDEX_RES9 = source.H3_INDEX_RES9,
        target.DIST_TO_STORE_A_METERS = source.DIST_TO_STORE_A_METERS,
        target.DIST_TO_STORE_B_METERS = source.DIST_TO_STORE_B_METERS,
        target.DIST_TO_STORE_C_METERS = source.DIST_TO_STORE_C_METERS,
        target.IN_CATCHMENT_STORE_A = source.IN_CATCHMENT_STORE_A,
        target.IN_CATCHMENT_STORE_B = source.IN_CATCHMENT_STORE_B,
        target.IN_CATCHMENT_STORE_C = source.IN_CATCHMENT_STORE_C,
        target.IN_ANY_CATCHMENT = source.IN_ANY_CATCHMENT,
        target.NEAREST_STORE_ID = source.NEAREST_STORE_ID,
        target.MIN_DIST_TO_STORE_METERS = source.MIN_DIST_TO_STORE_METERS,
        target.INGESTION_TIMESTAMP = source.INGESTION_TIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        PING_ID, DEVICE_ID, LATITUDE, LONGITUDE, PING_LOCATION,
        H3_INDEX_RES9, H3_INDEX_RES8, PING_TIMESTAMP, PING_DATE,
        PING_HOUR, IS_MORNING_PEAK, DIST_TO_STORE_A_METERS,
        DIST_TO_STORE_B_METERS, DIST_TO_STORE_C_METERS,
        IN_CATCHMENT_STORE_A, IN_CATCHMENT_STORE_B, IN_CATCHMENT_STORE_C,
        IN_ANY_CATCHMENT, NEAREST_STORE_ID, MIN_DIST_TO_STORE_METERS,
        INGESTION_TIMESTAMP
    )
    VALUES (
        source.PING_ID, source.DEVICE_ID, source.LATITUDE, source.LONGITUDE, source.PING_LOCATION,
        source.H3_INDEX_RES9, source.H3_INDEX_RES8, source.PING_TIMESTAMP, source.PING_DATE,
        source.PING_HOUR, source.IS_MORNING_PEAK, source.DIST_TO_STORE_A_METERS,
        source.DIST_TO_STORE_B_METERS, source.DIST_TO_STORE_C_METERS,
        source.IN_CATCHMENT_STORE_A, source.IN_CATCHMENT_STORE_B, source.IN_CATCHMENT_STORE_C,
        source.IN_ANY_CATCHMENT, source.NEAREST_STORE_ID, source.MIN_DIST_TO_STORE_METERS,
        source.INGESTION_TIMESTAMP
    );


-- ----------------------------------------------------------------------------
-- 3. DOWNSTREAM DAG TASK (GOLD REFRESH LOGGING)
-- ----------------------------------------------------------------------------
-- Dependent task that executes automatically after the Silver CDC task succeeds.
CREATE OR REPLACE TASK GEOPULSE_DB.GOLD.TASK_REFRESH_CANNIBALIZATION_MART
    WAREHOUSE = COMPUTE_WH
    AFTER GEOPULSE_DB.SILVER.TASK_PROCESS_CDC_PINGS
    COMMENT = 'Downstream DAG task triggered after Silver CDC completes to log mart readiness'
AS
    INSERT INTO GEOPULSE_DB.GOLD.MART_REFRESH_LOG (MART_NAME, REFRESH_STATUS, LOGGED_AT)
    VALUES ('GOLD.STORE_CANNIBALIZATION_SUMMARY', 'REFRESHED_VIA_CDC_DAG', CURRENT_TIMESTAMP());

-- Operational log table for DAG execution monitoring
CREATE TABLE IF NOT EXISTS GEOPULSE_DB.GOLD.MART_REFRESH_LOG (
    LOG_ID          INTEGER AUTOINCREMENT,
    MART_NAME       VARCHAR(128),
    REFRESH_STATUS  VARCHAR(64),
    LOGGED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- ----------------------------------------------------------------------------
-- 4. TASK ACTIVATION & MONITORING COMMANDS
-- ----------------------------------------------------------------------------
-- Tasks are created suspended by default. Enable root task and child task:
-- ALTER TASK GEOPULSE_DB.GOLD.TASK_REFRESH_CANNIBALIZATION_MART RESUME;
-- ALTER TASK GEOPULSE_DB.SILVER.TASK_PROCESS_CDC_PINGS RESUME;

-- Query recent task execution history:
SELECT 
    NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATE,
    ERROR_CODE,
    ERROR_MESSAGE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    DATEDIFF('millisecond', SCHEDULED_TIME, COMPLETED_TIME) AS RUNTIME_MS
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_CDC_PINGS',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
