-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Silver Layer Spatial Tables DDL
-- ============================================================================
-- Schema     : GEOPULSE_DB.SILVER
-- Target     : Dimension & Fact Tables with Native GEOGRAPHY & H3 Hexagonal Grid
-- Tables     : 1. SILVER.STORE_CATCHMENTS (Dimension)
--              2. SILVER.SPATIAL_PINGS    (Fact)
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA SILVER;

-- ----------------------------------------------------------------------------
-- 1. SPATIAL DIMENSION TABLE: STORE CATCHMENTS
-- ----------------------------------------------------------------------------
-- Stores retail footprint anchors, point coordinates, target 250m catchment
-- circular polygons, and H3 resolution 9 cell attributes.
CREATE OR REPLACE TABLE GEOPULSE_DB.SILVER.STORE_CATCHMENTS (
    -- Primary Identifier
    STORE_ID                    VARCHAR(64)         NOT NULL,
    STORE_NAME                  VARCHAR(128)        NOT NULL,
    STATUS                      VARCHAR(32)         NOT NULL COMMENT 'Operational status: EXISTING, PROPOSED, ALTERNATIVE',
    
    -- Coordinates & Native Geography Types
    LATITUDE                    FLOAT               NOT NULL,
    LONGITUDE                   FLOAT               NOT NULL,
    STORE_LOCATION              GEOGRAPHY           NOT NULL COMMENT 'WGS84 Point geography: ST_POINT(lon, lat)',
    
    -- Spatial Catchment Configuration
    CATCHMENT_RADIUS_METERS     NUMBER(10, 2)       NOT NULL DEFAULT 250.0 COMMENT 'Target foot-traffic catchment buffer (250m)',
    CATCHMENT_POLYGON           GEOGRAPHY                    COMMENT 'Buffered circular catchment zone: ST_BUFFER(location, 250)',
    
    -- Discrete Hexagonal Spatial Index
    H3_CENTROID_RES9            VARCHAR(15)                  COMMENT 'H3 Resolution 9 index containing the store anchor',
    
    -- Location Metadata
    ADDRESS                     VARCHAR(255),
    BOROUGH                     VARCHAR(64)         DEFAULT 'Manhattan',
    NEIGHBORHOOD                VARCHAR(128),
    
    -- Audit Timestamps
    CREATED_AT                  TIMESTAMP_NTZ       DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                  TIMESTAMP_NTZ       DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT PK_STORE_CATCHMENTS PRIMARY KEY (STORE_ID)
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Dimension table defining retail store spatial anchors, 250m catchment buffers, and metadata';

-- Seed the 3 Manhattan retail stores into the dimension table
INSERT OVERWRITE INTO GEOPULSE_DB.SILVER.STORE_CATCHMENTS (
    STORE_ID,
    STORE_NAME,
    STATUS,
    LATITUDE,
    LONGITUDE,
    STORE_LOCATION,
    CATCHMENT_RADIUS_METERS,
    CATCHMENT_POLYGON,
    H3_CENTROID_RES9,
    ADDRESS,
    BOROUGH,
    NEIGHBORHOOD
)
VALUES
    (
        'STORE_A_EXISTING',
        'Herald Square Flagship',
        'EXISTING',
        40.754900,
        -73.984000,
        ST_POINT(-73.984000, 40.754900),
        250.0,
        ST_BUFFER(ST_POINT(-73.984000, 40.754900), 250.0),
        '892a100d66bffff',
        '151 W 34th St, New York, NY 10001',
        'Manhattan',
        'Herald Square / Midtown South'
    ),
    (
        'STORE_B_PROPOSED_5TH',
        'Fifth Avenue Luxury Corridor',
        'PROPOSED',
        40.756500,
        -73.981500,
        ST_POINT(-73.981500, 40.756500),
        250.0,
        ST_BUFFER(ST_POINT(-73.981500, 40.756500), 250.0),
        '892a100d66fffff',
        '500 5th Ave, New York, NY 10110',
        'Manhattan',
        'Midtown East / 5th Ave'
    ),
    (
        'STORE_C_ALT_WEST',
        'West 34th District Alternative',
        'ALTERNATIVE',
        40.751000,
        -73.993000,
        ST_POINT(-73.993000, 40.751000),
        250.0,
        ST_BUFFER(ST_POINT(-73.993000, 40.751000), 250.0),
        '892a100d2d3ffff',
        '400 W 34th St, New York, NY 10001',
        'Manhattan',
        'Hudson Yards / Penn Station West'
    );

-- ----------------------------------------------------------------------------
-- 2. SPATIAL FACT TABLE: SPATIAL PINGS
-- ----------------------------------------------------------------------------
-- Cleansed and enriched mobile ping records containing native Snowflake
-- GEOGRAPHY points, H3 Resolution 9 indices, geodesic distances, and catchment flags.
CREATE OR REPLACE TABLE GEOPULSE_DB.SILVER.SPATIAL_PINGS (
    -- Surrogate & Natural Identifiers
    PING_ID                     VARCHAR(64)         NOT NULL COMMENT 'Deterministic MD5/SHA2 hash or UUID identifier',
    DEVICE_ID                   VARCHAR(64)         NOT NULL COMMENT 'Mobile device UUID identifier',
    
    -- Spatial Geography & H3 Attributes
    LATITUDE                    FLOAT               NOT NULL COMMENT 'WGS84 Latitude coordinate',
    LONGITUDE                   FLOAT               NOT NULL COMMENT 'WGS84 Longitude coordinate',
    PING_LOCATION               GEOGRAPHY           NOT NULL COMMENT 'Native Snowflake point: ST_POINT(lon, lat)',
    H3_INDEX_RES9               VARCHAR(15)         NOT NULL COMMENT 'H3 Resolution 9 hexagonal grid index (~100m edge)',
    H3_INDEX_RES8               VARCHAR(15)                  COMMENT 'H3 Resolution 8 parent hexagonal index (~460m edge)',
    
    -- Temporal Dimensions & Commuter Peak Classification
    PING_TIMESTAMP              TIMESTAMP_NTZ       NOT NULL COMMENT 'Event timestamp (2026-11-01)',
    PING_DATE                   DATE                NOT NULL COMMENT 'Extracted date partition key',
    PING_HOUR                   NUMBER(2, 0)        NOT NULL COMMENT 'Hour of day (0-23)',
    IS_MORNING_PEAK             BOOLEAN             NOT NULL COMMENT 'Flag for morning transit cannibalization window (07:00-09:59)',
    
    -- Geodesic Distance Computations (Meters)
    DIST_TO_STORE_A_METERS      FLOAT                        COMMENT 'Great-circle distance to STORE_A_EXISTING (Herald Sq)',
    DIST_TO_STORE_B_METERS      FLOAT                        COMMENT 'Great-circle distance to STORE_B_PROPOSED_5TH (5th Ave)',
    DIST_TO_STORE_C_METERS      FLOAT                        COMMENT 'Great-circle distance to STORE_C_ALT_WEST (West 34th)',
    
    -- Catchment Threshold Flags (250m)
    IN_CATCHMENT_STORE_A        BOOLEAN             NOT NULL DEFAULT FALSE COMMENT 'Distance to Store A <= 250m',
    IN_CATCHMENT_STORE_B        BOOLEAN             NOT NULL DEFAULT FALSE COMMENT 'Distance to Store B <= 250m',
    IN_CATCHMENT_STORE_C        BOOLEAN             NOT NULL DEFAULT FALSE COMMENT 'Distance to Store C <= 250m',
    IN_ANY_CATCHMENT            BOOLEAN             NOT NULL DEFAULT FALSE COMMENT 'Falls within at least one store catchment',
    
    -- Nearest Store Association & Metrics
    NEAREST_STORE_ID            VARCHAR(64)                  COMMENT 'Foreign key identifier of the closest retail store',
    MIN_DIST_TO_STORE_METERS    FLOAT                        COMMENT 'Distance in meters to nearest store',
    
    -- Audit Metadata
    INGESTION_TIMESTAMP         TIMESTAMP_NTZ       DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT PK_SPATIAL_PINGS PRIMARY KEY (PING_ID),
    CONSTRAINT FK_SPATIAL_PINGS_STORE FOREIGN KEY (NEAREST_STORE_ID)
        REFERENCES GEOPULSE_DB.SILVER.STORE_CATCHMENTS (STORE_ID)
)
CLUSTER BY (PING_DATE, H3_INDEX_RES9)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Cleaned and enriched mobile ping fact table with H3 indices and catchment metrics';

-- ----------------------------------------------------------------------------
-- 3. BRONZE TO SILVER ELT TRANSFORMATION PIPELINE (TEMPLATE)
-- ----------------------------------------------------------------------------
/*
INSERT INTO GEOPULSE_DB.SILVER.SPATIAL_PINGS (
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
    MIN_DIST_TO_STORE_METERS
)
WITH raw_stream AS (
    SELECT
        DEVICE_ID,
        LATITUDE,
        LONGITUDE,
        PING_TIMESTAMP,
        ST_POINT(LONGITUDE, LATITUDE) AS ping_loc,
        -- Native Snowflake H3 Indexing Function (or parsed from enrichment)
        H3_POINT_TO_CELL_STRING(ST_POINT(LONGITUDE, LATITUDE), 9) AS h3_res9,
        H3_POINT_TO_CELL_STRING(ST_POINT(LONGITUDE, LATITUDE), 8) AS h3_res8,
        PING_TIMESTAMP::DATE AS p_date,
        HOUR(PING_TIMESTAMP) AS p_hour,
        (HOUR(PING_TIMESTAMP) BETWEEN 7 AND 9) AS is_peak
    FROM GEOPULSE_DB.BRONZE.RAW_GPS_PINGS
),
spatial_distances AS (
    SELECT
        r.*,
        ST_DISTANCE(r.ping_loc, (SELECT STORE_LOCATION FROM GEOPULSE_DB.SILVER.STORE_CATCHMENTS WHERE STORE_ID = 'STORE_A_EXISTING')) AS d_a,
        ST_DISTANCE(r.ping_loc, (SELECT STORE_LOCATION FROM GEOPULSE_DB.SILVER.STORE_CATCHMENTS WHERE STORE_ID = 'STORE_B_PROPOSED_5TH')) AS d_b,
        ST_DISTANCE(r.ping_loc, (SELECT STORE_LOCATION FROM GEOPULSE_DB.SILVER.STORE_CATCHMENTS WHERE STORE_ID = 'STORE_C_ALT_WEST')) AS d_c
    FROM raw_stream r
)
SELECT
    MD5(CONCAT(DEVICE_ID, PING_TIMESTAMP::VARCHAR)) AS PING_ID,
    DEVICE_ID,
    LATITUDE,
    LONGITUDE,
    ping_loc AS PING_LOCATION,
    h3_res9 AS H3_INDEX_RES9,
    h3_res8 AS H3_INDEX_RES8,
    PING_TIMESTAMP,
    p_date AS PING_DATE,
    p_hour AS PING_HOUR,
    is_peak AS IS_MORNING_PEAK,
    ROUND(d_a, 2) AS DIST_TO_STORE_A_METERS,
    ROUND(d_b, 2) AS DIST_TO_STORE_B_METERS,
    ROUND(d_c, 2) AS DIST_TO_STORE_C_METERS,
    (d_a <= 250.0) AS IN_CATCHMENT_STORE_A,
    (d_b <= 250.0) AS IN_CATCHMENT_STORE_B,
    (d_c <= 250.0) AS IN_CATCHMENT_STORE_C,
    (d_a <= 250.0 OR d_b <= 250.0 OR d_c <= 250.0) AS IN_ANY_CATCHMENT,
    CASE 
        WHEN d_a <= LEAST(d_b, d_c) THEN 'STORE_A_EXISTING'
        WHEN d_b <= LEAST(d_a, d_c) THEN 'STORE_B_PROPOSED_5TH'
        ELSE 'STORE_C_ALT_WEST'
    END AS NEAREST_STORE_ID,
    ROUND(LEAST(d_a, d_b, d_c), 2) AS MIN_DIST_TO_STORE_METERS
FROM spatial_distances;
*/
