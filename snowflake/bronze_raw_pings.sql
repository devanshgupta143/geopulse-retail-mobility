-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Bronze Layer Ingestion DDL
-- ============================================================================
-- Schema     : GEOPULSE_DB.BRONZE
-- Target     : RAW_GPS_PINGS (Staging Table)
-- Components : File Formats (CSV & Parquet), Internal Stage, Audit Metadata
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA BRONZE;

-- ----------------------------------------------------------------------------
-- 1. FILE FORMAT DEFINITIONS
-- ----------------------------------------------------------------------------

-- Delimited CSV file format for raw telematics ping streams
CREATE OR REPLACE FILE FORMAT GEOPULSE_DB.BRONZE.CSV_MOBILITY_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\0'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('', 'NULL', 'null', '\\N')
    COMMENT = 'CSV file format for raw mock GPS pings ingestion';

-- Columnar Parquet file format for enriched and partitioned mobility payloads
CREATE OR REPLACE FILE FORMAT GEOPULSE_DB.BRONZE.PARQUET_MOBILITY_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
    BINARY_AS_TEXT = FALSE
    COMMENT = 'Parquet file format for columnar mobility metrics ingestion';

-- JSON file format for store geofence configurations
CREATE OR REPLACE FILE FORMAT GEOPULSE_DB.BRONZE.JSON_GEOFENCE_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    ALLOW_DUPLICATE = FALSE
    COMMENT = 'JSON file format for store geofences metadata payload';

-- ----------------------------------------------------------------------------
-- 2. INTERNAL STAGES DEFINITION
-- ----------------------------------------------------------------------------

-- Internal stage for uploading raw telemetry batches and metadata
CREATE OR REPLACE STAGE GEOPULSE_DB.BRONZE.RAW_MOBILITY_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT = GEOPULSE_DB.BRONZE.CSV_MOBILITY_FORMAT
    COMMENT = 'Internal landing stage for raw GPS CSV/Parquet files and geofence JSONs';

-- ----------------------------------------------------------------------------
-- 3. BRONZE RAW STAGING TABLE DDL
-- ----------------------------------------------------------------------------

-- Staging table for raw GPS mobility pings with audit metadata & VARIANT flexibility
CREATE OR REPLACE TABLE GEOPULSE_DB.BRONZE.RAW_GPS_PINGS (
    -- Telemetry Attributes
    DEVICE_ID               VARCHAR(64)         NOT NULL COMMENT 'Unique mobile device UUID',
    LATITUDE                FLOAT               NOT NULL COMMENT 'WGS84 Latitude coordinate',
    LONGITUDE               FLOAT               NOT NULL COMMENT 'WGS84 Longitude coordinate',
    PING_TIMESTAMP          TIMESTAMP_NTZ       NOT NULL COMMENT 'Event timestamp in UTC / Local',
    
    -- Semi-Structured Payload Preservation
    RAW_PAYLOAD             VARIANT                      COMMENT 'Full unparsed JSON/Parquet payload record',
    
    -- Ingestion Lineage & Audit Columns
    METADATA_FILENAME       VARCHAR(255)                 COMMENT 'Source file name in stage',
    METADATA_FILE_ROW_NUMBER NUMBER(18, 0)               COMMENT 'Row index in source file',
    INGESTION_TIMESTAMP     TIMESTAMP_NTZ       DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when row was loaded into Snowflake'
)
DATA_RETENTION_TIME_IN_DAYS = 3
COMMENT = 'Raw, append-only staging table for mobile device GPS pings';

-- Staging table for store geofence metadata
CREATE OR REPLACE TABLE GEOPULSE_DB.BRONZE.RAW_STORE_GEOFENCES (
    STORE_ID                VARCHAR(64)         NOT NULL COMMENT 'Unique store identifier',
    STORE_NAME              VARCHAR(128)        NOT NULL COMMENT 'Store display name',
    STATUS                  VARCHAR(32)                  COMMENT 'Store operational status (EXISTING, PROPOSED, ALTERNATIVE)',
    LATITUDE                FLOAT               NOT NULL COMMENT 'Anchor center latitude',
    LONGITUDE               FLOAT               NOT NULL COMMENT 'Anchor center longitude',
    CATCHMENT_RADIUS_METERS NUMBER(10, 2)       DEFAULT 250.0 COMMENT 'Walking catchment threshold in meters',
    RAW_PAYLOAD             VARIANT                      COMMENT 'Complete GeoJSON / metadata JSON object',
    INGESTION_TIMESTAMP     TIMESTAMP_NTZ       DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Staging table for store spatial anchors and catchment configurations';

-- ----------------------------------------------------------------------------
-- 4. COPY PATTERNS (TEMPLATES)
-- ----------------------------------------------------------------------------

/*
-- Example CSV Ingestion Command:
COPY INTO GEOPULSE_DB.BRONZE.RAW_GPS_PINGS (
    DEVICE_ID,
    LATITUDE,
    LONGITUDE,
    PING_TIMESTAMP,
    METADATA_FILENAME,
    METADATA_FILE_ROW_NUMBER
)
FROM (
    SELECT 
        $1::VARCHAR,
        $2::FLOAT,
        $3::FLOAT,
        $4::TIMESTAMP_NTZ,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @GEOPULSE_DB.BRONZE.RAW_MOBILITY_STAGE/mock_gps_pings.csv
)
FILE_FORMAT = (FORMAT_NAME = GEOPULSE_DB.BRONZE.CSV_MOBILITY_FORMAT)
ON_ERROR = 'CONTINUE';

-- Example Parquet Ingestion Command:
COPY INTO GEOPULSE_DB.BRONZE.RAW_GPS_PINGS (
    DEVICE_ID,
    LATITUDE,
    LONGITUDE,
    PING_TIMESTAMP,
    RAW_PAYLOAD,
    METADATA_FILENAME,
    METADATA_FILE_ROW_NUMBER
)
FROM (
    SELECT 
        $1:DeviceID::VARCHAR,
        $1:Lat::FLOAT,
        $1:Long::FLOAT,
        $1:Timestamp::TIMESTAMP_NTZ,
        $1,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER
    FROM @GEOPULSE_DB.BRONZE.RAW_MOBILITY_STAGE/enriched_gps_pings.parquet
)
FILE_FORMAT = (FORMAT_NAME = GEOPULSE_DB.BRONZE.PARQUET_MOBILITY_FORMAT)
ON_ERROR = 'SKIP_FILE';
*/
