-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Spatial Utility SQL Functions & UDFs
-- ============================================================================
-- Schema     : GEOPULSE_DB.SILVER / GEOPULSE_DB.GOLD
-- Target     : Reusable SQL Functions for Geospatial Analytics & Spatial Geometry
-- Categories : 1. Bounding Box & Coordinate Validation Functions
--              2. Distance Calculations & Catchment Formatting
--              3. H3 Hierarchical Grid Resolution Transformations
--              4. Cannibalization & Mobility Analytics Helpers
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA SILVER;

-- ----------------------------------------------------------------------------
-- 1. BOUNDING BOX & COORDINATE VALIDATION FUNCTIONS
-- ----------------------------------------------------------------------------

-- Validates whether a (lat, lon) coordinate pair falls inside Midtown Manhattan study bounds
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_IS_POINT_IN_MANHATTAN(
    LATITUDE FLOAT,
    LONGITUDE FLOAT
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Asserts whether coordinates reside within the Manhattan retail study boundary'
AS
$$
    LATITUDE IS NOT NULL 
    AND LONGITUDE IS NOT NULL
    AND LATITUDE BETWEEN 40.700000 AND 40.850000
    AND LONGITUDE BETWEEN -74.050000 AND -73.900000
$$;

-- Safely creates a GEOGRAPHY Point handling NULLs and invalid coordinate ranges
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_SAFE_ST_POINT(
    LONGITUDE FLOAT,
    LATITUDE FLOAT
)
RETURNS GEOGRAPHY
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Constructs a valid WGS84 Point geography or returns NULL if coordinates are invalid'
AS
$$
    CASE 
        WHEN LATITUDE BETWEEN -90.0 AND 90.0 
         AND LONGITUDE BETWEEN -180.0 AND 180.0 
        THEN ST_POINT(LONGITUDE, LATITUDE)
        ELSE NULL 
    END
$$;


-- ----------------------------------------------------------------------------
-- 2. DISTANCE CALCULATIONS & CATCHMENT FORMATTING
-- ----------------------------------------------------------------------------

-- Converts distance in meters to estimated walking time in minutes (assuming 80 meters/min)
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_METERS_TO_WALK_MINUTES(
    METERS FLOAT
)
RETURNS NUMBER(6, 1)
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Estimates pedestrian walking time in minutes based on average 80m/min urban pace'
AS
$$
    ROUND(GREATEST(0.0, METERS) / 80.0, 1)
$$;

-- Converts meters to feet for US standard retail reporting (1m = 3.28084 ft)
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_METERS_TO_FEET(
    METERS FLOAT
)
RETURNS NUMBER(10, 2)
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Converts geodesic distance from meters to international feet'
AS
$$
    ROUND(GREATEST(0.0, METERS) * 3.28084, 2)
$$;

-- Evaluates if two (lat, lon) coordinates fall within a target catchment radius in meters
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_IS_WITHIN_CATCHMENT(
    LAT_1 FLOAT,
    LON_1 FLOAT,
    LAT_2 FLOAT,
    LON_2 FLOAT,
    THRESHOLD_METERS FLOAT
)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Returns TRUE if geodesic distance between coordinate pairs is <= threshold meters'
AS
$$
    ST_DISTANCE(
        ST_POINT(LON_1, LAT_1),
        ST_POINT(LON_2, LAT_2)
    ) <= NVL(THRESHOLD_METERS, 250.0)
$$;

-- Formats raw meter measurements into readable distance labels ('185 m' or '1.42 km')
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_FORMAT_DISTANCE_LABEL(
    METERS FLOAT
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Formats distance meters into human-readable metric labels (e.g., 150 m, 1.25 km)'
AS
$$
    CASE
        WHEN METERS IS NULL THEN 'N/A'
        WHEN METERS < 1000.0 THEN CONCAT(ROUND(METERS, 0)::VARCHAR, ' m')
        ELSE CONCAT(ROUND(METERS / 1000.0, 2)::VARCHAR, ' km')
    END
$$;


-- ----------------------------------------------------------------------------
-- 3. H3 GRID RESOLUTION TRANSFORMATIONS
-- ----------------------------------------------------------------------------

-- Rolls up H3 Resolution 9 index to parent Resolution 8 (~460m edge length)
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_H3_RES9_TO_PARENT_RES8(
    H3_RES9_INDEX VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Rolls up an H3 Resolution 9 cell to parent Resolution 8 for neighborhood aggregations'
AS
$$
    CASE
        WHEN H3_RES9_INDEX IS NOT NULL AND LENGTH(H3_RES9_INDEX) = 15
        THEN H3_CELL_TO_PARENT(H3_RES9_INDEX, 8)
        ELSE NULL
    END
$$;

-- Rolls up H3 Resolution 9 index to macro District Resolution 7 (~1.2km edge length)
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_H3_RES9_TO_DISTRICT_RES7(
    H3_RES9_INDEX VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Rolls up an H3 Resolution 9 cell to macro district Resolution 7'
AS
$$
    CASE
        WHEN H3_RES9_INDEX IS NOT NULL AND LENGTH(H3_RES9_INDEX) = 15
        THEN H3_CELL_TO_PARENT(H3_RES9_INDEX, 7)
        ELSE NULL
    END
$$;

-- Converts an H3 cell index directly to its boundary GEOGRAPHY polygon
CREATE OR REPLACE FUNCTION GEOPULSE_DB.SILVER.FN_H3_CELL_BOUNDARY_POLYGON(
    H3_INDEX VARCHAR
)
RETURNS GEOGRAPHY
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Converts an H3 hexagonal index into native Snowflake Polygon GEOGRAPHY'
AS
$$
    H3_CELL_TO_BOUNDARY_GEOGRAPHY(H3_INDEX)
$$;


-- ----------------------------------------------------------------------------
-- 4. CANNIBALIZATION & MOBILITY METRIC HELPERS
-- ----------------------------------------------------------------------------

-- Safely calculates cannibalization percentage avoiding division-by-zero
CREATE OR REPLACE FUNCTION GEOPULSE_DB.GOLD.FN_CALCULATE_CANNIBALIZATION_PCT(
    OVERLAP_DEVICES NUMBER,
    BASE_STORE_DEVICES NUMBER
)
RETURNS NUMBER(6, 2)
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Safely computes percentage of base store foot-traffic cannibalized by overlapping devices'
AS
$$
    CASE
        WHEN BASE_STORE_DEVICES IS NULL OR BASE_STORE_DEVICES = 0 THEN 0.00
        ELSE ROUND((OVERLAP_DEVICES / BASE_STORE_DEVICES) * 100.0, 2)
    END
$$;

-- Categorizes cannibalization severity into standard executive risk tiers
CREATE OR REPLACE FUNCTION GEOPULSE_DB.GOLD.FN_CANNIBALIZATION_RISK_TIER(
    CANNIBALIZATION_PCT NUMBER
)
RETURNS VARCHAR(32)
LANGUAGE SQL
IMMUTABLE
COMMENT = 'Categorizes cannibalization ratio into SEVERE (>=25%), MODERATE (>=15%), or LOW (<15%)'
AS
$$
    CASE
        WHEN CANNIBALIZATION_PCT >= 25.0 THEN 'SEVERE_CANNIBALIZATION_RISK'
        WHEN CANNIBALIZATION_PCT >= 15.0 THEN 'MODERATE_CANNIBALIZATION_RISK'
        ELSE 'LOW_CANNIBALIZATION_RISK'
    END
$$;
