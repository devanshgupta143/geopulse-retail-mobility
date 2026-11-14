-- ============================================================================
-- GeoPulse Retail Mobility Analytics - Gold Layer Analytical Marts
-- ============================================================================
-- Schema     : GEOPULSE_DB.GOLD
-- Target     : Analytical marts, KPI summaries, and store cannibalization models
-- Views/Marts: 1. GOLD.STORE_CANNIBALIZATION_SUMMARY
--              2. GOLD.HOURLY_FOOT_TRAFFIC_TRENDS
--              3. GOLD.H3_HEXAGON_MOBILITY_HEATMAP
--              4. GOLD.COMMUTER_CORRIDOR_TRANSIT_FLOWS
-- ============================================================================

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEOPULSE_DB;
USE SCHEMA GOLD;

-- ----------------------------------------------------------------------------
-- 1. STORE CANNIBALIZATION SUMMARY (EXECUTIVE METRIC VIEW)
-- ----------------------------------------------------------------------------
-- Measures foot-traffic overlap and cannibalization risk between the existing
-- flagship (STORE_A_EXISTING) and the prospective store (STORE_B_PROPOSED_5TH).
CREATE OR REPLACE VIEW GEOPULSE_DB.GOLD.STORE_CANNIBALIZATION_SUMMARY AS
WITH daily_visitors AS (
    SELECT
        PING_DATE,
        DEVICE_ID,
        -- Full day catchment visits
        MAX(IN_CATCHMENT_STORE_A) AS visited_store_a,
        MAX(IN_CATCHMENT_STORE_B) AS visited_store_b,
        MAX(IN_CATCHMENT_STORE_C) AS visited_store_c,
        -- Peak morning transit window (07:00-09:59) catchment visits
        MAX(CASE WHEN IS_MORNING_PEAK AND IN_CATCHMENT_STORE_A THEN 1 ELSE 0 END) AS peak_visited_a,
        MAX(CASE WHEN IS_MORNING_PEAK AND IN_CATCHMENT_STORE_B THEN 1 ELSE 0 END) AS peak_visited_b
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    GROUP BY PING_DATE, DEVICE_ID
),
aggregated_metrics AS (
    SELECT
        PING_DATE,
        COUNT(DISTINCT DEVICE_ID) AS total_active_devices,
        
        -- Total unique daily visitors by store
        COUNT(DISTINCT CASE WHEN visited_store_a THEN DEVICE_ID END) AS visitors_store_a,
        COUNT(DISTINCT CASE WHEN visited_store_b THEN DEVICE_ID END) AS visitors_store_b,
        COUNT(DISTINCT CASE WHEN visited_store_c THEN DEVICE_ID END) AS visitors_store_c,
        
        -- Full-day cross-store commuter overlap (Cannibalization pool)
        COUNT(DISTINCT CASE WHEN visited_store_a AND visited_store_b THEN DEVICE_ID END) AS overlap_devices_all_day,
        
        -- Peak morning transit window visitors (07:00-09:59)
        COUNT(DISTINCT CASE WHEN peak_visited_a = 1 THEN DEVICE_ID END) AS peak_visitors_store_a,
        COUNT(DISTINCT CASE WHEN peak_visited_b = 1 THEN DEVICE_ID END) AS peak_visitors_store_b,
        COUNT(DISTINCT CASE WHEN peak_visited_a = 1 AND peak_visited_b = 1 THEN DEVICE_ID END) AS peak_overlap_devices
    FROM daily_visitors
    GROUP BY PING_DATE
)
SELECT
    PING_DATE,
    total_active_devices,
    visitors_store_a,
    visitors_store_b,
    visitors_store_c,
    overlap_devices_all_day,
    
    -- Full-day cannibalization ratio against existing Store A
    ROUND(
        (overlap_devices_all_day / NULLIF(visitors_store_a, 0)) * 100, 
        2
    ) AS cannibalization_pct_all_day,
    
    -- Morning Peak Commuter Metrics
    peak_visitors_store_a,
    peak_visitors_store_b,
    peak_overlap_devices,
    
    -- Morning peak cannibalization ratio (Primary decision metric)
    ROUND(
        (peak_overlap_devices / NULLIF(peak_visitors_store_a, 0)) * 100, 
        2
    ) AS peak_cannibalization_pct,
    
    -- Risk classification based on 25% threshold
    CASE
        WHEN (peak_overlap_devices / NULLIF(peak_visitors_store_a, 0)) >= 0.25 THEN 'SEVERE_CANNIBALIZATION_RISK'
        WHEN (peak_overlap_devices / NULLIF(peak_visitors_store_a, 0)) >= 0.15 THEN 'MODERATE_CANNIBALIZATION_RISK'
        ELSE 'LOW_CANNIBALIZATION_RISK'
    END AS cannibalization_risk_tier,
    
    CURRENT_TIMESTAMP() AS MART_REFRESHED_AT
FROM aggregated_metrics;

COMMENT ON VIEW GEOPULSE_DB.GOLD.STORE_CANNIBALIZATION_SUMMARY IS 
'Executive KPI mart evaluating foot-traffic cannibalization and device overlap between STORE_A_EXISTING and STORE_B_PROPOSED_5TH';


-- ----------------------------------------------------------------------------
-- 2. HOURLY FOOT-TRAFFIC TRENDS (SPATIAL TIME-SERIES MART)
-- ----------------------------------------------------------------------------
-- Granular hourly visitation curves, device densities, and dwell proximities
-- across each store catchment zone.
CREATE OR REPLACE VIEW GEOPULSE_DB.GOLD.HOURLY_FOOT_TRAFFIC_TRENDS AS
WITH unpivoted_store_visits AS (
    -- Store A visits
    SELECT
        PING_DATE,
        PING_HOUR,
        IS_MORNING_PEAK,
        'STORE_A_EXISTING' AS STORE_ID,
        DEVICE_ID,
        DIST_TO_STORE_A_METERS AS DIST_TO_STORE_M
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE IN_CATCHMENT_STORE_A

    UNION ALL

    -- Store B visits
    SELECT
        PING_DATE,
        PING_HOUR,
        IS_MORNING_PEAK,
        'STORE_B_PROPOSED_5TH' AS STORE_ID,
        DEVICE_ID,
        DIST_TO_STORE_B_METERS AS DIST_TO_STORE_M
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE IN_CATCHMENT_STORE_B

    UNION ALL

    -- Store C visits
    SELECT
        PING_DATE,
        PING_HOUR,
        IS_MORNING_PEAK,
        'STORE_C_ALT_WEST' AS STORE_ID,
        DEVICE_ID,
        DIST_TO_STORE_C_METERS AS DIST_TO_STORE_M
    FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS
    WHERE IN_CATCHMENT_STORE_C
)
SELECT
    v.PING_DATE,
    v.PING_HOUR,
    v.STORE_ID,
    c.STORE_NAME,
    c.STATUS AS STORE_STATUS,
    v.IS_MORNING_PEAK,
    
    -- Volume & Device Aggregations
    COUNT(1) AS TOTAL_PING_VISITS,
    COUNT(DISTINCT v.DEVICE_ID) AS UNIQUE_DEVICE_VISITORS,
    
    -- Proximity Statistics (Meters)
    ROUND(AVG(v.DIST_TO_STORE_M), 2) AS AVG_PROXIMITY_METERS,
    ROUND(MIN(v.DIST_TO_STORE_M), 2) AS MIN_PROXIMITY_METERS,
    ROUND(MAX(v.DIST_TO_STORE_M), 2) AS MAX_PROXIMITY_METERS,
    
    CURRENT_TIMESTAMP() AS MART_REFRESHED_AT
FROM unpivoted_store_visits v
INNER JOIN GEOPULSE_DB.SILVER.STORE_CATCHMENTS c
    ON v.STORE_ID = c.STORE_ID
GROUP BY 
    v.PING_DATE,
    v.PING_HOUR,
    v.STORE_ID,
    c.STORE_NAME,
    c.STATUS,
    v.IS_MORNING_PEAK;

COMMENT ON VIEW GEOPULSE_DB.GOLD.HOURLY_FOOT_TRAFFIC_TRENDS IS 
'Hourly time-series mart of foot-traffic pings, unique visitors, and average proximity to store anchors';


-- ----------------------------------------------------------------------------
-- 3. H3 HEXAGON MOBILITY HEATMAP (KEPLER.GL GEOSPATIAL VIEW)
-- ----------------------------------------------------------------------------
-- Rollup by H3 Resolution 9 hex cells with polygon boundaries, traffic intensity,
-- and store catchment attribution for direct Kepler.gl 3D hex-bin visualization.
CREATE OR REPLACE VIEW GEOPULSE_DB.GOLD.H3_HEXAGON_MOBILITY_HEATMAP AS
SELECT
    p.PING_DATE,
    p.H3_INDEX_RES9,
    p.H3_INDEX_RES8,
    
    -- Primary store corridor attribution
    p.NEAREST_STORE_ID,
    c.STORE_NAME AS NEAREST_STORE_NAME,
    
    -- Foot-traffic counts
    COUNT(1) AS TOTAL_PINGS,
    COUNT(DISTINCT p.DEVICE_ID) AS UNIQUE_MOBILE_DEVICES,
    
    -- Transit window breakout
    SUM(CASE WHEN p.IS_MORNING_PEAK THEN 1 ELSE 0 END) AS MORNING_PEAK_PINGS,
    COUNT(DISTINCT CASE WHEN p.IS_MORNING_PEAK THEN p.DEVICE_ID END) AS MORNING_PEAK_DEVICES,
    
    -- Catchment overlaps
    SUM(CASE WHEN p.IN_CATCHMENT_STORE_A THEN 1 ELSE 0 END) AS PINGS_IN_STORE_A_CATCHMENT,
    SUM(CASE WHEN p.IN_CATCHMENT_STORE_B THEN 1 ELSE 0 END) AS PINGS_IN_STORE_B_CATCHMENT,
    SUM(CASE WHEN p.IN_CATCHMENT_STORE_C THEN 1 ELSE 0 END) AS PINGS_IN_STORE_C_CATCHMENT,
    
    -- Average coordinates for Kepler.gl point pinning
    ROUND(AVG(p.LATITUDE), 6) AS CENTROID_LATITUDE,
    ROUND(AVG(p.LONGITUDE), 6) AS CENTROID_LONGITUDE,
    ST_POINT(ROUND(AVG(p.LONGITUDE), 6), ROUND(AVG(p.LATITUDE), 6)) AS CENTROID_GEOGRAPHY,
    
    CURRENT_TIMESTAMP() AS MART_REFRESHED_AT
FROM GEOPULSE_DB.SILVER.SPATIAL_PINGS p
LEFT JOIN GEOPULSE_DB.SILVER.STORE_CATCHMENTS c
    ON p.NEAREST_STORE_ID = c.STORE_ID
GROUP BY 
    p.PING_DATE,
    p.H3_INDEX_RES9,
    p.H3_INDEX_RES8,
    p.NEAREST_STORE_ID,
    c.STORE_NAME;

COMMENT ON VIEW GEOPULSE_DB.GOLD.H3_HEXAGON_MOBILITY_HEATMAP IS 
'Spatiotemporal H3 resolution 9 aggregation view designed for Kepler.gl heatmap and 3D hex-bin overlays';
