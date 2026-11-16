{{
  config(
    materialized = 'incremental',
    unique_key = 'ping_id',
    incremental_strategy = 'merge',
    cluster_by = ['ping_date', 'h3_index_res9'],
    schema = 'GOLD',
    tags = ['marts', 'gold', 'fact', 'spatial']
  )
}}

/*
===============================================================================
Model: fct_spatial_mobility
Description: Core analytical fact table containing spatially enriched GPS pings.
             Computes H3 hexagonal indices (resolution 8 and 9), evaluates proximity
             and geofence membership against retail stores (Store A, Store B, Store C),
             and resolves nearest-store associations. Configured with Snowflake merge
             incremental updates and clustered on (ping_date, h3_index_res9).
===============================================================================
*/

with stg_pings as (
    select *
    from {{ ref('stg_gps_pings') }}
    {% if is_incremental() %}
    where ping_timestamp > (
        select coalesce(max(ping_timestamp), '1970-01-01'::timestamp_ntz)
        from {{ this }}
    )
    {% endif %}
),

spatial_calculations as (
    select
        p.ping_id,
        p.device_id,
        p.latitude,
        p.longitude,
        p.ping_timestamp,
        p.ingestion_timestamp,
        p.ping_date,
        p.ping_hour,
        p.is_morning_peak,
        p.is_evening_peak,

        -- H3 Hexagonal Cell Indexing (Res 8: ~460m edge, Res 9: ~174m edge)
        h3_point_to_cell_string(st_point(p.longitude, p.latitude), 9) as h3_index_res9,
        h3_point_to_cell_string(st_point(p.longitude, p.latitude), 8) as h3_index_res8,

        -- Haversine Distance to STORE_A_EXISTING (40.7549, -73.9840)
        round(
            2 * 6371000 * asin(
                sqrt(
                    power(sin(radians(40.7549 - p.latitude) / 2), 2) +
                    cos(radians(p.latitude)) * cos(radians(40.7549)) *
                    power(sin(radians(-73.9840 - p.longitude) / 2), 2)
                )
            ),
            2
        ) as distance_store_a_meters,

        -- Haversine Distance to STORE_B_PROPOSED_5TH (40.7565, -73.9815)
        round(
            2 * 6371000 * asin(
                sqrt(
                    power(sin(radians(40.7565 - p.latitude) / 2), 2) +
                    cos(radians(p.latitude)) * cos(radians(40.7565)) *
                    power(sin(radians(-73.9815 - p.longitude) / 2), 2)
                )
            ),
            2
        ) as distance_store_b_meters,

        -- Haversine Distance to STORE_C_ALT_WEST (40.7510, -73.9930)
        round(
            2 * 6371000 * asin(
                sqrt(
                    power(sin(radians(40.7510 - p.latitude) / 2), 2) +
                    cos(radians(p.latitude)) * cos(radians(40.7510)) *
                    power(sin(radians(-73.9930 - p.longitude) / 2), 2)
                )
            ),
            2
        ) as distance_store_c_meters

    from stg_pings p
),

classified_mobility as (
    select
        ping_id,
        device_id,
        latitude,
        longitude,
        ping_timestamp,
        ingestion_timestamp,
        ping_date,
        ping_hour,
        is_morning_peak,
        is_evening_peak,
        h3_index_res9,
        h3_index_res8,
        distance_store_a_meters,
        distance_store_b_meters,
        distance_store_c_meters,

        -- Catchment Membership (250m threshold)
        (distance_store_a_meters <= 250.0) as is_in_store_a_catchment,
        (distance_store_b_meters <= 250.0) as is_in_store_b_catchment,
        (distance_store_c_meters <= 250.0) as is_in_store_c_catchment,

        -- Multi-store Catchment Flag
        case
            when (case when distance_store_a_meters <= 250.0 then 1 else 0 end +
                  case when distance_store_b_meters <= 250.0 then 1 else 0 end +
                  case when distance_store_c_meters <= 250.0 then 1 else 0 end) > 1
            then true
            else false
        end as is_multi_catchment_overlap,

        -- Nearest Store Resolution
        case
            when distance_store_a_meters <= distance_store_b_meters
             and distance_store_a_meters <= distance_store_c_meters
            then 'STORE_A_EXISTING'
            when distance_store_b_meters <= distance_store_a_meters
             and distance_store_b_meters <= distance_store_c_meters
            then 'STORE_B_PROPOSED_5TH'
            else 'STORE_C_ALT_WEST'
        end as nearest_store_id,

        least(distance_store_a_meters, distance_store_b_meters, distance_store_c_meters) as nearest_store_distance_meters,

        -- Primary Assigned Catchment (within 250m threshold)
        case
            when distance_store_a_meters <= 250.0
             and distance_store_a_meters <= distance_store_b_meters
             and distance_store_a_meters <= distance_store_c_meters
            then 'STORE_A_EXISTING'
            when distance_store_b_meters <= 250.0
             and distance_store_b_meters <= distance_store_a_meters
             and distance_store_b_meters <= distance_store_c_meters
            then 'STORE_B_PROPOSED_5TH'
            when distance_store_c_meters <= 250.0
             and distance_store_c_meters <= distance_store_a_meters
             and distance_store_c_meters <= distance_store_b_meters
            then 'STORE_C_ALT_WEST'
            else null
        end as assigned_catchment_store_id,

        current_timestamp() as dw_inserted_at
    from spatial_calculations
)

select * from classified_mobility
