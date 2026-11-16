{{
  config(
    materialized = 'ephemeral',
    tags = ['intermediate', 'sessionization', 'dwell_time']
  )
}}

/*
===============================================================================
Model: int_device_store_sessions
Description: Reconstructs discrete store visit sessions from consecutive GPS pings
             falling within a 250-meter store catchment. Applies a 15-minute inactivity
             threshold (gap > 900 seconds) to partition sessions and computes total
             dwell duration in minutes alongside ping density per visit.
===============================================================================
*/

with stg_pings as (
    select * from {{ ref('stg_gps_pings') }}
),

store_catchments as (
    select
        store_id,
        store_name,
        store_type,
        center_latitude,
        center_longitude,
        catchment_radius_meters
    from {{ source('silver', 'STORE_CATCHMENTS') }}
),

pings_in_catchments as (
    select
        p.ping_id,
        p.device_id,
        p.latitude,
        p.longitude,
        p.ping_timestamp,
        p.ping_date,
        p.ping_hour,
        p.is_morning_peak,
        p.is_evening_peak,
        s.store_id,
        s.store_name,
        s.store_type,
        -- Haversine formula calculation in meters
        (
            2 * 6371000 * asin(
                sqrt(
                    power(sin(radians(s.center_latitude - p.latitude) / 2), 2) +
                    cos(radians(p.latitude)) * cos(radians(s.center_latitude)) *
                    power(sin(radians(s.center_longitude - p.longitude) / 2), 2)
                )
            )
        ) as distance_to_store_meters
    from stg_pings p
    cross join store_catchments s
    where (
        2 * 6371000 * asin(
            sqrt(
                power(sin(radians(s.center_latitude - p.latitude) / 2), 2) +
                cos(radians(p.latitude)) * cos(radians(s.center_latitude)) *
                power(sin(radians(s.center_longitude - p.longitude) / 2), 2)
            )
        )
    ) <= s.catchment_radius_meters
),

session_boundaries as (
    select
        ping_id,
        device_id,
        store_id,
        store_name,
        store_type,
        ping_timestamp,
        ping_date,
        distance_to_store_meters,
        lag(ping_timestamp) over (
            partition by device_id, store_id
            order by ping_timestamp
        ) as prev_ping_timestamp,
        case
            when lag(ping_timestamp) over (
                partition by device_id, store_id
                order by ping_timestamp
            ) is null then 1
            when datediff('second', lag(ping_timestamp) over (
                partition by device_id, store_id
                order by ping_timestamp
            ), ping_timestamp) > 900 then 1
            else 0
        end as is_new_session_flag
    from pings_in_catchments
),

session_grouped as (
    select
        ping_id,
        device_id,
        store_id,
        store_name,
        store_type,
        ping_timestamp,
        ping_date,
        distance_to_store_meters,
        sum(is_new_session_flag) over (
            partition by device_id, store_id
            order by ping_timestamp
            rows between unbounded preceding and current row
        ) as session_seq
    from session_boundaries
),

aggregated_sessions as (
    select
        md5(concat_ws('|',
            device_id,
            store_id,
            cast(session_seq as string),
            to_varchar(min(ping_timestamp), 'YYYY-MM-DD HH24:MI:SS')
        )) as session_id,
        device_id,
        store_id,
        store_name,
        store_type,
        min(ping_timestamp) as session_start_time,
        max(ping_timestamp) as session_end_time,
        cast(date_trunc('day', min(ping_timestamp)) as date) as session_date,
        cast(date_part('hour', min(ping_timestamp)) as integer) as start_hour,
        round(datediff('second', min(ping_timestamp), max(ping_timestamp)) / 60.0, 2) as dwell_duration_minutes,
        count(*) as pings_in_session,
        round(min(distance_to_store_meters), 2) as min_distance_meters,
        round(avg(distance_to_store_meters), 2) as avg_distance_meters
    from session_grouped
    group by
        device_id,
        store_id,
        store_name,
        store_type,
        session_seq
)

select
    session_id,
    device_id,
    store_id,
    store_name,
    store_type,
    session_start_time,
    session_end_time,
    session_date,
    start_hour,
    dwell_duration_minutes,
    pings_in_session,
    min_distance_meters,
    avg_distance_meters,
    case
        when dwell_duration_minutes >= 5.0 then true
        else false
    end as is_meaningful_visit
from aggregated_sessions
