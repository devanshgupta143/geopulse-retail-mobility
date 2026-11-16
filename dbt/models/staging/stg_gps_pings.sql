{{
  config(
    materialized = 'view',
    schema = 'SILVER',
    tags = ['staging', 'mobility', 'pings']
  )
}}

/*
===============================================================================
Model: stg_gps_pings
Description: Cleans, deduplicates, and standardizes raw mobile telemetry GPS pings.
             Applies Manhattan geographic boundary filtering (40.70 to 40.85 N,
             -74.05 to -73.90 W), computes surrogate keys, and derives temporal
             attributes (ping_date, ping_hour, is_morning_peak).
===============================================================================
*/

with raw_pings as (
    select
        coalesce(
            nullif(trim(ping_id), ''),
            md5(concat(trim(device_id), '|', to_varchar(ping_timestamp, 'YYYY-MM-DD HH24:MI:SS.FF3')))
        ) as ping_id,
        trim(device_id) as device_id,
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude,
        cast(ping_timestamp as timestamp_ntz) as ping_timestamp,
        cast(ingestion_timestamp as timestamp_ntz) as ingestion_timestamp,
        raw_payload,
        file_source
    from {{ source('bronze', 'RAW_GPS_PINGS') }}
    where latitude is not null
      and longitude is not null
      and ping_timestamp is not null
      -- Filter to Manhattan retail study bounding box
      and latitude between 40.700000 and 40.850000
      and longitude between -74.050000 and -73.900000
),

deduplicated_pings as (
    select
        ping_id,
        device_id,
        latitude,
        longitude,
        ping_timestamp,
        ingestion_timestamp,
        cast(date_trunc('day', ping_timestamp) as date) as ping_date,
        cast(date_part('hour', ping_timestamp) as integer) as ping_hour,
        case
            when date_part('hour', ping_timestamp) in (7, 8, 9) then true
            else false
        end as is_morning_peak,
        case
            when date_part('hour', ping_timestamp) in (17, 18, 19) then true
            else false
        end as is_evening_peak,
        raw_payload,
        file_source
    from raw_pings
    qualify row_number() over (
        partition by device_id, ping_timestamp
        order by ingestion_timestamp desc, ping_id desc
    ) = 1
)

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
    raw_payload,
    file_source
from deduplicated_pings
