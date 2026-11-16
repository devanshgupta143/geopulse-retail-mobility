/*
===============================================================================
Singular Test: assert_valid_dwell_durations
Description: Verifies that all computed store session dwell times fall within
             statistically valid bounds [0.0, 1440.0 minutes (24 hours)].
             Returns any anomalous records violating temporal integrity.
===============================================================================
*/

select
    session_id,
    device_id,
    store_id,
    session_start_time,
    session_end_time,
    dwell_duration_minutes
from {{ ref('int_device_store_sessions') }}
where dwell_duration_minutes < 0.0
   or dwell_duration_minutes > 1440.0
