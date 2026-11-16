{{
  config(
    materialized = 'table',
    schema = 'GOLD',
    tags = ['marts', 'gold', 'cannibalization', 'analytics']
  )
}}

/*
===============================================================================
Model: mart_store_cannibalization
Description: Analytical Gold mart evaluating spatial cannibalization and cross-shopping
             dynamics between STORE_A_EXISTING (Herald Square) and STORE_B_PROPOSED_5TH
             (5th Avenue). Aggregates daily foot traffic volumes, calculates shared
             device audience sizes, cross-visitation rates, Jaccard similarity indices,
             and net new incremental audience captures.
===============================================================================
*/

with daily_device_activity as (
    select
        ping_date,
        device_id,
        max(case when is_in_store_a_catchment then 1 else 0 end) as visited_store_a,
        max(case when is_in_store_b_catchment then 1 else 0 end) as visited_store_b,
        max(case when is_in_store_c_catchment then 1 else 0 end) as visited_store_c,
        max(case when is_in_store_a_catchment and is_morning_peak then 1 else 0 end) as visited_store_a_morning_peak,
        max(case when is_in_store_b_catchment and is_morning_peak then 1 else 0 end) as visited_store_b_morning_peak,
        max(case when is_in_store_a_catchment and is_evening_peak then 1 else 0 end) as visited_store_a_evening_peak,
        max(case when is_in_store_b_catchment and is_evening_peak then 1 else 0 end) as visited_store_b_evening_peak,
        count(case when is_in_store_a_catchment then 1 end) as store_a_pings,
        count(case when is_in_store_b_catchment then 1 end) as store_b_pings
    from {{ ref('fct_spatial_mobility') }}
    group by
        ping_date,
        device_id
),

daily_store_metrics as (
    select
        ping_date,
        count(distinct case when visited_store_a = 1 then device_id end) as store_a_unique_visitors,
        count(distinct case when visited_store_b = 1 then device_id end) as store_b_unique_visitors,
        count(distinct case when visited_store_c = 1 then device_id end) as store_c_unique_visitors,
        count(distinct case when visited_store_a = 1 and visited_store_b = 1 then device_id end) as shared_visitors_a_and_b,
        count(distinct case when visited_store_a = 1 and visited_store_c = 1 then device_id end) as shared_visitors_a_and_c,
        count(distinct case when visited_store_a_morning_peak = 1 and visited_store_b_morning_peak = 1 then device_id end) as shared_morning_peak_visitors,
        count(distinct case when visited_store_a_evening_peak = 1 and visited_store_b_evening_peak = 1 then device_id end) as shared_evening_peak_visitors,
        sum(store_a_pings) as store_a_total_pings,
        sum(store_b_pings) as store_b_total_pings
    from daily_device_activity
    group by
        ping_date
),

cannibalization_calculations as (
    select
        ping_date,
        store_a_unique_visitors,
        store_b_unique_visitors,
        store_c_unique_visitors,
        shared_visitors_a_and_b,
        shared_visitors_a_and_c,
        shared_morning_peak_visitors,
        shared_evening_peak_visitors,
        store_a_total_pings,
        store_b_total_pings,

        -- Incremental net audience gained by Store B (not visiting Store A)
        greatest(0, store_b_unique_visitors - shared_visitors_a_and_b) as net_incremental_store_b_visitors,

        -- Percentage of Store A's customer base captured/cannibalized by Store B
        round(
            100.0 * shared_visitors_a_and_b / nullif(store_a_unique_visitors, 0),
            2
        ) as store_a_cannibalization_rate_pct,

        -- Percentage of Store B visitors who also patronize Store A (cross-shopping)
        round(
            100.0 * shared_visitors_a_and_b / nullif(store_b_unique_visitors, 0),
            2
        ) as store_b_cross_shopping_rate_pct,

        -- Jaccard Similarity: Intersection over Union of visitor sets
        round(
            shared_visitors_a_and_b / nullif(
                store_a_unique_visitors + store_b_unique_visitors - shared_visitors_a_and_b,
                0
            ),
            4
        ) as jaccard_similarity_coefficient,

        -- Peak hour cannibalization share
        round(
            100.0 * (shared_morning_peak_visitors + shared_evening_peak_visitors) /
            nullif(shared_visitors_a_and_b, 0),
            2
        ) as peak_hours_overlap_pct,

        -- Cannibalization risk categorization threshold
        case
            when (100.0 * shared_visitors_a_and_b / nullif(store_a_unique_visitors, 0)) >= 30.0
                then 'HIGH_CANNIBALIZATION_RISK'
            when (100.0 * shared_visitors_a_and_b / nullif(store_a_unique_visitors, 0)) >= 15.0
                then 'MODERATE_CANNIBALIZATION_RISK'
            else 'LOW_CANNIBALIZATION_EXPANSION'
        end as risk_classification,

        current_timestamp() as calculation_timestamp
    from daily_store_metrics
)

select * from cannibalization_calculations
