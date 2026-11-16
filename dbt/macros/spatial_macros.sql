/*
===============================================================================
Macro Suite: Spatial Utilities for Snowflake
Description: Reusable Jinja macros providing spherical Haversine distance,
             H3 hexagonal indexing wrappers, and catchment boundary classification.
===============================================================================
*/

{% macro calculate_haversine_meters(lat1, lon1, lat2, lon2) %}
{#
    Calculates the great-circle distance between two geographic coordinates in meters
    using the Haversine formula on a mean Earth radius of 6,371,000 meters.
#}
(
    2 * 6371000 * asin(
        sqrt(
            power(sin(radians(({{ lat2 }}) - ({{ lat1 }})) / 2), 2) +
            cos(radians({{ lat1 }})) * cos(radians({{ lat2 }})) *
            power(sin(radians(({{ lon2 }}) - ({{ lon1 }})) / 2), 2)
        )
    )
)
{% endmacro %}


{% macro assign_h3_res9(lat, lon) %}
{#
    Transforms WGS-84 (lat, lon) into an H3 Resolution 9 hexagonal cell address string.
    Note: Snowflake ST_POINT takes (longitude, latitude) according to standard GeoJSON/WKT conventions.
#}
h3_point_to_cell_string(st_point({{ lon }}, {{ lat }}), 9)
{% endmacro %}


{% macro assign_h3_index(lat, lon, resolution=9) %}
{#
    Parametric H3 indexing macro allowing dynamic resolution assignment (e.g., 7, 8, 9, 10).
#}
h3_point_to_cell_string(st_point({{ lon }}, {{ lat }}), {{ resolution }})
{% endmacro %}


{% macro classify_catchment_zone(dist_col, threshold=250.0) %}
{#
    Evaluates whether a calculated spatial distance falls within the defined retail catchment radius.
    Returns a boolean expression (TRUE if within threshold, FALSE otherwise).
#}
(case when {{ dist_col }} <= {{ threshold }} then true else false end)
{% endmacro %}


{% macro is_in_bounding_box(lat_col, lon_col, min_lat=40.700000, max_lat=40.850000, min_lon=-74.050000, max_lon=-73.900000) %}
{#
    Filters coordinates to the Manhattan study area bounding box.
#}
(
    {{ lat_col }} between {{ min_lat }} and {{ max_lat }}
    and {{ lon_col }} between {{ min_lon }} and {{ max_lon }}
)
{% endmacro %}
