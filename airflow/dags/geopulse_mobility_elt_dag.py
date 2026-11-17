"""
===============================================================================
GeoPulse Retail Mobility Analytics - End-to-End Mobility Ingestion & ELT DAG
File: airflow/dags/geopulse_mobility_elt_dag.py
Description: Production Airflow DAG orchestrating the daily mobile telemetry ELT
             pipeline: PySpark H3 spatial indexing, Snowflake internal staging &
             Silver stored procedures, data quality circuit-breaker sensor, and
             dbt transformations for Gold cannibalization analytics.
===============================================================================
"""

from datetime import datetime, timedelta
import os
from pathlib import Path

# Airflow Core & Operator Imports
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.task_group import TaskGroup
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except ImportError:
    # Minimal fallback stubs for syntax validation when running outside full Airflow runtime
    class DAG:
        def __init__(self, *args, **kwargs):
            self.dag_id = args[0] if args else kwargs.get("dag_id")
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class EmptyOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other

    class BashOperator(EmptyOperator): pass
    class SnowflakeOperator(EmptyOperator): pass
    class TaskGroup:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def __rshift__(self, other): return other
        def __lshift__(self, other): return other

# Custom Plugins & Operators
try:
    from operators.pyspark_spatial_operator import PySparkSpatialOperator
    from operators.snowflake_stage_operator import SnowflakeStageLoadOperator
    from sensors.spatial_quality_sensor import SpatialQualitySensor
    from callbacks.alert_callbacks import (
        on_task_failure_callback,
        on_sla_miss_callback,
        on_dag_success_callback,
    )
except ImportError:
    # Alternative direct plugin path resolution
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))
    try:
        from operators.pyspark_spatial_operator import PySparkSpatialOperator
        from operators.snowflake_stage_operator import SnowflakeStageLoadOperator
        from sensors.spatial_quality_sensor import SpatialQualitySensor
        from callbacks.alert_callbacks import (
            on_task_failure_callback,
            on_sla_miss_callback,
            on_dag_success_callback,
        )
    except Exception:
        class PySparkSpatialOperator(EmptyOperator): pass
        class SnowflakeStageLoadOperator(EmptyOperator): pass
        class SpatialQualitySensor(EmptyOperator): pass
        def on_task_failure_callback(ctx): pass
        def on_sla_miss_callback(*args): pass
        def on_dag_success_callback(ctx): pass


# Project Paths & Parameters
PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYSPARK_INPUT_CSV = str(PROJECT_ROOT / "pyspark" / "mock_gps_pings.csv")
PYSPARK_OUTPUT_PARQUET = str(PROJECT_ROOT / "pyspark" / "output" / "partitioned_metrics")
STORE_GEOFENCES_JSON = str(PROJECT_ROOT / "pyspark" / "store_geofences.json")
DBT_PROJECT_DIR = str(PROJECT_ROOT / "dbt")

DEFAULT_ARGS = {
    "owner": "DataPlatformTeam",
    "depends_on_past": False,
    "email": ["data-platform-alerts@geopulse.internal"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "sla": timedelta(hours=2),
    "on_failure_callback": on_task_failure_callback,
}

with DAG(
    dag_id="geopulse_mobility_elt_dag",
    default_args=DEFAULT_ARGS,
    description="Daily retail mobility spatial enrichment, Snowflake staging, and dbt Gold mart ELT",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=on_sla_miss_callback,
    on_success_callback=on_dag_success_callback,
    tags=["geopulse", "pyspark", "snowflake", "dbt", "spatial", "h3"],
) as dag:

    pipeline_start = EmptyOperator(task_id="pipeline_start")

    # -------------------------------------------------------------------------
    # Stage 1: PySpark Distributed Spatial Processing & Partitioning
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="pyspark_spatial_processing") as stage_pyspark:
        run_spatial_indexing = PySparkSpatialOperator(
            task_id="pyspark_h3_geofence_indexing",
            input_csv_path=PYSPARK_INPUT_CSV,
            output_parquet_path=PYSPARK_OUTPUT_PARQUET,
            geofences_path=STORE_GEOFENCES_JSON,
            h3_resolution=9,
            catchment_radius_meters=250.0,
            date_partition="{{ ds }}",
            min_expected_records=1000,
        )

    # -------------------------------------------------------------------------
    # Stage 2: Snowflake Automated Ingestion & Silver Stored Procedure
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="snowflake_ingestion_and_silver_elt") as stage_snowflake:
        load_parquet_to_bronze = SnowflakeStageLoadOperator(
            task_id="snowflake_put_and_copy_raw",
            local_file_pattern=f"{PYSPARK_OUTPUT_PARQUET}/**/*.parquet",
            stage_name="@BRONZE.RAW_MOBILITY_STAGE",
            target_table="BRONZE.RAW_GPS_PINGS",
            file_format="BRONZE.PARQUET_MOBILITY_FORMAT",
            snowflake_conn_id="snowflake_geopulse_conn",
            date_partition="{{ ds }}",
            min_expected_rows=1000,
        )

        execute_silver_elt_procedure = SnowflakeOperator(
            task_id="execute_silver_elt_procedure",
            snowflake_conn_id="snowflake_geopulse_conn",
            sql="CALL SILVER.SP_TRANSFORM_SILVER_LAYER();",
        )

        load_parquet_to_bronze >> execute_silver_elt_procedure

    # -------------------------------------------------------------------------
    # Stage 3: Data Quality Circuit-Breaker Sensor Gate
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="data_quality_circuit_breaker") as stage_quality_gate:
        audit_spatial_health = SpatialQualitySensor(
            task_id="verify_spatial_quality_and_clustering",
            table_name="SILVER.SPATIAL_PINGS",
            audit_view="SILVER.VW_CLUSTERING_AUDIT",
            snowflake_conn_id="snowflake_geopulse_conn",
            min_expected_rows=1000,
            max_allowed_null_rate=0.01,
            poke_interval=30,
            timeout=600,
            mode="reschedule",
        )

    # -------------------------------------------------------------------------
    # Stage 4: dbt Semantic & Gold Mart Transformations
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="dbt_gold_transformations") as stage_dbt:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
        )

        dbt_seed = BashOperator(
            task_id="dbt_seed",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed --select manhattan_transit_hubs",
        )

        dbt_run = BashOperator(
            task_id="dbt_run_models",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        )

        dbt_test = BashOperator(
            task_id="dbt_test_models",
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        )

        dbt_deps >> dbt_seed >> dbt_run >> dbt_test

    pipeline_end = EmptyOperator(task_id="pipeline_end")

    # Pipeline Topology
    pipeline_start >> stage_pyspark >> stage_snowflake >> stage_quality_gate >> stage_dbt >> pipeline_end
