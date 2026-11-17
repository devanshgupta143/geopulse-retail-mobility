"""
===============================================================================
GeoPulse Retail Mobility Analytics - Warehouse Maintenance & Optimization DAG
File: airflow/dags/geopulse_warehouse_maintenance_dag.py
Description: Weekly scheduled maintenance DAG running micro-partition clustering
             evaluations, refreshing Gold cannibalization snapshot tables, purging
             archived staging files, and tracking warehouse compute credit consumption.
===============================================================================
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

# Airflow Core & Operator Imports
try:
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.task_group import TaskGroup
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except ImportError:
    class DAG:
        def __init__(self, *args, **kwargs):
            self.dag_id = args[0] if args else kwargs.get("dag_id")
        def __enter__(self): return self
        def __exit__(self, *args): pass

    class EmptyOperator:
        def __init__(self, *args, **kwargs): pass
        def __rshift__(self, other): return other

    class SnowflakeOperator(EmptyOperator): pass
    class TaskGroup:
        def __init__(self, *args, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass

# Callback Imports
try:
    from callbacks.alert_callbacks import (
        on_task_failure_callback,
        on_sla_miss_callback,
        on_dag_success_callback,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugins"))
    try:
        from callbacks.alert_callbacks import (
            on_task_failure_callback,
            on_sla_miss_callback,
            on_dag_success_callback,
        )
    except Exception:
        def on_task_failure_callback(ctx): pass
        def on_sla_miss_callback(*args): pass
        def on_dag_success_callback(ctx): pass


DEFAULT_ARGS = {
    "owner": "DataPlatformTeam",
    "depends_on_past": False,
    "email": ["data-platform-alerts@geopulse.internal"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=1),
    "on_failure_callback": on_task_failure_callback,
}

SNOWFLAKE_CONN_ID = "snowflake_geopulse_conn"

with DAG(
    dag_id="geopulse_warehouse_maintenance_dag",
    default_args=DEFAULT_ARGS,
    description="Weekly Snowflake clustering optimization, Gold snapshot refresh, and stage cleanup",
    schedule_interval="0 3 * * 0",  # Every Sunday at 03:00 UTC
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=on_sla_miss_callback,
    on_success_callback=on_dag_success_callback,
    tags=["geopulse", "snowflake", "maintenance", "clustering", "cleanup"],
) as dag:

    start_maintenance = EmptyOperator(task_id="start_maintenance")

    # -------------------------------------------------------------------------
    # TaskGroup: Snowflake Partition Clustering & SOS Audit
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="clustering_and_performance_audit") as clustering_group:
        audit_spatial_clustering = SnowflakeOperator(
            task_id="audit_spatial_pings_clustering",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="""
                SELECT SYSTEM$CLUSTERING_INFORMATION('SILVER.SPATIAL_PINGS', '(PING_DATE, H3_INDEX_RES9)');
            """,
        )

        refresh_search_optimization = SnowflakeOperator(
            task_id="verify_search_optimization_status",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="""
                SHOW TABLES LIKE 'SPATIAL_PINGS' IN SCHEMA SILVER;
            """,
        )

        audit_spatial_clustering >> refresh_search_optimization

    # -------------------------------------------------------------------------
    # TaskGroup: Gold Mart Analytical Snapshotting
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="gold_mart_snapshots") as snapshot_group:
        create_cannibalization_weekly_snapshot = SnowflakeOperator(
            task_id="snapshot_gold_cannibalization_metrics",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="""
                CREATE TABLE IF NOT EXISTS GOLD.STORE_CANNIBALIZATION_WEEKLY_SNAPSHOT (
                    SNAPSHOT_DATE DATE,
                    PING_DATE DATE,
                    STORE_A_UNIQUE_VISITORS NUMBER,
                    STORE_B_UNIQUE_VISITORS NUMBER,
                    SHARED_VISITORS_A_AND_B NUMBER,
                    NET_INCREMENTAL_STORE_B_VISITORS NUMBER,
                    STORE_A_CANNIBALIZATION_RATE_PCT NUMBER(5,2),
                    STORE_B_CROSS_SHOPPING_RATE_PCT NUMBER(5,2),
                    JACCARD_SIMILARITY_COEFFICIENT NUMBER(5,4),
                    RISK_CLASSIFICATION VARCHAR(50),
                    SNAPSHOT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                );

                INSERT INTO GOLD.STORE_CANNIBALIZATION_WEEKLY_SNAPSHOT
                SELECT
                    CURRENT_DATE() AS SNAPSHOT_DATE,
                    PING_DATE,
                    STORE_A_UNIQUE_VISITORS,
                    STORE_B_UNIQUE_VISITORS,
                    SHARED_VISITORS_A_AND_B,
                    NET_INCREMENTAL_STORE_B_VISITORS,
                    STORE_A_CANNIBALIZATION_RATE_PCT,
                    STORE_B_CROSS_SHOPPING_RATE_PCT,
                    JACCARD_SIMILARITY_COEFFICIENT,
                    RISK_CLASSIFICATION,
                    CURRENT_TIMESTAMP()
                FROM GOLD.STORE_CANNIBALIZATION_SUMMARY;
            """,
        )

    # -------------------------------------------------------------------------
    # TaskGroup: Storage & Stage Purge Management
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="storage_and_stage_hygiene") as purge_group:
        purge_processed_stage_files = SnowflakeOperator(
            task_id="purge_archived_raw_mobility_stage_files",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="""
                REMOVE @BRONZE.RAW_MOBILITY_STAGE PATTERN = '.*_processed_.*\\.parquet';
            """,
        )

    # -------------------------------------------------------------------------
    # TaskGroup: FinOps & Warehouse Credit Consumption Audit
    # -------------------------------------------------------------------------
    with TaskGroup(group_id="finops_credit_metering") as finops_group:
        log_warehouse_credit_usage = SnowflakeOperator(
            task_id="audit_warehouse_credit_consumption",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="""
                SELECT
                    WAREHOUSE_NAME,
                    SUM(CREDITS_USED) AS TOTAL_CREDITS_CONSUMED,
                    MIN(START_TIME) AS AUDIT_WINDOW_START,
                    MAX(END_TIME) AS AUDIT_WINDOW_END
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
                  AND WAREHOUSE_NAME IN ('COMPUTE_WH', 'TRANSFORM_WH')
                GROUP BY WAREHOUSE_NAME;
            """,
        )

    end_maintenance = EmptyOperator(task_id="end_maintenance")

    # Maintenance Workflow Dependencies
    start_maintenance >> clustering_group >> snapshot_group >> purge_group >> finops_group >> end_maintenance
