#!/usr/bin/env python3
"""
===============================================================================
GeoPulse Retail Mobility Analytics - Airflow Environment Initialization Script
File: airflow/scripts/init_connections.py
Description: Idempotently provisions Airflow Connections and Variables required
             by the GeoPulse data platform. Configures the Snowflake warehouse
             connection, Manhattan spatial boundaries, catchment thresholds,
             and pipeline filesystem paths.
===============================================================================
"""

import json
import logging
import os
import sys
from pathlib import Path

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("geopulse.airflow.init")

# Project root resolution
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_default_variables():
    """Returns the standardized project variables dictionary."""
    return {
        "geopulse_project_root": os.getenv("GEOPULSE_PROJECT_ROOT", str(PROJECT_ROOT)),
        "geopulse_pyspark_input_path": os.getenv(
            "GEOPULSE_PYSPARK_INPUT_PATH",
            str(PROJECT_ROOT / "pyspark" / "mock_gps_pings.csv")
        ),
        "geopulse_pyspark_output_path": os.getenv(
            "GEOPULSE_PYSPARK_OUTPUT_PATH",
            str(PROJECT_ROOT / "pyspark" / "output" / "partitioned_metrics")
        ),
        "geopulse_store_geofences_path": os.getenv(
            "GEOPULSE_STORE_GEOFENCES_PATH",
            str(PROJECT_ROOT / "pyspark" / "store_geofences.json")
        ),
        "geopulse_catchment_radius_meters": float(os.getenv("GEOPULSE_CATCHMENT_RADIUS", "250.0")),
        "geopulse_h3_resolution": int(os.getenv("GEOPULSE_H3_RESOLUTION", "9")),
        "manhattan_min_lat": float(os.getenv("MANHATTAN_MIN_LAT", "40.700000")),
        "manhattan_max_lat": float(os.getenv("MANHATTAN_MAX_LAT", "40.850000")),
        "manhattan_min_lon": float(os.getenv("MANHATTAN_MIN_LON", "-74.050000")),
        "manhattan_max_lon": float(os.getenv("MANHATTAN_MAX_LON", "-73.900000")),
        "geopulse_slack_webhook_url": os.getenv(
            "SLACK_WEBHOOK_URL",
            "https://hooks.slack.com/services/MOCK/SPATIAL/ALERTS"
        ),
        "geopulse_alert_email": os.getenv(
            "ALERT_EMAIL",
            "data-platform-alerts@geopulse.internal"
        ),
        "snowflake_stage_name": os.getenv("SNOWFLAKE_STAGE_NAME", "@BRONZE.RAW_MOBILITY_STAGE"),
        "snowflake_raw_table": os.getenv("SNOWFLAKE_RAW_TABLE", "BRONZE.RAW_GPS_PINGS"),
        "snowflake_silver_table": os.getenv("SNOWFLAKE_SILVER_TABLE", "SILVER.SPATIAL_PINGS"),
    }


def get_snowflake_connection_config():
    """Constructs Snowflake connection parameters from environment variables."""
    account = os.getenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "GEOPULSE_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "SILVER")
    role = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
    user = os.getenv("SNOWFLAKE_USER", "DEV_ANALYTICS_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD", "DevPassSecure123!")

    extra = {
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "role": role,
        "insecure_mode": False,
    }

    return {
        "conn_id": "snowflake_geopulse_conn",
        "conn_type": "snowflake",
        "host": f"{account}.snowflakecomputing.com",
        "login": user,
        "password": password,
        "schema": schema,
        "extra": json.dumps(extra)
    }


def seed_variables():
    """Seeds or updates Airflow Variables idempotently."""
    try:
        from airflow.models import Variable
        logger.info("Initializing Airflow Variables via ORM...")
        variables = get_default_variables()
        for key, value in variables.items():
            Variable.set(key=key, value=value, serialize_json=isinstance(value, (dict, list)))
            logger.info(f"Variable '{key}' successfully synchronized.")
        logger.info("All Airflow Variables provisioned.")
    except ImportError:
        logger.warning("Airflow is not installed in the active environment. Simulating Variable seeding.")
        for key, value in get_default_variables().items():
            logger.info(f"[SIMULATED] Airflow Variable '{key}' = {value}")


def seed_connections():
    """Seeds or updates Airflow Connections idempotently."""
    conn_config = get_snowflake_connection_config()
    conn_id = conn_config["conn_id"]

    try:
        from airflow.models import Connection
        from airflow.utils.session import create_session

        logger.info(f"Synchronizing Connection '{conn_id}'...")
        with create_session() as session:
            existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            if existing:
                existing.conn_type = conn_config["conn_type"]
                existing.host = conn_config["host"]
                existing.login = conn_config["login"]
                existing.password = conn_config["password"]
                existing.schema = conn_config["schema"]
                existing.extra = conn_config["extra"]
                logger.info(f"Updated existing Connection '{conn_id}'.")
            else:
                new_conn = Connection(
                    conn_id=conn_config["conn_id"],
                    conn_type=conn_config["conn_type"],
                    host=conn_config["host"],
                    login=conn_config["login"],
                    password=conn_config["password"],
                    schema=conn_config["schema"],
                    extra=conn_config["extra"],
                )
                session.add(new_conn)
                logger.info(f"Created new Connection '{conn_id}'.")
            session.commit()
    except ImportError:
        logger.warning("Airflow is not installed in the active environment. Simulating Connection creation.")
        logger.info(f"[SIMULATED] Connection '{conn_id}' configured for account: {conn_config['host']}")


def main():
    """Entry point for initializing connections and variables."""
    logger.info("Starting GeoPulse Airflow connection and variable seeding...")
    seed_variables()
    seed_connections()
    logger.info("Airflow environment initialization complete.")


if __name__ == "__main__":
    main()
