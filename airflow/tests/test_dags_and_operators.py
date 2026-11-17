"""
===============================================================================
GeoPulse Retail Mobility Analytics - Airflow DAG & Operator Unit Test Suite
File: airflow/tests/test_dags_and_operators.py
Description: Validates DAG integrity, DagBag import hygiene, cycle-free topology,
             schedule intervals, task counts, and mocked execution logic for
             PySparkSpatialOperator, SnowflakeStageLoadOperator, and SpatialQualitySensor.
===============================================================================
"""

import ast
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure Airflow plugins and dags are available on sys.path
TEST_DIR = Path(__file__).resolve().parent
AIRFLOW_ROOT = TEST_DIR.parent
DAGS_DIR = AIRFLOW_ROOT / "dags"
PLUGINS_DIR = AIRFLOW_ROOT / "plugins"

sys.path.insert(0, str(PLUGINS_DIR))
sys.path.insert(0, str(DAGS_DIR))

# Import Custom Operators and Sensors
from operators.pyspark_spatial_operator import PySparkSpatialOperator
from operators.snowflake_stage_operator import SnowflakeStageLoadOperator
from sensors.spatial_quality_sensor import SpatialQualitySensor
from callbacks.alert_callbacks import (
    on_task_failure_callback,
    on_sla_miss_callback,
    on_dag_success_callback,
)


class TestDagIntegrity:
    """Verifies that all DAG definition files compile cleanly with valid schedules and topologies."""

    def test_dag_python_syntax(self):
        """Verifies that all DAG python files are free of syntax errors via AST parsing."""
        dag_files = list(DAGS_DIR.glob("*.py"))
        assert len(dag_files) >= 2, f"Expected at least 2 DAG files in {DAGS_DIR}, found {len(dag_files)}"

        for dag_file in dag_files:
            with open(dag_file, "r", encoding="utf-8") as f:
                source_code = f.read()
            tree = ast.parse(source_code, filename=str(dag_file))
            assert tree is not None, f"Failed to parse AST for {dag_file.name}"

    def test_dagbag_import_and_integrity(self):
        """Tests Airflow DagBag loading, import error absence, and cycle detection."""
        try:
            from airflow.models import DagBag
            dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

            # Check for zero import errors
            assert len(dagbag.import_errors) == 0, (
                f"DagBag import errors detected: {dagbag.import_errors}"
            )

            # Verify geopulse_mobility_elt_dag
            elt_dag = dagbag.get_dag("geopulse_mobility_elt_dag")
            assert elt_dag is not None, "geopulse_mobility_elt_dag not found in DagBag"
            assert elt_dag.schedule_interval in ("@daily", "0 0 * * *")
            assert elt_dag.catchup is False
            assert len(elt_dag.tasks) >= 6

            # Verify geopulse_warehouse_maintenance_dag
            maint_dag = dagbag.get_dag("geopulse_warehouse_maintenance_dag")
            assert maint_dag is not None, "geopulse_warehouse_maintenance_dag not found in DagBag"
            assert maint_dag.schedule_interval == "0 3 * * 0"
            assert maint_dag.catchup is False
            assert len(maint_dag.tasks) >= 4

        except ImportError:
            # Fallback direct module execution check if airflow binary is not installed
            import importlib.util
            for dag_file in DAGS_DIR.glob("*.py"):
                spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                assert hasattr(mod, "dag"), f"DAG object missing in {dag_file.name}"


class TestPySparkSpatialOperator:
    """Tests execution, parameter validation, and XCom metric generation for PySparkSpatialOperator."""

    @pytest.fixture
    def mock_geofences_file(self, tmp_path):
        geofences_path = tmp_path / "test_geofences.json"
        data = {
            "stores": [
                {"store_id": "STORE_A_EXISTING", "latitude": 40.7549, "longitude": -73.9840},
                {"store_id": "STORE_B_PROPOSED_5TH", "latitude": 40.7565, "longitude": -73.9815},
            ]
        }
        with open(geofences_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        return str(geofences_path)

    def test_pyspark_spatial_operator_success(self, tmp_path, mock_geofences_file):
        output_dir = tmp_path / "output_parquet"
        input_csv = tmp_path / "mock_pings.csv"
        with open(input_csv, "w", encoding="utf-8") as f:
            f.write("ping_id,device_id,latitude,longitude,ping_timestamp\n")
            for i in range(150):
                f.write(f"p_{i},dev_{i},40.7550,-73.9830,2026-11-15 10:00:00\n")

        operator = PySparkSpatialOperator(
            task_id="test_spatial_op",
            input_csv_path=str(input_csv),
            output_parquet_path=str(output_dir),
            geofences_path=mock_geofences_file,
            h3_resolution=9,
            catchment_radius_meters=250.0,
            min_expected_records=50,
        )

        mock_ti = MagicMock()
        mock_context = {"task_instance": mock_ti, "ds": "2026-11-15"}

        result = operator.execute(context=mock_context)

        assert result["status"] == "SUCCESS"
        assert result["enriched_pings_count"] >= 50
        assert result["partition_count"] >= 1
        assert result["h3_resolution"] == 9
        mock_ti.xcom_push.assert_any_call(key="spatial_metrics", value=result)

    def test_pyspark_spatial_operator_sla_failure(self, tmp_path, mock_geofences_file):
        output_dir = tmp_path / "output_parquet"
        input_csv = tmp_path / "empty_pings.csv"
        with open(input_csv, "w", encoding="utf-8") as f:
            f.write("ping_id,device_id,latitude,longitude,ping_timestamp\n")

        operator = PySparkSpatialOperator(
            task_id="test_spatial_fail",
            input_csv_path=str(input_csv),
            output_parquet_path=str(output_dir),
            geofences_path=mock_geofences_file,
            min_expected_records=500000,  # Unattainable threshold
        )

        with pytest.raises(ValueError, match="SLA Violation"):
            operator.execute(context={})


class TestSnowflakeStageLoadOperator:
    """Tests Snowflake stage upload and COPY INTO operator behavior."""

    def test_snowflake_stage_operator_execution(self, tmp_path):
        test_file = tmp_path / "metrics.parquet"
        test_file.touch()

        operator = SnowflakeStageLoadOperator(
            task_id="test_stage_load",
            local_file_pattern=str(tmp_path / "*.parquet"),
            stage_name="@BRONZE.RAW_MOBILITY_STAGE",
            target_table="BRONZE.RAW_GPS_PINGS",
            min_expected_rows=10,
        )

        mock_ti = MagicMock()
        mock_context = {"task_instance": mock_ti}

        result = operator.execute(context=mock_context)
        assert result["status"] == "SUCCESS"
        assert result["rows_loaded"] >= 10
        assert result["target_table"] == "BRONZE.RAW_GPS_PINGS"


class TestSpatialQualitySensor:
    """Tests circuit-breaker evaluations on coordinate bounds and H3 integrity."""

    def test_sensor_passing_audit(self):
        sensor = SpatialQualitySensor(
            task_id="test_dq_sensor",
            table_name="SILVER.SPATIAL_PINGS",
            min_expected_rows=100,
            max_allowed_null_rate=0.01,
        )

        mock_ti = MagicMock()
        mock_context = {"task_instance": mock_ti}

        status = sensor.poke(context=mock_context)
        assert status is True
        mock_ti.xcom_push.assert_called_once()

    def test_sensor_circuit_breaker_violation(self):
        sensor = SpatialQualitySensor(
            task_id="test_dq_fail",
            table_name="SILVER.SPATIAL_PINGS",
            min_expected_rows=1000000,  # Exceeds mock count
        )
        assert sensor.poke(context={}) is False


class TestAlertCallbacks:
    """Tests notification generation and dispatch functions."""

    @patch("callbacks.alert_callbacks._send_slack_notification")
    def test_failure_callback(self, mock_notify):
        mock_notify.return_value = True
        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.dag_id = "test_dag"
        mock_ti.log_url = "http://localhost:8080/log"
        mock_ti.xcom_pull.return_value = {"total_pings_read": 200000}

        context = {
            "task_instance": mock_ti,
            "exception": ValueError("Mocked error"),
            "execution_date": "2026-11-15T00:00:00",
        }

        # Should execute cleanly without raising unhandled exceptions
        on_task_failure_callback(context)
        on_sla_miss_callback("test_dag", "task_a", "task_b", [], [])
        on_dag_success_callback(context)
