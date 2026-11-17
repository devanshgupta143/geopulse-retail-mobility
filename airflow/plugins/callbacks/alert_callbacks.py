"""
===============================================================================
GeoPulse Retail Mobility Analytics - Alerting & SLA Callbacks Plugin
File: airflow/plugins/callbacks/alert_callbacks.py
Description: Structured Slack Webhook and observability callbacks for Airflow.
             Enriches task failure, SLA miss, and DAG success alerts with spatial
             telemetry metrics, execution logs, and warehouse partition statistics.
===============================================================================
"""

import json
import logging
import os
import urllib.request
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger("airflow.callbacks.geopulse_alerts")

# Default Slack webhook and notification settings
DEFAULT_SLACK_WEBHOOK = os.getenv(
    "SLACK_WEBHOOK_URL",
    "https://hooks.slack.com/services/MOCK/SPATIAL/ALERTS"
)
ALERT_CHANNEL = os.getenv("ALERT_CHANNEL", "#geopulse-mobility-alerts")


def _send_slack_notification(payload: Dict[str, Any], webhook_url: Optional[str] = None) -> bool:
    """Dispatches formatted JSON blocks payload to the configured Slack Webhook."""
    url = webhook_url or DEFAULT_SLACK_WEBHOOK
    try:
        if "MOCK" in url or not url.startswith("http") or requests is None:
            logger.info(f"[SIMULATED SLACK DISPATCH] Channel: {ALERT_CHANNEL}\n{json.dumps(payload, indent=2)}")
            return True

        response = requests.post(url, json=payload, timeout=10)
        if response.status_code == 200:
            logger.info("Slack alert delivered successfully.")
            return True
        else:
            logger.warning(f"Slack delivery returned status {response.status_code}: {response.text}")
            return False
    except Exception as exc:
        logger.error(f"Failed to transmit alert to Slack: {exc}")
        return False


def on_task_failure_callback(context: Dict[str, Any]) -> None:
    """
    Triggered when an individual task instance fails.
    Extracts run metadata, execution logs, and upstream spatial metrics from XCom.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else context.get("task", {}).task_id
    dag_id = ti.dag_id if ti else "unknown_dag"
    run_id = context.get("run_id", "manual_run")
    log_url = ti.log_url if ti else "http://localhost:8080"
    exception = context.get("exception", "No exception message captured")
    execution_date = str(context.get("logical_date") or context.get("execution_date"))

    # Pull spatial XCom metrics if available
    spatial_metrics = {}
    if ti and hasattr(ti, "xcom_pull"):
        spatial_metrics = ti.xcom_pull(key="spatial_metrics") or {}

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚨 Airflow Task Failure: {dag_id}.{task_id}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:* `{task_id}`"},
                {"type": "mrkdwn", "text": f"*Execution Date:* `{execution_date}`"},
                {"type": "mrkdwn", "text": f"*Run ID:* `{run_id}`"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Exception Details:*\n```{str(exception)[:400]}```",
            },
        },
    ]

    if spatial_metrics:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Spatial Pipeline Metrics:*\n"
                    f"• Processed Pings: `{spatial_metrics.get('total_pings_read', 'N/A')}`\n"
                    f"• Enriched Count: `{spatial_metrics.get('enriched_pings_count', 'N/A')}`\n"
                    f"• Partitions: `{spatial_metrics.get('partition_count', 'N/A')}`"
                ),
            },
        })

    blocks.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "View Airflow Log", "emoji": True},
                "url": log_url,
                "style": "danger",
            }
        ],
    })

    payload = {"channel": ALERT_CHANNEL, "blocks": blocks}
    _send_slack_notification(payload)


def on_sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: List[Any],
    blocking_tis: List[Any],
) -> None:
    """
    Triggered when an execution window breaches the registered SLA deadline.
    """
    dag_id = getattr(dag, "dag_id", str(dag))
    logger.warning(f"SLA Miss detected on DAG: {dag_id}. Tasks: {task_list}")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"⏱️ Airflow SLA Miss Alert: {dag_id}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                {"type": "mrkdwn", "text": f"*SLA Threshold:* `Breached`"},
                {"type": "mrkdwn", "text": f"*Affected Tasks:* `{task_list}`"},
                {"type": "mrkdwn", "text": f"*Blocking Tasks:* `{blocking_task_list or 'None'}`"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Downstream retail analytics and Gold marts may experience data latency.",
            },
        },
    ]

    payload = {"channel": ALERT_CHANNEL, "blocks": blocks}
    _send_slack_notification(payload)


def on_dag_success_callback(context: Dict[str, Any]) -> None:
    """
    Triggered upon successful completion of the end-to-end DAG run.
    """
    dag_id = context.get("dag", {}).dag_id if hasattr(context.get("dag"), "dag_id") else "geopulse_dag"
    execution_date = str(context.get("logical_date") or context.get("execution_date"))
    run_id = context.get("run_id", "scheduled_run")

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"✅ Airflow DAG Success: {dag_id}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:* `{dag_id}`"},
                {"type": "mrkdwn", "text": f"*Status:* `SUCCESS`"},
                {"type": "mrkdwn", "text": f"*Execution Date:* `{execution_date}`"},
                {"type": "mrkdwn", "text": f"*Run ID:* `{run_id}`"},
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "All PySpark spatial indexing, Snowflake stages, and dbt Gold marts refreshed cleanly.",
            },
        },
    ]

    payload = {"channel": ALERT_CHANNEL, "blocks": blocks}
    _send_slack_notification(payload)
