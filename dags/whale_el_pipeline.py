"""
whale_el_pipeline.py — Airflow DAG for the Whale Alert EL Pipeline.

Schedule: Every 30 minutes.

Tasks:
  1. extract   — Scrape whale-alert.io using WhaleClient.
  2. transform — Convert records → Pandas DataFrame → CSV string (in XCom).
  3. load      — Upload the CSV to MinIO via StorageClient.

Error handling:
  - Each task has retries=3 with a 60-second delay.
  - Downstream failures do not block unrelated tasks.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# NOTE: Airflow imports sys.path-aware modules from /opt/airflow/src
from src.clients.whale_client import WhaleClient, WhaleRecord
from src.clients.storage_client import StorageClient
from src.utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=60),
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def task_extract(**context: Any) -> List[Dict[str, Any]]:
    """
    Extract task: fetches whale data and pushes serializable dicts via XCom.

    Returns:
        List of dicts (one per WhaleRecord) pushed to XCom.
    """
    logger.info("=== TASK: extract — START ===")

    with WhaleClient() as client:
        records: List[WhaleRecord] = client.extract()

    serialised = [r.to_dict() for r in records]
    logger.info("=== TASK: extract — %d records extracted ===", len(serialised))
    return serialised  # Airflow auto-pushes return value to XCom


def task_load(**context: Any) -> str:
    """
    Load task: pulls records from XCom, converts to CSV, uploads to MinIO.

    Returns:
        The S3 object key of the uploaded file.
    """
    logger.info("=== TASK: load — START ===")

    # Pull records produced by the extract task
    ti = context["ti"]
    records: List[Dict[str, Any]] = ti.xcom_pull(task_ids="extract")

    if not records:
        raise ValueError("No records received from extract task via XCom.")

    # Build DataFrame (used purely as an export container)
    df = pd.DataFrame(records)
    csv_content: str = df.to_csv(index=False)

    # Dynamic filename: whale_data_YYYYMMDD_HHMM.csv
    now_utc: datetime = datetime.now(timezone.utc)
    object_key: str = now_utc.strftime("whale_data_%Y%m%d_%H%M.csv")

    logger.info("Target object key: %s", object_key)

    storage = StorageClient()
    storage.ensure_bucket_exists()
    bytes_uploaded: int = storage.upload_csv(csv_content, object_key)

    logger.info(
        "=== TASK: load — COMPLETE | file=%s | bytes=%d ===",
        object_key,
        bytes_uploaded,
    )
    return object_key


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="whale_el_pipeline",
    description="EL pipeline: extract whale data from whale-alert.io and load to MinIO",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["whale-alert", "el", "minio"],
    doc_md=__doc__,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=task_load,
    )

    # Pipeline order
    extract_task >> load_task
