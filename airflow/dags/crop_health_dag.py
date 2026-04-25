"""
airflow/dags/crop_health_dag.py
---------------------------------
Daily DAG — Airflow orchestrates, Databricks does its own ingestion.

Architecture:
  - Airflow tasks 1 & 2 validate APIs are reachable + write local parquet
    (used by Kafka producer later)
  - Task 3 triggers the Databricks job which independently fetches
    Sentinel-2 and weather data, runs T1/T2/T3, writes processed.parquet
    to the Unity Catalog Volume
  - Task 4 downloads processed.parquet from the Volume to local disk
  - Tasks 5 & 6 publish Kafka alerts and update the Streamlit dashboard

Task flow:
  ingest_sentinel2  ──┐
                       ├──▶ trigger_databricks_job ──▶ download_from_databricks
  ingest_weather    ──┘              │                          │
                                (Spark does                publish_to_kafka
                              its own fetch                     │
                              + T1/T2/T3)              update_dashboard_data
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")


def _db_headers() -> dict:
    return {"Authorization": f"Bearer {os.environ['DATABRICKS_TOKEN']}"}


def _db_host() -> str:
    return os.environ["DATABRICKS_HOST"].rstrip("/")


def _volume_path() -> str:
    return os.environ["DATABRICKS_VOLUME_PATH"]


# ─── Task 1: Ingest Sentinel-2 locally (validates API + produces local parquet) ─


def task_ingest_sentinel2(**context) -> str:
    from ingestion.sentinel2_ingestor import ingest_sentinel2

    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    output = f"{DATA_DIR}/sentinel2_manifest.parquet"
    path = ingest_sentinel2(output_path=output, days_back=7)
    logger.info("Sentinel-2 manifest written to %s", path)
    return path


# ─── Task 2: Ingest weather locally ──────────────────────────────────────────


def task_ingest_weather(**context) -> str:
    from ingestion.weather_ingestor import ingest_weather

    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    output = f"{DATA_DIR}/weather.parquet"
    path = ingest_weather(output_path=output, days_back=7)
    logger.info("Weather data written to %s", path)
    return path


# ─── Task 3: Trigger Databricks job ──────────────────────────────────────────


def task_trigger_databricks(**context) -> str:
    """
    Triggers the Databricks job which:
      - Fetches Sentinel-2 + weather data independently (no file push needed)
      - Runs T1 (NDVI) + T2 (Weather enrichment) + T3 (Anomaly flagging)
      - Writes processed.parquet to the Unity Catalog Volume

    Polls every 20s until the job finishes (max 30 min).
    """
    host = _db_host()
    headers = _db_headers()
    job_id = int(os.environ["DATABRICKS_JOB_ID"])

    # Trigger
    resp = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": job_id},
        timeout=30,
    )
    resp.raise_for_status()
    run_id = resp.json()["run_id"]
    logger.info("Databricks job triggered — job_id=%s run_id=%s", job_id, run_id)

    # Poll until done
    deadline = time.time() + 30 * 60

    while time.time() < deadline:
        time.sleep(20)
        r = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=headers,
            params={"run_id": run_id},
            timeout=30,
        )
        r.raise_for_status()
        state = r.json()["state"]
        life_cycle = state["life_cycle_state"]
        result = state.get("result_state", "—")
        logger.info("run_id=%s  state=%s  result=%s", run_id, life_cycle, result)

        if life_cycle == "TERMINATED":
            if result == "SUCCESS":
                logger.info("Databricks job completed successfully.")
                break
            else:
                raise RuntimeError(
                    f"Databricks job failed (result={result}). "
                    f"Check: {host}/#job/{job_id}/run/{run_id}"
                )
        elif life_cycle in ("INTERNAL_ERROR", "SKIPPED"):
            raise RuntimeError(f"Databricks run ended with state={life_cycle}")
    else:
        raise TimeoutError(f"Databricks run_id={run_id} timed out after 30 minutes.")

    return f"{_volume_path()}/processed.parquet"


# ─── Task 4: Download processed parquet from Volume ──────────────────────────
def task_download_from_databricks(**context) -> str:
    host = _db_host()
    headers = _db_headers()
    volume_dir = context["ti"].xcom_pull(task_ids="trigger_databricks_job")
    local_out = f"{DATA_DIR}/processed.parquet"

    # UC Files API: list directory contents
    # Correct endpoint for listing is /api/2.0/fs/directories
    folder = volume_dir.strip("/")
    list_url = f"{host}/api/2.0/fs/directories/{folder}"
    logger.info("Listing directory: %s", list_url)

    r = requests.get(list_url, headers=headers, timeout=30)
    if r.status_code == 404:
        raise RuntimeError(f"Directory not found: {volume_dir}")
    r.raise_for_status()

    contents = r.json().get("contents", [])
    logger.info("Contents: %s", [f["name"] for f in contents])

    part_files = [
        f
        for f in contents
        if f["name"].endswith(".parquet") and not f["name"].startswith("_")
    ]

    if not part_files:
        raise RuntimeError(
            f"No part file found inside {volume_dir}. "
            f"Contents: {[f['name'] for f in contents]}"
        )

    part_path = part_files[0]["path"].strip("/")
    download_url = f"{host}/api/2.0/fs/files/{part_path}"
    logger.info("Downloading: %s", download_url)

    for attempt in range(1, 4):
        try:
            resp = requests.get(download_url, headers=headers, stream=True, timeout=120)
            resp.raise_for_status()
            Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
            with open(local_out, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    fh.write(chunk)
            size = Path(local_out).stat().st_size
            logger.info("Downloaded → %s (%d bytes)", local_out, size)
            return local_out
        except requests.RequestException as exc:
            logger.warning("Attempt %d failed: %s", attempt, exc)
            if attempt == 3:
                raise RuntimeError("Failed to download after 3 attempts")
            time.sleep(2**attempt)


# ─── Task 5: Publish anomalies → Kafka ───────────────────────────────────────


def task_publish_to_kafka(**context) -> int:
    from streaming.kafka_producer import publish_anomalies

    local_path = context["ti"].xcom_pull(task_ids="download_from_databricks")
    count = publish_anomalies(parquet_path=local_path)
    logger.info("Published %d anomaly alerts to Kafka.", count)
    return count


# ─── Task 6: Update dashboard snapshot ───────────────────────────────────────


def task_update_dashboard(**context) -> None:
    processed_path = context["ti"].xcom_pull(task_ids="download_from_databricks")
    dashboard_data = f"{DATA_DIR}/dashboard_latest.parquet"

    if processed_path and Path(processed_path).exists():
        shutil.copy(processed_path, dashboard_data)
        logger.info("Dashboard parquet updated: %s", dashboard_data)
    else:
        logger.warning("No processed parquet found — dashboard not updated.")

    meta = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "dag_run_id": context["run_id"],
    }
    Path(f"{DATA_DIR}/meta.json").write_text(json.dumps(meta))
    logger.info("Dashboard meta written: %s", meta)


# ─── DAG definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="crop_health_pipeline",
    description="Daily crop health: Sentinel-2 + Weather → Databricks Spark → Kafka → Streamlit",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["satellite", "agriculture", "ndvi", "kafka", "databricks"],
) as dag:

    ingest_s2 = PythonOperator(
        task_id="ingest_sentinel2",
        python_callable=task_ingest_sentinel2,
    )

    ingest_wx = PythonOperator(
        task_id="ingest_weather",
        python_callable=task_ingest_weather,
    )

    trigger_db = PythonOperator(
        task_id="trigger_databricks_job",
        python_callable=task_trigger_databricks,
    )

    download = PythonOperator(
        task_id="download_from_databricks",
        python_callable=task_download_from_databricks,
    )

    publish = PythonOperator(
        task_id="publish_to_kafka",
        python_callable=task_publish_to_kafka,
    )

    update_dash = PythonOperator(
        task_id="update_dashboard_data",
        python_callable=task_update_dashboard,
    )

    # ─── Dependency graph ─────────────────────────────────────────────────
    #
    #  ingest_sentinel2 ──┐
    #                      ├──▶ trigger_databricks_job ──▶ download_from_databricks
    #  ingest_weather   ──┘       (Spark fetches its             │
    #                              own data + T1/T2/T3)   publish_to_kafka
    #                                                            │
    #                                                   update_dashboard_data
    #
    [ingest_s2, ingest_wx] >> trigger_db >> download >> publish >> update_dash
