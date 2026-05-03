# 🌾 Crop Health Monitoring Pipeline

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apachespark&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?logo=apachekafka&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Notebook-FF3621?logo=databricks&logoColor=white)

End-to-end Data Engineering pipeline that ingests Sentinel-2 satellite imagery
and weather data, computes NDVI vegetation indices, detects crop stress anomalies,
and serves a live dashboard.

## Recent Updates

- Dashboard now aggregates pixel-level rows to one row per day per AOI for KPIs and trend/weather charts.
- Spatial map still uses raw pixel-level lat/lon points for anomaly localization.
- Databricks notebook is tracked in `databricks/crop_health_pipeline.ipynb`.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER (Airflow DAG · 06:00 UTC daily)      │
│                                                                             │
│  ┌──────────────────────┐          ┌──────────────────────┐                │
│  │  Sentinel-2 STAC API │          │   Open-Meteo API     │                │
│  │  (Element-84, free)  │          │  (weather, free)     │                │
│  │  sentinel2_ingestor  │          │  weather_ingestor    │                │
│  └──────────┬───────────┘          └──────────┬───────────┘                │
│             │  scene manifest.parquet          │  weather.parquet           │
└─────────────┼──────────────────────────────────┼───────────────────────────┘
              │                                  │
              └──────────────┬───────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TRANSFORMATION LAYER (PySpark · Databricks Community)    │
│                                                                             │
│   T1: NDVI Calculation      (NIR-Red)/(NIR+Red) per pixel per date         │
│   T2: Weather Enrichment    Join weather by AOI + date                      │
│   T3: Anomaly Flagging      30-day rolling NDVI · flag drop > 20%          │
│                                                                             │
│   Output → Delta Lake on DBFS  /FileStore/crop_health/processed/            │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │  processed.parquet
              ┌────────────────────┴────────────────────┐
              │                                         │
              ▼                                         ▼
┌─────────────────────────┐              ┌──────────────────────────────────┐
│   STREAMING LAYER       │              │   STORAGE LAYER                  │
│                         │              │                                  │
│  kafka_producer.py      │              │  Delta Lake (Databricks DBFS)    │
│  → topic: crop-alerts   │              │  ├── /CDL_samples/               │
│                         │              │  ├── /s2_sampled/                │
│  kafka_consumer.py      │              │  └── /crop_health/processed/     │
│  → alerts.jsonl         │              │                                  │
│                         │              │  Local (Docker volume)           │
│  Kafka UI :8081         │              │  └── data/*.parquet              │
└─────────────┬───────────┘              └──────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              VISUALISATION LAYER (Streamlit · :8501)                        │
│                                                                             │
│  Tab 1: NDVI Trend        Daily AOI trend + rolling mean + anomaly overlay  │
│  Tab 2: Weather           Daily temp / precip / NDVI relationships           │
│  Tab 3: Kafka Alerts      Real-time anomaly table from alerts.jsonl         │
│  Tab 4: Spatial View      Raw pixel map (lat/lon) coloured by NDVI          │
└─────────────────────────────────────────────────────────────────────────────┘

ORCHESTRATION: Apache Airflow (Docker · :8080)
  DAG: crop_health_pipeline  ·  schedule: 0 6 * * *
  ingest_sentinel2 ──┐
                      ├──▶ run_transformations ──▶ publish_to_kafka ──▶ update_dashboard_data
  ingest_weather   ──┘
```

---

## Data Sources

| Source                                                                          | Type                                        | Frequency | Auth        |
| ------------------------------------------------------------------------------- | ------------------------------------------- | --------- | ----------- |
| [Sentinel-2 L2A via Element-84 STAC](https://earth-search.aws.element84.com/v1) | Satellite imagery (NIR, Red, SCL bands)     | Daily     | None (free) |
| [Open-Meteo](https://api.open-meteo.com)                                        | Hourly weather (temp, precipitation, solar) | Hourly    | None (free) |

---

## Stack & Justification

| Layer            | Technology                            | Why                                                                |
| ---------------- | ------------------------------------- | ------------------------------------------------------------------ |
| Orchestration    | **Apache Airflow 2.8**                | Industry standard; handles DAG dependencies, retries, scheduling   |
| Batch ingestion  | **Python + httpx**                    | Async-capable, lightweight; ideal for REST/STAC APIs               |
| Streaming        | **Apache Kafka + confluent-kafka**    | Durable message queue for anomaly alerts; scales horizontally      |
| Processing       | **PySpark on Databricks Community**   | Free Spark cluster; matches the reference NASA notebooks exactly   |
| Storage          | **Delta Lake (DBFS) + local Parquet** | ACID transactions, time travel; free on Databricks                 |
| Transformation   | **PySpark + pandas fallback**         | Full Spark on Databricks; pandas for local CI/testing              |
| Visualisation    | **Streamlit**                         | Zero-config Python dashboards; free public URL via Streamlit Cloud |
| Containerisation | **Docker + docker-compose**           | Reproducible local deployment; one command to start everything     |

---

## Project Structure

```
crop-health-pipeline/
├── databricks/
│   └── crop_health_pipeline.ipynb  # Databricks transformation notebook
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       └── crop_health_dag.py       # Main Airflow DAG
├── ingestion/
│   ├── sentinel2_ingestor.py        # Sentinel-2 STAC ingestion (+ retry)
│   └── weather_ingestor.py          # Open-Meteo ingestion (+ retry)
├── processing/
│   └── transformations.py           # T1 NDVI · T2 Enrich · T3 Anomaly (PySpark)
├── streaming/
│   ├── kafka_producer.py            # Publish anomaly alerts to Kafka
│   └── kafka_consumer.py            # Consume and write alerts.jsonl
├── dashboard/
│   ├── app.py                       # Streamlit dashboard
│   ├── Dockerfile
│   └── requirements.txt
├── data/
│   ├── dashboard_latest.parquet     # Dashboard input snapshot
│   ├── processed.parquet            # Processed output
│   └── meta.json                    # Last run metadata
├── tests/
│   └── test_transformations.py      # 12 unit / data quality tests
├── docker-compose.yml
├── requirements.txt
├── reset.ps1
└── README.md
```

---

## Quick Start (Docker)

```bash
# 1. Clone the repo
git clone https://github.com/Saifeddine-Rejeb/crop-health-pipeline
cd crop-health-pipeline

# 2. Create the shared data directory
# Bash:
mkdir -p data
# PowerShell:
mkdir data

# 3. Start everything
docker compose up --build -d

# 4. Wait ~60s for Airflow to initialise, then open:
#    Airflow UI  →  http://localhost:8080  (admin / admin)
#    Dashboard   →  http://localhost:8501
#    Kafka UI    →  http://localhost:8081

# 5. Trigger a manual pipeline run
docker compose exec airflow-webserver airflow dags trigger crop_health_pipeline
```

### Environment note

- Ensure `.env` is UTF-8 (without BOM) and contains `AIRFLOW_UID=50000`.

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Transformations

### T1 – NDVI Calculation

```
NDVI = (NIR - Red) / (NIR + Red)
```

Computed per Sentinel-2 pixel per scene date. Edge case: returns `NaN` when NIR + Red = 0.

### T2 – Weather Enrichment

Daily means/sums aggregated from hourly Open-Meteo data and joined to satellite observations by date. Adds `avg_temp_c`, `total_precip_mm`, `avg_solar_radiation`.

### T3 – Anomaly Flagging

30-day trailing rolling mean NDVI computed per pixel. Any observation where NDVI drops > 20% below the rolling baseline is flagged `is_anomaly = True` and published to Kafka.

### Dashboard Aggregation Logic

- Raw dashboard input is loaded from `data/dashboard_latest.parquet`.
- For charts/KPIs, data is aggregated in `dashboard/app.py` to one row per day per AOI (`datetime`, `aoi_bbox`).
- For the spatial map, raw pixel-level rows are used directly so point-level anomalies remain visible.

---

## Logs & Retry

- Structured logs in every module via Python `logging` with timestamps and levels.
- **Retry with exponential back-off** on all HTTP calls in `sentinel2_ingestor.py` and `weather_ingestor.py` (3 attempts, 2ˢ delay).
- Airflow DAG configured with `retries=2`, `retry_delay=5min` on all tasks.

---

## Credentials / Access

| Service                    | URL                   | Credentials   |
| -------------------------- | --------------------- | ------------- |
| Airflow                    | http://localhost:8080 | admin / admin |
| Streamlit dashboard        | http://localhost:8501 | public        |
| Kafka UI                   | http://localhost:8081 | public        |
