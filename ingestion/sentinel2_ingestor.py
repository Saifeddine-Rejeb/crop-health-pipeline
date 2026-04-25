"""
ingestion/sentinel2_ingestor.py
--------------------------------
Fetches Sentinel-2 L2A imagery metadata + band URLs from the
Element-84 STAC API (free, no auth required).
Actual band sampling runs on Databricks; this module writes
the scene manifest to a local parquet file consumed by the DAG.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
import pandas as pd
from geojson import Feature, Polygon

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

STAC_URL = "https://earth-search.aws.element84.com/v1/search"
COLLECTION = "sentinel-2-l2a"

# Bounding boxes in EPSG:4326 (lon_min, lat_min, lon_max, lat_max)
# Lower Mississippi delta study areas (converted from EPSG:5070 originals)
DEFAULT_BBOXES = [
    (-91.5, 32.5, -90.5, 33.0),   # AOI-1
    (-90.8, 31.5, -90.0, 32.5),   # AOI-2
]


def query_stac(
    bbox: tuple[float, float, float, float],
    start_date: str,
    end_date: str,
    max_cloud: float = 30.0,
    limit: int = 100,
    max_retries: int = 3,
) -> list[dict]:
    """
    Query STAC API for available Sentinel-2 scenes.
    Retries on transient HTTP errors (retry on critical step ✅).

    Args:
        bbox: (lon_min, lat_min, lon_max, lat_max) in EPSG:4326
        start_date: ISO format e.g. "2024-01-01"
        end_date:   ISO format e.g. "2024-12-31"
        max_cloud:  maximum cloud cover percentage
        limit:      page size
        max_retries: number of retry attempts

    Returns:
        list of STAC feature dicts
    """
    lon_min, lat_min, lon_max, lat_max = bbox
    geometry = Feature(
        geometry=Polygon([
            [(lon_min, lat_min), (lon_max, lat_min),
             (lon_max, lat_max), (lon_min, lat_max),
             (lon_min, lat_min)]
        ])
    ).geometry

    all_features = []
    page = 1

    while True:
        payload = {
            "datetime": f"{start_date}T00:00:00Z/{end_date}T23:59:59Z",
            "intersects": geometry,
            "collections": [COLLECTION],
            "limit": limit,
            "page": page,
            "query": {"eo:cloud_cover": {"lt": max_cloud}},
        }

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    "STAC query page=%d bbox=%s attempt=%d", page, bbox, attempt
                )
                resp = httpx.post(STAC_URL, json=payload, timeout=30)
                resp.raise_for_status()
                break
            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                logger.warning("STAC request failed (attempt %d): %s", attempt, exc)
                if attempt == max_retries:
                    logger.error("Max retries exceeded for bbox=%s page=%d", bbox, page)
                    raise
                time.sleep(2 ** attempt)  # exponential back-off

        data = resp.json()
        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        logger.info("Fetched %d scenes so far (page %d)", len(all_features), page)
        page += 1

    return all_features


def scenes_to_dataframe(features: list[dict]) -> pd.DataFrame:
    """Flatten STAC features into a tidy DataFrame (scene manifest)."""
    rows = []
    for f in features:
        props = f.get("properties", {})
        assets = f.get("assets", {})
        rows.append({
            "scene_id":     f["id"],
            "tile":         f["id"].split("_")[1] if "_" in f["id"] else None,
            "datetime":     props.get("datetime", "")[:10],
            "cloud_cover":  props.get("eo:cloud_cover"),
            "bbox":         str(f.get("bbox")),
            "nir_href":     assets.get("nir", {}).get("href"),
            "red_href":     assets.get("red", {}).get("href"),
            "scl_href":     assets.get("scl", {}).get("href"),
        })
    return pd.DataFrame(rows)


def ingest_sentinel2(
    output_path: str = "data/sentinel2_manifest.parquet",
    bboxes: Optional[list] = None,
    days_back: int = 7,
) -> str:
    """
    Main ingestion entry point called by the Airflow DAG.

    Args:
        output_path: where to write the scene manifest parquet
        bboxes: list of (lon_min, lat_min, lon_max, lat_max) tuples
        days_back: how many days of history to ingest

    Returns:
        output_path (for XCom passing in Airflow)
    """
    bboxes = bboxes or DEFAULT_BBOXES
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    all_dfs = []
    for bbox in bboxes:
        try:
            features = query_stac(bbox, start_date, end_date)
            df = scenes_to_dataframe(features)
            df["aoi_bbox"] = str(bbox)
            all_dfs.append(df)
            logger.info("AOI %s → %d scenes", bbox, len(df))
        except Exception as exc:
            logger.error("Failed ingesting AOI %s: %s", bbox, exc)

    if not all_dfs:
        logger.warning("No scenes fetched — writing empty manifest")
        combined = pd.DataFrame()
    else:
        combined = pd.concat(all_dfs, ignore_index=True)

    combined.to_parquet(output_path, index=False)
    logger.info("Wrote %d scenes to %s", len(combined), output_path)
    return output_path


if __name__ == "__main__":
    ingest_sentinel2()
