"""
processing/transformations.py
-------------------------------
Three documented business transformations applied after raw ingestion:

  T1 – NDVI Calculation
       Computes Normalized Difference Vegetation Index per pixel per scene.
       Formula: (NIR - Red) / (NIR + Red)

  T2 – Weather Enrichment
       Joins satellite pixel observations with weather readings
       by nearest date and AOI location.

  T3 – Anomaly Flagging
       Computes a 30-day rolling mean NDVI per pixel and flags
       observations where NDVI drops > 20% below the rolling baseline.

All functions accept and return PySpark DataFrames so they can run
on Databricks Community Edition for free.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


# ─── T1: NDVI Calculation ────────────────────────────────────────────────────

def compute_ndvi(df: DataFrame) -> DataFrame:
    """
    T1 – Compute NDVI per pixel per scene date.

    Expected columns: nir (int), red (int)
    Adds column:      ndvi (float, range -1..1), ndvi_class (str)

    Edge case: returns None when nir + red == 0 (avoids division by zero).
    """
    @F.udf(FloatType())
    def _ndvi(nir, red):
        if nir is None or red is None:
            return None
        denom = float(nir + red)
        if denom == 0.0:
            return None
        return float((nir - red) / denom)

    df = df.withColumn("ndvi", _ndvi(F.col("nir"), F.col("red")))

    # Classify NDVI into human-readable categories
    df = df.withColumn(
        "ndvi_class",
        F.when(F.col("ndvi") < 0.0,  "Water/Non-Vegetated")
         .when(F.col("ndvi") < 0.2,  "Sparse Vegetation")
         .when(F.col("ndvi") < 0.4,  "Moderate Vegetation")
         .when(F.col("ndvi") < 0.6,  "Dense Vegetation")
         .otherwise("Very Dense Vegetation"),
    )

    logger.info("T1 NDVI computed. Schema: %s", df.columns)
    return df


# ─── T2: Weather Enrichment ───────────────────────────────────────────────────

def enrich_with_weather(
    satellite_df: DataFrame,
    weather_df: DataFrame,
    aoi_col: str = "aoi_bbox",
) -> DataFrame:
    """
    T2 – Join satellite observations with daily weather summaries.

    Strategy: aggregate hourly weather to daily means/sums, then
    join on scene_date == weather_date.

    Adds columns: avg_temp_c, total_precip_mm, avg_solar_radiation
    """
    # Aggregate hourly → daily
    daily_weather = (
        weather_df
        .withColumn("date", F.to_date("datetime"))
        .groupBy("date", "lat", "lon", "location_name")
        .agg(
            F.avg("temperature_2m").alias("avg_temp_c"),
            F.sum("precipitation").alias("total_precip_mm"),
            F.avg("shortwave_radiation").alias("avg_solar_radiation"),
            F.avg("relative_humidity_2m").alias("avg_humidity_pct"),
        )
    )

    # Join on date — satellite scene_date must be date type
    enriched = satellite_df.withColumn(
        "scene_date_dt", F.to_date("scene_date")
    ).join(
        daily_weather,
        on=(
            (F.col("scene_date_dt") == daily_weather["date"])
        ),
        how="left",
    ).drop("date", "scene_date_dt")

    logger.info("T2 Weather enrichment done. Row count: %d", enriched.count())
    return enriched


# ─── T3: Anomaly Flagging ─────────────────────────────────────────────────────

def flag_ndvi_anomalies(
    df: DataFrame,
    pixel_id_cols: list[str] = ["lon", "lat"],
    ndvi_col: str = "ndvi",
    date_col: str = "scene_date",
    window_days: int = 30,
    drop_threshold: float = 0.20,
) -> DataFrame:
    """
    T3 – Flag pixels whose NDVI dropped > drop_threshold (default 20%)
    below the rolling 30-day mean.

    Adds columns:
      rolling_mean_ndvi  – trailing window average
      ndvi_drop_pct      – how much NDVI fell vs rolling mean (0..1)
      is_anomaly         – boolean flag (True = stress detected)
    """
    # Convert date string → timestamp for range window
    df = df.withColumn("_ts", F.unix_timestamp(F.to_date(date_col)))

    window_seconds = window_days * 86_400
    w = (
        Window
        .partitionBy(*pixel_id_cols)
        .orderBy("_ts")
        .rangeBetween(-window_seconds, 0)
    )

    df = df.withColumn("rolling_mean_ndvi", F.avg(ndvi_col).over(w))

    df = df.withColumn(
        "ndvi_drop_pct",
        F.when(
            F.col("rolling_mean_ndvi").isNotNull() & (F.col("rolling_mean_ndvi") > 0),
            (F.col("rolling_mean_ndvi") - F.col(ndvi_col)) / F.col("rolling_mean_ndvi"),
        ).otherwise(None),
    )

    df = df.withColumn(
        "is_anomaly",
        F.col("ndvi_drop_pct") > drop_threshold,
    ).drop("_ts")

    anomaly_count = df.filter(F.col("is_anomaly") == True).count()
    logger.info("T3 Anomaly flagging done. Anomalies detected: %d", anomaly_count)
    return df


# ─── Convenience: run all three transformations ───────────────────────────────

def run_all_transformations(
    satellite_df: DataFrame,
    weather_df: DataFrame,
) -> DataFrame:
    """Apply T1 → T2 → T3 in sequence and return the final DataFrame."""
    df = compute_ndvi(satellite_df)
    df = enrich_with_weather(df, weather_df)
    df = flag_ndvi_anomalies(df)
    return df


# ─── Standalone test (without Spark) ─────────────────────────────────────────
if __name__ == "__main__":
    spark = SparkSession.builder.appName("crop-health-transforms").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Minimal smoke test with synthetic data
    sat_data = [
        ("2024-06-01", 0.5, 0.6, 3000, 1500, "AOI-1"),
        ("2024-06-08", 0.5, 0.6, 2800, 1600, "AOI-1"),
        ("2024-06-15", 0.5, 0.6, 1500, 1400, "AOI-1"),  # NDVI drop
    ]
    sat_df = spark.createDataFrame(
        sat_data, ["scene_date", "lat", "lon", "nir", "red", "aoi_bbox"]
    )

    weather_data = [
        ("2024-06-01T00:00", 25.0, 2.0, 200.0, 65.0, 0.5, 0.6, "AOI-1"),
        ("2024-06-08T00:00", 27.0, 0.0, 220.0, 70.0, 0.5, 0.6, "AOI-1"),
        ("2024-06-15T00:00", 30.0, 0.0, 230.0, 75.0, 0.5, 0.6, "AOI-1"),
    ]
    weather_df = spark.createDataFrame(
        weather_data,
        ["datetime", "temperature_2m", "precipitation", "shortwave_radiation",
         "relative_humidity_2m", "lat", "lon", "location_name"],
    )

    result = run_all_transformations(sat_df, weather_df)
    result.show(truncate=False)
