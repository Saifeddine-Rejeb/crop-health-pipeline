"""
tests/test_transformations.py
-------------------------------
Unit tests for the three business transformations.
Run with: pytest tests/ -v

Tests do NOT require a running Spark cluster — they use
pandas-based equivalent logic so CI stays fast and free.
"""

import numpy as np
import pandas as pd
import pytest


# ─── Helpers mirroring transformations.py (pandas version) ───────────────────

def compute_ndvi_pandas(df: pd.DataFrame) -> pd.DataFrame:
    denom = df["nir"].astype(float) + df["red"].astype(float)
    df["ndvi"] = np.where(denom == 0, np.nan, (df["nir"] - df["red"]) / denom)
    return df


def enrich_with_weather_pandas(sat_df: pd.DataFrame, wx_df: pd.DataFrame) -> pd.DataFrame:
    wx_df = wx_df.copy()
    wx_df["date"] = pd.to_datetime(wx_df["datetime"]).dt.date
    daily = (
        wx_df.groupby("date")
        .agg(avg_temp_c=("temperature_2m", "mean"),
             total_precip_mm=("precipitation", "sum"))
        .reset_index()
    )
    sat_df = sat_df.copy()
    sat_df["date"] = pd.to_datetime(sat_df["scene_date"]).dt.date
    return sat_df.merge(daily, on="date", how="left").drop(columns=["date"])


def flag_anomalies_pandas(df: pd.DataFrame, drop_threshold: float = 0.20) -> pd.DataFrame:
    df = df.sort_values("scene_date").copy()
    df["rolling_mean_ndvi"] = df["ndvi"].rolling(window=5, min_periods=1).mean()
    df["ndvi_drop_pct"] = (
        (df["rolling_mean_ndvi"] - df["ndvi"])
        / df["rolling_mean_ndvi"].replace(0, np.nan)
    )
    df["is_anomaly"] = df["ndvi_drop_pct"] > drop_threshold
    return df


# ─── T1 Tests ────────────────────────────────────────────────────────────────

class TestNDVI:

    def test_ndvi_normal(self):
        """Standard calculation: (3000-1500)/(3000+1500) = 0.333..."""
        df = pd.DataFrame({"nir": [3000], "red": [1500]})
        result = compute_ndvi_pandas(df)
        assert pytest.approx(result["ndvi"].iloc[0], abs=1e-4) == 1500 / 4500

    def test_ndvi_zero_denominator_returns_nan(self):
        """nir == red == 0 must not divide by zero."""
        df = pd.DataFrame({"nir": [0], "red": [0]})
        result = compute_ndvi_pandas(df)
        assert np.isnan(result["ndvi"].iloc[0])

    def test_ndvi_range(self):
        """NDVI must be in [-1, 1] for valid reflectance values."""
        df = pd.DataFrame({
            "nir": [4000, 1000, 2500, 3500],
            "red": [500,  3000, 2500, 3500],
        })
        result = compute_ndvi_pandas(df)
        valid = result["ndvi"].dropna()
        assert (valid >= -1.0).all() and (valid <= 1.0).all()

    def test_ndvi_negative_for_water(self):
        """Red > NIR ⟹ negative NDVI (water / non-vegetation)."""
        df = pd.DataFrame({"nir": [800], "red": [3000]})
        result = compute_ndvi_pandas(df)
        assert result["ndvi"].iloc[0] < 0


# ─── T2 Tests ────────────────────────────────────────────────────────────────

class TestWeatherEnrichment:

    @pytest.fixture
    def sample_sat(self):
        return pd.DataFrame({
            "scene_date": ["2024-06-01", "2024-06-08"],
            "nir": [3000, 2800],
            "red": [1500, 1600],
        })

    @pytest.fixture
    def sample_weather(self):
        return pd.DataFrame({
            "datetime":      ["2024-06-01", "2024-06-08"],
            "temperature_2m": [25.0, 27.0],
            "precipitation":  [2.0,  0.0],
        })

    def test_enrichment_adds_columns(self, sample_sat, sample_weather):
        result = enrich_with_weather_pandas(sample_sat, sample_weather)
        assert "avg_temp_c" in result.columns
        assert "total_precip_mm" in result.columns

    def test_enrichment_row_count_preserved(self, sample_sat, sample_weather):
        result = enrich_with_weather_pandas(sample_sat, sample_weather)
        assert len(result) == len(sample_sat)

    def test_enrichment_values_correct(self, sample_sat, sample_weather):
        result = enrich_with_weather_pandas(sample_sat, sample_weather)
        assert result.loc[result["scene_date"] == "2024-06-01", "avg_temp_c"].iloc[0] == 25.0


# ─── T3 Tests ────────────────────────────────────────────────────────────────

class TestAnomalyFlagging:

    def _make_df(self, ndvi_values: list[float]) -> pd.DataFrame:
        dates = pd.date_range("2024-01-01", periods=len(ndvi_values), freq="7D")
        return pd.DataFrame({
            "scene_date": dates.strftime("%Y-%m-%d"),
            "ndvi": ndvi_values,
        })

    def test_anomaly_flagged_on_big_drop(self):
        """A 50% NDVI drop should be flagged as anomaly."""
        ndvi = [0.6, 0.6, 0.6, 0.6, 0.3]  # last value is 50% drop
        df = flag_anomalies_pandas(self._make_df(ndvi))
        assert df["is_anomaly"].iloc[-1] == True

    def test_no_anomaly_on_stable_ndvi(self):
        """Stable NDVI should never trigger anomaly flag."""
        ndvi = [0.5, 0.51, 0.49, 0.50, 0.52]
        df = flag_anomalies_pandas(self._make_df(ndvi))
        assert not df["is_anomaly"].any()

    def test_rolling_mean_is_computed(self):
        """rolling_mean_ndvi column must exist and be non-NaN after warm-up."""
        ndvi = [0.5, 0.5, 0.5, 0.5, 0.5]
        df = flag_anomalies_pandas(self._make_df(ndvi))
        assert "rolling_mean_ndvi" in df.columns
        assert df["rolling_mean_ndvi"].notna().all()
