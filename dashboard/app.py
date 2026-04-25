"""
dashboard/app.py
-----------------
Streamlit dashboard for the Crop Health Monitoring pipeline.
Shows NDVI trends, weather enrichment, and live Kafka anomaly alerts.
"""

import json
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Crop Health Monitor",
    page_icon="🌾",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR    = os.getenv("DATA_DIR", "data")
ALERTS_FILE = os.path.join(DATA_DIR, "alerts.jsonl")
PROC_FILE   = os.path.join(DATA_DIR, "dashboard_latest.parquet")
META_FILE   = os.path.join(DATA_DIR, "meta.json")

# ─── Load helpers ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_processed() -> pd.DataFrame:
    try:
        df = pd.read_parquet(PROC_FILE)
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
        return df
    except Exception:
        return _demo_data()


@st.cache_data(ttl=300)
def load_dashboard_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Aggregate pixel-level data to one row per day per AOI."""
    if df_raw.empty:
        return df_raw
    return (
        df_raw
        .groupby(["datetime", "aoi_bbox"], as_index=False)
        .agg(
            ndvi=("ndvi", "mean"),
            rolling_mean_ndvi=("rolling_mean_ndvi", "mean"),
            ndvi_drop_pct=("ndvi_drop_pct", "mean"),
            is_anomaly=("is_anomaly", "max"),
            avg_temp_c=("avg_temp_c", "mean"),
            total_precip_mm=("total_precip_mm", "sum"),
            avg_solar_radiation=("avg_solar_radiation", "mean"),
            avg_humidity_pct=("avg_humidity_pct", "mean"),
            scene_count=("ndvi", "count"),
        )
        .sort_values("datetime")
    )


def load_alerts() -> pd.DataFrame:
    try:
        lines = Path(ALERTS_FILE).read_text().strip().splitlines()
        rows = [json.loads(l) for l in lines if l.strip()]
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def load_meta() -> dict:
    try:
        return json.loads(Path(META_FILE).read_text())
    except Exception:
        return {"last_updated": "N/A", "dag_run_id": "N/A"}


def _demo_data() -> pd.DataFrame:
    """Synthetic data so the dashboard renders without running the pipeline."""
    np.random.seed(42)
    dates = pd.date_range("2024-03-01", periods=90, freq="D")
    rows = []
    for aoi in ["AOI-1", "AOI-2"]:
        ndvi = 0.55 + 0.1 * np.sin(np.linspace(0, 3.14, 90)) + np.random.normal(0, 0.02, 90)
        ndvi[70:75] *= 0.6
        for i, d in enumerate(dates):
            rows.append({
                "datetime":          d,
                "ndvi":              float(ndvi[i]),
                "rolling_mean_ndvi": float(pd.Series(ndvi[:i+1]).rolling(30, min_periods=1).mean().iloc[-1]),
                "ndvi_drop_pct":     max(0, float((ndvi[:i+1].mean() - ndvi[i]) / max(ndvi[:i+1].mean(), 0.001))),
                "is_anomaly":        bool(ndvi[i] < 0.38),
                "avg_temp_c":        float(25 + 5 * np.sin(i / 30) + np.random.normal(0, 1)),
                "total_precip_mm":   float(max(0, np.random.normal(2, 3))),
                "aoi_bbox":          aoi,
                "scene_id":          f"S2_{aoi}_{d.strftime('%Y%m%d')}",
                "cloud_cover":       float(np.random.uniform(0, 25)),
            })
    return pd.DataFrame(rows)


# ─── Sidebar ─────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("🌾 Crop Health")
    st.caption("Powered by Sentinel-2 + Open-Meteo")
    st.divider()

    meta = load_meta()
    st.metric("Last pipeline run", meta.get("last_updated", "N/A")[:16])

    df_raw = load_processed()
    df_all = load_dashboard_df(df_raw)

    # ── AOI filter ────────────────────────────────────────────────────────
    if not df_all.empty and "aoi_bbox" in df_all.columns:
        aoi_options = ["All"] + sorted(df_all["aoi_bbox"].unique().tolist())
        selected_aoi = st.selectbox("Area of Interest", aoi_options)
    else:
        selected_aoi = "All"

    # ── Date range filter ─────────────────────────────────────────────────
    if not df_all.empty and "datetime" in df_all.columns:
        df_all["datetime"] = pd.to_datetime(df_all["datetime"])
        min_date = df_all["datetime"].min().date()
        max_date = df_all["datetime"].max().date()

        date_range = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

        # Apply date filter only when both dates are selected
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            df_all = df_all[
                (df_all["datetime"].dt.date >= date_range[0]) &
                (df_all["datetime"].dt.date <= date_range[1])
            ]

    st.divider()
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)

if auto_refresh:
    st.rerun()

# ─── Apply AOI filter ─────────────────────────────────────────────────────────

df = df_all.copy()
if selected_aoi != "All" and "aoi_bbox" in df.columns:
    df = df[df["aoi_bbox"] == selected_aoi]

# ─── Header ───────────────────────────────────────────────────────────────────

st.title("🛰️ Crop Health Monitoring Dashboard")
st.caption(f"Sentinel-2 NDVI analysis · Updated {meta.get('last_updated', 'N/A')[:16]} UTC")
st.divider()

# ─── KPI row ─────────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)

if not df.empty:
    latest_ndvi   = df["ndvi"].dropna().iloc[-1] if "ndvi" in df.columns else 0
    mean_ndvi     = df["ndvi"].mean() if "ndvi" in df.columns else 0
    anomaly_count = int(df["is_anomaly"].sum()) if "is_anomaly" in df.columns else 0
    scene_count   = len(df)
else:
    latest_ndvi = mean_ndvi = 0
    anomaly_count = scene_count = 0

c1.metric("Latest NDVI",     f"{latest_ndvi:.3f}", help="Most recent pixel NDVI")
c2.metric("Mean NDVI",       f"{mean_ndvi:.3f}",   help="Average NDVI across period")
c3.metric("🚨 Anomalies",    anomaly_count,         delta=f"{'⚠️ Stress detected' if anomaly_count > 0 else '✅ Normal'}", delta_color="inverse")
c4.metric("Scenes ingested", scene_count)

st.divider()

# ─── Tab layout ──────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "📈 NDVI Trend",
    "🌤️ Weather Enrichment",
    "🚨 Anomaly Alerts",
    "🗺️ Spatial View",
])

# ── Tab 1: NDVI Trend ─────────────────────────────────────────────────────────
with tab1:
    st.subheader("NDVI Time Series with Anomaly Overlay")

    if not df.empty and "ndvi" in df.columns and "datetime" in df.columns:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["datetime"], y=df["ndvi"],
            mode="lines+markers",
            name="NDVI",
            line=dict(color="#4ade80", width=2),
            marker=dict(
                color=df["is_anomaly"].map({True: "red", False: "#4ade80"}) if "is_anomaly" in df.columns else "#4ade80",
                size=6,
            ),
        ))
        if "rolling_mean_ndvi" in df.columns:
            fig.add_trace(go.Scatter(
                x=df["datetime"], y=df["rolling_mean_ndvi"],
                mode="lines",
                name="30-day rolling mean",
                line=dict(color="orange", width=2, dash="dash"),
            ))
        if "is_anomaly" in df.columns:
            anomalies = df[df["is_anomaly"] == True]
            for _, row in anomalies.iterrows():
                fig.add_vrect(
                    x0=row["datetime"], x1=row["datetime"],
                    fillcolor="red", opacity=0.15, line_width=0,
                )
        fig.update_layout(
            height=400,
            template="plotly_dark",
            xaxis_title="Date",
            yaxis_title="NDVI",
            legend=dict(orientation="h", y=1.05),
            margin=dict(l=0, r=0, t=30, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No NDVI data available. Run the pipeline first.")

    if not df.empty and "ndvi" in df.columns:
        st.subheader("NDVI Distribution")
        fig2 = px.histogram(df.dropna(subset=["ndvi"]), x="ndvi", nbins=40,
                            color_discrete_sequence=["#4ade80"],
                            template="plotly_dark")
        fig2.update_layout(height=250, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig2, use_container_width=True)


# ── Tab 2: Weather Enrichment ────────────────────────────────────────────────
with tab2:
    st.subheader("Weather Conditions vs NDVI")

    if not df.empty and "avg_temp_c" in df.columns:
        col_a, col_b = st.columns(2)
        with col_a:
            fig_t = px.line(df.dropna(subset=["avg_temp_c"]),
                            x="datetime", y="avg_temp_c",
                            title="Daily Avg Temperature (°C)",
                            template="plotly_dark",
                            color_discrete_sequence=["#fb923c"])
            fig_t.update_layout(height=280, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_t, use_container_width=True)

        with col_b:
            if "total_precip_mm" in df.columns:
                fig_p = px.bar(df.dropna(subset=["total_precip_mm"]),
                               x="datetime", y="total_precip_mm",
                               title="Daily Precipitation (mm)",
                               template="plotly_dark",
                               color_discrete_sequence=["#60a5fa"])
                fig_p.update_layout(height=280, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_p, use_container_width=True)

        if "ndvi" in df.columns:
            st.subheader("NDVI vs Temperature")
            scatter_df = df.dropna(subset=["ndvi", "avg_temp_c"])
            fig_s = px.scatter(
                scatter_df,
                x="avg_temp_c", y="ndvi",
                color="is_anomaly" if "is_anomaly" in scatter_df.columns else None,
                color_discrete_map={True: "red", False: "#4ade80"},
                template="plotly_dark",
                labels={"avg_temp_c": "Avg Temp (°C)", "ndvi": "NDVI"},
            )
            if len(scatter_df) > 1:
                x = scatter_df["avg_temp_c"].values
                y = scatter_df["ndvi"].values
                z = np.polyfit(x, y, 1)
                p = np.poly1d(z)
                x_line = np.linspace(x.min(), x.max(), 100)
                fig_s.add_trace(go.Scatter(
                    x=x_line, y=p(x_line),
                    mode="lines",
                    name="Trend",
                    line=dict(color="orange", width=2, dash="dash"),
                ))
            fig_s.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig_s, use_container_width=True)
    else:
        st.info("Weather enrichment data not yet available.")


# ── Tab 3: Kafka Alerts ───────────────────────────────────────────────────────
with tab3:
    st.subheader("🚨 Real-Time NDVI Anomaly Alerts (from Kafka)")

    alerts_df = load_alerts()

    if not alerts_df.empty:
        st.success(f"**{len(alerts_df)} alerts** received from Kafka topic `crop-alerts`")

        display_cols = [c for c in ["consumed_at", "scene_date", "lat", "lon",
                                     "ndvi", "ndvi_drop_pct", "aoi"] if c in alerts_df.columns]
        st.dataframe(
            alerts_df[display_cols].sort_values("consumed_at", ascending=False)
            if "consumed_at" in alerts_df.columns else alerts_df[display_cols],
            use_container_width=True,
            height=400,
        )

        if "ndvi_drop_pct" in alerts_df.columns:
            fig_a = px.histogram(alerts_df, x="ndvi_drop_pct",
                                  title="Distribution of NDVI Drop %",
                                  color_discrete_sequence=["red"],
                                  template="plotly_dark")
            st.plotly_chart(fig_a, use_container_width=True)
    else:
        st.info("No alerts yet. Alerts appear here after the pipeline runs and publishes to Kafka.")

    if st.button("🔄 Refresh alerts"):
        st.cache_data.clear()
        st.rerun()


# ── Tab 4: Spatial View ──────────────────────────────────────────────────────
with tab4:
    st.subheader("Spatial Distribution of NDVI Anomalies")

    # Use raw pixel-level data for the map, filtered by AOI only (not aggregated).
    map_source = df_raw.copy()
    if selected_aoi != "All" and "aoi_bbox" in map_source.columns:
        map_source = map_source[map_source["aoi_bbox"] == selected_aoi]

    if not map_source.empty and "ndvi" in map_source.columns and "lat" in map_source.columns and "lon" in map_source.columns:
        map_df = map_source.dropna(subset=["lat", "lon", "ndvi"])
        if not map_df.empty:
            fig_map = px.scatter_mapbox(
                map_df,
                lat="lat", lon="lon",
                color="ndvi",
                color_continuous_scale="RdYlGn",
                range_color=[0, 0.8],
                size=[4] * len(map_df),
                zoom=7,
                mapbox_style="carto-darkmatter",
                title="NDVI by Location",
                hover_data=["scene_id", "ndvi", "is_anomaly"] if "scene_id" in map_df.columns else ["ndvi"],
            )
            fig_map.update_layout(height=500, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("No spatial data available.")
    else:
        st.info("Lat/lon data not in processed output.")

# ─── Footer ───────────────────────────────────────────────────────────────────
st.divider()
st.caption("Pipeline: Sentinel-2 STAC API + Open-Meteo → PySpark (Databricks) → Delta Lake → Airflow → Kafka → Streamlit")