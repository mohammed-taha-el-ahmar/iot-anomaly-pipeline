"""
Live alert dashboard. Polls the gold Delta table and renders current
device anomaly status plus a rolling alert timeline.
"""
import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

GOLD_PATH = os.environ.get("GOLD_PATH", "/opt/data/delta/gold/anomaly_alerts")
REFRESH_SECONDS = int(os.environ.get("DASHBOARD_REFRESH_SECONDS", "15"))

st.set_page_config(page_title="IoT Anomaly Dashboard", layout="wide")


@st.cache_resource
def get_spark():
    builder = SparkSession.builder.appName("dashboard").config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    ).config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def load_gold() -> pd.DataFrame:
    spark = get_spark()
    df = spark.read.format("delta").load(GOLD_PATH)
    return df.toPandas()


def main():
    st.title("Real-Time IoT Anomaly Detection")
    placeholder = st.empty()

    while True:
        try:
            df = load_gold()
        except Exception as e:
            st.warning(f"Gold table not ready yet: {e}")
            time.sleep(REFRESH_SECONDS)
            continue

        with placeholder.container():
            if df.empty:
                st.info("No data yet — waiting for streaming jobs to produce output.")
            else:
                latest_window = df["window"].apply(lambda w: w["end"]).max()
                current = df[df["window"].apply(lambda w: w["end"]) == latest_window]

                col1, col2, col3 = st.columns(3)
                col1.metric("Devices reporting", current["device_id"].nunique())
                col2.metric(
                    "Active critical alerts",
                    int((current["alert_severity"] == "critical").sum()),
                )
                col3.metric(
                    "Active warnings",
                    int((current["alert_severity"] == "warning").sum()),
                )

                st.subheader("Current device status")
                st.dataframe(
                    current[
                        [
                            "device_id",
                            "temp_mean",
                            "vib_mean",
                            "alert_severity",
                        ]
                    ].sort_values("alert_severity", ascending=False),
                    use_container_width=True,
                )

                st.subheader("Alert timeline")
                df["window_end"] = df["window"].apply(lambda w: w["end"])
                fig = px.scatter(
                    df,
                    x="window_end",
                    y="device_id",
                    color="alert_severity",
                    color_discrete_map={
                        "critical": "red",
                        "warning": "orange",
                        "normal": "lightgray",
                    },
                )
                st.plotly_chart(fig, use_container_width=True)

        time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()
