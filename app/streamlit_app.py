"""Local dashboard over samples/ (no AWS): run `streamlit run app/streamlit_app.py`."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

SAMPLES = Path(__file__).resolve().parent.parent / "samples"
TAXI_ONLY = ["yellow", "green"]
TYPE_COLORS = {"yellow": "#f2c200", "green": "#2e9e44", "fhv": "#7f7f7f", "hvfhv": "#3b6fd6"}
ZONE_ID_PROPERTY = "locationid"  # string property in NYC Open Data dataset 8meu-9t5y
MAP_CENTER = {"lat": 40.70, "lon": -73.94}


@st.cache_data
def load_fact() -> pd.DataFrame:
    return pd.read_csv(SAMPLES / "fact_zone_month.csv")


@st.cache_data
def load_zones() -> dict:
    return json.loads((SAMPLES / "taxi_zones.geojson").read_text())


def apply_definition(fact: pd.DataFrame, taxi_only: bool, exclude_shared: bool) -> pd.DataFrame:
    df = fact[fact["taxi_type"].isin(TAXI_ONLY)] if taxi_only else fact
    trips = df["trips"] - df["shared_trips"] if exclude_shared else df["trips"]
    return df.assign(trips=trips)


def choropleth(df: pd.DataFrame, zones: dict, color: str, **kwargs):
    return px.choropleth_map(df, geojson=zones, locations="zone_id", featureidkey=f"properties.{ZONE_ID_PROPERTY}",
                             color=color, center=MAP_CENTER, zoom=9, opacity=0.7, map_style="carto-positron",
                             **kwargs)


def by_zone(df: pd.DataFrame) -> pd.DataFrame:
    zoned = df.dropna(subset=["pickup_location_id"])
    return zoned.assign(zone_id=zoned["pickup_location_id"].astype(int).astype(str))


def main() -> None:
    st.set_page_config(page_title="NYC TLC trip types", layout="wide")
    st.title("Which TLC trip type is used most? (Feb 2019 → latest)")
    fact, zones = load_fact(), load_zones()

    taxi_only = st.sidebar.radio("Types", ["All types", "Taxi only (yellow + green)"]) != "All types"
    exclude_shared = st.sidebar.checkbox("Exclude shared/pooled trips")
    df = apply_definition(fact, taxi_only, exclude_shared)

    st.header("Original question: H1 2024")
    h1 = fact[fact["year_month"].between("2024-01", "2024-06")].groupby("taxi_type", as_index=False)["trips"].sum()
    left, right = st.columns(2)
    left.plotly_chart(px.bar(h1[h1["taxi_type"].isin(TAXI_ONLY)], x="taxi_type", y="trips", color="taxi_type",
                             color_discrete_map=TYPE_COLORS, title="Taxi only"), use_container_width=True)
    right.plotly_chart(px.bar(h1, x="taxi_type", y="trips", color="taxi_type", color_discrete_map=TYPE_COLORS,
                              title="All types"), use_container_width=True)

    st.header("Trend by type (H1, H2)")
    trend = df.groupby(["year_month", "taxi_type"], as_index=False)["trips"].sum()
    st.plotly_chart(px.line(trend, x="year_month", y="trips", color="taxi_type", color_discrete_map=TYPE_COLORS),
                    use_container_width=True)

    st.header("Trips by pickup zone (H5)")
    month = st.select_slider("Month", options=sorted(df["year_month"].unique()))
    monthly = by_zone(df[df["year_month"] == month]).groupby("zone_id", as_index=False)["trips"].sum()
    st.plotly_chart(choropleth(monthly, zones, "trips"), use_container_width=True)

    st.header("Dominant type per zone, whole period (H5)")
    totals = by_zone(df).groupby(["zone_id", "taxi_type"], as_index=False)["trips"].sum()
    dominant = totals.loc[totals.groupby("zone_id")["trips"].idxmax()]
    st.plotly_chart(choropleth(dominant, zones, "taxi_type", color_discrete_map=TYPE_COLORS,
                               hover_data=["trips"]), use_container_width=True)
    st.caption("Zones only; trips with null pickup zone (mostly fhv) are not mapped.")


main()
