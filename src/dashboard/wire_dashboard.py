#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import pandas as pd
import streamlit as st
import redis
import plotly.graph_objects as go


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

STREAM_PLOT    = os.getenv("REDIS_STREAM_PLOT", "wire:plot")
STREAM_METRICS = os.getenv("REDIS_STREAM_METRICS", "wire:metrics")

MAX_POINTS = int(os.getenv("MAX_POINTS", "3000"))
REFRESH_S  = float(os.getenv("REFRESH_SECONDS", "1.0"))

# Linie brechen, wenn Drahtlänge stark springt (Reset/Neustart)
BREAK_DELTA = float(os.getenv("BREAK_DELTA_MM", "5000"))  # mm


def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)


def read_metrics(r):
    x = r.xrevrange(STREAM_METRICS, "+", "-", count=1)
    if not x:
        return 0, 0
    f = x[0][1]
    return int(f.get("total_profiles", 0)), int(f.get("nio_profiles", 0))


def read_plot(r):
    items = r.xrevrange(STREAM_PLOT, "+", "-", count=MAX_POINTS)
    if not items:
        return pd.DataFrame()

    rows = []
    for _id, f in reversed(items):
        rows.append({
            "length_mm": float(f.get("length_mm", "nan")),
            "diameter_mm": float(f.get("diameter_mm", "nan")),
            "hard_temp": float(f.get("hard_temp", "nan")) if "hard_temp" in f else float("nan"),
            "temper_temp": float(f.get("temper_temp", "nan")) if "temper_temp" in f else float("nan"),
        })

    df = (
        pd.DataFrame(rows)
        .dropna(subset=["length_mm"])
        .sort_values("length_mm")
        .set_index("length_mm")
    )
    return df


st.set_page_config(page_title="Wire Dashboard", layout="wide")
st.title("Wire Dashboard (Redis Streams)")

auto = st.sidebar.checkbox("Auto-Refresh", value=True)
st.sidebar.caption(f"Refresh: {REFRESH_S}s | Max points: {MAX_POINTS} | Break Δ: {int(BREAK_DELTA)} mm")

r = get_redis()

total, nio = read_metrics(r)
c1, c2 = st.columns(2)
c1.metric("Erkannte Profile (gesamt)", total)
c2.metric("n.i.O. Profile", nio)

df = read_plot(r)

if df.empty:
    st.info("Noch keine Daten in Redis Streams. Läuft wire-visualizer?")
else:
    st.subheader("Durchmesser + Temperaturen über Drahtlänge (2 Y-Achsen)")

    # Arrays für Plotly + Breaks bei Sprüngen
    x = df.index.to_list()
    y_d = df["diameter_mm"].tolist()
    y_h = df["hard_temp"].tolist()
    y_t = df["temper_temp"].tolist()

    for i in range(1, len(x)):
        if abs(x[i] - x[i - 1]) > BREAK_DELTA:
            y_d[i] = None
            y_h[i] = None
            y_t[i] = None

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y_d, name="Durchmesser [mm]", yaxis="y1"))
    fig.add_trace(go.Scatter(x=x, y=y_h, name="Härtetemperatur [°C]", yaxis="y2"))
    fig.add_trace(go.Scatter(x=x, y=y_t, name="Anlasstemperatur [°C]", yaxis="y2"))

    fig.update_layout(
        xaxis=dict(title="Drahtlänge [mm]"),
        yaxis=dict(title="Durchmesser [mm]"),
        yaxis2=dict(title="Temperatur [°C]", overlaying="y", side="right"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.25, xanchor="left", x=0),
        margin=dict(l=40, r=40, t=40, b=60),
    )

    st.plotly_chart(fig, use_container_width=True)

if auto:
    time.sleep(REFRESH_S)
    st.rerun()