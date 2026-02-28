import json
import os
import threading
import time
from collections import deque
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from confluent_kafka import Consumer

import redis


# -----------------------------
# Config (ENV)
# -----------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"))
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wire-dashboard")
OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

TOPIC_RAW = os.getenv("KAFKA_INPUT_TOPIC", os.getenv("INPUT_TOPIC", "1031103_1000"))
TOPIC_PROFILES = os.getenv("KAFKA_PROFILES_TOPIC", f"{TOPIC_RAW}_profiles")
TOPIC_NIO = os.getenv("KAFKA_NIO_TOPIC", f"{TOPIC_RAW}_profile_events")

FIELD_LENGTH = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
FIELD_DIAMETER = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")
FIELD_HARD_TEMP = os.getenv("FIELD_HARD_TEMP", "fHaertetemperatur (Haerten)")
FIELD_TEMPER_TEMP = os.getenv("FIELD_TEMPER_TEMP", "fAnlasstemperatur (Anlassen)")

MAX_POINTS = int(os.getenv("MAX_POINTS", "3000"))
MAX_NIO_DEBUG = int(os.getenv("MAX_NIO_DEBUG", "20"))
BREAK_DELTA = float(os.getenv("BREAK_DELTA_MM", "5000"))
REFRESH_S = float(os.getenv("REFRESH_SECONDS", "1.0"))
NOK_MM = float(os.getenv("NOK_DEVIATION_MM", "30.0"))

# Redis Streams (bleibt drin)
ENABLE_REDIS_CACHE = os.getenv("ENABLE_REDIS_CACHE", "true").lower() in ("1", "true", "yes", "y", "on")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

STREAM_PLOT = os.getenv("REDIS_STREAM_PLOT", "wire:plot")
STREAM_METRICS = os.getenv("REDIS_STREAM_METRICS", "wire:metrics")
REDIS_MAXLEN_PLOT = int(os.getenv("REDIS_MAXLEN_PLOT", str(MAX_POINTS)))
REDIS_MAXLEN_METRICS = int(os.getenv("REDIS_MAXLEN_METRICS", "1000"))


# -----------------------------
# Helpers
# -----------------------------
def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().replace(",", ".")
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None
    try:
        return float(x)
    except Exception:
        return None


def pd_value(record: dict, wanted_name: str) -> Optional[float]:
    for item in record.get("ProcessData", []):
        if item.get("Name") == wanted_name:
            return safe_float(item.get("Value"))
    return None


def parse_raw(b: bytes) -> Optional[dict]:
    try:
        data = json.loads(b.decode("utf-8"))
    except Exception:
        return None

    length = pd_value(data, FIELD_LENGTH)
    diam = pd_value(data, FIELD_DIAMETER)
    if length is None or diam is None:
        return None

    return {
        "length_mm": float(length),
        "diameter_mm": float(diam),
        "hard_temp": pd_value(data, FIELD_HARD_TEMP),
        "temper_temp": pd_value(data, FIELD_TEMPER_TEMP),
        "ts": data.get("Time"),
    }


def parse_profile(b: bytes) -> Optional[dict]:
    try:
        data = json.loads(b.decode("utf-8"))
    except Exception:
        return None

    p = {
        "profile_id": data.get("profile_id"),
        "L1": safe_float(data.get("L1")),
        "L2": safe_float(data.get("L2")),
        "L3": safe_float(data.get("L3")),
        "L4": safe_float(data.get("L4")),
        "status": str(data.get("status", "")).strip(),
        "reason": data.get("reason", ""),
    }
    if None in (p["L1"], p["L2"], p["L3"], p["L4"]):
        return None
    return p


def build_consumer() -> Consumer:
    c = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": OFFSET_RESET,
            "enable.auto.commit": True,
        }
    )
    c.subscribe([TOPIC_RAW, TOPIC_PROFILES, TOPIC_NIO])
    return c


# -----------------------------
# Redis (Streams)
# -----------------------------
def get_redis() -> Optional["redis.Redis"]:
    if not ENABLE_REDIS_CACHE:
        return None
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None


def xadd_plot(r: "redis.Redis", p: dict) -> None:
    fields = {"length_mm": str(p["length_mm"]), "diameter_mm": str(p["diameter_mm"])}
    if p.get("hard_temp") is not None:
        fields["hard_temp"] = str(p["hard_temp"])
    if p.get("temper_temp") is not None:
        fields["temper_temp"] = str(p["temper_temp"])
    if p.get("ts"):
        fields["ts"] = p["ts"]
    r.xadd(STREAM_PLOT, fields, maxlen=REDIS_MAXLEN_PLOT, approximate=True)


def xadd_metrics(r: "redis.Redis", total_profiles: int, nio_profiles: int, ts: Optional[str]) -> None:
    fields = {"total_profiles": str(total_profiles), "nio_profiles": str(nio_profiles)}
    if ts:
        fields["ts"] = ts
    r.xadd(STREAM_METRICS, fields, maxlen=REDIS_MAXLEN_METRICS, approximate=True)


def clear_streams(r: "redis.Redis") -> None:
    try:
        r.delete(STREAM_PLOT)
    except Exception:
        pass
    try:
        r.delete(STREAM_METRICS)
    except Exception:
        pass


# -----------------------------
# Shared state (thread-safe)
# -----------------------------
class State:
    def __init__(self):
        self.lock = threading.Lock()
        self.points = deque(maxlen=MAX_POINTS)

        self.total_profiles = 0
        self.nio_profiles = 0
        self.last_raw_ts = None
        self.last_error = None

        # running mean ref for L1..L4
        self.ref_count = 0
        self.ref_mean = {"L1": 0.0, "L2": 0.0, "L3": 0.0, "L4": 0.0}

        # last n.i.O. profiles debug rows
        self.nio_debug = deque(maxlen=MAX_NIO_DEBUG)

        # redis status
        self.redis_ok = False


def poll_loop(state: State, stop: threading.Event):
    # Redis: try connect, then retry every 5s if down
    r = get_redis()
    last_retry = time.time()

    def ensure_redis():
        nonlocal r, last_retry
        if not ENABLE_REDIS_CACHE:
            state.redis_ok = False
            return None
        if r is not None:
            state.redis_ok = True
            return r
        if time.time() - last_retry > 5:
            last_retry = time.time()
            r = get_redis()
        state.redis_ok = r is not None
        return r

    try:
        c = build_consumer()
    except Exception as e:
        with state.lock:
            state.last_error = f"Kafka Consumer start failed: {e}"
        return

    try:
        while not stop.is_set():
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                with state.lock:
                    state.last_error = str(msg.error())
                continue

            topic = msg.topic()
            with state.lock:
                state.last_error = None

            # RAW -> plot points (+ Redis stream)
            if topic == TOPIC_RAW:
                p = parse_raw(msg.value())
                if not p:
                    continue
                with state.lock:
                    state.points.append(p)
                    state.last_raw_ts = p["ts"]

                rr = ensure_redis()
                if rr is not None:
                    try:
                        xadd_plot(rr, p)
                    except Exception:
                        pass
                continue

            # profiles -> total + ref mean + debug for n.i.O. (evaluate vs mean BEFORE update)
            if topic == TOPIC_PROFILES:
                prof = parse_profile(msg.value())
                with state.lock:
                    state.total_profiles += 1

                    if prof:
                        if state.ref_count > 0:
                            mean = dict(state.ref_mean)
                            dev = {k: prof[k] - mean[k] for k in ("L1", "L2", "L3", "L4")}
                            absdev = {k: abs(dev[k]) for k in dev}
                        else:
                            mean, dev, absdev = None, None, None

                        if prof["status"] == "n.i.O.":
                            state.nio_debug.append(
                                {
                                    "profile_id": prof["profile_id"],
                                    "L1": prof["L1"],
                                    "L2": prof["L2"],
                                    "L3": prof["L3"],
                                    "L4": prof["L4"],
                                    "mean_L1": (mean["L1"] if mean else None),
                                    "mean_L2": (mean["L2"] if mean else None),
                                    "mean_L3": (mean["L3"] if mean else None),
                                    "mean_L4": (mean["L4"] if mean else None),
                                    "dev_L1": (dev["L1"] if dev else None),
                                    "dev_L2": (dev["L2"] if dev else None),
                                    "dev_L3": (dev["L3"] if dev else None),
                                    "dev_L4": (dev["L4"] if dev else None),
                                    "|L1-mean|": (absdev["L1"] if absdev else None),
                                    "|L2-mean|": (absdev["L2"] if absdev else None),
                                    "|L3-mean|": (absdev["L3"] if absdev else None),
                                    "|L4-mean|": (absdev["L4"] if absdev else None),
                                    "limit_mm": NOK_MM,
                                    "reason": prof["reason"],
                                }
                            )

                        # update mean AFTER evaluation
                        state.ref_count += 1
                        n = state.ref_count
                        for k in ("L1", "L2", "L3", "L4"):
                            state.ref_mean[k] += (prof[k] - state.ref_mean[k]) / n

                    # write metrics to Redis on each profile
                    rr = ensure_redis()
                    if rr is not None:
                        try:
                            xadd_metrics(rr, state.total_profiles, state.nio_profiles, state.last_raw_ts)
                        except Exception:
                            pass
                continue

            # n.i.O. events -> nio counter (+ Redis metrics)
            if topic == TOPIC_NIO:
                with state.lock:
                    state.nio_profiles += 1
                    rr = ensure_redis()
                    if rr is not None:
                        try:
                            xadd_metrics(rr, state.total_profiles, state.nio_profiles, state.last_raw_ts)
                        except Exception:
                            pass
                continue

    except Exception as e:
        with state.lock:
            state.last_error = f"Unexpected error: {e}"
    finally:
        try:
            c.close()
        except Exception:
            pass


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Wire Dashboard", layout="wide")
st.title("Wire Dashboard (Kafka + Redis Streams)")

auto = st.sidebar.checkbox("Auto-Refresh", value=True)
st.sidebar.caption(f"{BOOTSTRAP} | group.id={GROUP_ID} | offset={OFFSET_RESET}")
st.sidebar.caption(f"RAW={TOPIC_RAW}")
st.sidebar.caption(f"PROFILES={TOPIC_PROFILES}")
st.sidebar.caption(f"NIO={TOPIC_NIO}")
st.sidebar.caption(f"Redis enabled={ENABLE_REDIS_CACHE} | {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

# init once
if "state" not in st.session_state:
    st.session_state.state = State()
    st.session_state.stop = threading.Event()
    t = threading.Thread(target=poll_loop, args=(st.session_state.state, st.session_state.stop), daemon=True)
    st.session_state.thread = t
    t.start()

state: State = st.session_state.state

if st.button("Reset (RAM + Redis Streams)"):
    with state.lock:
        state.points.clear()
        state.total_profiles = 0
        state.nio_profiles = 0
        state.last_raw_ts = None
        state.last_error = None
        state.ref_count = 0
        state.ref_mean = {"L1": 0.0, "L2": 0.0, "L3": 0.0, "L4": 0.0}
        state.nio_debug.clear()

    rr = get_redis()
    if rr is not None:
        clear_streams(rr)

with state.lock:
    total = state.total_profiles
    nio = state.nio_profiles
    last_ts = state.last_raw_ts
    err = state.last_error
    redis_ok = state.redis_ok
    points = list(state.points)
    ref_count = state.ref_count
    ref_mean = dict(state.ref_mean)
    nio_debug = list(state.nio_debug)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Erkannte Profile (gesamt)", total)
c2.metric("n.i.O. Profile", nio)
c3.metric("Letzter RAW Timestamp", last_ts or "—")
c4.metric("Redis Cache", "OFF" if not ENABLE_REDIS_CACHE else ("OK" if redis_ok else "DOWN"))

if err:
    st.warning(f"Kafka: {err}")

if not points:
    st.info("Noch keine RAW-Daten. Läuft der Producer? Topic korrekt?")
else:
    df = pd.DataFrame(points).sort_values("length_mm").set_index("length_mm")

    x = df.index.to_list()
    y_d = df["diameter_mm"].tolist()
    y_h = df["hard_temp"].tolist()
    y_t = df["temper_temp"].tolist()

    # break lines when length jumps
    for i in range(1, len(x)):
        if abs(x[i] - x[i - 1]) > BREAK_DELTA:
            y_d[i] = None
            y_h[i] = None
            y_t[i] = None

    st.subheader("Durchmesser + Temperaturen über Drahtlänge (2 Y-Achsen)")
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
    st.plotly_chart(fig, width="stretch")

    with st.expander("Letzte Punkte (Debug)"):
        st.dataframe(df.tail(25), width="stretch")

    with st.expander("n.i.O. Profile (Debug)"):
        if nio_debug:
            st.dataframe(pd.DataFrame(nio_debug).tail(MAX_NIO_DEBUG), width="stretch")
        else:
            st.info("Noch keine n.i.O. Profile empfangen.")

    with st.expander("Aktueller Mittelwert (Referenz für L1–L4)"):
        if ref_count <= 0:
            st.info("Noch keine Profile erkannt.")
        else:
            st.write(f"n = {ref_count} | limit = ±{NOK_MM} mm")
            st.dataframe(pd.DataFrame([ref_mean]), width="stretch")

if auto:
    time.sleep(REFRESH_S)
    st.rerun()