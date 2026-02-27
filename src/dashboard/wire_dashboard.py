import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import redis
from confluent_kafka import Consumer


# -----------------------------
# ENV / Config
# -----------------------------
# Kafka
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", os.getenv("BOOTSTRAP_SERVERS", "kafka:9092"))
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wire-dashboard")
OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")  # safest for "see everything"

TOPIC_RAW = os.getenv("KAFKA_INPUT_TOPIC", os.getenv("INPUT_TOPIC", "1031103_1000"))
TOPIC_PROFILES = os.getenv("KAFKA_PROFILES_TOPIC", f"{TOPIC_RAW}_profiles")
TOPIC_NIO = os.getenv("KAFKA_NIO_TOPIC", f"{TOPIC_RAW}_profile_events")

FIELD_LENGTH = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
FIELD_DIAMETER = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")
FIELD_HARD_TEMP = os.getenv("FIELD_HARD_TEMP", "fHaertetemperatur (Haerten)")
FIELD_TEMPER_TEMP = os.getenv("FIELD_TEMPER_TEMP", "fAnlasstemperatur (Anlassen)")

MAX_POINTS = int(os.getenv("MAX_POINTS", "3000"))
REFRESH_S = float(os.getenv("REFRESH_SECONDS", "1.0"))
BREAK_DELTA = float(os.getenv("BREAK_DELTA_MM", "5000"))  # line break if length jumps too much

# Redis cache (optional, but ON by default for option 2)
ENABLE_REDIS_CACHE = os.getenv("ENABLE_REDIS_CACHE", "true").lower() in ("1", "true", "yes", "y", "on")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

STREAM_PLOT = os.getenv("REDIS_STREAM_PLOT", "wire:plot")
STREAM_METRICS = os.getenv("REDIS_STREAM_METRICS", "wire:metrics")
REDIS_MAXLEN_PLOT = int(os.getenv("REDIS_MAXLEN_PLOT", str(MAX_POINTS)))
REDIS_MAXLEN_METRICS = int(os.getenv("REDIS_MAXLEN_METRICS", "1000"))

# throttle for writing metrics (avoid xadd too often)
METRICS_FLUSH_EVERY_S = float(os.getenv("METRICS_FLUSH_EVERY_S", "1.0"))


# -----------------------------
# Helpers
# -----------------------------
def _pd_value(record: dict, wanted_name: str) -> Optional[float]:
    for item in record.get("ProcessData", []):
        if item.get("Name") == wanted_name:
            try:
                return float(item.get("Value"))
            except Exception:
                return None
    return None


@dataclass
class PlotPoint:
    length_mm: float
    diameter_mm: float
    hard_temp: Optional[float]
    temper_temp: Optional[float]
    ts: Optional[str]


def _parse_raw_message(raw: bytes) -> Optional[PlotPoint]:
    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        return None

    length = _pd_value(data, FIELD_LENGTH)
    diam = _pd_value(data, FIELD_DIAMETER)
    if length is None or diam is None:
        return None

    return PlotPoint(
        length_mm=float(length),
        diameter_mm=float(diam),
        hard_temp=_pd_value(data, FIELD_HARD_TEMP),
        temper_temp=_pd_value(data, FIELD_TEMPER_TEMP),
        ts=data.get("Time"),
    )


def _get_redis() -> Optional["redis.Redis"]:
    if not ENABLE_REDIS_CACHE:
        return None
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        return r
    except Exception:
        return None


def _redis_xadd_plot(r: "redis.Redis", p: PlotPoint) -> None:
    fields = {
        "length_mm": f"{p.length_mm}",
        "diameter_mm": f"{p.diameter_mm}",
    }
    if p.hard_temp is not None:
        fields["hard_temp"] = f"{p.hard_temp}"
    if p.temper_temp is not None:
        fields["temper_temp"] = f"{p.temper_temp}"
    if p.ts is not None:
        fields["ts"] = p.ts

    r.xadd(STREAM_PLOT, fields, maxlen=REDIS_MAXLEN_PLOT, approximate=True)


def _redis_xadd_metrics(r: "redis.Redis", total_profiles: int, nio_profiles: int, ts: Optional[str]) -> None:
    fields = {
        "total_profiles": str(total_profiles),
        "nio_profiles": str(nio_profiles),
    }
    if ts is not None:
        fields["ts"] = ts

    r.xadd(STREAM_METRICS, fields, maxlen=REDIS_MAXLEN_METRICS, approximate=True)


def _redis_clear_streams(r: "redis.Redis") -> None:
    try:
        r.delete(STREAM_PLOT)
    except Exception:
        pass
    try:
        r.delete(STREAM_METRICS)
    except Exception:
        pass


# -----------------------------
# Kafka Consumer Thread State
# -----------------------------
class StreamState:
    """Thread-safe shared state between Kafka polling thread and Streamlit UI."""

    def __init__(self, max_points: int):
        self.lock = threading.Lock()
        self.points: Deque[PlotPoint] = deque(maxlen=max_points)
        self.total_profiles = 0
        self.nio_profiles = 0
        self.last_raw_ts: Optional[str] = None
        self.last_error: Optional[str] = None

        # redis/cache status
        self.redis_connected: bool = False


def _build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": True,
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 10_000,
    }
    c = Consumer(conf)
    c.subscribe([TOPIC_RAW, TOPIC_PROFILES, TOPIC_NIO])
    return c


def _poll_loop(state: StreamState, stop_event: threading.Event) -> None:
    # Create Redis connection once; retry occasionally if down
    r = _get_redis()
    last_redis_retry = 0.0

    def ensure_redis() -> Optional["redis.Redis"]:
        nonlocal r, last_redis_retry
        if not ENABLE_REDIS_CACHE:
            state.redis_connected = False
            return None
        if r is not None:
            state.redis_connected = True
            return r
        # retry every 5s
        now = time.time()
        if now - last_redis_retry >= 5.0:
            last_redis_retry = now
            r = _get_redis()
        state.redis_connected = r is not None
        return r

    # Kafka
    try:
        consumer = _build_consumer()
    except Exception as e:
        with state.lock:
            state.last_error = f"Kafka Consumer konnte nicht starten: {e}"
        return

    last_metrics_flush = 0.0
    last_metrics_total = -1
    last_metrics_nio = -1
    last_metrics_ts: Optional[str] = None

    try:
        while not stop_event.is_set():
            msg = consumer.poll(0.5)

            if msg is None:
                # maybe flush metrics periodically even without messages
                now = time.time()
                if ENABLE_REDIS_CACHE and (now - last_metrics_flush) >= METRICS_FLUSH_EVERY_S:
                    rr = ensure_redis()
                    if rr is not None:
                        with state.lock:
                            t = state.total_profiles
                            n = state.nio_profiles
                            ts = state.last_raw_ts
                        if (t != last_metrics_total) or (n != last_metrics_nio) or (ts != last_metrics_ts):
                            try:
                                _redis_xadd_metrics(rr, t, n, ts)
                                last_metrics_total, last_metrics_nio, last_metrics_ts = t, n, ts
                            except Exception:
                                pass
                        last_metrics_flush = now
                continue

            if msg.error():
                with state.lock:
                    state.last_error = str(msg.error())
                continue

            # If we are here, message is OK -> clear any old error
            with state.lock:
                state.last_error = None

            topic = msg.topic()

            # 1) KPI events (counts) based on topic
            if topic == TOPIC_PROFILES:
                with state.lock:
                    state.total_profiles += 1

                now = time.time()
                if ENABLE_REDIS_CACHE and (now - last_metrics_flush) >= METRICS_FLUSH_EVERY_S:
                    rr = ensure_redis()
                    if rr is not None:
                        with state.lock:
                            t = state.total_profiles
                            n = state.nio_profiles
                            ts = state.last_raw_ts
                        try:
                            _redis_xadd_metrics(rr, t, n, ts)
                            last_metrics_total, last_metrics_nio, last_metrics_ts = t, n, ts
                        except Exception:
                            pass
                    last_metrics_flush = now
                continue

            if topic == TOPIC_NIO:
                with state.lock:
                    state.nio_profiles += 1

                now = time.time()
                if ENABLE_REDIS_CACHE and (now - last_metrics_flush) >= METRICS_FLUSH_EVERY_S:
                    rr = ensure_redis()
                    if rr is not None:
                        with state.lock:
                            t = state.total_profiles
                            n = state.nio_profiles
                            ts = state.last_raw_ts
                        try:
                            _redis_xadd_metrics(rr, t, n, ts)
                            last_metrics_total, last_metrics_nio, last_metrics_ts = t, n, ts
                        except Exception:
                            pass
                    last_metrics_flush = now
                continue

            # 2) RAW points for plotting
            if topic == TOPIC_RAW:
                p = _parse_raw_message(msg.value())
                if p is None:
                    continue

                with state.lock:
                    state.points.append(p)
                    state.last_raw_ts = p.ts

                if ENABLE_REDIS_CACHE:
                    rr = ensure_redis()
                    if rr is not None:
                        try:
                            _redis_xadd_plot(rr, p)
                        except Exception:
                            pass

                now = time.time()
                if ENABLE_REDIS_CACHE and (now - last_metrics_flush) >= METRICS_FLUSH_EVERY_S:
                    rr = ensure_redis()
                    if rr is not None:
                        with state.lock:
                            t = state.total_profiles
                            n = state.nio_profiles
                            ts = state.last_raw_ts
                        if (t != last_metrics_total) or (n != last_metrics_nio) or (ts != last_metrics_ts):
                            try:
                                _redis_xadd_metrics(rr, t, n, ts)
                                last_metrics_total, last_metrics_nio, last_metrics_ts = t, n, ts
                            except Exception:
                                pass
                    last_metrics_flush = now

    except Exception as e:
        with state.lock:
            state.last_error = f"Unexpected error: {e}"
    finally:
        try:
            consumer.close()
        except Exception:
            pass


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Wire Dashboard (Kafka + Redis Cache)", layout="wide")
st.title("Wire Dashboard (Kafka direkt + Redis Cache)")

auto = st.sidebar.checkbox("Auto-Refresh", value=True)

st.sidebar.markdown("### Kafka")
st.sidebar.code(
    "\n".join(
        [
            f"bootstrap = {BOOTSTRAP}",
            f"group.id = {GROUP_ID}",
            f"offset.reset = {OFFSET_RESET}",
            "",
            f"RAW      = {TOPIC_RAW}",
            f"PROFILES = {TOPIC_PROFILES}",
            f"NIO      = {TOPIC_NIO}",
        ]
    )
)

st.sidebar.markdown("### Redis Cache")
st.sidebar.write(f"enabled: **{ENABLE_REDIS_CACHE}**")
st.sidebar.code(
    "\n".join(
        [
            f"host = {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}",
            f"plot stream    = {STREAM_PLOT} (maxlen≈{REDIS_MAXLEN_PLOT})",
            f"metrics stream = {STREAM_METRICS} (maxlen≈{REDIS_MAXLEN_METRICS})",
        ]
    )
)
st.sidebar.caption(f"Refresh: {REFRESH_S}s | Max points (RAM): {MAX_POINTS} | Break Δ: {int(BREAK_DELTA)} mm")

# Init background thread once per session
if "stream_state" not in st.session_state:
    st.session_state.stream_state = StreamState(max_points=MAX_POINTS)
    st.session_state.stop_event = threading.Event()
    t = threading.Thread(
        target=_poll_loop,
        args=(st.session_state.stream_state, st.session_state.stop_event),
        daemon=True,
        name="kafka-poll-thread",
    )
    st.session_state.kafka_thread = t
    t.start()

state: StreamState = st.session_state.stream_state

col_a, col_b = st.columns([1, 3])

if col_a.button("Reset (RAM + Redis Cache)"):
    with state.lock:
        state.points.clear()
        state.total_profiles = 0
        state.nio_profiles = 0
        state.last_raw_ts = None
        state.last_error = None

    if ENABLE_REDIS_CACHE:
        rr = _get_redis()
        if rr is not None:
            _redis_clear_streams(rr)

# Snapshot for UI render
with state.lock:
    total = state.total_profiles
    nio = state.nio_profiles
    last_ts = state.last_raw_ts
    last_err = state.last_error
    redis_ok = state.redis_connected
    points = list(state.points)

# KPIs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Erkannte Profile (gesamt)", total)
c2.metric("n.i.O. Profile", nio)
c3.metric("Letzter RAW Timestamp", last_ts or "—")
c4.metric("Redis Cache", "OK" if (ENABLE_REDIS_CACHE and redis_ok) else ("OFF" if not ENABLE_REDIS_CACHE else "DOWN"))

if last_err:
    st.warning(f"Kafka Status: {last_err}")

# Plot
if not points:
    st.info("Noch keine RAW-Daten empfangen. Läuft der Producer? Ist das RAW-Topic korrekt?")
else:
    rows = [
        {
            "length_mm": p.length_mm,
            "diameter_mm": p.diameter_mm,
            "hard_temp": p.hard_temp if p.hard_temp is not None else float("nan"),
            "temper_temp": p.temper_temp if p.temper_temp is not None else float("nan"),
        }
        for p in points
    ]

    df = (
        pd.DataFrame(rows)
        .dropna(subset=["length_mm"])
        .sort_values("length_mm")
        .set_index("length_mm")
    )

    st.subheader("Durchmesser + Temperaturen über Drahtlänge (2 Y-Achsen)")

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

    # Streamlit warning says use width='stretch' instead of use_container_width=True
    st.plotly_chart(fig, width="stretch")

    with st.expander("Letzte Punkte (Debug)"):
        st.dataframe(df.tail(25), width="stretch")

# Auto refresh
if auto:
    time.sleep(REFRESH_S)
    st.rerun()