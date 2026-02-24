#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import signal
import logging
from dataclasses import dataclass
from typing import Optional, Dict

from confluent_kafka import Consumer, KafkaError, KafkaException

try:
    import redis  # pip install redis
except ImportError:
    redis = None


# -----------------------------
# Config
# -----------------------------
class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wire_visualizer")
    KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

    KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "wire_data")

    KAFKA_PROFILES_TOPIC = os.getenv("KAFKA_PROFILES_TOPIC", f"{KAFKA_INPUT_TOPIC}_profiles")
    KAFKA_NIO_TOPIC = os.getenv("KAFKA_NIO_TOPIC", f"{KAFKA_INPUT_TOPIC}_profile_events")

    # Feldnamen im JSON
    FIELD_LENGTH = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
    FIELD_DIAMETER = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")

    FIELD_HARD_TEMP = os.getenv("FIELD_HARD_TEMP", "fHaertetemperatur1 (Haerten)")
    FIELD_TEMPER_TEMP = os.getenv("FIELD_TEMPER_TEMP", "fAnlasstemperatur1 (Anlassen)")

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))

    REDIS_STREAM_PLOT = os.getenv("REDIS_STREAM_PLOT", "wire:plot")
    REDIS_STREAM_METRICS = os.getenv("REDIS_STREAM_METRICS", "wire:metrics")

    REDIS_MAXLEN_PLOT = int(os.getenv("REDIS_MAXLEN_PLOT", "50000"))
    REDIS_MAXLEN_METRICS = int(os.getenv("REDIS_MAXLEN_METRICS", "2000"))

    # Option A: Live Dashboard (matplotlib)
    ENABLE_PLOT = os.getenv("ENABLE_PLOT", "false").lower() == "true"
    PLOT_EVERY_N = int(os.getenv("PLOT_EVERY_N", "10"))         # bei 10Hz -> ~1Hz update
    MAX_PLOT_POINTS = int(os.getenv("MAX_PLOT_POINTS", "5000")) # damit Plot nicht unendlich wächst


# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("wire_visualizer")


# -----------------------------
# Data model
# -----------------------------
@dataclass
class PlotPoint:
    length_mm: float
    diameter_mm: float
    hard_temp: Optional[float]
    temper_temp: Optional[float]
    ts: Optional[str]


def build_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": Config.KAFKA_GROUP_ID,
        "auto.offset.reset": Config.KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 10_000,
    }
    c = Consumer(conf)
    c.subscribe([Config.KAFKA_INPUT_TOPIC, Config.KAFKA_PROFILES_TOPIC, Config.KAFKA_NIO_TOPIC])
    return c


def build_redis_client():
    if redis is None:
        log.warning("redis-py nicht installiert. Redis Stream Ausgabe deaktiviert. (pip install redis)")
        return None
    return redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT, db=Config.REDIS_DB, decode_responses=True)


def parse_raw_message(raw: bytes) -> Optional[PlotPoint]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None

    ts = data.get("Time")

    vals: Dict[str, float] = {}
    for entry in data.get("ProcessData", []):
        name = entry.get("Name", "")
        if name in (Config.FIELD_LENGTH, Config.FIELD_DIAMETER, Config.FIELD_HARD_TEMP, Config.FIELD_TEMPER_TEMP):
            try:
                vals[name] = float(entry["Value"])
            except Exception:
                pass

    if Config.FIELD_LENGTH not in vals or Config.FIELD_DIAMETER not in vals:
        return None

    return PlotPoint(
        length_mm=vals[Config.FIELD_LENGTH],
        diameter_mm=vals[Config.FIELD_DIAMETER],
        hard_temp=vals.get(Config.FIELD_HARD_TEMP),
        temper_temp=vals.get(Config.FIELD_TEMPER_TEMP),
        ts=ts,
    )


def write_plot_point(r, p: PlotPoint) -> None:
    if r is None:
        return
    fields = {
        "length_mm": f"{p.length_mm:.3f}",
        "diameter_mm": f"{p.diameter_mm:.3f}",
    }
    if p.hard_temp is not None:
        fields["hard_temp"] = f"{p.hard_temp:.3f}"
    if p.temper_temp is not None:
        fields["temper_temp"] = f"{p.temper_temp:.3f}"
    if p.ts:
        fields["ts"] = p.ts

    r.xadd(Config.REDIS_STREAM_PLOT, fields, maxlen=Config.REDIS_MAXLEN_PLOT, approximate=True)


def write_metrics(r, total_profiles: int, nio_profiles: int) -> None:
    if r is None:
        return
    r.xadd(
        Config.REDIS_STREAM_METRICS,
        {"total_profiles": str(total_profiles), "nio_profiles": str(nio_profiles), "ts": str(time.time())},
        maxlen=Config.REDIS_MAXLEN_METRICS,
        approximate=True,
    )


def run() -> None:
    r = build_redis_client()
    c = build_consumer()

    total_profiles = 0
    nio_profiles = 0
    msg_i = 0

    # ---- Option A: Live Dashboard (matplotlib) ----
    if Config.ENABLE_PLOT:
        from collections import deque as dq
        import matplotlib.pyplot as plt

        plt.ion()
        fig, ax = plt.subplots()

        (l_diam,) = ax.plot([], [], label="Durchmesser")
        (l_hard,) = ax.plot([], [], label="Härtetemperatur")
        (l_temp,) = ax.plot([], [], label="Anlasstemperatur")

        ax.set_xlabel("Drahtlänge [mm]")
        ax.set_title("Kafka Live – Durchmesser / Temperaturen über Länge")
        ax.legend()

        xs = dq(maxlen=Config.MAX_PLOT_POINTS)
        ys_d = dq(maxlen=Config.MAX_PLOT_POINTS)
        ys_h = dq(maxlen=Config.MAX_PLOT_POINTS)
        ys_t = dq(maxlen=Config.MAX_PLOT_POINTS)

        # Textbox für KPI
        kpi = ax.text(
            0.02, 0.98,
            "Profiles total=0 | n.i.O.=0",
            transform=ax.transAxes,
            va="top",
        )

    running = True

    def _shutdown(signum, _frame):
        nonlocal running
        log.info("Signal %d – Stop.", signum)
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info(
        "Visualizer gestartet | raw=%s | profiles=%s | nio=%s | redis=%s:%d | plot=%s",
        Config.KAFKA_INPUT_TOPIC, Config.KAFKA_PROFILES_TOPIC, Config.KAFKA_NIO_TOPIC,
        Config.REDIS_HOST, Config.REDIS_PORT, Config.ENABLE_PLOT
    )

    last_metrics_push = 0.0

    try:
        while running:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            topic = msg.topic()
            msg_i += 1

            # 1) Zähler über Profile-Topics
            if topic == Config.KAFKA_PROFILES_TOPIC:
                total_profiles += 1
            elif topic == Config.KAFKA_NIO_TOPIC:
                nio_profiles += 1

            # 2) Raw points -> Redis (+ optional Dashboard)
            if topic == Config.KAFKA_INPUT_TOPIC:
                pnt = parse_raw_message(msg.value())
                if pnt is not None:
                    write_plot_point(r, pnt)

                    if Config.ENABLE_PLOT:
                        import math as _m
                        xs.append(pnt.length_mm)
                        ys_d.append(pnt.diameter_mm)
                        ys_h.append(pnt.hard_temp if pnt.hard_temp is not None else _m.nan)
                        ys_t.append(pnt.temper_temp if pnt.temper_temp is not None else _m.nan)

                        if (msg_i % Config.PLOT_EVERY_N) == 0:
                            l_diam.set_data(xs, ys_d)
                            l_hard.set_data(xs, ys_h)
                            l_temp.set_data(xs, ys_t)

                            kpi.set_text(f"Profiles total={total_profiles} | n.i.O.={nio_profiles}")

                            ax.relim()
                            ax.autoscale_view()
                            fig.canvas.draw()
                            fig.canvas.flush_events()

            # 3) Metriken regelmäßig nach Redis
            now = time.time()
            if now - last_metrics_push >= 1.0:
                write_metrics(r, total_profiles, nio_profiles)
                last_metrics_push = now

    finally:
        c.close()
        log.info("Beendet | total_profiles=%d | nio_profiles=%d", total_profiles, nio_profiles)


if __name__ == "__main__":
    run()