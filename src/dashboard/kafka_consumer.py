#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import logging
import math
import os
import signal
import sys
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import Deque, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


# -------------------------------------------------
# Logging
# -------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("wire_profile_detector")


# -------------------------------------------------
# Config
# -------------------------------------------------
class Config:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_INPUT_TOPIC: str = os.getenv("KAFKA_INPUT_TOPIC", "wire_data")
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "wire_profile_detector")
    KAFKA_AUTO_OFFSET_RESET: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

    FIELD_LENGTH: str = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
    FIELD_DIAMETER: str = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")

    SMOOTH_WINDOW: int = int(os.getenv("SMOOTH_WINDOW", "3"))
    SIGMA_FACTOR: float = float(os.getenv("SIGMA_FACTOR", "3.0"))
    SIGMA_MIN_POINTS: int = int(os.getenv("SIGMA_MIN_POINTS", "20"))
    SIGMA_MIN_STD: float = float(os.getenv("SIGMA_MIN_STD", "0.05"))
    HYSTERESIS_COUNT: int = int(os.getenv("HYSTERESIS_COUNT", "3"))
    TREND_WINDOW: int = int(os.getenv("TREND_WINDOW", "10"))
    SLOPE_FLAT_THRESHOLD: float = float(os.getenv("SLOPE_FLAT_THRESHOLD", "0.003"))
    FLAT_LOOKBACK_PTS: int = int(os.getenv("FLAT_LOOKBACK_PTS", "5"))

    MIN_SPACE_MM: float = float(os.getenv("MIN_SPACE_MM", "200.0"))
    MIN_RISE_MM: float = float(os.getenv("MIN_RISE_MM", "100.0"))
    MIN_PLATEAU_MM: float = float(os.getenv("MIN_PLATEAU_MM", "500.0"))
    MIN_FALL_MM: float = float(os.getenv("MIN_FALL_MM", "100.0"))

    NOK_DEVIATION_MM: float = float(os.getenv("NOK_DEVIATION_MM", "30.0"))

    HEARTBEAT_EVERY_MSGS: int = int(os.getenv("HEARTBEAT_EVERY_MSGS", "100"))
    RESET_ON_NONMONOTONIC_LENGTH: bool = os.getenv("RESET_ON_NONMONOTONIC_LENGTH", "true").lower() == "true"
    NONMONO_RESET_DELTA_MM: float = float(os.getenv("NONMONO_RESET_DELTA_MM", "1000.0"))

    # Kafka topic creation
    EVENTS_TOPIC_PARTITIONS: int = int(os.getenv("EVENTS_TOPIC_PARTITIONS", "1"))
    EVENTS_TOPIC_REPLICATION: int = int(os.getenv("EVENTS_TOPIC_REPLICATION", "1"))

    @classmethod
    def output_topic(cls) -> str:
        # n.i.O.-Events
        return f"{cls.KAFKA_INPUT_TOPIC}_profile_events"

    @classmethod
    def profiles_topic(cls) -> str:
        # alle erkannten Profile
        return f"{cls.KAFKA_INPUT_TOPIC}_profiles"

    @classmethod
    def nio_topic(cls) -> str:
        # nur n.i.O.-Profile (gleiches Topic wie output_topic)
        return cls.output_topic()

    @classmethod
    def min_length_for_state(cls, state: "State") -> float:
        return {
            State.SPACE: cls.MIN_SPACE_MM,
            State.RISE: cls.MIN_RISE_MM,
            State.PLATEAU: cls.MIN_PLATEAU_MM,
            State.FALL: cls.MIN_FALL_MM,
        }.get(state, 0.0)


# -------------------------------------------------
# Kafka topic auto-create
# -------------------------------------------------
def ensure_topic_exists(topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS})
    md = admin.list_topics(timeout=10)

    if topic in md.topics and md.topics[topic].error is None:
        log.info("Topic exists: %s", topic)
        return

    log.info("Creating topic: %s", topic)
    new_topic = NewTopic(
        topic,
        num_partitions=Config.EVENTS_TOPIC_PARTITIONS,
        replication_factor=Config.EVENTS_TOPIC_REPLICATION,
    )
    futures = admin.create_topics([new_topic])

    try:
        futures[topic].result(timeout=15)
        log.info("Topic created: %s", topic)
    except Exception as e:
        # Often OK if created in parallel
        log.warning("Topic create skipped/failed (often OK if already exists): %s", e)


# -------------------------------------------------
# State Machine Types
# -------------------------------------------------
class State(Enum):
    WAIT_FOR_SPACE = auto()
    SPACE = auto()
    RISE = auto()
    PLATEAU = auto()
    FALL = auto()


@dataclass
class DataPoint:
    length_mm: float
    diameter_mm: float


class OnlineStats:
    """Online mean/std (Welford). Used for adaptive sigma thresholds -> no fixed diameter thresholds."""
    def __init__(self) -> None:
        self._n: int = 0
        self._mean: float = 0.0
        self._M2: float = 0.0

    def update(self, v: float) -> None:
        self._n += 1
        d = v - self._mean
        self._mean += d / self._n
        self._M2 += (v - self._mean) * d

    @property
    def count(self) -> int:
        return self._n

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def std(self) -> float:
        if self._n < 2:
            return Config.SIGMA_MIN_STD
        return max(math.sqrt(self._M2 / (self._n - 1)), Config.SIGMA_MIN_STD)

    def threshold_high(self, factor: float) -> float:
        return self._mean + factor * self.std

    def threshold_low(self, factor: float) -> float:
        return self._mean - factor * self.std

    def reset(self) -> None:
        self._n = 0
        self._mean = 0.0
        self._M2 = 0.0


def linear_regression_slope(xs: List[float], ys: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_xx = sum(x * x for x in xs)
    denom = n * sum_xx - sum_x * sum_x
    if abs(denom) < 1e-12:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denom


class HysteresisGate:
    def __init__(self, required: int) -> None:
        self._required = max(1, required)
        self._count: int = 0

    def feed(self, condition: bool) -> bool:
        self._count = (self._count + 1) if condition else 0
        return self._count >= self._required

    def reset(self) -> None:
        self._count = 0


@dataclass
class Profile:
    profile_id: int
    L1: float = 0.0
    L2: float = 0.0
    L3: float = 0.0
    L4: float = 0.0
    status: str = "i.O."
    reason: str = ""

    def to_dict(self) -> Dict:
        d = {
            "profile_id": self.profile_id,
            "L1": round(self.L1, 2),
            "L2": round(self.L2, 2),
            "L3": round(self.L3, 2),
            "L4": round(self.L4, 2),
            "status": self.status,
        }
        if self.reason:
            d["reason"] = self.reason
        return d


# -------------------------------------------------
# Reference means (NO LEARNING)
# - First profile initializes reference
# - Reference mean updates with EVERY profile (i.O. and n.i.O.)
# -------------------------------------------------
class ReferenceStats:
    def __init__(self) -> None:
        self._count: int = 0
        self._mean: Dict[str, float] = {k: 0.0 for k in ("L1", "L2", "L3", "L4")}

    @property
    def count(self) -> int:
        return self._count

    def ref_snapshot(self) -> Dict[str, float]:
        return {k: self._mean[k] for k in ("L1", "L2", "L3", "L4")}

    def update_reference(self, profile: Profile) -> None:
        self._count += 1
        for key in ("L1", "L2", "L3", "L4"):
            val = getattr(profile, key)
            self._mean[key] += (val - self._mean[key]) / self._count

    def evaluate(self, profile: Profile) -> Tuple[str, str]:
        # Start immediately: first profile sets baseline -> i.O.
        if self._count == 0:
            return "i.O.", "first profile initializes reference"

        reasons = []
        for key in ("L1", "L2", "L3", "L4"):
            dev = abs(getattr(profile, key) - self._mean[key])
            if dev > Config.NOK_DEVIATION_MM:
                reasons.append(
                    f"{key} Abw. {dev:.1f}mm > {Config.NOK_DEVIATION_MM:.1f}mm "
                    f"(ist={getattr(profile, key):.1f} ref={self._mean[key]:.1f})"
                )
        if reasons:
            return "n.i.O.", "; ".join(reasons)
        return "i.O.", ""


# -------------------------------------------------
# Profile Detector (adaptive, no fixed diameter thresholds)
# -------------------------------------------------
class ProfileStateMachine:
    def __init__(self, ref: ReferenceStats) -> None:
        lookback_size = Config.FLAT_LOOKBACK_PTS + Config.HYSTERESIS_COUNT + 2
        self._state: State = State.WAIT_FOR_SPACE
        self._ref = ref
        self._profile_counter: int = 0

        self._smooth_buf: Deque[float] = deque(maxlen=Config.SMOOTH_WINDOW)
        self._trend_buf: Deque[DataPoint] = deque(maxlen=Config.TREND_WINDOW)
        self._lookback: Deque[float] = deque(maxlen=lookback_size)

        self._seg_stats = OnlineStats()
        self._hyst_sigma = HysteresisGate(Config.HYSTERESIS_COUNT)
        self._hyst_flat = HysteresisGate(Config.HYSTERESIS_COUNT)

        self._seg_start: float = 0.0
        self._state_entry_length: float = 0.0
        self._segments: Dict[str, float] = {}

        log.info("=== Wire Profile Detector startet ===")
        log.info(
            "Config | In=%s Out(nio)=%s Out(all)=%s Broker=%s | NOK=±%.0fmm",
            Config.KAFKA_INPUT_TOPIC,
            Config.nio_topic(),
            Config.profiles_topic(),
            Config.KAFKA_BOOTSTRAP_SERVERS,
            Config.NOK_DEVIATION_MM,
        )
        log.info(
            "Detector | SMOOTH=%d SIGMA=%.1f MINPTS=%d HYST=%d TREND=%d FLAT=%.4f | MIN SPACE=%.0f RISE=%.0f PLATEAU=%.0f FALL=%.0f",
            Config.SMOOTH_WINDOW,
            Config.SIGMA_FACTOR,
            Config.SIGMA_MIN_POINTS,
            Config.HYSTERESIS_COUNT,
            Config.TREND_WINDOW,
            Config.SLOPE_FLAT_THRESHOLD,
            Config.MIN_SPACE_MM,
            Config.MIN_RISE_MM,
            Config.MIN_PLATEAU_MM,
            Config.MIN_FALL_MM,
        )

    def current_state(self) -> State:
        return self._state

    def feed(self, point: DataPoint) -> Optional[Profile]:
        self._smooth_buf.append(point.diameter_mm)
        smooth = sum(self._smooth_buf) / len(self._smooth_buf)

        sp = DataPoint(point.length_mm, smooth)
        self._trend_buf.append(sp)
        self._lookback.append(point.length_mm)

        slope = self._slope() if len(self._trend_buf) >= Config.TREND_WINDOW else 0.0
        is_flat = abs(slope) < Config.SLOPE_FLAT_THRESHOLD if len(self._trend_buf) >= Config.TREND_WINDOW else True

        # Build stats mainly on flat areas, but keep it stable during transitions too
        if self._state in (State.SPACE, State.PLATEAU):
            if is_flat:
                self._seg_stats.update(smooth)
        else:
            self._seg_stats.update(smooth)

        return self._transition(sp, point.length_mm, slope, is_flat)

    def _slope(self) -> float:
        buf = list(self._trend_buf)
        return linear_regression_slope([p.length_mm for p in buf], [p.diameter_mm for p in buf])

    def _sigma_ready(self) -> bool:
        return self._seg_stats.count >= Config.SIGMA_MIN_POINTS

    def _above_sigma(self, d: float) -> bool:
        return self._sigma_ready() and d > self._seg_stats.threshold_high(Config.SIGMA_FACTOR)

    def _below_sigma(self, d: float) -> bool:
        return self._sigma_ready() and d < self._seg_stats.threshold_low(Config.SIGMA_FACTOR)

    def _enough(self, current_length: float) -> bool:
        return (current_length - self._state_entry_length) >= Config.min_length_for_state(self._state)

    def _corrected_length(self, current_length: float) -> float:
        buf = list(self._lookback)
        offset = Config.FLAT_LOOKBACK_PTS + Config.HYSTERESIS_COUNT - 1
        if len(buf) > offset:
            return buf[-(offset + 1)]
        return current_length

    def _transition(self, sp: DataPoint, raw_length: float, slope: float, is_flat: bool) -> Optional[Profile]:
        prev = self._state
        d = sp.diameter_mm
        result: Optional[Profile] = None

        if self._state == State.WAIT_FOR_SPACE:
            self._seg_start = raw_length
            self._enter_state(State.SPACE, raw_length)

        elif self._state == State.SPACE:
            if self._hyst_sigma.feed(self._above_sigma(d)) and self._enough(raw_length):
                self._close_segment("L2", raw_length)
                self._enter_state(State.RISE, raw_length)

        elif self._state == State.RISE:
            if self._hyst_flat.feed(is_flat) and self._enough(raw_length):
                tp = self._corrected_length(raw_length)
                self._close_segment("L3", tp)
                self._enter_state(State.PLATEAU, raw_length)
                self._seg_start = tp
            elif slope < -(Config.SLOPE_FLAT_THRESHOLD * 3):
                log.info("Unerwarteter FALL in RISE – Reset")
                self._reset(raw_length)

        elif self._state == State.PLATEAU:
            if self._hyst_sigma.feed(self._below_sigma(d)) and self._enough(raw_length):
                self._close_segment("L1", raw_length)
                self._enter_state(State.FALL, raw_length)

        elif self._state == State.FALL:
            if self._hyst_flat.feed(is_flat) and self._enough(raw_length):
                tp = self._corrected_length(raw_length)
                self._close_segment("L4", tp)
                if self._all_segments_present():
                    result = self._finalize_profile()
                else:
                    log.warning("Unvollständige Segmente %s – verworfen", list(self._segments.keys()))
                    self._segments.clear()
                self._enter_state(State.SPACE, raw_length)
                self._seg_start = tp
            elif slope > (Config.SLOPE_FLAT_THRESHOLD * 3):
                log.info("Unerwarteter RISE in FALL – Reset")
                self._reset(raw_length)

        if self._state != prev:
            log.info("State %-14s → %-10s | L=%9.1f mm", prev.name, self._state.name, raw_length)

        return result

    def _enter_state(self, new_state: State, length: float) -> None:
        self._state = new_state
        self._state_entry_length = length
        self._seg_stats.reset()
        self._hyst_sigma.reset()
        self._hyst_flat.reset()
        self._lookback.clear()

    def _close_segment(self, label: str, end_length: float) -> None:
        seg_len = max(0.0, end_length - self._seg_start)
        self._segments[label] = seg_len
        self._seg_start = end_length

    def _all_segments_present(self) -> bool:
        return all(k in self._segments for k in ("L1", "L2", "L3", "L4"))

    def _reset(self, length: float) -> None:
        log.info("Profil-Reset | State=%s | Segmente verworfen=%s", self._state.name, list(self._segments.keys()))
        self._segments.clear()
        self._seg_start = length
        self._enter_state(State.SPACE, length)

    def _finalize_profile(self) -> Optional[Profile]:
        self._profile_counter += 1

        profile = Profile(
            profile_id=self._profile_counter,
            L1=self._segments.pop("L1", 0.0),
            L2=self._segments.pop("L2", 0.0),
            L3=self._segments.pop("L3", 0.0),
            L4=self._segments.pop("L4", 0.0),
        )

        status, reason = self._ref.evaluate(profile)
        profile.status = status
        profile.reason = reason

        # Log diffs vs mean (if mean exists) BEFORE update
        if self._ref.count > 0:
            exp = self._ref.ref_snapshot()
            diffs = {k: abs(getattr(profile, k) - exp[k]) for k in ("L1", "L2", "L3", "L4")}

            def chk(v: float) -> str:
                return "✅" if v <= Config.NOK_DEVIATION_MM else "❌"

            log.info(
                "Expected means (n=%d) | mean(L1)=%.1f mean(L2)=%.1f mean(L3)=%.1f mean(L4)=%.1f",
                self._ref.count,
                exp["L1"],
                exp["L2"],
                exp["L3"],
                exp["L4"],
            )
            log.info(
                "Diffs (±%.0fmm) | |L1-mean|=%.1f %s  |L2-mean|=%.1f %s  |L3-mean|=%.1f %s  |L4-mean|=%.1f %s",
                Config.NOK_DEVIATION_MM,
                diffs["L1"],
                chk(diffs["L1"]),
                diffs["L2"],
                chk(diffs["L2"]),
                diffs["L3"],
                chk(diffs["L3"]),
                diffs["L4"],
                chk(diffs["L4"]),
            )
        else:
            log.info("Expected means: not initialized yet (first profile will init reference)")

        # ✅ Update reference for EVERY profile (i.O. AND n.i.O.)
        self._ref.update_reference(profile)

        tail = f" | {profile.reason}" if profile.reason else ""
        log.info(
            "Profil #%-4d | L1=%6.1f  L2=%6.1f  L3=%6.1f  L4=%6.1f mm | %s%s",
            profile.profile_id,
            profile.L1,
            profile.L2,
            profile.L3,
            profile.L4,
            profile.status,
            tail,
        )
        if profile.status == "n.i.O.":
            log.warning("n.i.O. Profil #%d: %s", profile.profile_id, profile.reason)

        return profile


# -------------------------------------------------
# Kafka IO
# -------------------------------------------------
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
    c.subscribe([Config.KAFKA_INPUT_TOPIC])
    log.info("Kafka Consumer | Topic=%s | Broker=%s", Config.KAFKA_INPUT_TOPIC, Config.KAFKA_BOOTSTRAP_SERVERS)
    return c


def build_producer() -> Producer:
    conf = {
        "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",
    }
    p = Producer(conf)
    log.info("Kafka Producer | Broker=%s", Config.KAFKA_BOOTSTRAP_SERVERS)
    return p


def parse_message(raw: bytes) -> Optional[DataPoint]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("JSON-Fehler: %s", exc)
        return None

    vals: Dict[str, float] = {}
    for entry in data.get("ProcessData", []):
        name = entry.get("Name", "")
        if name in (Config.FIELD_LENGTH, Config.FIELD_DIAMETER):
            try:
                vals[name] = float(entry["Value"])
            except (KeyError, TypeError, ValueError):
                pass

    if Config.FIELD_LENGTH not in vals or Config.FIELD_DIAMETER not in vals:
        return None

    return DataPoint(length_mm=vals[Config.FIELD_LENGTH], diameter_mm=vals[Config.FIELD_DIAMETER])


def publish_profile(producer: Producer, topic: str, profile: Profile) -> None:
    producer.produce(
        topic=topic,
        key=str(profile.profile_id).encode(),
        value=json.dumps(profile.to_dict()).encode(),
    )
    producer.poll(0)


# -------------------------------------------------
# Main
# -------------------------------------------------
def run() -> None:
    # ensure topics exist
    ensure_topic_exists(Config.profiles_topic())  # alle erkannten Profile
    ensure_topic_exists(Config.nio_topic())       # nur n.i.O. (Events)

    ref = ReferenceStats()
    sm = ProfileStateMachine(ref)
    consumer = build_consumer()
    producer = build_producer()

    running = True

    def _shutdown(signum, _frame):
        nonlocal running
        log.info("Signal %d – Shutdown …", signum)
        running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    msg_count = 0
    profile_count = 0
    nio_count = 0
    last_len: Optional[float] = None

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            msg_count += 1
            point = parse_message(msg.value())
            if point is None:
                continue

            # reset if length jumps backwards a lot (new coil / reset / out-of-order)
            if Config.RESET_ON_NONMONOTONIC_LENGTH and last_len is not None and point.length_mm <= last_len:
                delta = last_len - point.length_mm
                if delta >= Config.NONMONO_RESET_DELTA_MM:
                    log.warning(
                        "Längen-Reset/Out-of-order: %.1f -> %.1f (Δ=%.1f). State reset.",
                        last_len,
                        point.length_mm,
                        delta,
                    )
                    sm._reset(point.length_mm)  # pragmatic reset
                last_len = point.length_mm
                continue
            last_len = point.length_mm

            if Config.HEARTBEAT_EVERY_MSGS > 0 and (msg_count % Config.HEARTBEAT_EVERY_MSGS == 0):
                log.info(
                    "Heartbeat | state=%s | L=%.1f | D=%.3f",
                    sm.current_state().name,
                    point.length_mm,
                    point.diameter_mm,
                )

            profile = sm.feed(point)
            if profile is not None:
                profile_count += 1

                # ✅ ALLE Profile senden (für Gesamtanzahl in der Visualisierung)
                publish_profile(producer, Config.profiles_topic(), profile)

                # ✅ NUR n.i.O. als Event senden (Aufgabenforderung)
                if profile.status == "n.i.O.":
                    publish_profile(producer, Config.nio_topic(), profile)
                    nio_count += 1

    finally:
        consumer.close()
        rem = producer.flush(timeout=10)
        if rem:
            log.warning("%d Nachrichten nicht zugestellt", rem)
        log.info("=== Beendet | Msg=%d Profile=%d n.i.O.=%d ===", msg_count, profile_count, nio_count)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        log.exception("Unbehandelter Fehler: %s", exc)
        sys.exit(1)