import os
import json
import math
import signal
import logging
from collections import deque
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

import redis
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


# -----------------------------
# Kafka / Topics / Fields
# -----------------------------
KAFKA_BROKER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_EINGANG  = os.getenv("KAFKA_INPUT_TOPIC", "1031103_1000")
CONSUMER_GROUP = os.getenv("KAFKA_GROUP_ID", "wire_profile_detector")
OFFSET_RESET   = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

FELD_LAENGE_MM      = os.getenv("FIELD_LENGTH", "fDrahtlaenge1 (Drehgeber)")
FELD_DURCHMESSER_MM = os.getenv("FIELD_DIAMETER", "fDurchmesser1 (Haerten)")

TOPIC_PROFILE_ALLE   = f"{TOPIC_EINGANG}_profiles"
TOPIC_PROFILE_EVENTS = f"{TOPIC_EINGANG}_profile_events"

OUT_PARTITIONS  = int(os.getenv("KAFKA_OUT_PARTITIONS", "1"))
OUT_REPLICATION = int(os.getenv("KAFKA_OUT_REPLICATION", "1"))

MAX_ABWEICHUNG_MM = float(os.getenv("NOK_DEVIATION_MM", "30.0"))

RESET_BEI_NICHT_MONOTONER_LAENGE = os.getenv("RESET_ON_NONMONOTONIC_LENGTH", "true").lower() in ("1", "true", "yes", "y", "on")
RESET_DELTA_MM                   = float(os.getenv("NONMONO_RESET_DELTA_MM", "1000.0"))


# -----------------------------
# Smoothing / Trend / Confirmation
# -----------------------------
GLAETTUNG_N = int(os.getenv("SMOOTH_N", "3"))
TREND_N     = int(os.getenv("TREND_N", "10"))

FLACH_STEIGUNG = float(os.getenv("FLAT_SLOPE", "0.003"))
ANZAHL_BESTAETIGUNGEN = int(os.getenv("CONFIRM_POINTS", "3"))

VERZOEGERUNG_KORREKTUR_PUNKTE = int(os.getenv("LAG_CORRECTION_POINTS", "5"))


# -----------------------------
# Sigma thresholds
# -----------------------------
SIGMA_K       = float(os.getenv("SIGMA_K", "3.0"))
SIGMA_MINPTS  = int(os.getenv("SIGMA_MINPTS", "20"))
SIGMA_MIN_STD = float(os.getenv("SIGMA_MIN_STD", "0.05"))

MINDESTLAENGE_MM = {
    "SPACE":   float(os.getenv("MIN_SPACE_MM", "200.0")),
    "RISE":    float(os.getenv("MIN_RISE_MM", "100.0")),
    "PLATEAU": float(os.getenv("MIN_PLATEAU_MM", "500.0")),
    "FALL":    float(os.getenv("MIN_FALL_MM", "100.0")),
}


# -----------------------------
# Redis (optional restart-state)
# -----------------------------
REDIS_AKTIV = os.getenv("ENABLE_REDIS_STATE", "false").lower() in ("1", "true", "yes", "y", "on")
REDIS_HOST  = os.getenv("REDIS_HOST", "redis")
REDIS_PORT  = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB    = int(os.getenv("REDIS_DB", "0"))

REDIS_STREAM_STATUS = os.getenv("REDIS_PROFILE_STATE_STREAM", "profiles:state")
REDIS_MAXLEN_STATUS = int(os.getenv("REDIS_MAXLEN_STATE", "2000"))


# -----------------------------
# Logging
# -----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] profile_detector — %(message)s",
)
log = logging.getLogger("profile_detector")


# -----------------------------
# Helpers
# -----------------------------
class HysteresisGate:
    """Gibt True zurück, wenn eine Bedingung N-mal hintereinander True war."""
    def __init__(self, required: int) -> None:
        self._required = max(1, int(required))
        self._count = 0

    def feed(self, condition: bool) -> bool:
        self._count = (self._count + 1) if condition else 0
        return self._count >= self._required

    def reset(self) -> None:
        self._count = 0


class ReferenceStats:
    """Laufender Mittelwert für L1..L4 (wird bei jedem Profil aktualisiert: i.O. und n.i.O.)."""
    def __init__(self) -> None:
        self.count: int = 0
        self.mean: Dict[str, float] = {k: 0.0 for k in ("L1", "L2", "L3", "L4")}

    def snapshot(self) -> Dict[str, float]:
        return dict(self.mean)

    def update(self, lengths: Dict[str, float]) -> None:
        self.count += 1
        for k in ("L1", "L2", "L3", "L4"):
            x = float(lengths.get(k, 0.0))
            self.mean[k] = self.mean[k] + (x - self.mean[k]) / self.count


def parse_float_sicher(x) -> Optional[float]:
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


def parse_messpunkt(raw: bytes) -> Optional[Tuple[float, float]]:
    try:
        data = json.loads(raw)
    except Exception:
        return None

    laenge = None
    durchmesser = None
    for item in data.get("ProcessData", []):
        name = item.get("Name")
        if name == FELD_LAENGE_MM:
            laenge = parse_float_sicher(item.get("Value"))
        elif name == FELD_DURCHMESSER_MM:
            durchmesser = parse_float_sicher(item.get("Value"))

    if laenge is None or durchmesser is None:
        return None
    return float(laenge), float(durchmesser)


def verbinde_redis():
    if not REDIS_AKTIV:
        return None
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        log.info("Redis ON | %s:%s db=%s", REDIS_HOST, REDIS_PORT, REDIS_DB)
        return r
    except Exception as e:
        log.warning("Redis OFF (connect failed): %s", e)
        return None


class OnlineStats:
    """Online-Berechnung von Mittelwert und Standardabweichung (Welford-Verfahren)."""
    def __init__(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def reset(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def update(self, x: float) -> None:
        self.n += 1
        d = x - self.mean
        self.mean += d / self.n
        self.m2 += d * (x - self.mean)

    def std(self) -> float:
        if self.n < 2:
            return max(SIGMA_MIN_STD, 1e-12)
        s = math.sqrt(self.m2 / (self.n - 1))
        return s if s >= SIGMA_MIN_STD else SIGMA_MIN_STD

    def upper(self) -> float:
        return self.mean + SIGMA_K * self.std()

    def lower(self) -> float:
        return self.mean - SIGMA_K * self.std()

    def ready(self) -> bool:
        return self.n >= SIGMA_MINPTS


@dataclass
class Profile:
    profile_id: int
    L1: float
    L2: float
    L3: float
    L4: float
    status: str = "i.O."
    reason: str = ""

    def to_dict(self) -> Dict:
        d = {
            "profile_id": self.profile_id,
            "L1": round(self.L1, 1),
            "L2": round(self.L2, 1),
            "L3": round(self.L3, 1),
            "L4": round(self.L4, 1),
            "status": self.status,
        }
        if self.reason:
            d["reason"] = self.reason
        return d


# -----------------------------
# Profile Detector
# -----------------------------
class DrahtProfilDetektor:
    """
    Zustandsautomat für die Profilerkennung:
    WAIT -> SPACE -> RISE -> PLATEAU -> FALL -> Profil fertig
    """

    def __init__(self, redis_client=None) -> None:
        self.redis = redis_client

        self.zustand = "WAIT"
        self.profil_nummer = 0

        self.segment_startlaenge = 0.0
        self.zustand_startlaenge = 0.0
        self.segment_laengen: Dict[str, float] = {}

        self.glaettung = deque(maxlen=GLAETTUNG_N)
        self.trend_punkte = deque(maxlen=TREND_N)
        self.laengen_historie = deque(maxlen=VERZOEGERUNG_KORREKTUR_PUNKTE + ANZAHL_BESTAETIGUNGEN + 20)

        self.gate_flat = HysteresisGate(ANZAHL_BESTAETIGUNGEN)
        self.gate_signal = HysteresisGate(ANZAHL_BESTAETIGUNGEN)

        self.stats = OnlineStats()
        self.ref = ReferenceStats()

        self._restore_from_redis()

    # ---- Redis state ----
    def _restore_from_redis(self) -> None:
        if self.redis is None:
            return
        try:
            last = self.redis.xrevrange(REDIS_STREAM_STATUS, count=1)
            if not last:
                return
            _, fields = last[0]
            payload = fields.get("json", "")
            if not payload:
                return
            state = json.loads(payload)

            self.profil_nummer = int(state.get("profil_nummer", 0))
            self.zustand = str(state.get("zustand", "WAIT"))
            self.segment_startlaenge = float(state.get("segment_startlaenge", 0.0))
            self.zustand_startlaenge = float(state.get("zustand_startlaenge", 0.0))
            self.segment_laengen = dict(state.get("segment_laengen", {}))

            ref_count = int(state.get("referenz_anzahl", 0))
            ref_mean = state.get("referenz_mean", {})
            if ref_count > 0 and isinstance(ref_mean, dict):
                self.ref.count = ref_count
                for k in ("L1", "L2", "L3", "L4"):
                    self.ref.mean[k] = float(ref_mean.get(k, 0.0))

            log.info(
                "Redis state restored | profile_id=%s | state=%s | ref_count=%s",
                self.profil_nummer, self.zustand, self.ref.count
            )
        except Exception as e:
            log.warning("Redis state restore failed: %s", e)

    def speichere_status_in_redis(self) -> None:
        if self.redis is None:
            return
        try:
            state = {
                "profil_nummer": self.profil_nummer,
                "zustand": self.zustand,
                "segment_startlaenge": self.segment_startlaenge,
                "zustand_startlaenge": self.zustand_startlaenge,
                "segment_laengen": self.segment_laengen,
                "referenz_anzahl": self.ref.count,
                "referenz_mean": self.ref.snapshot(),
            }
            self.redis.xadd(
                REDIS_STREAM_STATUS,
                {"json": json.dumps(state, separators=(",", ":"))},
                maxlen=REDIS_MAXLEN_STATUS,
                approximate=True,
            )
        except Exception as e:
            log.warning("Redis state save failed: %s", e)

    # ---- Detection helpers ----
    def trend_steigung(self) -> float:
        if len(self.trend_punkte) < 2:
            return 0.0
        (x1, y1) = self.trend_punkte[0]
        (x2, y2) = self.trend_punkte[-1]
        dx = x2 - x1
        if abs(dx) < 1e-9:
            return 0.0
        return (y2 - y1) / dx

    def mindestlaenge_erreicht(self, laenge_mm: float) -> bool:
        req = MINDESTLAENGE_MM.get(self.zustand, 0.0)
        return (laenge_mm - self.zustand_startlaenge) >= req

    def korrigierte_laenge(self, laenge_mm: float) -> float:
        # Wir gehen ein paar Punkte zurück, weil Glättung + Bestätigung etwas verzögert reagieren
        idx = VERZOEGERUNG_KORREKTUR_PUNKTE + ANZAHL_BESTAETIGUNGEN
        if len(self.laengen_historie) > idx:
            return self.laengen_historie[-(idx + 1)]
        return laenge_mm

    def zustand_wechseln(self, neuer_zustand: str, laenge_mm: float) -> None:
        self.zustand = neuer_zustand
        self.zustand_startlaenge = laenge_mm

        # Beim Zustandswechsel alles zurücksetzen, damit Grenzen/Trend nur für den neuen Abschnitt gelten
        self.stats.reset()
        self.trend_punkte.clear()
        self.laengen_historie.clear()

        self.gate_flat.reset()
        self.gate_signal.reset()

    def segment_abschliessen(self, segment_name: str, endlaenge_mm: float) -> None:
        self.segment_laengen[segment_name] = max(0.0, endlaenge_mm - self.segment_startlaenge)
        self.segment_startlaenge = endlaenge_mm

    def reset_for_next_profile(self, start_mm: float) -> None:
        self.segment_laengen.clear()
        self.segment_startlaenge = start_mm
        self.zustand_wechseln("SPACE", start_mm)

    # ---- Main step ----
    def verarbeite_punkt(self, laenge_mm: float, durchmesser_mm: float) -> Optional[Profile]:
        # Durchmesser glätten (kurzer Mittelwert), damit Rauschen weniger stört
        self.glaettung.append(durchmesser_mm)
        d_glatt = sum(self.glaettung) / len(self.glaettung)

        # Punkte für Trend/Steigung und eine kleine Längen-Historie merken
        self.trend_punkte.append((laenge_mm, d_glatt))
        self.laengen_historie.append(laenge_mm)

        ist_flach = abs(self.trend_steigung()) < FLACH_STEIGUNG

        # Grenzen (Mittelwert ± k*Std) im aktuellen Abschnitt berechnen
        # (Stats werden bei Zustandswechsel zurückgesetzt)
        self.stats.update(d_glatt)
        sigma_ready = self.stats.ready()
        ueber_oben = sigma_ready and d_glatt > self.stats.upper()
        unter_unten = sigma_ready and d_glatt < self.stats.lower()

        # Start: erster Punkt -> initialisieren und in SPACE wechseln
        if self.zustand == "WAIT":
            self.segment_startlaenge = laenge_mm
            self.zustand_wechseln("SPACE", laenge_mm)
            return None

        # Bedingungen bestätigen: erst nach mehreren Punkten in Folge umschalten
        if self.zustand in ("RISE", "FALL"):
            flat_ok = self.gate_flat.feed(ist_flach)
        else:
            self.gate_flat.reset()
            flat_ok = False

        if self.zustand == "SPACE":
            sig_ok = self.gate_signal.feed(ueber_oben)
        elif self.zustand == "PLATEAU":
            sig_ok = self.gate_signal.feed(unter_unten)
        else:
            self.gate_signal.reset()
            sig_ok = False

        # Übergang SPACE -> RISE (Anstieg beginnt)
        if self.zustand == "SPACE" and sig_ok and self.mindestlaenge_erreicht(laenge_mm):
            self.segment_abschliessen("L2", laenge_mm)
            self.zustand_wechseln("RISE", laenge_mm)
            return None

        # Übergang RISE -> PLATEAU (Korrektur, weil Bestätigung/Glättung verzögert)
        if self.zustand == "RISE" and flat_ok and self.mindestlaenge_erreicht(laenge_mm):
            tp = self.korrigierte_laenge(laenge_mm)
            self.segment_abschliessen("L3", tp)
            self.zustand_wechseln("PLATEAU", laenge_mm)
            self.segment_startlaenge = tp
            return None

        # Übergang PLATEAU -> FALL (Abfall beginnt)
        if self.zustand == "PLATEAU" and sig_ok and self.mindestlaenge_erreicht(laenge_mm):
            self.segment_abschliessen("L1", laenge_mm)
            self.zustand_wechseln("FALL", laenge_mm)
            return None

        # Übergang FALL -> Profil fertig (Korrektur wie oben)
        if self.zustand == "FALL" and flat_ok and self.mindestlaenge_erreicht(laenge_mm):
            tp = self.korrigierte_laenge(laenge_mm)
            self.segment_abschliessen("L4", tp)

            if all(k in self.segment_laengen for k in ("L1", "L2", "L3", "L4")):
                prof = self.profil_abschliessen_und_bewerten()
                # Nächstes Profil startet am Ende des Falls (tp)
                self.reset_for_next_profile(tp)
                self.speichere_status_in_redis()
                return prof

            # Falls unvollständig: Neustart bei SPACE
            self.reset_for_next_profile(tp)
            self.speichere_status_in_redis()
            return None

        return None

    def profil_abschliessen_und_bewerten(self) -> Profile:
        self.profil_nummer += 1

        lengths = {
            "L1": float(self.segment_laengen.pop("L1", 0.0)),
            "L2": float(self.segment_laengen.pop("L2", 0.0)),
            "L3": float(self.segment_laengen.pop("L3", 0.0)),
            "L4": float(self.segment_laengen.pop("L4", 0.0)),
        }

        prof = Profile(
            profile_id=self.profil_nummer,
            L1=lengths["L1"],
            L2=lengths["L2"],
            L3=lengths["L3"],
            L4=lengths["L4"],
            status="i.O.",
            reason="",
        )

        if self.ref.count == 0:
            prof.status = "i.O."
            prof.reason = "erstes Profil initialisiert Referenz"
        else:
            mw = self.ref.snapshot()
            reasons = []
            for k in ("L1", "L2", "L3", "L4"):
                dev = abs(lengths[k] - mw[k])
                if dev > MAX_ABWEICHUNG_MM:
                    reasons.append(
                        f"{k} Abw. {dev:.1f}mm > {MAX_ABWEICHUNG_MM:.1f}mm "
                        f"(ist={lengths[k]:.1f} ref={mw[k]:.1f})"
                    )
            if reasons:
                prof.status = "n.i.O."
                prof.reason = "; ".join(reasons)

        # Referenz wird mit jedem Profil aktualisiert (auch n.i.O.)
        self.ref.update(lengths)

        log.info(
            "Profil #%d | L1=%.1f L2=%.1f L3=%.1f L4=%.1f | %s",
            prof.profile_id, prof.L1, prof.L2, prof.L3, prof.L4, prof.status
        )
        if prof.status == "n.i.O." and prof.reason:
            log.warning("n.i.O. #%d: %s", prof.profile_id, prof.reason)

        return prof


# -----------------------------
# Kafka helpers
# -----------------------------
def ensure_topic_exists(topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    try:
        md = admin.list_topics(timeout=10)
        if topic in md.topics and md.topics[topic].error is None:
            log.info("Topic exists: %s", topic)
            return
    except Exception as e:
        log.warning("Could not list topics (continuing): %s", e)

    log.info("Creating topic: %s", topic)
    new_topic = NewTopic(topic, num_partitions=OUT_PARTITIONS, replication_factor=OUT_REPLICATION)
    futures = admin.create_topics([new_topic])

    try:
        futures[topic].result(timeout=15)
        log.info("Topic created: %s", topic)
    except Exception as e:
        # Meist ok: Topic existiert evtl. schon / gleichzeitiges Erstellen
        log.warning("Topic create skipped/failed (often OK if already exists): %s", e)


def baue_kafka_consumer() -> Consumer:
    c = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": OFFSET_RESET,
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC_EINGANG])
    log.info("Kafka Consumer | in=%s | broker=%s | group=%s | offset=%s",
             TOPIC_EINGANG, KAFKA_BROKER, CONSUMER_GROUP, OFFSET_RESET)
    return c


def delivery_report(err, msg):
    if err is not None:
        try:
            t = msg.topic()
        except Exception:
            t = "?"
        log.error("Kafka delivery failed | topic=%s | err=%s", t, err)


def baue_kafka_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",
    }
    p = Producer(conf)
    log.info("Kafka Producer | broker=%s", KAFKA_BROKER)
    return p


def sende_kafka_json(producer: Producer, topic: str, key: str, payload: dict):
    producer.produce(
        topic,
        key=str(key),
        value=json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


# -----------------------------
# Main
# -----------------------------
_running = True
def _handle_sig(sig, frame):
    global _running
    _running = False

signal.signal(signal.SIGINT, _handle_sig)
signal.signal(signal.SIGTERM, _handle_sig)


def main():
    log.info("Start | in=%s | broker=%s | group=%s | redis=%s",
             TOPIC_EINGANG, KAFKA_BROKER, CONSUMER_GROUP, "ON" if REDIS_AKTIV else "OFF")

    ensure_topic_exists(TOPIC_PROFILE_ALLE)
    ensure_topic_exists(TOPIC_PROFILE_EVENTS)

    r = verbinde_redis()
    det = DrahtProfilDetektor(redis_client=r)

    consumer = baue_kafka_consumer()
    producer = baue_kafka_producer()

    letzte_laenge: Optional[float] = None

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # Bei kurzfristigen Kafka-Fehlern nicht abbrechen
                log.warning("Kafka error: %s", msg.error())
                continue

            parsed = parse_messpunkt(msg.value())
            if parsed is None:
                continue

            laenge_mm, durchmesser_mm = parsed

            # Nur bei echtem Rücksprung (Länge wird kleiner) resetten; gleiche Länge ist ok
            if RESET_BEI_NICHT_MONOTONER_LAENGE and letzte_laenge is not None and laenge_mm < letzte_laenge:
                if (letzte_laenge - laenge_mm) >= RESET_DELTA_MM:
                    log.warning("Non-monotonic length jump -> reset | last=%.1f now=%.1f", letzte_laenge, laenge_mm)
                    det = DrahtProfilDetektor(redis_client=r)
                letzte_laenge = laenge_mm
                continue

            letzte_laenge = laenge_mm

            prof = det.verarbeite_punkt(laenge_mm, durchmesser_mm)
            if prof is None:
                continue

            payload = prof.to_dict()

            sende_kafka_json(producer, TOPIC_PROFILE_ALLE, key=prof.profile_id, payload=payload)

            if prof.status == "n.i.O.":
                event = {
                    "event": "profile_nok",
                    **payload,
                }
                sende_kafka_json(producer, TOPIC_PROFILE_EVENTS, key=prof.profile_id, payload=event)

    except KeyboardInterrupt:
        pass
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            producer.flush(10)
        except Exception:
            pass
        log.info("Stopped.")


if __name__ == "__main__":
    main()