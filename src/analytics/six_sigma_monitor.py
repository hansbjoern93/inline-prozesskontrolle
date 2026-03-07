import os
import json
import statistics
from collections import deque
from datetime import datetime, timezone

import redis
from confluent_kafka import Consumer, Producer


MESSGROESSEN = {
    "fHaertetemperatur (Haerten)": "haerten",
    "fAnlasstemperatur (Anlassen)": "anlassen",
}


def parse_float_sicher(x):
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


def parse_zeitstempel_iso(zeit_str: str) -> float:
    if zeit_str.endswith("Z"):
        zeit_str = zeit_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(zeit_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


class SechsSigmaMonitor:
    def __init__(self):
        kafka_broker = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic_eingang = os.getenv("INPUT_TOPIC", "1031103_1000")
        self.topic_alarm = os.getenv("ALERT_TOPIC", "1031103_801")
        self.fenster_sekunden = int(os.getenv("WINDOW_SECONDS", "300"))
        self.min_punkte = int(os.getenv("MIN_POINTS", "30"))
        self.offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

        self.consumer = Consumer(
            {
                "bootstrap.servers": kafka_broker,
                "group.id": os.getenv("KAFKA_GROUP_ID", "six-sigma-monitor"),
                "auto.offset.reset": self.offset_reset,
                "enable.auto.commit": True,
            }
        )
        self.producer = Producer({"bootstrap.servers": kafka_broker})

        # Messgröße -> deque[(zeitstempel, wert)]
        self.fenster = {m: deque() for m in MESSGROESSEN}

        # Messgröße -> laufendes Alarm-Ereignis oder None
        self.ereignisse = {m: None for m in MESSGROESSEN}

        self.redis_aktiv = os.getenv("ENABLE_REDIS_STATE", "false").lower() in (
            "1", "true", "yes", "y", "on"
        )
        self.redis = None
        self.redis_stream_prefix = os.getenv("REDIS_SIXSIGMA_STREAM_PREFIX", "sixsigma:window")
        self.redis_status_stream = os.getenv("REDIS_SIXSIGMA_STATE_STREAM", "sixsigma:state")
        self.redis_maxlen_fenster = int(
            os.getenv("REDIS_MAXLEN_WINDOW", str(max(1000, self.fenster_sekunden * 12)))
        )
        self.redis_maxlen_status = int(os.getenv("REDIS_MAXLEN_STATE", "1000"))

        if self.redis_aktiv:
            self.redis = self.verbinde_redis()
            if self.redis:
                self.lade_status_aus_redis()

    def verbinde_redis(self):
        try:
            r = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=int(os.getenv("REDIS_DB", "0")),
                decode_responses=True,
            )
            r.ping()
            return r
        except Exception:
            return None

    def redis_fenster_key(self, messgroesse: str) -> str:
        return f"{self.redis_stream_prefix}:{MESSGROESSEN[messgroesse]}"

    def lade_status_aus_redis(self):
        for messgroesse in MESSGROESSEN:
            key = self.redis_fenster_key(messgroesse)
            try:
                eintraege = self.redis.xrevrange(key, count=self.redis_maxlen_fenster)
            except Exception:
                eintraege = []

            punkte = []
            for _id, felder in reversed(eintraege):
                zeitstempel = parse_float_sicher(felder.get("ts"))
                wert = parse_float_sicher(felder.get("v"))
                if zeitstempel is not None and wert is not None:
                    punkte.append((zeitstempel, wert))

            if punkte:
                grenze = punkte[-1][0] - self.fenster_sekunden
                punkte = [(t, v) for (t, v) in punkte if t >= grenze]

            self.fenster[messgroesse] = deque(punkte)

        try:
            letzter_status = self.redis.xrevrange(self.redis_status_stream, count=1)
        except Exception:
            letzter_status = []

        if letzter_status:
            felder = letzter_status[0][1]
            try:
                ereignisse = json.loads(felder.get("events_json", "{}"))
            except Exception:
                ereignisse = {}

            for messgroesse in MESSGROESSEN:
                self.ereignisse[messgroesse] = ereignisse.get(messgroesse)

    def speichere_punkt_in_redis(self, messgroesse: str, zeitstempel: float, wert: float):
        if not self.redis:
            return
        try:
            self.redis.xadd(
                self.redis_fenster_key(messgroesse),
                {"ts": str(zeitstempel), "v": str(wert)},
                maxlen=self.redis_maxlen_fenster,
                approximate=True,
            )
        except Exception:
            pass

    def speichere_ereignisse_in_redis(self):
        if not self.redis:
            return
        try:
            self.redis.xadd(
                self.redis_status_stream,
                {"events_json": json.dumps(self.ereignisse, separators=(",", ":"))},
                maxlen=self.redis_maxlen_status,
                approximate=True,
            )
        except Exception:
            pass

    def aktualisiere_fenster(self, messgroesse: str, zeitstempel: float, wert: float):
        q = self.fenster[messgroesse]
        q.append((zeitstempel, wert))

        while q and (zeitstempel - q[0][0]) > self.fenster_sekunden:
            q.popleft()

        self.speichere_punkt_in_redis(messgroesse, zeitstempel, wert)

    def sende_alarm(self, payload: dict):
        self.producer.produce(
            self.topic_alarm,
            json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )
        self.producer.poll(0)

    def pruefe_alarm(self, messgroesse: str, zeitstempel: float, wert: float):
        q = self.fenster[messgroesse]

        if len(q) < self.min_punkte:
            return

        werte = [x for _, x in q]
        mittelwert = statistics.mean(werte)
        standardabweichung = statistics.stdev(werte)

        if standardabweichung == 0:
            return

        alarm = (
            wert > mittelwert + 3 * standardabweichung
            or wert < mittelwert - 3 * standardabweichung
        )

        ereignis = self.ereignisse[messgroesse]

        if alarm:
            if ereignis is None:
                self.sende_alarm(
                    {
                        "type": "alarm_start",
                        "timestamp": zeitstempel,
                        "metric": messgroesse,
                        "value": wert,
                    }
                )
                self.ereignisse[messgroesse] = {
                    "start": zeitstempel,
                    "end": zeitstempel,
                    "peak": wert,
                    "count": 1,
                }
                self.speichere_ereignisse_in_redis()
            else:
                ereignis["end"] = zeitstempel
                ereignis["count"] += 1
                if abs(wert - mittelwert) > abs(ereignis["peak"] - mittelwert):
                    ereignis["peak"] = wert
        else:
            if ereignis is not None and (zeitstempel - ereignis["end"]) >= 1.0:
                self.sende_alarm(
                    {
                        "type": "alarm_summary",
                        "event_start": ereignis["start"],
                        "event_end": ereignis["end"],
                        "duration_sec": round(ereignis["end"] - ereignis["start"], 2),
                        "metric": messgroesse,
                        "peak_value": ereignis["peak"],
                        "alarm_count": ereignis["count"],
                    }
                )
                self.ereignisse[messgroesse] = None
                self.speichere_ereignisse_in_redis()

    def run(self):
        self.consumer.subscribe([self.topic_eingang])

        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            try:
                daten = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue

            zeit_str = daten.get("Time")
            if not zeit_str:
                continue

            zeitstempel = parse_zeitstempel_iso(zeit_str)

            haertetemperatur = None
            anlasstemperatur = None

            for eintrag in daten.get("ProcessData", []):
                name = eintrag.get("Name")
                if name == "fHaertetemperatur (Haerten)":
                    haertetemperatur = parse_float_sicher(eintrag.get("Value"))
                elif name == "fAnlasstemperatur (Anlassen)":
                    anlasstemperatur = parse_float_sicher(eintrag.get("Value"))

            if haertetemperatur is not None:
                messgroesse = "fHaertetemperatur (Haerten)"
                self.aktualisiere_fenster(messgroesse, zeitstempel, haertetemperatur)
                self.pruefe_alarm(messgroesse, zeitstempel, haertetemperatur)

            if anlasstemperatur is not None:
                messgroesse = "fAnlasstemperatur (Anlassen)"
                self.aktualisiere_fenster(messgroesse, zeitstempel, anlasstemperatur)
                self.pruefe_alarm(messgroesse, zeitstempel, anlasstemperatur)


if __name__ == "__main__":
    SechsSigmaMonitor().run()