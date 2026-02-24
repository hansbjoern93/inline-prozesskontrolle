import os
import json
import time
import statistics
from collections import deque
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer


class SixSigmaMonitor:

    def __init__(self):
        bootstrap   = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
        self.input  = os.getenv("INPUT_TOPIC", "1031103_1000")
        self.output = os.getenv("ALERT_TOPIC", "1031103_801")
        self.window = int(os.getenv("WINDOW_SECONDS", "300"))

        self.consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "six-sigma-monitor", "auto.offset.reset": "latest"})
        self.producer = Producer({"bootstrap.servers": bootstrap})

        self.windows = {"fHaertetemperatur (Haerten)": deque(), "fAnlasstemperatur (Anlassen)": deque()}
        self.events  = {"fHaertetemperatur (Haerten)": None,    "fAnlasstemperatur (Anlassen)": None}

    def update_window(self, metric, ts, value):
        self.windows[metric].append((ts, value))
        while self.windows[metric] and ts - self.windows[metric][0][0] > self.window:
            self.windows[metric].popleft()

    def send(self, payload):
        self.producer.produce(self.output, json.dumps(payload).encode())
        self.producer.poll(0)
        print(f"ALARM: {payload}", flush=True)

    def check_and_alert(self, metric, ts, value):
        w = self.windows[metric]
        if len(w) < 30:
            return

        mean     = statistics.mean([v for _, v in w])
        stdev    = statistics.stdev([v for _, v in w])
        in_alarm = value > mean + 3 * stdev or value < mean - 3 * stdev
        e        = self.events[metric]

        if in_alarm:
            if e is None:
                # Sofort beim ersten Ausreißer alarmieren
                self.send({"type": "alarm_start", "timestamp": ts, "metric": metric, "value": value})
                self.events[metric] = {"start": ts, "end": ts, "peak": value, "count": 1}
            else:
                e["end"] = ts
                e["count"] += 1
                if abs(value - mean) > abs(e["peak"] - mean):
                    e["peak"] = value

        elif e is not None and ts - e["end"] >= 1.0:
            # Zusammenfassung wenn Ereignis vorbei
            self.send({"type": "alarm_summary", "event_start": e["start"], "event_end": e["end"],
                       "duration_sec": round(e["end"] - e["start"], 2),
                       "metric": metric, "peak_value": e["peak"], "alarm_count": e["count"]})
            self.events[metric] = None

    def run(self):
        self.consumer.subscribe([self.input])
        print("SixSigmaMonitor gestartet...", flush=True)

        processed  = 0
        last_print = time.time()

        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                if time.time() - last_print > 5:
                    print(f"[alive] processed={processed}", flush=True)
                    last_print = time.time()
                continue

            if msg.error():
                print(msg.error(), flush=True)
                continue

            data = json.loads(msg.value().decode())
            ts   = datetime.fromisoformat(data["Time"]).replace(tzinfo=timezone.utc).timestamp()

            hard = anneal = None
            for item in data["ProcessData"]:
                if item["Name"] == "fHaertetemperatur (Haerten)":
                    hard = item["Value"]
                elif item["Name"] == "fAnlasstemperatur (Anlassen)":
                    anneal = item["Value"]

            if hard is not None:
                self.update_window("fHaertetemperatur (Haerten)", ts, hard)
                self.check_and_alert("fHaertetemperatur (Haerten)", ts, hard)

            if anneal is not None:
                self.update_window("fAnlasstemperatur (Anlassen)", ts, anneal)
                self.check_and_alert("fAnlasstemperatur (Anlassen)", ts, anneal)

            processed += 1
            if time.time() - last_print > 5:
                print(f"[alive] processed={processed} hard={hard} anneal={anneal}", flush=True)
                last_print = time.time()


if __name__ == "__main__":
    SixSigmaMonitor().run()
