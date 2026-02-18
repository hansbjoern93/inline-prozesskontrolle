import os, json, time
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Consumer, Producer


SPEED_NAME = "fGeschwindigkeit1 (Drehgeber)"
TEMP_NAME  = "fHaertetemperatur (Haerten)"


def parse_ts(ts_str: str) -> float:
    # "2026-03-01T08:45:55.400"  (ohne TZ) -> behandeln wir als UTC
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def get_pd_value(record: dict, wanted_name: str):
    for item in record.get("ProcessData", []):
        if item.get("Name") == wanted_name:
            return item.get("Value")
    return None


def main():
    bootstrap = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("INPUT_TOPIC", "1031103_1000")
    output_topic = os.getenv("OUTPUT_TOPIC", "1031103_1000_corr")
    window_size = float(os.getenv("WINDOW_SIZE_SEC", "300"))
    min_points = int(os.getenv("MIN_POINTS", "1000"))

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "corr-analyzer",
        "auto.offset.reset": "latest",   # wichtig: nicht 15 Mio alte Nachrichten lesen
        "enable.auto.commit": True
    })
    producer = Producer({"bootstrap.servers": bootstrap})
    consumer.subscribe([input_topic])

    window_start = None
    speeds, temps = [], []

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        try:
            record = json.loads(msg.value().decode("utf-8"))
            ts_str = record.get("Time")
            if not ts_str:
                continue

            ts = parse_ts(ts_str)
            sp = get_pd_value(record, SPEED_NAME)
            tp = get_pd_value(record, TEMP_NAME)
            if sp is None or tp is None:
                continue

            if window_start is None:
                window_start = ts

            speeds.append(float(sp))
            temps.append(float(tp))

            if (ts - window_start) >= window_size:
                if len(speeds) >= min_points:
                    corr = float(np.corrcoef(speeds, temps)[0, 1])
                    out = {
                        "window_start": window_start,
                        "window_end": ts,
                        "n": len(speeds),
                        "corr_pearson": corr
                    }
                    producer.produce(output_topic, json.dumps(out).encode("utf-8"))
                    producer.flush(2)

                window_start = None
                speeds.clear()
                temps.clear()

        except Exception:
            continue


if __name__ == "__main__":
    main()
