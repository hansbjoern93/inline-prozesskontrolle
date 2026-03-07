import os
import json
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Consumer, Producer


FELD_GESCHWINDIGKEIT = "fGeschwindigkeit1 (Drehgeber)"
FELD_HAERTETEMPERATUR = "fHaertetemperatur (Haerten)"


def parse_zeitstempel(zeit_str: str) -> float:
    # Zeitstempel ohne Zeitzone werden als UTC behandelt
    dt = datetime.fromisoformat(zeit_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def hole_prozesswert(datensatz: dict, feldname: str):
    for eintrag in datensatz.get("ProcessData", []):
        if eintrag.get("Name") == feldname:
            return eintrag.get("Value")
    return None


def baue_kafka_consumer(kafka_broker: str, topic_eingang: str):
    consumer = Consumer(
        {
            "bootstrap.servers": kafka_broker,
            "group.id": "korrelations_analyse",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic_eingang])
    return consumer


def baue_kafka_producer(kafka_broker: str):
    return Producer({"bootstrap.servers": kafka_broker})


def main():
    kafka_broker = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
    topic_eingang = os.getenv("INPUT_TOPIC", "1031103_1000")
    topic_ausgang = os.getenv("OUTPUT_TOPIC", "1031103_1000_corr")
    fenster_sekunden = float(os.getenv("WINDOW_SIZE_SEC", "300"))
    min_punkte = int(os.getenv("MIN_POINTS", "1000"))

    consumer = baue_kafka_consumer(kafka_broker, topic_eingang)
    producer = baue_kafka_producer(kafka_broker)

    fenster_start = None
    geschwindigkeiten = []
    haertetemperaturen = []

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        try:
            datensatz = json.loads(msg.value().decode("utf-8"))
            zeit_str = datensatz.get("Time")
            if not zeit_str:
                continue

            zeitstempel = parse_zeitstempel(zeit_str)

            geschwindigkeit = hole_prozesswert(datensatz, FELD_GESCHWINDIGKEIT)
            haertetemperatur = hole_prozesswert(datensatz, FELD_HAERTETEMPERATUR)

            if geschwindigkeit is None or haertetemperatur is None:
                continue

            if fenster_start is None:
                fenster_start = zeitstempel

            geschwindigkeiten.append(float(geschwindigkeit))
            haertetemperaturen.append(float(haertetemperatur))

            if (zeitstempel - fenster_start) >= fenster_sekunden:
                if len(geschwindigkeiten) >= min_punkte:
                    korrelation = float(np.corrcoef(geschwindigkeiten, haertetemperaturen)[0, 1])

                    ergebnis = {
                    "fenster_start": fenster_start,
                    "fenster_ende": zeitstempel,
                   "anzahl_punkte": len(geschwindigkeiten),
                    "pearson_korrelation": korrelation,
                    }
                    producer.produce(topic_ausgang, json.dumps(ergebnis).encode("utf-8"))
                    producer.flush(2)

                fenster_start = None
                geschwindigkeiten.clear()
                haertetemperaturen.clear()

        except Exception:
            continue


if __name__ == "__main__":
    main() 