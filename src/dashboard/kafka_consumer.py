import json
import uuid
from confluent_kafka import Consumer, KafkaError
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092')
    parser.add_argument('--topic', type=str, default='1031103_1000')
    args = parser.parse_args()

    # Generate a random group ID to ensure we always read from the beginning
    random_group = f"mini-dashboard-{uuid.uuid4().hex[:8]}"

    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': random_group,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([args.topic])

    print(f"--- Mini Dashboard started (Observing {args.topic}) ---")
    print("Waiting for data... (Press Ctrl+C to stop)")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: continue
                else:
                    print(msg.error())
                    break

            # Parse and display
            data = json.loads(msg.value().decode('utf-8'))
            ts = data.get('Time', 'N/A')
            
            # Extract some interesting values
            p_data = {item['Name']: item['Value'] for item in data.get('ProcessData', [])}
            temp = p_data.get('fHaertetemperatur (Haerten)', 0)
            diam = p_data.get('fDurchmesser1 (Haerten)', 0)
            speed = p_data.get('fGeschwindigkeit1 (Drehgeber)', 0)

            print(f"[{ts}] Temp: {temp:6.1f}°C | Diam: {diam:5.2f}mm | Speed: {speed:5.1f}mm/s")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()
