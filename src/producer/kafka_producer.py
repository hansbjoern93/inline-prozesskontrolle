import json
import time
import argparse
from datetime import datetime
from confluent_kafka import Producer
import sys
import os

# --- Configuration ---
DEFAULT_TOPIC = "1031103_1000"
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:29092")
DATA_FILE = '../data/process_data.json'

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        # Debug: Print first few successes to confirm flow
        if msg.offset() < 5 or msg.offset() % 1000 == 0:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

def main():
    parser = argparse.ArgumentParser(description="Simulate Data Stream from JSON file")
    parser.add_argument('--speed', type=float, default=1.0, help="Speed factor (e.g. 1.0 = real-time, 10.0 = 10x speed, 0 = min speed)")
    parser.add_argument('--file', type=str, default=DATA_FILE, help="Path to input JSON file")
    parser.add_argument('--topic', type=str, default=DEFAULT_TOPIC, help="Kafka topic to produce to")
    parser.add_argument('--bootstrap-servers', type=str, default=BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument('--max-records', type=int, default=None, help="Stop after N records")
    args = parser.parse_args()

    # Create Producer
    conf = {'bootstrap.servers': args.bootstrap_servers}
    producer = Producer(conf)

    print(f"Starting Producer...")
    print(f"Connecting to: {args.bootstrap_servers}")
    print(f"Reading from: {args.file}")
    print(f"Topic: {args.topic}")
    print(f"Speed Factor: {args.speed}x")

    with open(args.file, 'r') as f:
        first_ts = None
        start_time_wall = time.time()
        count = 0
        
        for line in f:
            if args.max_records and count >= args.max_records:
                break
                
            try:
                record = json.loads(line)
                # Extract time for simulation pacing
                ts_str = record.get('Time')
                if not ts_str: continue
                
                # ISO format: 2026-03-01T08:00:00.100+00:00
                # Handle Z or offset if needed.
                # Assuming Python 3.7+ fromisoformat handles simple offsets
                ts_str = ts_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(ts_str)
                timestamp = dt.timestamp()
                
                if first_ts is None:
                    first_ts = timestamp
                    start_time_wall = time.time()
                
                # Calculate required delay
                if args.speed > 0:
                    target_delay = (timestamp - first_ts) / args.speed
                    elapsed_wall = time.time() - start_time_wall
                    sleep_time = target_delay - elapsed_wall
                    
                    if sleep_time > 0:
                        time.sleep(sleep_time)

                # Send message
                # TODO: Determine if we want to use 'Time' or 'Ring ID' as key
                key = record.get('Ring ID', None)
                value = json.dumps(record)
                
                producer.produce(args.topic, key=str(key) if key else None, value=value, callback=delivery_report)
                producer.flush(timeout=1.0) # Force send immediately
                
                count += 1
                if count % 1000 == 0:
                    print(f"Sent {count} messages...", end='\r')
                    
            except Exception as e:
                print(f"Error processing line: {e}")
                continue

    producer.flush()
    print(f"\nDone. Sent {count} messages.")

if __name__ == '__main__':
    main()
