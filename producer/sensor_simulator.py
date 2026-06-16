"""
Simulates multi-device IoT sensor telemetry and publishes to Kafka.

Each device emits readings (temperature, vibration, pressure) on its own
key (device_id) so Kafka partitioning preserves per-device ordering.
Occasionally injects anomalous spikes to exercise downstream detection.

Usage:
    python sensor_simulator.py --devices 20 --rate 5 --anomaly-prob 0.01
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Producer

TOPIC = "sensor-readings"


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        "linger.ms": 20,
        "batch.num.messages": 500,
        "compression.type": "snappy",
    })


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for {msg.key()}: {err}")


def generate_reading(device_id: str, baseline: dict, anomaly_prob: float) -> dict:
    is_anomaly = random.random() < anomaly_prob
    multiplier = np.random.uniform(2.5, 4.0) if is_anomaly else 1.0

    temperature = np.random.normal(baseline["temp_mean"], baseline["temp_std"]) * (
        multiplier if is_anomaly else 1.0
    )
    vibration = np.random.normal(baseline["vib_mean"], baseline["vib_std"]) * (
        multiplier if is_anomaly else 1.0
    )
    pressure = np.random.normal(baseline["pressure_mean"], baseline["pressure_std"])

    return {
        "device_id": device_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(float(temperature), 2),
        "vibration_mm_s": round(float(vibration), 3),
        "pressure_psi": round(float(pressure), 2),
        "injected_anomaly": is_anomaly,  # ground truth, for offline eval only
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--devices", type=int, default=20)
    parser.add_argument("--rate", type=float, default=5.0, help="messages/sec per device")
    parser.add_argument("--anomaly-prob", type=float, default=0.01)
    args = parser.parse_args()

    producer = build_producer(args.bootstrap_servers)

    device_ids = [f"device-{i:03d}" for i in range(args.devices)]
    baselines = {
        d: {
            "temp_mean": random.uniform(40, 60),
            "temp_std": random.uniform(1, 3),
            "vib_mean": random.uniform(0.5, 2.0),
            "vib_std": random.uniform(0.1, 0.4),
            "pressure_mean": random.uniform(80, 120),
            "pressure_std": random.uniform(2, 5),
        }
        for d in device_ids
    }

    interval = 1.0 / args.rate
    print(f"[producer] streaming {len(device_ids)} devices @ {args.rate}/s each -> topic '{TOPIC}'")

    try:
        while True:
            for device_id in device_ids:
                reading = generate_reading(device_id, baselines[device_id], args.anomaly_prob)
                producer.produce(
                    TOPIC,
                    key=device_id.encode("utf-8"),
                    value=json.dumps(reading).encode("utf-8"),
                    callback=delivery_report,
                )
            producer.poll(0)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("[producer] stopping...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
