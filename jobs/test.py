from confluent_kafka import Producer
import json
import time

p = Producer({'bootstrap.servers': 'localhost:9092'})  # or broker:29092 if inside container

sample = {
    "deviceId": "Vehicle_001",
    "timestamp": time.time(),
    "overspeed_events": 2,
    "smooth_brakes": 4
}

p.produce("driver_performance_metrics", key=sample["deviceId"], value=json.dumps(sample).encode("utf-8"))
p.flush()
print("✅ Test message sent.")
