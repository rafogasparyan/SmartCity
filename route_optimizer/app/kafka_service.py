import os
import json
import asyncio
from confluent_kafka import Consumer, Producer
from app.models import GpsData
from app.redis_service import RedisService
from app.route_processor import RouteProcessor

async def main():
    print("🔍 kafka_service.py is being executed...")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    redis = RedisService()

    consumer = Consumer({
        "bootstrap.servers": kafka_bootstrap,
        "group.id": "route-optimizer-group",
        "auto.offset.reset": "earliest"
    })
    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    processor = RouteProcessor(producer, redis)

    consumer.subscribe(["gps_data"])

    print("🚀 Listening for GPS data...")
    try:
        while True:
            print("-" * 45)
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(0.5)
                continue
            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                continue

            try:
                gps_dict = json.loads(msg.value().decode("utf-8"))
                gps_data = GpsData(**gps_dict)
                print(f"📍 Received location: {gps_data.location[0]}, {gps_data.location[1]}")
                await processor.handle_gps_data(gps_data)
                consumer.commit()
            except Exception as e:
                print(f"❌ Error processing message: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    import asyncio

    try:
        print("🌀 Bootstrapping Kafka route optimizer...")
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Failed to start: {type(e).__name__} - {e}")
