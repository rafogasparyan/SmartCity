import json
from app.models import GpsData
from app.google_roads_service import GoogleRoadsService
from app.redis_service import RedisService
from confluent_kafka import Producer


class RouteProcessor:
    def __init__(self, producer: Producer, redis_service: RedisService):
        self.buffer = []
        self.redis = redis_service
        self.producer = producer
        self.roads_service = GoogleRoadsService()

    async def handle_gps_data(self, gps_data: GpsData):
        if not gps_data.location or len(gps_data.location) != 2:
            print(f"⚠️ Invalid GPS data: {gps_data}")
            return

        self.buffer.append((gps_data.location[0], gps_data.location[1]))

        if len(self.buffer) >= 3:
            snapped_points = self.roads_service.snap_to_roads(self.buffer)
            self.buffer.clear()

            if snapped_points:
                key = f"route:{gps_data.location[0]}:{gps_data.location[1]}"
                self.redis.set(key, json.dumps(snapped_points[0].dict()))
                await self.process_snapped_point(gps_data, snapped_points[0])
            else:
                print("⚠️ No snapped points returned from Google Roads API.")

    async def process_snapped_point(self, gps_data: GpsData, snapped_point):
        output = {
            "deviceID": gps_data.deviceID,
            "placeId": snapped_point.placeId,
            "timestamp": gps_data.timestamp.isoformat()
        }

        try:
            self.producer.produce(
                "optimized_routes",
                key=gps_data.deviceID,
                value=json.dumps(output)
            )
            self.producer.flush()
            print(f"✅ Produced optimized route for device: {gps_data.deviceID}")
        except Exception as e:
            print(f"❌ Failed to produce to Kafka: {e}")
