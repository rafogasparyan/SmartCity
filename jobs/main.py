import os
import time
import traceback
import uuid
import random

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
from weather_service import WeatherService
import random

weather_service = WeatherService()

LONDON_COORDINATES = {
    "latitude": 51.5074,
    "longitude": -0.1278
}
BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904
}


DANGEROUS_WEATHERS = [
    ("Clear sky", 22, "Normal"),
    ("Fog", 5, "Low-visibility"),
    ("Heavy Rain", 12, "Wet"),
    ("Snow", -1, "Snow-covered"),
    ("Ice", -3, "Icy")
]
# Calculate the movement increment
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["latitude"] - LONDON_COORDINATES["latitude"]) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES["longitude"] - LONDON_COORDINATES["longitude"]) / 100

# Environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

random.seed(42)
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time


def generate_weather_data(device_id, timestamp, location):
    # Fetch real time weather data from Open-Mateo
    weather_data = weather_service.get_weather(location["latitude"], location["longitude"])

    if weather_data:
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "location": location,
            "timestamp": timestamp,
            "temperature": weather_data["temperature"],
            "weatherCondition": weather_data["weatherCondition"],
            # "precipitation": weather_data["precipitation"],
            "windSpeed": weather_data["windSpeed"],
            # "humidity": weather_data["humidity"],
            # "airQualityIndex": weather_data["airQualityIndex"]
        }
    else:
        return {
            "id": uuid.uuid4(),
            "deviceId": device_id,
            "location": location,
            "timestamp": timestamp,
            "temperature": random.uniform(-5, 26),
            "weatherCondition": random.choice(["Sunny", "Cloudy", "Rain", "Snow"]),
            "precipitation": random.uniform(0, 25),
            "windSpeed": random.uniform(0, 100),
            "humidity": random.randint(0, 100),  # percentage
            "airQualityIndex": random.uniform(0, 500)  # AQL Value goes here
        }


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "incidentId": uuid.uuid4(),
        "type": random.choice(["Accident", "Fire", "Police", "None"]),
        "timestamp": timestamp,
        "location": location,
        "status": random.choice(["Active", "Resolved"]),
        "description": "Description of the incident"
    }


def generate_gps_data(device_id, timestamp, location, vehicle_type="private"):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "timestamp": timestamp,
        "speed": random.uniform(100, 140),
        "direction": "North-East",
        "vehicle_type": vehicle_type,
        "location": [location["latitude"], location["longitude"]]
    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "deviceId": device_id,
        "cameraID": camera_id,
        "location": location,
        "timestamp": timestamp,
        "snapshot": "Base64EncodedString"
    }


def simulate_vehicle_movement():
    global start_location

    # move towards birmingham
    start_location["latitude"] += LATITUDE_INCREMENT
    start_location["longitude"] += LONGITUDE_INCREMENT

    # add some randomness
    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longitude"] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()

    # Randomize car capabilities (for realism)
    car_capabilities = random.choice([
        {"make": "Tesla", "model": "Model 3", "year": 2023, "auto_braking_supported": True, "abs_supported": True, "traction_control_supported": True},
        {"make": "Lada", "model": "2107", "year": 1987, "auto_braking_supported": False, "abs_supported": False, "traction_control_supported": False},
        {"make": "Toyota", "model": "Corolla", "year": 2015, "auto_braking_supported": False, "abs_supported": True, "traction_control_supported": True},
        {"make": "BMW", "model": "X5", "year": 2022, "auto_braking_supported": True, "abs_supported": True, "traction_control_supported": True},
    ])

    return {
        "id": str(uuid.uuid4()),
        "deviceId": device_id,
        "timestamp": get_next_time().isoformat(),
        "location": {"latitude": location["latitude"], "longitude": location["longitude"]},
        "speed": random.uniform(10, 40),
        "direction": "North-East",
        "make": car_capabilities["make"],
        "model": car_capabilities["model"],
        "year": car_capabilities["year"],
        "fuelType": "Hybrid",  # you can randomize this too later if you want
        "auto_braking_supported": car_capabilities["auto_braking_supported"],
        "abs_supported": car_capabilities["abs_supported"],
        "traction_control_supported": car_capabilities["traction_control_supported"]
    }



def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serialaizable")


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data["id"]),
        value=json.dumps(data, default=json_serializer).encode("utf-8"),
        on_delivery=delivery_report
    )

    producer.flush()



def simulate_journey_original(producer, device_id):
    a = 0
    while a < 5:
        a = a + 1
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data["timestamp"],
                                                           vehicle_data["location"], "Camera123")
        weather_data = generate_weather_data(device_id, vehicle_data["timestamp"], vehicle_data["location"])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data["timestamp"],
                                                                   vehicle_data["location"])


        if (vehicle_data["location"]["latitude"] >= BIRMINGHAM_COORDINATES['latitude'] and
            vehicle_data["location"]["longitude"] <= BIRMINGHAM_COORDINATES["longitude"]):
            print(f"vehicle_data['location']['latitude']: {vehicle_data['location']['latitude']}")
            print("Vehicle has reached Birmingham. Simulation ending...")
            break


        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        time.sleep(1)


from datetime import datetime, timedelta

from datetime import datetime, timedelta

def simulate_journey(producer, device_id):
    scenarios = [
        {
            "desc": "🚗 Lada in fog at high speed",
            "vehicle": {
                "make": "Lada", "model": "2107", "year": 1987,
                "auto_braking_supported": False, "abs_supported": False, "traction_control_supported": False,
                "speed": 85  # overspeed
            },
            "weather": {"weatherCondition": "Fog", "temperature": 5, "windSpeed": 10},
            "incident": {"type": "None", "status": "Resolved"},
            "speed_limit": 60
        },
        {
            "desc": "🚙 BMW on wet road with accident ahead",
            "vehicle": {
                "make": "BMW", "model": "X5", "year": 2022,
                "auto_braking_supported": True, "abs_supported": True, "traction_control_supported": True,
                "speed": 100  # overspeed
            },
            "weather": {"weatherCondition": "Rain", "temperature": 12, "windSpeed": 20},
            "incident": {"type": "Accident", "status": "Active"},
            "speed_limit": 70
        }
    ]

    for case in scenarios:
        print(f"\n>>> Sending scenario: {case['desc']}")
        location = simulate_vehicle_movement()

        # ⏪ Send timestamps 40 seconds in the past
        base_time = datetime.utcnow() - timedelta(seconds=40)

        # VEHICLE
        vehicle_data = {
            "id": str(uuid.uuid4()),
            "deviceId": device_id,
            "timestamp": base_time.isoformat(),
            "location": location,
            "speed": case["vehicle"]["speed"],
            "direction": "North-East",
            "fuelType": "Hybrid",
            **case["vehicle"]
        }
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)

        # GPS — simulate burst within same old window
        for i in range(6):
            gps_time = (base_time + timedelta(seconds=i)).isoformat()
            gps_data = {
                "id": str(uuid.uuid4()),
                "deviceId": device_id,
                "timestamp": gps_time,
                "speed": case["vehicle"]["speed"],
                "direction": "North-East",
                "vehicle_type": "private",
                "location": [location["latitude"], location["longitude"]],
                "SpeedLimit": case["speed_limit"]
            }
            produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
            time.sleep(0.2)

        # WEATHER
        weather_data = {
            "id": str(uuid.uuid4()),
            "deviceId": device_id,
            "timestamp": base_time.isoformat(),
            "location": location,
            **case["weather"]
        }
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)

        # EMERGENCY
        emergency_data = {
            "id": str(uuid.uuid4()),
            "deviceId": device_id,
            "timestamp": base_time.isoformat(),
            "location": location,
            "incidentId": str(uuid.uuid4()),
            **case["incident"],
            "description": "Test scenario incident"
        }
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        time.sleep(1)

    print("✅ Journey simulation complete. Now waiting 30s for metrics to finalize...")

    # ✅ Inject a late GPS + SpeedLimit record to trigger watermark-based window finalization
    late_time = datetime.utcnow() + timedelta(seconds=30)
    late_timestamp = late_time.isoformat()

    # Use the last known location from previous scenario
    late_gps = {
        "id": str(uuid.uuid4()),
        "deviceId": device_id,
        "timestamp": late_timestamp,
        "speed": 50,
        "direction": "North-East",
        "vehicle_type": "private",
        "location": [location["latitude"], location["longitude"]],
        "SpeedLimit": 60
    }
    produce_data_to_kafka(producer, GPS_TOPIC, late_gps)

    late_speed = {
        "id": str(uuid.uuid4()),
        "deviceId": device_id,
        "timestamp": late_timestamp,
        "SpeedLimit": 60
    }
    produce_data_to_kafka(producer, "optimized_routes", late_speed)

    print("⏳ Late record sent to push watermark and close window...")
    time.sleep(20)  # Give Spark time




if __name__ == "__main__":

    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 5,
        "compression.type": "snappy",
        "error_cb": lambda err: print(f"Kafka error: {err}")
    }

    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, "Vehicle_CodeWithYu-123")

    except KeyboardInterrupt:
        print("Simulation ended by the user")
    except Exception as e:
        traceback.print_exc()
        print(f"Unexpected Error occurred: {e}")
