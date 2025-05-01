import json, time
from confluent_kafka import Consumer, Producer
from langchain_openai import ChatOpenAI
from langchain.chains import RetrievalQA
from vector_store import get_vectordb
from settings import KAFKA_BOOTSTRAP, GROUP_ID
import redis

topics_in = ["gps_data", "weather_data", "traffic_data", "emergency_data", "optimized_routes"]
topic_out = "driver_alerts"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest"
})
consumer.subscribe(topics_in)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)
vectordb = get_vectordb()
llm = ChatOpenAI(model_name="gpt-4o", temperature=0.2)

qa = RetrievalQA.from_chain_type(llm, retriever=vectordb.as_retriever())

state = {}


def build_prompt(s):
    parts = []

    # Vehicle identity and capabilities
    parts.append(f"The vehicle is a {s.get('year', 'unknown')} {s.get('make', 'unknown')} {s.get('model', 'unknown')}.")
    if s.get("auto_braking_supported"):
        parts.append("It supports auto-braking.")
    else:
        parts.append("It does not support auto-braking.")
    if s.get("abs_supported"):
        parts.append("It has ABS braking system.")
    else:
        parts.append("It does not have ABS braking system.")
    if s.get("traction_control_supported"):
        parts.append("It has traction control system.")
    else:
        parts.append("It does not have traction control system.")

    speed = round(s.get("speed", 0), 2)
    limit = round(s.get("SpeedLimit", 90), 2)
    parts.append(f"The car is moving at {speed} km/h where the speed limit is {limit} km/h.")

    weather = s.get("weatherCondition", "Clear sky")
    road = s.get("road_cond", "Normal")
    parts.append(f"Weather is {weather}, and road condition is {road}.")

    # Enhanced decision-making guidance
    parts.append("Evaluate whether the vehicle should slow down, maintain speed, or speed up.")
    parts.append(
        "Only suggest to speed up if all conditions are excellent: clear weather, dry road, and vehicle has all modern safety features.")
    parts.append(
        "If current speed is significantly below the limit and conditions are good, modestly increasing speed may be safe.")
    parts.append(
        "If speed is too close to the limit or the conditions are risky (e.g., fog, snow, wet roads), suggest slowing down.")
    parts.append(
        "Return ONLY a JSON object with keys: action (slow_down / maintain_speed / speed_up), target_speed (int), and reason (string).")

    return {"query": " ".join(parts)}


while True:
    msg = consumer.poll(0.2)
    if not msg or msg.error():
        continue

    try:
        data = json.loads(msg.value())
    except Exception as e:
        print(f"⚠️ Bad JSON message: {msg.value()}")
        continue

    dev = data.get("deviceId") or data.get("deviceID")
    if not dev:
        print(f"⚠️ No deviceId in message: {data}")
        continue

    state.setdefault(dev, {}).update(data)
    print(f"📥 Received data for device {dev}: {state[dev]}")

    if time.time() - state[dev].get("_ts", 0) < 2:
        continue

    state[dev]["_ts"] = time.time()

    query = build_prompt(state[dev])["query"]

    answer = qa.invoke({"query": query})["result"]
    print(f"💬 LLM recommendation: {answer}")
    
    
    clean = (
        answer.strip()
        .replace("```json", "")
        .replace("```", "")
        .replace("json\n", "")
        .replace("json", "")
        .strip()
    )

    

    try:
        parsed = json.loads(clean)
        alert = {"deviceId": dev, **parsed, "ts": int(time.time()*1000)}
        
        key = f"alerts:{dev}"
        redis_client.lpush(key, json.dumps(alert))
        redis_client.ltrim(key, 0, 4)
        
        recent_alerts = redis_client.lrange(key, 1, 5)  # skip the one we just pushed at index 0
        duplicate = False
    
        for prev_raw in recent_alerts:
            prev = json.loads(prev_raw)
            if prev["action"] == alert["action"] and abs(prev["target_speed"] - alert["target_speed"]) < 2:
                print(f"⚠️ Skipping duplicate advice for {dev}")
                duplicate = True
                break
    
        if not duplicate:
            producer.produce(topic_out, key=dev, value=json.dumps(alert).encode())
            producer.poll(0)
            print(f"📤 Sent alert for {dev} to topic '{topic_out}'")
    
    except Exception as e:
        print(f"❌ Failed to parse LLM output as JSON: {e}")
        print(" Raw cleaned LLM output:")
        print(clean)


