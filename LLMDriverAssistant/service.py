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
    return dict(
        speed=s.get("speed", 0),
        limit=s.get("SpeedLimit", 90),
        weather=s.get("weatherCondition", "Clear"),
        road_cond=s.get("road_cond", "Normal"),
        make=s.get("make", "Unknown"),
        model=s.get("model", "Unknown"),
        year=s.get("year", 1990),
        auto_braking_supported=s.get("auto_braking_supported", False),
        abs_supported=s.get("abs_supported", False),
        traction_control_supported=s.get("traction_control_supported", False)
    )

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
    prompt_vars = build_prompt(state[dev])

    query = (
        f"The vehicle is a {prompt_vars['year']} {prompt_vars['make']} {prompt_vars['model']}. "
        f"It {'supports' if prompt_vars['auto_braking_supported'] else 'does not support'} auto-braking. "
        f"It {'has' if prompt_vars['abs_supported'] else 'does not have'} ABS braking system. "
        f"It {'has' if prompt_vars['traction_control_supported'] else 'does not have'} traction control system. "
        f"The car is moving at {prompt_vars['speed']} km/h where speed limit is {prompt_vars['limit']} km/h. "
        f"Weather is {prompt_vars['weather']}, road condition is {prompt_vars['road_cond']}. "
        "Return ONLY a JSON object with keys: action, target_speed, reason. "
        "⚠️ Never suggest to speed up unless the car and conditions are perfect."
    )

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


