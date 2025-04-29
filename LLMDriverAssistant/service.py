import json, time
from confluent_kafka import Consumer, Producer
from langchain_openai import ChatOpenAI
from langchain.chains import RetrievalQA
from vector_store import get_vectordb
from settings import KAFKA_BOOTSTRAP, GROUP_ID

topics_in = ["gps_data", "weather_data", "traffic_data", "emergency_data", "optimized_routes"]
topic_out = "driver_alerts"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest"
})
consumer.subscribe(topics_in)

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

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
        producer.produce(topic_out, key=dev, value=json.dumps(alert).encode())
        producer.poll(0)
        print(f"📤 Sent alert for {dev} to topic '{topic_out}'")
    except Exception as e:
        print(f"❌ Failed to parse LLM output as JSON: {e}")
        print(" Raw cleaned LLM output:")
        print(clean)

