import json, time
from confluent_kafka import Consumer, Producer
from langchain_openai import ChatOpenAI
from langchain.chains import RetrievalQA
from vector_store import get_vectordb
from settings import KAFKA_BOOTSTRAP, GROUP_ID, MODEL, TEMPERATURE


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
llm = ChatOpenAI(model_name = MODEL, temperature = float(TEMPERATURE))
qa = RetrievalQA.from_chain_type(llm, retriever = vectordb.as_retriever())

state = {}

def build_prompt(s):
    return dict(
        speed = s.get("speed", 0),
        limit = s.get("SpeedLimit", 90),
        weather = s.get("weatherCondition", "Clear"),
        road_cond = s.get("road_cond", "Normal"),
    )



while True:
    msg = consumer.poll(0.2)
    if not msg:
        continue

    value = msg.value()
    if not value:
        print("⚠️ Received empty Kafka message")
        continue

    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        print(f"⚠️ Bad JSON message: {value}")
        continue

    dev = data.get("deviceId")
    if not dev:
        print(f"⚠️ No deviceId in message: {data}")
        continue

    state.setdefault(dev, {}).update(data)

    if time.time() - state[dev].get("_ts", 0) < 1:
        continue
    state[dev]["_ts"] = time.time()

    print(f"📥 Received data for device {dev}: {state[dev]}")

    prompt_vars = build_prompt(state[dev])
    query = (
      f"The car is moving at {prompt_vars['speed']} km/h where speed limit is {prompt_vars['limit']} km/h, "
      f"weather is {prompt_vars['weather']}, road condition is {prompt_vars['road_cond']}. "
      "Provide ONE short, specific recommendation (max 20 words)."
    )

    answer = qa({"query": query})["result"]
    print(f"💬 LLM recommendation: {answer}")

    alert = {
        "deviceId": dev,
        "recommendation": answer,
        "ts": int(time.time() * 1000)
    }

    producer.produce(topic_out, key=dev, value=json.dumps(alert).encode())
    producer.poll(0)
    print(f"📤 Sent alert for {dev} to topic '{topic_out}'")
