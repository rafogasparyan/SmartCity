"""
Driver-Performance Evaluator (Strict Limit)
––––––––––––––––––––––––––––––––––––––––––––
• Consumes  : driver_performance_summaries
• Produces  : driver_performance_evaluations
• LLM model : GPT-4o via LangChain
• LLM limit : 2 total requests per run (strict)
"""

import os, json, time, signal, sys, re
from confluent_kafka import Consumer, Producer, KafkaException
from langchain_openai import ChatOpenAI

# --------------------------------------------------------------------------- #
# 1 . Environment & constants
# --------------------------------------------------------------------------- #
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
GROUP_ID = os.getenv("GROUP_ID", "driver-perf-eval")
METRICS_TP = "driver_performance_metrics"
OUT_TP = "driver_performance_evaluations"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

BATCH_SIZE = 10
MAX_CALLS = 2
llm_calls = 0  # global call counter

llm = ChatOpenAI(model_name=OPENAI_MODEL, temperature=0.2)

# --------------------------------------------------------------------------- #
# 2 . Kafka setup
# --------------------------------------------------------------------------- #
consumer_cfg = {
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest",
}
producer_cfg = {"bootstrap.servers": BOOTSTRAP}

for attempt in range(10):
    try:
        consumer = Consumer(consumer_cfg)
        producer = Producer(producer_cfg)
        consumer.subscribe([METRICS_TP])
        break
    except KafkaException as e:
        print(f"Kafka not available, retrying in 3s... ({attempt + 1}/10)")
        time.sleep(3)
else:
    raise RuntimeError("❌ Kafka not available after multiple retries.")

# --------------------------------------------------------------------------- #
# 3 . Helpers
# --------------------------------------------------------------------------- #
def strip_code_fence(txt: str) -> str:
    return re.sub(r"```(json)?|```", "", txt, flags=re.IGNORECASE).strip()

def graceful_shutdown(*_):
    print("Shutting down …")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_shutdown)
signal.signal(signal.SIGTERM, graceful_shutdown)

def create_prompt_for_batch(batch):
    prompt = """You are a strict driving-style assessor. Each entry below contains JSON driving metrics. 

Rules for evaluating safety score (0–100, higher = safer):
- Penalty: 2 points × overspeed_events
- Penalty: 0.4 points × max_speed_over_limit
- If risky_weather_overspeed > 0 add 10-point penalty
- If ignored_alerts ≥ 2 add 10-point penalty
- Clamp rating to 0–100

Return ONLY valid JSON list like:
[
  {
    "index": <int>,
    "rating": <int>,
    "class": "Excellent|Good|Fair|Poor|Dangerous",
    "strengths": "<string>",
    "weaknesses": "<string>",
    "improvement_tips": ["<tip1>", "<tip2>"]
  },
  ...
]

Input metrics:\n\n"""
    for i, metrics in enumerate(batch):
        prompt += f"[{i}]:\n{json.dumps(metrics, indent=2)}\n\n"
    return prompt

# --------------------------------------------------------------------------- #
# 4 . Main loop
# --------------------------------------------------------------------------- #
print("🚀 Driver-Performance Evaluator started.")
batch_buffer = []

while True:
    if llm_calls >= MAX_CALLS:
        print("✅ Reached 2 OpenAI requests. Exiting.")
        break

    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue

    try:
        metrics = json.loads(msg.value())
        batch_buffer.append(metrics)
    except Exception as exc:
        print(f"⚠️ Bad JSON on {METRICS_TP}: {exc}")
        continue

    if len(batch_buffer) < BATCH_SIZE:
        continue

    mid = BATCH_SIZE // 2
    batches = [batch_buffer[:mid], batch_buffer[mid:]]

    for batch in batches:
        if llm_calls >= MAX_CALLS:
            print("🚫 Max LLM request limit reached.")
            break

        prompt = create_prompt_for_batch(batch)

        try:
            raw = llm.invoke(prompt).content
            llm_calls += 1
            clean = strip_code_fence(raw)
            evaluations = json.loads(clean)

            for evaluation in evaluations:
                idx = evaluation["index"]
                metric = batch[idx]

                evaluation.update(
                    deviceId=metric["deviceId"],
                    period=metric["period"],
                    ts=int(time.time() * 1000),
                )

                producer.produce(
                    OUT_TP,
                    key=metric["deviceId"],
                    value=json.dumps(evaluation).encode(),
                )
                print(f"✅ Published: {metric['deviceId']} → {evaluation['rating']}")

            producer.poll(0)

        except Exception as exc:
            print("❌ Failed to handle LLM output:", exc)
            print("Raw:", raw if "raw" in locals() else "(none)")

    batch_buffer.clear()
