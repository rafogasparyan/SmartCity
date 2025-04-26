import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_DIR = os.getenv("VECTOR_DIR", "/app/vectors")
GROUP_ID = "driver-assistant"
MODEL = os.getenv("MODEL", "gpt-4o-mini")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0.2"))
