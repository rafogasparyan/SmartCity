import redis
import os

class RedisService:
    def __init__(self):
        redis_url = os.getenv("REDIS_CONNECTION", "redis://localhost:6379")
        self.client = redis.Redis.from_url(redis_url)

    def set(self, key, value, expiry=3600):
        self.client.setex(key, expiry, value)

    def get(self, key):
        return self.client.get(key)
