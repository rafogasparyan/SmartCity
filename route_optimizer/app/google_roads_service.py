import os
import requests
from app.models import SnappedPoint
from typing import List, Tuple
from tenacity import retry, stop_after_attempt, wait_fixed
import json
import random
from app.models import RawSnappedPoint

class GoogleRoadsService:
    def __init__(self):
        self.api_key = os.getenv("GOOGLE_API_KEY")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def snap_to_roads(self, coords: List[Tuple[float, float]]) -> List[SnappedPoint]:
        path = "%7C".join([f"{lat},{lng}" for lat, lng in coords])
        url = f"https://roads.googleapis.com/v1/snapToRoads?interpolate=true&path={path}&key={self.api_key}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("🛰️  Raw Google Roads API response:", json.dumps(data, indent=2))

        return [RawSnappedPoint(**pt) for pt in data.get("snappedPoints", [])]

    def get_speed_limit(self, place_id):
        url = f"https://roads.googleapis.com/v1/speedLimits?placeId={place_id}&key={self.api_key}"
        response = requests.get(url)
        if not response.ok:
            print(f"❌ Failed to get speed limit for {place_id}: {response.status_code}")
            return None
        data = response.json()
        speed_limits = data.get("speedLimits")
        if speed_limits:
            return speed_limits[0].get("speedLimit")  # in KPH
        return None

    def assign_random_speed_limit(self):
        return random.choice([30, 40, 50, 60, 80, 90, 100, 120])
