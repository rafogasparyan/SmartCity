import os
import requests
from app.models import SnappedPoint
from typing import List, Tuple
from tenacity import retry, stop_after_attempt, wait_fixed
import json


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

        return [SnappedPoint(**pt) for pt in data.get("snappedPoints", [])]
