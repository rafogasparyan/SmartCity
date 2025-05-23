
from typing import List, Optional
from uuid import uuid4
from datetime import datetime
from pydantic import BaseModel, Field

class GpsData(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    deviceID: str = Field(..., alias="deviceId")
    timestamp: datetime
    speed: float
    direction: str
    vehicle_type: str = Field("private", alias="vehicle_type")
    location: list[float]

    class Config:
        populate_by_name = True


class Location(BaseModel):
    latitude: float
    longitude: float

class RawSnappedPoint(BaseModel):
    location: Location
    place_id: Optional[str] = Field(None, alias="placeId")
    original_index: Optional[int] = Field(None, alias="originalIndex")

    class Config:
        allow_population_by_field_name = True
        populate_by_name = True
        extra = "ignore"

# Used after assigning speed limit
class SnappedPoint(RawSnappedPoint):
    speed_limit: Optional[float] = None

