from datetime import datetime, timedelta
import geometry
import json
from shapely.geometry import Point, Polygon
import random
import uuid

def random_date_from(date,
                     min_td=timedelta(seconds=0),
                     max_td=timedelta(seconds=0)):
    """
    Produces a datetime at a random offset from :date:.
    """
    min_s = min(min_td.total_seconds(), max_td.total_seconds())
    max_s = max(min_td.total_seconds(), max_td.total_seconds())
    offset = random.uniform(min_s, max_s)
    return date + timedelta(seconds=offset)

def random_string(k, chars=None):
    """
    Create a random string of length :k: from the set of uppercase letters
    and numbers.

    Optionally use the set of characters :chars:.
    """
    if chars is None:
        chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(random.choices(chars, k=k))

class JsonEncoder(json.JSONEncoder):
    """
    Provides json encoding for some special types:
        - datetime -> Unix timestamp
        - Point/Polygon -> GeoJSON Feature
        - tuple -> list
        - UUID -> str
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.timestamp()
        if isinstance(obj, Point) or isinstance(obj, Polygon):
            return geometry.to_feature(obj)
        if isinstance(obj, tuple):
            return list(obj)
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)