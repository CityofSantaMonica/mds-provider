from datetime import datetime, timedelta
from fake.geometry import to_feature
import json
from shapely.geometry import Point, Polygon
import random
import string
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
        chars = string.ascii_uppercase + string.digits 
    return "".join(random.choices(chars, k=k))

def random_file_url(company):
    return "https://{}.co/{}.jpg".format(
        "-".join(company.split()), random_string(7)
    ).lower()


class CustomJsonEncoder(json.JSONEncoder):
    """
    Provides json encoding for some special types:
        - datetime -> date_format or string
        - Point/Polygon -> GeoJSON Feature
        - tuple -> list
        - UUID -> str
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a `CustomJsonEncoder` with an optional :date_format:
           - `unix` to format dates as Unix timestamps
           - `iso8601` to format dates as ISO 8601 strings
           - `<python format string>` for custom formats
        """
        if "date_format" in kwargs:
            self.date_format = kwargs["date_format"]
            del kwargs["date_format"]

        json.JSONEncoder.__init__(self, *args, **kwargs)

    def default(self, obj):
        if isinstance(obj, datetime):
            if self.date_format == "unix":
                return obj.timestamp()
            elif self.date_format == "iso8601":
                return obj.isoformat()
            elif self.date_format is not None:
                return obj.strftime(self.date_format)
            else:
                return str(obj)

        if isinstance(obj, Point) or isinstance(obj, Polygon):
            return to_feature(obj)

        if isinstance(obj, tuple):
            return list(obj)

        if isinstance(obj, uuid.UUID):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

