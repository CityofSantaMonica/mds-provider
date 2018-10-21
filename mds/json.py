"""
Work with MDS Provider data as (Geo)JSON files and objects.
"""

from datetime import datetime
import fiona
import json
import os
import pandas as pd
from pathlib import Path
import requests
import shapely.geometry
import shapely.ops
from uuid import UUID


def parse_boundary(boundary_file="boundary.geojson", downloads=None):
    """
    Read boundary data from :boundary_file: into a shapely.geometry.Polygon

    If :boundary_file: is a URL, download and save to the directory :downloads:.
    """
    if boundary_file.startswith("http") and boundary_file.endswith(".geojson"):
        r = requests.get(boundary_file)
        file_name = boundary_file.split("/")[-1]
        path = file_name if downloads is None else os.path.join(downloads, file_name)

        with open(path, "w") as f:
            json.dump(r.json(), f)

        boundary_file = path

    # meld all the features together into a unified polygon
    features = fiona.open(boundary_file)
    polygons = [shapely.geometry.shape(feature["geometry"]) for feature in features]
    polygons_meld = shapely.ops.cascaded_union(polygons)

    return shapely.geometry.Polygon(polygons_meld)

def extract_point(feature):
    """
    Extract the coordinates from the given GeoJSON :feature: as a shapely.geometry.Point
    """
    coords = feature["geometry"]["coordinates"]
    return shapely.geometry.Point(coords[0], coords[1])

def to_feature(shape, properties={}):
    """
    Create a GeoJSON Feature object for the given shapely.geometry :shape:.

    Optionally give the Feature a :properties: dict.
    """
    feature = shapely.geometry.mapping(shape)
    feature["properties"] = properties
    feature["geometry"] = {}

    if isinstance(shape, shapely.geometry.Point):
        feature["geometry"]["coordinates"] = list(feature["coordinates"])
    else:
        # assume shape is polygon (multipolygon will break)
        feature["geometry"]["coordinates"] = [list(list(coords) for coords in part) for part in feature["coordinates"]]

    # 'type' at the top level is Feature, and in 'geometry' should be set
    feature["geometry"]["type"] = feature["type"]
    feature["type"] = "Feature"
    # 'coordinates' in the top level is not valid geoJSON.
    feature.pop("coordinates")
    return feature

def read_data_file(src, record_type):
    """
    Read data from the :src: MDS Provider JSON file, where :record_type: is one of
        - status_changes
        - trips

    Return a tuple:
        - the version string
        - a DataFrame of the record collection
    """
    if isinstance(src, Path):
        payload = json.load(src.open("r"))
    else:
        payload = json.load(open(src, "r"))

    if isinstance(payload, list):
        data = []
        version = payload[0]["version"]

        for page in payload:
            if page["version"] == version:
                data.extend(page["data"][record_type])
            else:
                raise ValueError("Version mismatch detected.")
    else:
        data = payload["data"][record_type]
        version = payload["version"]

    return version, pd.DataFrame.from_records(data)


class CustomJsonEncoder(json.JSONEncoder):
    """
    Provides json encoding for some special types:

        - datetime to date_format or str
        - Point/Polygon to GeoJSON Feature
        - tuple to list
        - UUID to str
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a new `CustomJsonEncoder`.

        Optional keyword arguments:

        :date_format: Configure how dates are formatted using one of:

            - unix: format dates as integer milliseconds since Unix epoch (MDS default)
            - iso8601: format dates as ISO 8601 strings
            - python format string: custom format
        """
        self.date_format = kwargs.pop("date_format", "unix")

        json.JSONEncoder.__init__(self, *args, **kwargs)

    def default(self, obj):
        if isinstance(obj, datetime):
            if self.date_format == "unix":
                return int(round(obj.timestamp() * 1000))
            elif self.date_format == "iso8601":
                return obj.isoformat()
            elif self.date_format is not None:
                return obj.strftime(self.date_format)
            else:
                return str(obj)

        if isinstance(obj, shapely.geometry.Point) or isinstance(obj, shapely.geometry.Polygon):
            return to_feature(obj)

        if isinstance(obj, tuple):
            return list(obj)

        if isinstance(obj, UUID):
            return str(obj)

        return json.JSONEncoder.default(self, obj)
