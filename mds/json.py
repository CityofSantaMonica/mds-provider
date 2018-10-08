"""
Work with MDS Provider data as (Geo)JSON files and objects.
"""

import fiona
from datetime import datetime
import geopandas
import json
import os
import pandas as pd
import requests
import shapely.geometry
from shapely.geometry import Point, Polygon
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
    return Point(coords[0], coords[1])

def to_feature(shape, properties={}):
    """
    Create a GeoJSON Feature object for the given shapely.geometry :shape:.

    Optionally give the Feature a :properties: dict.
    """
    collection = to_feature_collection(shape)
    feature = collection["features"][0]
    feature["properties"] = properties

    # remove some unnecessary and redundant data
    if "id" in feature:
        del feature["id"]
    if isinstance(shape, Point) and "bbox" in feature:
        del feature["bbox"]

    return dict(feature)

def to_feature_collection(shape):
    """
    Create a GeoJSON FeatureCollection object for the given shapely.geometry :shape:.
    """
    collection = geopandas.GeoSeries([shape]).__geo_interface__
    return dict(collection)

def read_data_file(src, record_type):
    """
    Read data from the :src: MDS Provider JSON file, where :record_type: is one of
        - status_changes
        - trips

    Return a tuple:
        - the version string
        - a DataFrame of the record collection
    """
    with open(src, "r") as f:
        payload = json.load(f)
        data = payload["data"][record_type]
        return payload["version"], pd.DataFrame.from_records(data)


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

        if isinstance(obj, UUID):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

