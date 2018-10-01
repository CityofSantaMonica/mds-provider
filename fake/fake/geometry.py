import fiona
import geopandas
import json
import math
import os
import random
import requests
import shapely.geometry
import shapely.ops


__default_boundary_file = "boundary.geojson"


def parse_boundary(boundary_file, downloads=None):
    """
    Read boundary data from :boundary_file: into a shapely.geometry.Polygon

    If :boundary_file: is a URL, download and save to the directory :downloads:.
    """
    if boundary_file.startswith("http") and boundary_file.endswith(".geojson"):
        r = requests.get(boundary_file)
        path = __default_boundary_file if downloads is None else \
               os.path.join(downloads, __default_boundary_file)
        with open(path, "w") as f:
            json.dump(r.json(), f)
        boundary_file = path

    # meld all the features together into a unified polygon
    features = fiona.open(boundary_file)
    polygons = [shapely.geometry.shape(feature["geometry"]) for feature in features]
    polygons_meld = shapely.ops.cascaded_union(polygons)

    return shapely.geometry.Polygon(polygons_meld)

def point_within(boundary):
    """
    Create a random point somewhere within :boundary:
    """
    # expand the bounds into the "4 corners"
    min_x, min_y, max_x, max_y = boundary.bounds

    # helper computes a new random point
    def compute():
        return shapely.geometry.Point(random.uniform(min_x, max_x),
                                      random.uniform(min_y, max_y))

    # loop until we get an interior point
    point = compute()
    while not boundary.contains(point):
        point = compute()

    return point

def point_nearby(point, dist, bearing=None):
    """
    Create a random point :dist: meters from :point:

    Uses the Haversine formula to compute a new lat/lon given a distance and
    bearing. Uses the provided bearing, or random if None.

    See: http://www.movable-type.co.uk/scripts/latlong.html#destPoint
    """
    lat1 = math.radians(point.y)
    lon1 = math.radians(point.x)
    ang_dist = dist / 6378100 # radius of Earth in meters
    bearing = random.uniform(0, 2*math.pi) if bearing is None else bearing

    # calc the new latitude
    lat2 = math.asin(math.sin(lat1) * math.cos(ang_dist) + 
                     math.cos(lat1) * math.sin(ang_dist) * math.cos(bearing))

    # calc the new longitude
    lon2 = lon1 + math.atan2(math.sin(bearing) * math.sin(ang_dist) * math.cos(lat1),
                             math.cos(ang_dist) - math.sin(lat1) * math.sin(lat2))

    # return the new point
    return shapely.geometry.Point(math.degrees(lon2), math.degrees(lat2))

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
    if isinstance(shape, shapely.geometry.Point) and "bbox" in feature:
        del feature["bbox"]

    return dict(feature)

def to_feature_collection(shape):
    """
    Create a GeoJSON FeatureCollection object for the given shapely.geometry :shape:.
    """
    collection = geopandas.GeoSeries([shape]).__geo_interface__
    return dict(collection)

def extract_point(feature):
    """
    Extract the coordinates from the given :feature: as a shapely.geometry.Point
    """
    coords = feature["geometry"]["coordinates"]
    return shapely.geometry.Point(coords[0], coords[1])

