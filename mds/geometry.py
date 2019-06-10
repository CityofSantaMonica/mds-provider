"""
Helpers for GeoJSON-based geometry objects.
"""

import json
import os

import fiona
import requests
import shapely.geometry
import shapely.ops


def parse_boundary(boundary_file, **kwargs):
    """
    Read boundary geometry from a local or remote geojson file.

    Parameters:
        boundary_file: str
            A path to a local file or URL to a remote file with boundary data in geojson format.

        output: str, optional
            If boundary_file is a URL, download and save the file to this directory.

    Return:
        shapely.geometry.Polygon
            A single unioned Polygon representing the (composite) boundary.
    """
    if boundary_file.lower().startswith("http") and boundary_file.lower().endswith(".geojson"):
        r = requests.get(boundary_file)
        file_name = boundary_file.split("/")[-1]
        path = os.path.join(kwargs.get("output", "."), file_name)

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
    Extract coordinates from a GeoJSON feature.

    Parameters:
        feature: dict-like
            GeoJSON structure with coordinate geometry.

    Return:
        shapely.geometry.Point
            The Point representation of the coordinate geometry.
    """
    coords = feature["geometry"]["coordinates"]
    return shapely.geometry.Point(coords[0], coords[1])


def to_feature(shape, properties={}):
    """
    Create a GeoJSON Feature object from geometry.

    Parameters:
        shape: shapely.geometry.Shape
            The geometry defining this Feature.

        properties: dict, optional
            Entries for the Feature's properties collection.

    Return:
        dict
            The GeoJSON Feature object.
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
