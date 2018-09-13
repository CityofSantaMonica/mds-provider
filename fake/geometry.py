import fiona
import math
import random
import requests
import shapely.geometry
import shapely.ops

__default_boundary_file = "boundary.geojson"

def make_boundary(boundary_file):
    """
    Read boundary data from :boundary_file: into a shapely.geometry.Polygon
    """
    if boundary_file.startswith("http") and boundary_file.endswith(".geojson"):
        print("downloading boundary_file from: ", boundary_file)
        r = requests.get(boundary_file)
        with open(__default_boundary_file, "w") as f:
            f.write(r.text)
        boundary_file = __default_boundary_file
    # meld all the features together into a unified polygon
    features = fiona.open(boundary_file)
    polygons = [shapely.geometry.shape(feature["geometry"]) for feature in features]
    polygons_meld = shapely.ops.cascaded_union(polygons)
    return shapely.geometry.Polygon(polygons_meld)

def random_point_within(boundary):
    """
    Create a random point somewhere within :boundary:
    """
    # expand the bounds into the "4 corners"
    min_x, min_y, max_x, max_y = boundary.bounds
    # helper computes a new random point
    def compute():
        return shapely.geometry.Point(random.uniform(min_x, max_x),
                                      random.uniform(min_y, max_y))
    point = compute()
    # loop until we get an interior point
    while not boundary.contains(point):
        point = compute()
    return point

def nearby_point(point, dist, bearing=None):
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
