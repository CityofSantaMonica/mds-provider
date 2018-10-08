"""
Generating random geometry.
"""

import math
import random
from shapely.geometry import Point
import shapely.ops


def point_within(boundary):
    """
    Create a random point somewhere within the Polygon :boundary:
    """
    # expand the bounds into the "4 corners"
    min_x, min_y, max_x, max_y = boundary.bounds

    # helper computes a new random point
    def compute():
        return Point(random.uniform(min_x, max_x),
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
    return Point(math.degrees(lon2), math.degrees(lat2))

