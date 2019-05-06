"""
Generating random geometry.
"""

import math
import random
from shapely.geometry import Point
import shapely.ops


def point_within(boundary):
    """
    Create a random point somewhere within the boundary.

    Parameters:
        boundary: shapely.geometry.Polygon
            The geometry of the boundary.

    Returns:
        shapely.geometry.Point
            A point inside the boundary.
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


def point_nearby(point, dist, bearing=None, boundary=None):
    """
    Create a random point nearby another point.

    Uses the Haversine formula to compute a new lat/lon given a distance and bearing.  
    See: http://www.movable-type.co.uk/scripts/latlong.html#destPoint

    Parameters:
        point: shapely.geometry.Point
            The reference point from which to generate a new point.

        dist: numeric
            A distance in meters away from the reference point to generate the new point.

        bearing: numeric, optional
            The bearing in radians away from the reference point to generate the new point. By default, random.

        boundary: shapely.geometry.Polygon, optional
            The returned point should lie within this boundary if possible.

            If it proves difficult to find a point at the specified distance within the boundary,
            the returned point may lie less than dist meters from point.

    Returns:
        shapely.geometry.Point
            The newly calculated point.
    """
    if boundary is None:
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
    else:
        MAX_TRIES = 50 if bearing is None else 1

        for _ in range(MAX_TRIES):
            end_point = point_nearby(point, dist, bearing)
            if boundary.contains(end_point):
                return end_point

        # If we got here it's possible there was no point at that exact distance and bearing
        # from our starting point within the boundary; or maybe we were just unlucky.
        # Shrink the distance to the endpoint until we find one inside the boundary.
        if not boundary.contains(point):
            raise ValueError(f"Cannot find point nearby the starting point {point}, which is outside the given boundary.")

        while not boundary.contains(end_point):
            dist = dist * 0.9
            end_point = point_nearby(point, dist, bearing)

        return end_point
