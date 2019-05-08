"""
Encoding MDS Provider data into useful formats.
"""

from datetime import datetime
import json
from shapely.geometry import Point, Polygon
from uuid import UUID

from .geometry import to_feature
from .versions import UnsupportedVersionError, Version


class MdsJsonEncoder(json.JSONEncoder):
    """
    Version-aware JSON encoder for MDS, handling additional datatypes:

    * datetime to date_format or str
    * Point/Polygon to GeoJSON Feature dict
    * tuple to list
    * UUID to str
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize a new MdsJsonEncoder.

        Parameters:
            date_format: str
                Configure how dates are formatted using one of:
                * unix: format dates as a numeric offset from Unix epoch (default, Version-aware)
                * iso8601: format dates as ISO 8601 strings
                * python format string: custom format

            version: str, Version, optional
                The MDS version to target.
        """
        self.date_format = kwargs.pop("date_format", "unix")
        self.version = Version(kwargs.pop("version", Version.mds_lower()))
        json.JSONEncoder.__init__(self, *args, **kwargs)

    def default(self, obj):
        """
        Implement serialization for some special types.
        """
        if isinstance(obj, datetime):
            if self.date_format == "unix":
                if not self.version.supported:
                    raise UnsupportedVersionError(self.version)
                elif self.version < Version("0.3.0"):
                    return obj.timestamp()
                else:
                    return int(round(obj.timestamp() * 1000))
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
