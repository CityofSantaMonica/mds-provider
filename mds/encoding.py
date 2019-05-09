"""
Encoding MDS Provider data.
"""

import json
from datetime import datetime
from uuid import UUID

from shapely.geometry import Point, Polygon

from mds import geometry
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

    def __repr__(self):
        return f"<mds.encoding.MdsJsonEncoder ('{self.version}', '{self.date_format}')>"

    def default(self, obj):
        """
        Implement serialization for some special types.
        """
        if isinstance(obj, datetime):
            if self.date_format == "unix":
                if self.version.unsupported:
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
            return geometry.to_feature(obj)

        if isinstance(obj, tuple):
            return list(obj)

        if isinstance(obj, UUID):
            return str(obj)

        return json.JSONEncoder.default(self, obj)
