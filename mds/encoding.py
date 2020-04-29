"""
Encoding and decoding MDS Provider data.
"""

import json
import datetime
import pathlib
import uuid

import dateutil.parser
import shapely.geometry

import mds.geometry
from .versions import Version


class JsonEncoder(json.JSONEncoder):
    """
    Version-aware encoder for MDS json types:

    * datetime to date_format or str
    * Path to str
    * Point/Polygon to GeoJSON Feature dict
    * tuple to list
    * UUID to str
    * Version to str
    """

    def __init__(self, *args, **kwargs):
        """
        Parameters:
            date_format: str
                Configure how dates are formatted using one of:
                * unix: format dates as a numeric offset from Unix epoch (default, Version-aware)
                * iso8601: format dates as ISO 8601 strings
                * python timespec: format dates as ISO 8601 strings to the specified component (hours, minutes, seconds)
                * python format string: custom format

            version: str, Version, optional
                The MDS version to target.
        """
        self.version = Version(kwargs.pop("version", Version.mds_lower()))
        self.version.raise_if_unsupported()

        self.date_format = kwargs.pop("date_format", "unix")
        self.timestamp_encoder = TimestampEncoder(date_format=self.date_format, version=self.version)

        json.JSONEncoder.__init__(self, *args, **kwargs)

    def __repr__(self):
        return f"<mds.encoding.JsonEncoder ('{self.version}', '{self.date_format}')>"

    def default(self, obj):
        """
        Implement serialization for some special types.
        """
        if isinstance(obj, datetime.datetime):
            return self.timestamp_encoder.encode(obj)

        if isinstance(obj, pathlib.Path):
            return str(obj)

        if isinstance(obj, shapely.geometry.Point) or isinstance(obj, shapely.geometry.Polygon):
            return mds.geometry.to_feature(obj)

        if isinstance(obj, tuple):
            return list(obj)

        if isinstance(obj, uuid.UUID):
            return str(obj)

        if isinstance(obj, Version):
            return str(obj)

        return json.JSONEncoder.default(self, obj)


class TimestampEncoder():
    """
    Version-aware encoder for MDS timestamps.
    """

    def __init__(self, **kwargs):
        """
        Parameters:
            date_format: str, optional
                Configure how dates are formatted using one of:
                * unix: format dates as a numeric offset from Unix epoch (default, Version-aware)
                * iso8601: format dates as ISO 8601 strings
                * python timespec: format dates as ISO 8601 strings to the specified component (hours, minutes, seconds)
                * python format string: custom format

            version: str, Version, optional
                The MDS version to target.
        """
        self.date_format = kwargs.get("date_format", "unix")

        self.version = Version(kwargs.get("version", Version.mds_lower()))
        self.version.raise_if_unsupported()

    def __repr__(self):
        return f"<mds.encoding.TimestampEncoder ('{self.version}', '{self.date_format}')>"

    def encode(self, data):
        """
        Encode MDS timestamps for transport.

        Parameters:
            data: datetime
                Datetime to encode.

        Return:
            str
        """
        date_format = "auto" if self.date_format == "iso8601" else self.date_format

        if date_format == "unix":
            return str(int(round(data.timestamp() * 1000)))
        try:
            return data.isoformat(timespec=date_format)
        except ValueError:
            return data.strftime(date_format)


class TimestampDecoder():
    """
    Version-aware decoder for MDS timestamps.
    """

    def __init__(self, **kwargs):
        """
        Parameters:
            version: str, Version, optional
                The MDS version to target.
        """
        self.version = Version(kwargs.get("version", Version.mds_lower()))
        self.version.raise_if_unsupported()

    def __repr__(self):
        return f"<mds.encoding.TimestampDecoder ('{self.version}')>"

    def decode(self, data):
        """
        Decode a MDS timestamp representation into a datetime.

        Parameters:
            data: str, int, float
                Data representing a datetime as text or UNIX timestamp.

        Return:
            datetime
        """
        try:
            return datetime.datetime.fromtimestamp(int(data / 1000.0), tz=datetime.timezone.utc)
        except:
            return dateutil.parser.parse(data)
