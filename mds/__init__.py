"""
Tools for working with Mobility Data Specification Provider data.
"""

from mds._version import __mds_version__, __version__


STATUS_CHANGES = "status_changes"

TRIPS = "trips"


def MDS_VERSION():
    """
    :returns: The minimum MDS version supported by this version of `mds-provider`.
    """
    return __mds_version__


def VERSION():
    """
    :returns: The version of `mds-provider` currently being used.
    """
    return __version__
