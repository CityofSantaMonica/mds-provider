"""
Work with MDS versions.
"""

import packaging


__version__ = "0.3.0"
__mds_min_version__ = "0.3.0"


class Version():
    """
    Simple representation of a version string.
    """
    def __init__(self, version):
        """
        Initialize a new `Version` representation of the given version string.
        """
        if isinstance(version, str):
            self.version_string = version
        elif isinstance(version, Version):
            self.version_string = version.version_string
        else:
            raise TypeError("version")

        self.__version = self.__parse__(self.version_string)

    def __parse__(self, version):
        """
        Create the internal representation of a version string.
        """
        return packaging.version.parse(version)

    def __eq__(self, version2):
        return self.__version.__eq__(version2.__version)

    def __ge__(self, version2):
        return self.__version.__ge__(version2.__version)

    def __gt__(self, version2):
        return self.__version.__gt__(version2.__version)

    def __le__(self, version2):
        return self.__version.__le__(version2.__version)

    def __lt__(self, version2):
        return self.__version.__lt__(version2.__version)

    def __ne__(self, version2):
        return self.__version.__ne__(version2.__version)

    def __repr__(self):
        return self.version_string

    def tuple(self):
        """
        Get the int tuple representation of this `Version`.
        """
        return self.__version.release

    @classmethod
    def Library(cls):
        """
        Returns the Version of the library currently being used.
        """
        return Version(__version__)

    @classmethod
    def MDS(cls):
        """
        Returns the minimum Version of MDS supported by the library version.
        """
        return Version(__mds_min_version__)


def mds_version_supported(version):
    """
    Return True if the given MDS version is supported by the library version.
    """
    version = version if isinstance(version, Version) else Version(version)
    return Version.MDS() <= version
