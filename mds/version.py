"""
Work with MDS versions.
"""

from packaging.version import parse as _packaging_version_parse


__version__ = "0.4.0"
__mds_min_version__ = "0.3.0"


class Version():
    """
    Represents a version in semver format `MAJOR.MINOR.PATCH`.
    """

    def __init__(self, version):
        """
        Initialize a new Version.
        """
        self.__version = self.__parse(version)

    @property
    def tuple(self):
        """
        An int tuple representation of this Version.
        """
        return self.__version.release

    @property
    def version_string(self):
        """
        A str representation of this Version.
        """
        return ".".join(map(str, self.tuple))

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

    @classmethod
    def Supported(cls, version):
        """
        Return True if the given MDS version is supported by the library version.
        """
        version = version if isinstance(version, Version) else Version(version)
        return Version.MDS() <= version

    @staticmethod
    def __parse(version):
        """
        Create the internal representation of a version string.
        """
        if isinstance(version, Version):
            version = version.version_string
        elif not isinstance(version, str):
            raise TypeError("version")

        return _packaging_version_parse(version)
