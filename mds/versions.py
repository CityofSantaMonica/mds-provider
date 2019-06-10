"""
Work with MDS versions.
"""

import sys

import packaging.version


__version__ = "0.5.0"
__mds_lower_version__ = "0.2.0"
__mds_upper_version__ = "0.4.0"


class UnexpectedVersionError(ValueError):
    """
    Model an error for an unexpected MDS version.
    """

    def __init__(self, unexpected, expected):
        super().__init__(f"MDS version {unexpected} was unexpected; expected {expected}.")


class UnsupportedVersionError(ValueError):
    """
    Model an error for an unsupported MDS version.
    """

    def __init__(self, version):
        super().__init__(f"MDS version {version} is not supported by the current version of this library.")


class Version():
    """
    Represents a version in semver format `MAJOR.MINOR.PATCH`. See https://semver.org/ for more.

    Versions can be specified by omitting the right-most components:

    `MAJOR.MINOR.X`, `MAJOR.MINOR`, `MAJOR.X`, `MAJOR`

    Any omitted components are assumed to be supported in full; e.g. `MAJOR.MINOR.X` implies
    everything from `MAJOR.MINOR.0` up to but not including `MAJOR.MINOR+1.0` is supported.

    Pre-release versions are also supported, e.g. `MAJOR.MINOR.PATCH-alpha1`.
    """

    def __init__(self, version):
        """
        Initialize a new Version.

        Parameters:
            version: str, Version
                The semver-formatted version string; or another Version instance.
        """
        if isinstance(version, Version):
            version = str(version)
        if not isinstance(version, str):
            raise TypeError("version")

        self._version = self._parse(version)
        self._legacy = None

        if isinstance(self._version, packaging.version.LegacyVersion):
            # versions like "0.3.x" or "0.x"
            try:
                # assume the highest PATCH support
                major, minor, legacy = str(self._version).split(".")
                self._version = self._parse(f"{major}.{minor}.{sys.maxsize}")
                # note the highest valid version tuple index, and the "legacy" data
                self._legacy = (1, legacy)
            except:
                # assume the highest MINOR.PATCH support
                major, legacy = str(self._version).split(".")
                self._version = self._parse(f"{major}.{sys.maxsize}.{sys.maxsize}")
                # note the highest valid version tuple index, and the "legacy" data
                self._legacy = (0, legacy)
        elif len(self.tuple) < 2:
            # MAJOR only versions like "0", "1"
            self._version = self._parse(f"{self.tuple[0]}.{sys.maxsize}.{sys.maxsize}")
            self._legacy = (0, None)
        elif len(self.tuple) < 3:
            # MAJOR.MINOR only version like "0.3"
            self._version = self._parse(f"{self.tuple[0]}.{self.tuple[1]}.{sys.maxsize}")
            self._legacy = (1, None)

    def _parse(self, version):
        return packaging.version.parse(version)

    def __repr__(self):
        if self._legacy:
            _,legacy = self._legacy
            parts = [p for p in [*self.tuple, legacy] if p is not None]
            return ".".join(map(str, parts))
        else:
            return ".".join(map(str, self.tuple))

    @property
    def header(self):
        """
        A str representation of this Version instance suitable for use in an MDS API header value.
        """
        if len(self.tuple) < 2:
            return f"{self}.0"
        else:
            return f"{self.tuple[0]}.{self.tuple[1]}"

    @property
    def supported(self):
        """
        True if this Version instance is supported by the library version.
        """
        return Version.is_supported(self)

    @property
    def unsupported(self):
        """
        True if this Version instance is not supported by the library version.
        """
        return not self.supported

    @property
    def tuple(self):
        """
        An int tuple representation of this Version.
        """
        if self._legacy:
            index, _ = self._legacy
            parts = [p for p in self._version.release if self._version.release.index(p) <= index]
            return tuple(parts)
        else:
            return self._version.release

    def __eq__(self, version2):
        return self._version.__eq__(version2._version)

    def __ge__(self, version2):
        return self._version.__ge__(version2._version)

    def __gt__(self, version2):
        return self._version.__gt__(version2._version)

    def __le__(self, version2):
        return self._version.__le__(version2._version)

    def __lt__(self, version2):
        return self._version.__lt__(version2._version)

    def __ne__(self, version2):
        return self._version.__ne__(version2._version)

    @classmethod
    def library(cls):
        """
        The Version of the library currently being used.

        Return:
            Version
        """
        return Version(__version__)

    @classmethod
    def mds(cls):
        """
        The MDS Version range supported by the library version.

        Return:
            tuple (lower: Version, upper: Version)
                The partially-closed range [lower, upper) of version compatibility.
                lower is the smallest version supported by the library version.
                upper is the smallest version not supported by the library version.
        """
        return cls.mds_lower(), cls.mds_upper()

    @classmethod
    def mds_lower(cls):
        """
        The smallest MDS Version supported by the library version.

        Return:
            Version
        """
        return Version(__mds_lower_version__)

    @classmethod
    def mds_upper(cls):
        """
        The smallest MDS Version not supported by the library version.

        Return:
            Version
        """
        return Version(__mds_upper_version__)

    @classmethod
    def is_supported(cls, version):
        """
        True if the given MDS version is supported by the library version.

        Parameters:
            version: str, Version
                The MDS version to test for support.

        Return:
            bool
        """
        version = Version(version)
        lower, upper = Version.mds()
        return lower <= version < upper
