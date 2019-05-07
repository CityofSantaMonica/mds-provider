"""
Obtain Provider information from MDS Provider registry files.
"""

import csv
from pathlib import Path
import requests
from uuid import UUID


PROVIDER_REGISTRY = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/providers.csv"
DEFAULT_REF = "master"


class Provider():
    """
    A simple model for an entry in the Provider registry.
    """

    _registry = {}

    def __init__(self, identifier=None, config={}, ref=DEFAULT_REF, path=None, **kwargs):
        """
        Initialize a new Provider instance.

        Parameters:
            identifier: str, UUID, optional
                A provider_id or provider_name to look for in the registry.

            config: dict, optional
                Attributes to merge with this Provider instance.

            ref: str, Version
                The reference (git commit, branch, tag, or version) at which to query the registry.

            path: str, Path, optional
                A path to a local registry file.

            provider_name: str, optional
                The name of the provider from the registry.

            provider_id: str, UUID
                The unique identifier for the provider from the registry.

            url: str
                The provider's website url from the registry.

            mds_api_url: str
                The provider's base API url from the registry.

            gbfs_api_url: str
                The provider's GBFS API url from the registry.
        """
        # parsing a Provider record
        if not identifier:
            self.provider_name = kwargs.pop("provider_name", None)

            provider_id = kwargs.pop("provider_id", None)
            self.provider_id = provider_id if isinstance(provider_id, UUID) else UUID(provider_id)

            self.url = self._clean_url(kwargs.pop("url", None))
            self.mds_api_url = self._clean_url(kwargs.pop("mds_api_url", None))
            self.gbfs_api_url = self._clean_url(kwargs.pop("gbfs_api_url", None))

            for k,v in kwargs.items():
                setattr(self, k, v)

        # interrogate the registry
        else:
            try:
                identifier = UUID(identifier)
            except ValueError:
                pass

            # obtain the registry
            registry = Provider.get_registry(ref=ref, path=path)

            # filter for matching provider(s)
            registry = [p for p in registry if any([
                isinstance(identifier, str) and p.provider_name.lower() == identifier.lower(),
                isinstance(identifier, UUID) and p.provider_id == identifier
            ])]

            if len(registry) == 1:
                # re-init with the record from registry and config
                _kwargs = { **vars(registry[0]), **(config or {}) }
                Provider.__init__(self, **_kwargs)
            else:
                raise ValueError(f"Can not obtain a single provider matching '{identifier}'.")

    def __repr__(self):
        return f"<mds.providers.Provider ('{self.provider_name}', '{str(self.provider_id)}', '{self.mds_api_url}')>"

    @classmethod
    def get_registry(cls, ref=DEFAULT_REF, path=None):
        """
        Parse a Provider registry file into a list of Provider instances.

        Parameters:
            ref: str, Version
                The reference (git commit, branch, tag, or version) at which to query the registry.
                By default, download from GitHub master.

            path: str, Path, optional
                A path to a local registry file to skip the GitHub download.

        Return
            list
                A list of Provider instances from the registry.
        """
        def _get(ref, path):
            if path:
                if not isinstance(path, Path):
                    path = Path(path)
                with path.open("r") as f:
                    return cls._parse_csv(f.readlines())
            else:
                url = PROVIDER_REGISTRY.format(ref or DEFAULT_REF)
                with requests.get(url, stream=True) as r:
                    lines = (line.decode("utf-8").replace(", ", ",") for line in r.iter_lines())
                    return cls._parse_csv(lines)

        # get/cache this registry reference
        key = (ref, path)
        if key not in cls._registry:
            cls._registry[key] = _get(*key)

        return cls._registry[key]

    @staticmethod
    def _clean_url(url):
        """
        Helper to return a normalized URL
        """
        if url:
            url = url.lower().rstrip("/")
            return url if url.startswith("https://") else f"https://{url}"
        else:
            return None

    @staticmethod
    def _parse_csv(lines):
        """
        Helper parses CSV lines into a list of Provider instances.
        """
        return [Provider(**record) for record in csv.DictReader(lines)]
