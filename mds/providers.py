"""
Work with the official MDS Providers registry.
"""

import csv
import requests
from uuid import UUID


PROVIDER_REGISTRY = "https://raw.githubusercontent.com/CityOfLosAngeles/mobility-data-specification/{}/providers.csv"
DEFAULT_REF = "master"


class Provider():
    """
    A simple model for an entry in the Provider registry.
    """
    def __init__(self, *args, **kwargs):
        self.provider_name = kwargs.pop("provider_name", None)

        provider_id = kwargs.pop("provider_id", None)
        self.provider_id = provider_id if isinstance(provider_id, UUID) else UUID(provider_id)

        self.url = self._clean_url(kwargs.pop("url", None))
        self.mds_api_url = self._clean_url(kwargs.pop("mds_api_url", None))
        self.gbfs_api_url = self._clean_url(kwargs.pop("gbfs_api_url", None))

        for k,v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return f"<Provider name:'{self.provider_name}' api_url:'{self.mds_api_url}' id:'{str(self.provider_id)}'>"

    def _clean_url(self, url):
        """
        Helper to return a normalized URL
        """
        if url:
            url = url.lower().rstrip("/")
            return url if url.startswith("https://") else f"https://{url}"
        else:
            return None

    def configure(self, config, use_id=False):
        """
        Merge Provider-specific data from :config: with this provider.

        Returns a new Provider with the merged data.

        :use_id: is a flag that, when True, will lookup this Provider by provider_id inside the :config:
        (e.g. for using a dict of configuration for different Providers). If the provider_id isn't found,
        returns this Provider un-modified.
        """
        if use_id:
            if self.provider_id in config:
                config = config[self.provider_id]
            elif str(self.provider_id) in config:
                config = config[str(self.provider_id)]
            else:
                return self

        _kwargs = { **vars(self), **config }
        return Provider(**_kwargs)


def filter(providers, names):
    """
    Filters a list of Provider instances, given one or more case-insensitive names.
    """
    if names is None or len(names) == 0:
        return providers

    if isinstance(names, str):
        names = [names]

    names = [n.lower() for n in names]

    return [p for p in providers if p.provider_name.lower() in names]


def get_registry(ref=DEFAULT_REF, file=None):
    """
    Parse a Provider registry file; by default, download the official registry from GitHub `master`.

    Optionally download from the specified :ref:, which could be any of:
        - git branch name
        - commit hash (long or short)
        - git tag

    Or use the :file: kwarg to skip the download and parse a local registry file.
    """
    providers = []

    def __parse(lines):
        for record in csv.DictReader(lines):
            providers.append(Provider(**record))

    if file:
        with open(file, "r") as f:
            __parse(f.readlines())
    else:
        url = PROVIDER_REGISTRY.format(ref or DEFAULT_REF)
        with requests.get(url, stream=True) as r:
            lines = (line.decode("utf-8").replace(", ", ",") for line in r.iter_lines())
            __parse(lines)

    return providers
