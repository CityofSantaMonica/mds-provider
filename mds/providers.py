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

    def configure(self, config):
        """
        Merge attributes from :config: into this Provider instance.
        """
        _kwargs = { **vars(self), **config }
        Provider.__init__(self, **_kwargs)
        return self

    @classmethod
    def Get(cls, provider, config=None, ref=DEFAULT_REF, file=None):
        """
        Obtain a Provider instance from the registry or a local file, using the given identifier
        (provider_id or provider_name).

        :config: A dict of attributes to merge with the Provider instance.
        """
        registry = get_registry(ref=ref, file=file)

        providers = [p for p in registry if p.provider_name.lower() == provider.lower() or
                                            (isinstance(provider, UUID) and p.provider_id == provider) or
                                            (isinstnace(provider, str) and p.provider_id == UUID(provider))]

        if len(providers) == 1:
            return providers[0].configure(config) if config else providers[0]
        else:
            raise ValueError(f"Can not obtain a single provider with name '{name}'.")


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
