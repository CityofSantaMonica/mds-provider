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
    __registry = {}

    def __init__(self, identifier=None, config={}, ref=DEFAULT_REF, path=None, **kwargs):
        """
        Initialize a new Provider instance.

        Interrogate the Provider registry or a local file path, using the given identifier
        (provider_id or provider_name).

        Optionally pass a configuration dict to merge with the Provider instance.
        """
        # parsing a Provider record
        if not identifier:
            self.provider_name = kwargs.pop("provider_name", None)

            provider_id = kwargs.pop("provider_id", None)
            self.provider_id = provider_id if isinstance(provider_id, UUID) else UUID(provider_id)

            self.url = self.__clean_url(kwargs.pop("url", None))
            self.mds_api_url = self.__clean_url(kwargs.pop("mds_api_url", None))
            self.gbfs_api_url = self.__clean_url(kwargs.pop("gbfs_api_url", None))

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
        return f"<Provider provider_name:'{self.provider_name}' provider_id:'{str(self.provider_id)}' mds_api_url:'{self.mds_api_url}'>"

    @classmethod
    def get_registry(cls, ref=DEFAULT_REF, path=None):
        """
        Parse a Provider registry file into a list of Provider instances.

        By default, download the official registry from GitHub `master`.

        Optionally download from the specified :ref:, which could be any of:
            - git branch name
            - commit hash (long or short)
            - git tag

        Or use the :path: kwarg to skip the download and parse a local registry file.
        """
        def __get():
            if path:
                with open(path, "r") as f:
                    return cls.__parse_csv(f.readlines())
            else:
                url = PROVIDER_REGISTRY.format(ref or DEFAULT_REF)
                with requests.get(url, stream=True) as r:
                    lines = (line.decode("utf-8").replace(", ", ",") for line in r.iter_lines())
                    return cls.__parse_csv(lines)

        # get/cache this registry reference
        key = (ref, path)
        if key not in cls.__registry:
            cls.__registry[key] = __get()

        return cls.__registry[key]

    @staticmethod
    def __clean_url(url):
        """
        Helper to return a normalized URL
        """
        if url:
            url = url.lower().rstrip("/")
            return url if url.startswith("https://") else f"https://{url}"
        else:
            return None

    @staticmethod
    def __parse_csv(lines):
        """
        Helper parses CSV lines into a list of Provider instances.
        """
        return [Provider(**record) for record in csv.DictReader(lines)]
