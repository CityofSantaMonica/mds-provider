"""
Work with Providers from the registry.
"""
import csv
import pathlib
import uuid

import requests

import mds.github
from .schemas import STATUS_CHANGES, TRIPS
from .versions import Version


class Provider():
    """
    A simple model for an entry in a Provider registry.
    """

    def __init__(self, identifier=None, ref=mds.github.MDS_DEFAULT_REF, path=None, **kwargs):
        """
        Initialize a new Provider instance.

        Parameters:
            identifier: str, UUID, Provider, optional
                The provider_id or provider_name from the registry.

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

            Additional keyword parameters are set as attributes on the Provider instance.
        """
        # parsing a Provider record
        if not identifier:
            self.provider_name = kwargs.pop("provider_name", None)

            provider_id = kwargs.pop("provider_id", None)
            self.provider_id = provider_id if isinstance(provider_id, uuid.UUID) else uuid.UUID(provider_id)

            self.auth_type = kwargs.pop("auth_type", "Bearer")
            self.gbfs_api_url = self._clean_url(kwargs.pop("gbfs_api_url", None))
            self.headers = kwargs.pop("headers", {})
            self.mds_api_suffix = kwargs.pop("mds_api_suffix", None)
            self.mds_api_url = self._clean_url(kwargs.pop("mds_api_url", None))
            self.registry_path = path
            self.registry_ref = ref
            self.url = self._clean_url(kwargs.pop("url", None))

            try:
                self.version = Version(ref)
            except:
                pass

            for k,v in kwargs.items():
                setattr(self, k, v)

        # copy Provider instance
        elif isinstance(identifier, Provider):
            _kwargs = vars(identifier)
            _kwargs.update(kwargs)
            Provider.__init__(self, ref=identifier.registry_ref, path=identifier.registry_path, **_kwargs)

        # interrogate the registry
        else:
            provider = Registry(ref=ref, path=path).find(identifier, **kwargs)
            if provider:
                Provider.__init__(self, provider)

    def __repr__(self):
        ref = self.registry_ref or self.registry_path
        return f"<mds.providers.Provider ('{ref}', '{self.provider_name}', '{str(self.provider_id)}', '{self.mds_api_url}')>"

    @property
    def endpoints(self):
        endpoint = [self.mds_api_url]
        if self.mds_api_suffix:
            endpoint.append(self.mds_api_suffix.rstrip("/"))
        return {
            STATUS_CHANGES: "/".join(endpoint + [STATUS_CHANGES]),
            TRIPS: "/".join(endpoint + [TRIPS])
        }

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


class Registry():
    """
    Represents a local or remote Provider registry.

    See: https://github.com/CityOfLosAngeles/mobility-data-specification/blob/master/providers.csv
    """

    _registry = {}

    def __init__(self, ref=mds.github.MDS_DEFAULT_REF, path=None, **kwargs):
        """
        Parameters:
            ref: str, Version
                The reference (git commit, branch, tag, or version) at which to query the registry.
                By default, download from GitHub master.

            path: str, Path, optional
                A path to a local registry file to skip the GitHub download.
        """
        key = (str(ref), path)
        if key not in self._registry:
            self._registry[key] = self._get_registry(*key)

        self.providers = self._registry[key]
        self.ref = ref
        self.path = path

    def __repr__(self):
        data = "'" + "', '".join([str(self.ref or self.path), str(len(self.providers)) + " providers"]) + "'"
        return f"<mds.files.Registry ({data})>"

    def find(self, provider, **kwargs):
        """
        Find a Provider instance in this Registry.

        Parameters:
            provider: str, UUID
                A provider_id or provider_name to look for in the registry.

            Additional keyword arguments are set as attributes on the Provider instance.

        Return:
            Provider
                The matching Provider instance, or None.
        """
        try:
            provider = uuid.UUID(provider)
        except ValueError:
            pass

        # filter for matching provider(s)
        found = next((p for p in self.providers if any([
            isinstance(provider, str) and p.provider_name.lower() == provider.lower(),
            isinstance(provider, uuid.UUID) and p.provider_id == provider
        ])), None)

        # re-init with the record from registry and config
        return Provider(found, **kwargs) if found else None

    @staticmethod
    def _get_registry(ref, path):
        if path:
            path = pathlib.Path(path)
            with path.open("r") as f:
                return Registry._parse_csv(f.readlines(), ref=ref, path=path)
        else:
            url = mds.github.registry_url(ref)
            with requests.get(url, stream=True) as r:
                lines = (line.decode("utf-8").replace(", ", ",") for line in r.iter_lines())
                return Registry._parse_csv(lines, ref=ref, path=path)

    @staticmethod
    def _parse_csv(lines, **kwargs):
        """
        Parse CSV lines into a list of Provider instances.
        """
        return [Provider(**record, **kwargs) for record in csv.DictReader(lines)]
