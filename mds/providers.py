"""
Work with Providers from the registry.
"""

from uuid import UUID

from mds import github


class Provider():
    """
    A simple model for an entry in the Provider registry.
    """

    def __init__(self, identifier=None, ref=github.MDS_DEFAULT_REF, path=None, **kwargs):
        """
        Initialize a new Provider instance.

        Parameters:
            identifier: str, UUID, optional
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
            self.provider_id = provider_id if isinstance(provider_id, UUID) else UUID(provider_id)

            self.url = self._clean_url(kwargs.pop("url", None))
            self.mds_api_url = self._clean_url(kwargs.pop("mds_api_url", None))
            self.gbfs_api_url = self._clean_url(kwargs.pop("gbfs_api_url", None))
            self.registry_ref = ref
            self.registry_path = path
            self.version = None

            try:
                self.version = Version(self.registry_ref)
            except:
                pass

            for k,v in kwargs.items():
                setattr(self, k, v)

        # interrogate the registry
        else:
            from .files import RegistryFile
            provider = RegistryFile(ref=ref, path=path).get(identifier)
            if provider:
                Provider.__init__(self, **vars(provider), **kwargs)

    def __repr__(self):
        ref = self.registry_ref or self.registry_path
        return f"<mds.providers.Provider ('{ref}', '{self.provider_name}', '{str(self.provider_id)}', '{self.mds_api_url}')>"

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
