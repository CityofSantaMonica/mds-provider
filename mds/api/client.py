"""
MDS Provider API client implementation. 
"""

from datetime import datetime
import time

from ..encoding import MdsJsonEncoder
from ..providers import Provider
from ..schemas import STATUS_CHANGES, TRIPS
from ..versions import UnsupportedVersionError, Version

from .auth import auth_types


class ProviderClient():
    """
    Client for MDS Provider APIs.
    """

    def __init__(self, provider=None, **kwargs):
        """
        Initialize a new ProviderClient object.

        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier that this client uses by default.

            config: dict, optional
                Attributes to merge with the Provider instance.

            version: str, Version, optional
                The MDS version to target. By default, use Version.mds_lower().
        """
        self.config = kwargs.pop("config", None)

        if provider:
            provider = provider.provider_name if isinstance(provider, Provider) else provider
            self.provider = Provider(provider, self.config)

        self.version = Version(kwargs.pop("version", Version.mds_lower()))

        if self.version.unsupported:
            raise UnsupportedVersionError(self.version)

        self.encoder = ProviderClient._encoder_or_raise(self.version)

    def _date_format(self, dt, version=None):
        """
        Format datetimes for querystrings.
        """
        if dt is None:
            return None
        if not isinstance(dt, datetime):
            return int(dt)
        if version is None or version == self.version:
            return self.encoder.encode(dt)
        else:
            return ProviderClient._encoder_or_raise(version).encode(dt)

    def _media_type_version_header(self):
        """
        The custom MDS media-type and version header, using this client's version
        """
        return f"application/vnd.mds.provider+json;version={self.version.header}"

    def _prepare_get(self, provider, record_type, **kwargs):
        """
        Prepare parameters for a GET request to an endpoint of the given type.

        Returns:
            tuple (provider: Provider, record_type: str, params: dict, paging: bool, rate_limit: int)
        """
        config = kwargs.pop("config", self.config)
        provider = self._provider_or_raise(provider, config)
        paging = bool(kwargs.pop("paging", True))
        rate_limit = int(kwargs.pop("rate_limit", 0))
        version = Version(kwargs.pop("version", self.version))

        # select the appropriate time range parameter names from record_type and version
        if record_type == STATUS_CHANGES or version < Version("0.3.0"):
            lo_key, hi_key = "start_time", "end_time"
        else:
            lo_key, hi_key = "min_end_time", "max_end_time"

        # get each of the time parameters, formatted for this version
        times = {}
        times[lo_key] = self._date_format(kwargs.pop(lo_key, None), version=version)
        times[hi_key] = self._date_format(kwargs.pop(hi_key, None), version=version)

        # combine with leftover kwargs
        params = {
            **times,
            **kwargs
        }

        return provider, record_type, params, paging, rate_limit

    def _provider_or_raise(self, provider, config):
        """
        Get a Provider instance from the argument, self, or raise an error.
        """
        provider = provider or self.provider

        if provider is None:
            raise ValueError("Provider instance not found for this ProviderClient.")

        if isinstance(provider, Provider):
            provider = provider.provider_name

        return Provider(provider, config)

    def get_status_changes(self, provider=None, **kwargs):
        """
        Request status changes, returning a list of non-empty payloads.

        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier to issue this request to.
                By default issue the request to this client's Provider instance.

            config: dict, optional
                Attributes to merge with the Provider instance.

            start_time: datetime, int, optional
                Filters for status changes where event_time occurs at or after the given time.
                Should be a datetime or int UNIX milliseconds.

            end_time: datetime, int, optional
                Filters for status changes where event_time occurs before the given time.
                Should be a datetime or int UNIX milliseconds.

            paging: bool, optional
                True (default) to follow paging and request all available data.
                False to request only the first page.

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed through as API request parameters.
        Returns:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        prepared = self._prepare_get(provider, STATUS_CHANGES, **kwargs)
        return ProviderClient._request(*prepared)

    def get_trips(self, provider=None, **kwargs):
        """
        Request trips, returning a list of non-empty payloads.

        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier to issue this request to.
                By default issue the request to this client's Provider instance.

            config: dict, optional
                Attributes to merge with the Provider instance.

            device_id: str, UUID, optional
                Filters for trips taken by the given device.

            vehicle_id: str, optional
                Filters for trips taken by the given vehicle.

            start_time: datetime, float, optional
                Filters for trips where start_time occurs at or after the given time.
                Should be a datetime or float UNIX seconds.
                Only valid when version < Version("0.3.0").

            end_time: datetime, float, optional
                Filters for trips where end_time occurs at or before the given time.
                Should be a datetime or float UNIX seconds.
                Only valid when version < Version("0.3.0").

            min_end_time: datetime, int, optional
                Filters for trips where end_time occurs at or after the given time.
                Should be a datetime or int UNIX milliseconds.
                Only valid when version >= Version("0.3.0").

            max_end_time: datetime, int, optional
                Filters for trips where end_time occurs before the given time.
                Should be a datetime or int UNIX milliseconds.
                Only valid when version >= Version("0.3.0").

            paging: bool, optional
                True (default) to follow paging and request all available data.
                False to request only the first page.

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed through as API request parameters.

        Returns:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        prepared = self._prepare_get(provider, TRIPS, **kwargs)
        return ProviderClient._request(*prepared)

    @staticmethod
    def _build_url(provider, record_type):
        """
        Build an API url for a provider's endpoint.
        """
        url = [provider.mds_api_url]

        if hasattr(provider, "mds_api_suffix"):
            url.append(getattr(provider, "mds_api_suffix").rstrip("/"))

        url.append(record_type)

        return "/".join(url)

    @staticmethod
    def _describe(res):
        """
        Prints details about the given response.
        """
        print(f"Requested {res.url}, Response Code: {res.status_code}")
        print("Response Headers:")
        for k,v in res.headers.items():
            print(f"{k}: {v}")

        if r.status_code is not 200:
            print(r.text)

    @staticmethod
    def _encoder_or_raise(version):
        """
        Gets a MdsJsonEncoder instance for the given version, if supported.
        """
        if version.supported:
            return MdsJsonEncoder(date_format="unix", version=version)
        else:
            raise UnsupportedVersionError(version)

    @staticmethod
    def _has_data(page, record_type):
        """
        Checks if this page has a "data" property with a non-empty payload.
        """
        data = page["data"] if "data" in page else {"__payload__": []}
        payload = data[record_type] if record_type in data else []
        print(f"Got payload with {len(payload)} {record_type}")
        return len(payload) > 0

    @staticmethod
    def _next_url(page):
        """
        Gets the next URL or None from page.
        """
        return page["links"].get("next") if "links" in page else None

    @staticmethod
    def _request(provider, record_type, params, paging, rate_limit):
        """
        Send one or more requests to a provider's endpoint.

        Returns a list of payloads, with length corresponding to the number of non-empty responses.
        """
        url = ProviderClient._build_url(provider, record_type)
        results = []

        # establish an authenticated session
        session = ProviderClient._session(provider)

        # get the initial page of data
        r = session.get(url, params=params)

        if r.status_code is not 200:
            ProviderClient._describe(r)
            return results

        this_page = r.json()

        if ProviderClient._has_data(this_page, record_type):
            results.append(this_page)

        # get subsequent pages of data
        next_url = ProviderClient._next_url(this_page)
        while paging and next_url:
            r = session.get(next_url)

            if r.status_code is not 200:
                ProviderClient._describe(r)
                break

            this_page = r.json()

            if ProviderClient._has_data(this_page, record_type):
                results.append(this_page)

            next_url = ProviderClient._next_url(this_page)

            if next_url and rate_limit:
                time.sleep(rate_limit)

        return results

    @staticmethod
    def _session(provider):
        """
        Establish an authenticated session with the provider.

        The provider is checked against all immediate subclasses of AuthorizationToken (and that class itself)
        and the first supported implementation is used to establish the authenticated session.

        Raises a ValueError if no supported implementation can be found.
        """
        for auth_type in auth_types():
            if getattr(auth_type, "can_auth")(provider):
                return auth_type(provider).session

        raise ValueError(f"Couldn't find a supported auth type for {provider}")
