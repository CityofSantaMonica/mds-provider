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
                The MDS version to target.
        """
        self.config = kwargs.pop("config", None)

        if provider:
            provider = provider.provider_name if isinstance(provider, Provider) else provider
            self.provider = Provider(provider, self.config)

        self.version = Version(kwargs.pop("version", Version.mds_lower()))

        if not self.version.supported:
            raise UnsupportedVersionError(self.version)

        self.encoder = MdsJsonEncoder(date_format="unix", version=self.version)

    def _media_type_version_header(self):
        """
        The custom MDS media-type and version header, using this client's version
        """
        return f"application/vnd.mds.provider+json;version={self.version.header}"

    def _session(self, provider):
        """
        Internal helper to establish an authenticated session with the provider.

        The provider is checked against all immediate subclasses of AuthorizationToken (and that class itself)
        and the first supported implementation is used to establish the authenticated session.

        Raises a ValueError if no supported implementation can be found.
        """
        for auth_type in auth_types():
            if getattr(auth_type, "can_auth")(provider):
                return auth_type(provider).session

        raise ValueError(f"Couldn't find a supported auth type for {provider}")

    def _build_url(self, provider, endpoint):
        """
        Build an API url for a provider's endpoint.
        """
        url = provider.mds_api_url

        if hasattr(provider, "mds_api_suffix"):
            url += "/" + getattr(provider, "mds_api_suffix").rstrip("/")

        url += "/" + endpoint

        return url

    def _request(self, provider, endpoint, params, paging, rate_limit):
        """
        Send one or more requests to a provider's endpoint.

        Returns a list of payloads, with length corresponding to the number of non-empty responses.
        """
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

        def _has_data(page):
            """
            Checks if this page has a "data" property with a non-empty payload.
            """
            data = page["data"] if "data" in page else {"__payload__": []}
            payload = data[endpoint] if endpoint in data else []
            print(f"Got payload with {len(payload)} {endpoint}")
            return len(payload) > 0

        def _next_url(page):
            """
            Gets the next URL or None from page.
            """
            return page["links"].get("next") if "links" in page else None

        url = self._build_url(provider, endpoint)
        results = []

        # establish an authenticated session
        session = self._session(provider)

        # get the initial page of data
        r = session.get(url, params=params)

        if r.status_code is not 200:
            _describe(r)
            return results

        this_page = r.json()

        if _has_data(this_page):
            results.append(this_page)

        # get subsequent pages of data
        next_url = _next_url(this_page)
        while paging and next_url:
            r = session.get(next_url)

            if r.status_code is not 200:
                _describe(r)
                break

            this_page = r.json()

            if _has_data(this_page):
                results.append(this_page)

            next_url = _next_url(this_page)

            if next_url and rate_limit:
                time.sleep(rate_limit)

        return results

    def _date_format(self, dt):
        """
        Format datetimes for querystrings.
        """
        if dt is None:
            return None
        return self.encoder.encode(dt) if isinstance(dt, datetime) else int(dt)

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

        Returns:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        config = kwargs.pop("config", self.config)
        provider = self._provider_or_raise(provider, config)
        start_time = self._date_format(kwargs.pop("start_time", None))
        end_time = self._date_format(kwargs.pop("end_time", None))
        paging = bool(kwargs.pop("paging", True))
        rate_limit = int(kwargs.pop("rate_limit", 0))

        params = {
            **dict(start_time=start_time, end_time=end_time),
            **kwargs
        }

        return self._request(provider, STATUS_CHANGES, params, paging, rate_limit)

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

            min_end_time: datetime, int, optional
                Filters for trips where end_time occurs at or after the given time.
                Should be a datetime or int UNIX milliseconds.

            max_end_time: datetime, int, optional
                Filters for trips where end_time occurs before the given time.
                Should be a datetime or int UNIX milliseconds.

            paging: bool, optional
                True (default) to follow paging and request all available data.
                False to request only the first page.

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

        Returns:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        config = kwargs.pop("config", self.config)
        provider = self._provider_or_raise(provider, config)
        min_end_time = self._date_format(kwargs.pop("min_end_time", None))
        max_end_time = self._date_format(kwargs.pop("max_end_time", None))
        paging = bool(kwargs.pop("paging", True))
        rate_limit = int(kwargs.pop("rate_limit", 0))

        params = { 
            **dict(device_id=device_id, vehicle_id=vehicle_id, min_end_time=min_end_time, max_end_time=max_end_time),
            **kwargs
        }

        return self._request(provider, TRIPS, params, paging, rate_limit)
