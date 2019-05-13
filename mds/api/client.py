"""
MDS Provider API client implementation.
"""

import time
from datetime import datetime

from ..encoding import TimestampEncoder
from ..files import ConfigFile
from ..providers import Provider
from ..schemas import STATUS_CHANGES, TRIPS
from ..versions import UnsupportedVersionError, Version
from .auth import auth_types


class Client():
    """
    Client for MDS Provider APIs.
    """

    def __init__(self, provider=None, **kwargs):
        """
        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier that this client queries by default.

            config: dict, optional
                Attributes to merge with the Provider instance.

            version: str, Version, optional
                The MDS version to target. By default, use Version.mds_lower().
        """
        self.config = kwargs.pop("config", {})
        if isinstance(self.config, ConfigFile):
            self.config = self.config.dump()

        # look for version first in config, then kwargs, then use default
        self.version = Version(self.config.pop("version", kwargs.pop("version", Version.mds_lower())))
        if self.version.unsupported:
            raise UnsupportedVersionError(self.version)

        self.encoder = self._encoder_or_raise(self.version)
        self.provider = None

        if provider:
            self.provider = Provider(provider, ref=self.version, **self.config)

    def __repr__(self):
        data = [str(self.version)]
        if self.provider:
            data.append(self.provider.provider_name)
        data = "'" + "', '".join(data) + "'"
        return f"<mds.api.Client ({data})>"

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
            return self._encoder_or_raise(version).encode(dt)

    def _media_type_version_header(self, version):
        """
        The custom MDS media-type and version header, using this client's version
        """
        return "Accept", f"application/vnd.mds.provider+json;version={version.header}"

    def _provider_or_raise(self, provider, **kwargs):
        """
        Get a Provider instance from the argument, self, or raise an error.
        """
        provider = provider or self.provider

        if provider is None:
            raise ValueError("Provider instance not found for this Client.")

        return Provider(provider, **kwargs)

    def get(self, record_type, provider=None, **kwargs):
        """
        Request Provider data, returning a list of non-empty payloads.

        Parameters:
            record_type: str
                The type of MDS Provider record ("status_changes" or "trips").

            provider: str, UUID, Provider, optional
                Provider instance or identifier to issue this request to.
                By default issue the request to this client's Provider instance.

            config: dict, ConfigFile, optional
                Attributes to merge with the Provider instance.

            end_time: datetime, int, optional
                Filters for records occuring before the given time.
                Should be a datetime or Version-specific numeric UNIX timestamp.

            max_end_time: datetime, int, optional
                Filters for trips where end_time occurs before the given time.
                Should be a datetime or int UNIX milliseconds.
                Only valid when version >= Version("0.3.0") and requesting trips.

            min_end_time: datetime, int, optional
                Filters for trips where end_time occurs at or after the given time.
                Should be a datetime or int UNIX milliseconds.
                Only valid when version >= Version("0.3.0") and requesting trips.

            paging: bool, optional
                True (default) to follow paging and request all available data.
                False to request only the first page.

            start_time: datetime, int, optional
                Filters for records occuring at or after the given time.
                Should be a datetime or Version-specific numeric UNIX timestamp.

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed through as API request parameters.

        Return:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        config = kwargs.pop("config", self.config)
        provider = self._provider_or_raise(provider, **config)
        paging = bool(kwargs.pop("paging", True))
        rate_limit = int(kwargs.pop("rate_limit", 0))
        version = Version(kwargs.pop("version", self.version))

        # select the appropriate time range parameter names from record_type and version

        times = {}
        # the querystring for status_changes and trips < 0.3.0
        start, end = kwargs.pop("start_time", None), kwargs.pop("end_time", None)

        if record_type == STATUS_CHANGES or version < Version("0.3.0"):
            times["start_time"] = start
            times["end_time"] = end
        else:
            # set to the new querystring arg, but allow use of either new or old
            times["min_end_time"] = self._date_format(kwargs.pop("min_end_time", start), version=version)
            times["max_end_time"] = self._date_format(kwargs.pop("max_end_time", end), version=version)

        # combine with leftover kwargs
        params = {
            **times,
            **kwargs
        }

        if not hasattr(provider, "headers"):
            setattr(provider, "headers", {})

        provider.headers.update(dict([(self._media_type_version_header(version))]))

        # request
        return self._request(provider, record_type, params, paging, rate_limit)

    def get_status_changes(self, provider=None, **kwargs):
        """
        Request status changes, returning a list of non-empty payloads.

        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier to issue this request to.
                By default issue the request to this client's Provider instance.

            config: dict, ConfigFile, optional
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

        Return:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        return self.get(STATUS_CHANGES, provider, **kwargs)

    def get_trips(self, provider=None, **kwargs):
        """
        Request trips, returning a list of non-empty payloads.

        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier to issue this request to.
                By default issue the request to this client's Provider instance.

            config: dict, ConfigFile, optional
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

        Return:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        return self.get(TRIPS, provider, **kwargs)

    @staticmethod
    def _encoder_or_raise(version):
        """
        Gets a TimestampEncoder instance for the given version, if supported.
        """
        if version.supported:
            return TimestampEncoder(date_format="unix", version=version)
        else:
            raise UnsupportedVersionError(version)

    @staticmethod
    def _request(provider, record_type, params, paging, rate_limit):
        """
        Send one or more requests to a provider's endpoint.

        Returns a list of payloads, with length corresponding to the number of non-empty responses.
        """
        url = provider.endpoints[record_type]
        results = []

        # establish an authenticated session
        session = Client._session(provider)

        # get the initial page of data
        r = session.get(url, params=params)

        if r.status_code is not 200:
            Client._describe(r)
            return results

        this_page = r.json()

        if Client._has_data(this_page, record_type):
            results.append(this_page)

        # get subsequent pages of data
        next_url = Client._next_url(this_page)
        while paging and next_url:
            r = session.get(next_url)

            if r.status_code is not 200:
                Client._describe(r)
                break

            this_page = r.json()

            if Client._has_data(this_page, record_type):
                results.append(this_page)

            next_url = Client._next_url(this_page)

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

        raise ValueError(f"A supported auth type for {provider.provider_name} could not be found.")

    @staticmethod
    def _describe(res):
        """
        Prints details about the given response.
        """
        print(f"Requested {res.url}, Response Code: {res.status_code}")
        print("Response Headers:")
        for k,v in res.headers.items():
            print(f"{k}: {v}")

        if res.status_code is not 200:
            print(res.text)

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
