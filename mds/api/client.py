"""
MDS Provider API client implementation.
"""

import datetime
import time

from ..encoding import TimestampEncoder, TimestampDecoder
from ..files import ConfigFile
from ..providers import Provider
from ..schemas import STATUS_CHANGES, TRIPS
from ..versions import UnsupportedVersionError, Version
from .auth import auth_types


_V040_ = Version("0.4.0")


class Client():
    """
    Client for MDS Provider APIs.
    """

    def __init__(self, provider=None, config={}, **kwargs):
        """
        Parameters:
            provider: str, UUID, Provider, optional
                Provider instance or identifier that this client queries by default.

            config: dict, ConfigFile, optional
                Attributes to merge with the Provider instance.

            version: str, Version, optional
                The MDS version to target. By default, use Version.mds_lower().

        Extra keyword arguments are taken as config attributes for the Provider.
        """
        if isinstance(config, ConfigFile):
            config = config.dump()

        # look for version first in config, then kwargs, then use default
        self.version = Version(config.pop("version", kwargs.pop("version", Version.mds_lower())))
        if self.version.unsupported:
            raise UnsupportedVersionError(self.version)

        # merge config with the rest of kwargs
        self.config = { **config, **kwargs }

        self.provider = None
        if provider:
            self.provider = Provider(provider, ref=self.version, **self.config)

    def __repr__(self):
        data = [str(self.version)]
        if self.provider:
            data.append(self.provider.provider_name)
        data = "'" + "', '".join(data) + "'"
        return f"<mds.api.Client ({data})>"

    def _date_format(self, dt, version, record_type):
        """
        Format datetimes for querystrings.
        """
        if dt is None:
            return None
        if not isinstance(dt, datetime.datetime):
            # convert to datetime using decoder
            dt = TimestampDecoder(version=version).decode(dt)

        if version >= _V040_ and record_type in [STATUS_CHANGES, TRIPS]:
            encoder = TimestampEncoder(version=version, date_format="hours")
        else:
            encoder = TimestampEncoder(version=version, date_format="unix")

        return encoder.encode(dt)

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
                When version < 0.4.0 and requesting status_changes, filters for events occurring before the given time.
                When version >= 0.4.0 and requesting trips, filters for trips ending within the hour of the given timestamp.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            event_time: datetime, int, optional
                When version >= 0.4.0 and requesting status_changes, filters for events occurring within the hour of the given timestamp.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            max_end_time: datetime, int, optional
                When version < 0.4.0 and requesting trips, filters for trips where end_time occurs before the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            min_end_time: datetime, int, optional
                when version < 0.4.0 and requesting trips, filters for trips where end_time occurs at or after the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            paging: bool, optional
                When version < 0.4.0, True (default) to follow paging and request all available data. False to request only the first page.
                Unsupported for version >= 0.4.0.

            start_time: datetime, int, optional
                When version < 0.4.0 and requesting status_changes, filters for events occuring at or after the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed through as API request parameters.

        Return:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        version = Version(kwargs.pop("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        if version < _V040_:
            if record_type not in [STATUS_CHANGES, TRIPS]:
                raise ValueError(f"MDS Version {version} only supports {STATUS_CHANGES} and {TRIPS}.")
            # adjust time query formats
            if record_type == STATUS_CHANGES:
                kwargs["start_time"] = self._date_format(kwargs.pop("start_time", None), version, record_type)
                kwargs["end_time"] = self._date_format(kwargs.pop("end_time", None), version, record_type)
            elif record_type == TRIPS:
                kwargs["min_end_time"] = self._date_format(kwargs.pop("min_end_time", None), version, record_type)
                kwargs["max_end_time"] = self._date_format(kwargs.pop("max_end_time", None), version, record_type)
        else:
            # these time parameters are required for the indicated record_type
            req_params = { STATUS_CHANGES: "event_time", TRIPS: "end_time" }
            if record_type in req_params and req_params[record_type] not in kwargs:
                raise TypeError(f"The '{req_params[record_type]}' query parameter is required for {record_type} requests.")
            # adjust time query formats
            if record_type == STATUS_CHANGES:
                kwargs["event_time"] = self._date_format(kwargs.pop("event_time"), version, record_type)
            elif record_type == TRIPS:
                kwargs["end_time"] = self._date_format(kwargs.pop("end_time"), version, record_type)
                # remove unsupported params
                kwargs.pop("device_id", None)
                kwargs.pop("vehicle_id", None)

        config = kwargs.pop("config", self.config)
        provider = self._provider_or_raise(provider, **config)
        rate_limit = int(kwargs.pop("rate_limit", 0))

        # paging is only supported for status_changes and trips prior to version 0.4.0
        paging_supported = (record_type in [STATUS_CHANGES, TRIPS] and version < _V040_) or record_type not in [STATUS_CHANGES, TRIPS]
        paging = paging_supported and bool(kwargs.pop("paging", True))

        if not hasattr(provider, "headers"):
            setattr(provider, "headers", {})

        provider.headers.update(dict([(self._media_type_version_header(version))]))

        # request
        return self._request(provider, record_type, kwargs, paging, rate_limit)

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
                When version < 0.4.0, filters for events occuring at or after the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            end_time: datetime, int, optional
                When version < 0.4.0, filters for events occurring before the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            event_time: datetime, int, optional
                When version >= 0.4.0, filters for events occurring within the hour of the given timestamp.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            paging: bool, optional
                When version < 0.4.0, True (default) to follow paging and request all available data. False to request only the first page.
                Unsupported for version >= 0.4.0.

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed through as API request parameters.

        Return:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        if version >= _V040_ and "event_time" not in kwargs:
            raise TypeError("The 'event_time' query parameter is required for status_changes requests.")

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
                When version < 0.4.0, filters for trips taken by the given device.
                Invalid for other use-cases.

            vehicle_id: str, optional
                When version < 0.4.0, filters for trips taken by the given vehicle.
                Invalid for other use-cases.

            end_time: datetime, int, optional
                When version >= 0.4.0, filters for trips ending within the hour of the given timestamp.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            max_end_time: datetime, int, optional
                When version < 0.4.0, filters for trips where end_time occurs before the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            min_end_time: datetime, int, optional
                when version < 0.4.0, filters for trips where end_time occurs at or after the given time.
                Invalid for other use-cases.
                Should be a datetime or int UNIX milliseconds.

            paging: bool, optional
                When version < 0.4.0, True (default) to follow paging and request all available data. False to request only the first page.
                Unsupported for version >= 0.4.0,

            rate_limit: int, optional
                Number of seconds of delay to insert between paging requests.

            version: str, Version, optional
                The MDS version to target.

            Additional keyword arguments are passed through as API request parameters.

        Return:
            list
                The non-empty payloads (e.g. payloads with data records), one for each requested page.
        """
        version = Version(kwargs.get("version", self.version))
        if version.unsupported:
            raise UnsupportedVersionError(version)

        if version >= _V040_ and "end_time" not in kwargs:
            raise TypeError("The 'end_time' query parameter is required for trips requests.")

        return self.get(TRIPS, provider, **kwargs)

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

        payload = r.json()
        if Client._has_data(payload, record_type):
            results.append(payload)

        # get subsequent pages of data
        next_url = Client._next_url(payload)
        while paging and next_url:
            r = session.get(next_url)

            if r.status_code is not 200:
                Client._describe(r)
                break

            payload = r.json()
            if Client._has_data(payload, record_type):
                results.append(payload)

            next_url = Client._next_url(payload)

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
