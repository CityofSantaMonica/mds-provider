"""
MDS Provider API client implementation. 
"""

from datetime import datetime
import json
import mds
from mds.api.auth import OAuthClientCredentialsAuth
from mds.providers import get_registry, Provider


class ProviderClient(OAuthClientCredentialsAuth):
    """
    Client for MDS Provider APIs
    """
    def __init__(self, providers=None, ref=None):
        """
        Initialize a new ProviderClient object.

        :providers: is a list of Providers this client tracks by default. If None is given, downloads and uses the official Provider registry.

        When using the official Providers registry, :ref: could be any of:
            - git branch name
            - commit hash (long or short)
            - git tag
        """
        self.providers = providers if providers is not None else get_registry(ref)

    def _auth_session(self, provider):
        """
        Internal helper to establish an authenticated session with the :provider:.
        """
        if hasattr(provider, "token") and not hasattr(provider, "token_url"):
            # auth token defined by provider
            return self.auth_token_session(provider)
        else:
            # OAuth 2.0 client_credentials grant flow
            return self.oauth_session(provider)

    def _build_url(self, provider, endpoint):
        """
        Internal helper for building API urls.
        """
        url = provider.mds_api_url

        if hasattr(provider, "mds_api_suffix"):
            url += "/" + getattr(provider, "mds_api_suffix").rstrip("/")

        url += "/" + endpoint

        return url

    def _request(self, providers, endpoint, params, page):
        """
        Internal helper for sending requests.

        Returns a map of provider => payload(s).
        """
        # create a request url for each provider
        urls = [self._build_url(p, endpoint) for p in providers]

        # keyed by provider
        records = {}

        for i in range(len(providers)):
            provider, url = providers[i], urls[i]

            # establish an authenticated session
            session = self._auth_session(provider)

            # get the initial page of data
            payload = session.get(url, params=params).json()

            # track the list of pages per provider
            records[provider] = [payload]

            def __next(payload):
                """
                Helper to get the next URL or None
                """
                return payload["links"].get("next") if "links" in payload else None

            # get subsequent pages of data
            next = __next(payload)
            while page and next is not None:
                payload = session.get(next)
                records[provider].extend(payload)
                next = __next(payload)

        return records

    def _date_format(self, dt):
        """
        Internal helper to format datetimes for querystrings.
        """
        return int(dt.timestamp()) if isinstance(dt, datetime) else int(dt)

    def get_status_changes(self,
                           providers=None,
                           start_time=None,
                           end_time=None,
                           bbox=None,
                           page=True,
                           **kwargs):
        """
        Request Status Changes data. Supported keyword args:

            - `providers`: One or more Providers to issue this request to.
                           The default is to issue the request to all Providers.

            - `start_time`: Filters for status changes where `event_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for status changes where `event_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

            - `bbox`: Filters for status changes where `event_location` is within defined bounding-box.
                      The order is defined as: southwest longitude, southwest latitude, 
                      northeast longitude, northeast latitude (separated by commas).

                      e.g.

                      bbox=-122.4183,37.7758,-122.4120,37.7858

            - `page`: True (default) to follow paging and request all available data.
                      False to request only the first page.
        """
        if providers is None:
            providers = self.providers

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params together
        params = { **dict(start_time=start_time, end_time=end_time, bbox=bbox), **kwargs }

        # make the request(s)
        status_changes = self._request(providers, mds.STATUS_CHANGES, params, page)

        return status_changes

    def get_trips(self,
                  providers=None,
                  device_id=None,
                  vehicle_id=None,
                  start_time=None,
                  end_time=None,
                  bbox=None,
                  page=True,
                  **kwargs):
        """
        Request Trips data. Supported keyword args:

            - `providers`: One or more Providers to issue this request to.
                           The default is to issue the request to all Providers.

            - `device_id`: Filters for trips taken by the given device.

            - `vehicle_id`: Filters for trips taken by the given vehicle.

            - `start_time`: Filters for trips where `start_time` occurs at or after the given time
                            Should be a datetime object or numeric representation of UNIX seconds

            - `end_time`: Filters for trips where `end_time` occurs at or before the given time
                          Should be a datetime object or numeric representation of UNIX seconds

            - `bbox`: Filters for trips where and point within `route` is within defined bounding-box.
                      The order is defined as: southwest longitude, southwest latitude, 
                      northeast longitude, northeast latitude (separated by commas).

                      e.g.

                      bbox=-122.4183,37.7758,-122.4120,37.7858

            - `page`: True (default) to follow paging and request all available data.
                      False to request only the first page.
        """
        if providers is None:
            providers = self.providers

        # convert datetimes to querystring friendly format
        if start_time is not None:
            start_time = self._date_format(start_time)
        if end_time is not None:
            end_time = self._date_format(end_time)

        # gather all the params togethers
        params = { 
            **dict(device_id=device_id,
                   vehicle_id = vehicle_id,
                   start_time=start_time,
                   end_time=end_time,
                   bbox=bbox),
            **kwargs
        }

        # make the request(s)
        trips = self._request(providers, mds.TRIPS, params, page)

        return trips

