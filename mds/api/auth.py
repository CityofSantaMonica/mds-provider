"""
Authentication module for MDS API calls.
"""

import requests


class AuthorizationToken():
    """
    Represents an authenticated session via an Authorization token header.

    To implement a new token-based auth type, create a subclass of AuthorizationToken and implement:

        __init__(self, provider)
            Initialize self.session.

        @classmethod
        can_auth(cls, provider): bool
            return True if the auth type can be used on the provider.

    See OAuthClientCredentialsAuth for an example implementation.
    """
    def __init__(self, provider):
        """
        Establishes a session for the provider and includes the Authorization token header.
        """
        session = requests.Session()
        session.headers.update({ "Authorization": f"{provider.auth_type} {provider.token}" })

        headers = getattr(provider, "headers", None)
        if headers:
            session.headers.update(headers)

        self.session = session

    @classmethod
    def can_auth(cls, provider):
        """
        Returns True if this auth type can be used for the provider.
        """
        return all([
            hasattr(provider, "auth_type"),
            hasattr(provider, "token"),
            not hasattr(provider, "token_url")
        ])


class OAuthClientCredentialsAuth(AuthorizationToken):
    """
    Represents an authenticated session via OAuth 2.0 client_credentials grant flow.
    """
    def __init__(self, provider):
        """
        Acquires a Bearer token before establishing a session with the provider.
        """
        payload = {
            "client_id": provider.client_id,
            "client_secret": provider.client_secret,
            "grant_type": "client_credentials",
            "scope": provider.scope.split(",")
        }
        r = requests.post(provider.token_url, data=payload)
        provider.token = r.json()["access_token"]

        AuthorizationToken.__init__(self, provider)

    @classmethod
    def can_auth(cls, provider):
        """
        Returns True if this auth type can be used for the provider.
        """
        return all([
            hasattr(provider, "client_id"),
            hasattr(provider, "client_secret"),
            hasattr(provider, "scope")
        ])


class SpinClientCredentialsAuth(AuthorizationToken):
    """
    Represents an authenticated session via the Spin authentication scheme, documented at:
    https://web.spin.pm/datafeeds

    Currently, your config needs:

    * email
    * password
    * mds_api_url (see https://github.com/CityOfLosAngeles/mobility-data-specification/pull/296)
    * token_url (try https://web.spin.pm/api/v1/auth_tokens)
    """
    def __init__(self, provider):
        """
        Acquires the bearer token for Spin before establishing a session.
        """
        payload = {
            "email": provider.email,
            "password": provider.password,
            "grant_type": "api"
        }
        r = requests.post(provider.token_url, params=payload)
        provider.token = r.json()["jwt"]

        AuthorizationToken.__init__(self, provider)

    @classmethod
    def can_auth(cls, provider):
        """
        Returns True if this auth type can be used for the provider.
        """
        return all([
            provider.provider_name.lower() == "spin",
            hasattr(provider, "email"),
            hasattr(provider, "password"),
            hasattr(provider, "token_url")
        ])


def auth_types():
    """
    Return a list of all supported authentication types.
    """
    types = AuthorizationToken.__subclasses__()
    types.append(AuthorizationToken)

    return types
