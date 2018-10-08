"""
Authentication module for MDS API calls.
"""

import requests
from requests import Session


class BearerTokenAuth():
    """
    Mixin implementing Bearer Token Authorization.
    """
    def bearer_token_session(self, provider):
        """
        Establishes a session with the `Authorization: Bearer :token:` header.
        """
        session = Session()
        session.headers.update({ "Authorization": f"Bearer {provider.bearer_token}" })

        return session


class OAuthClientCredentialsAuth(BearerTokenAuth):
    """
    Mixin implementing OAuth 2.0 client_credentials grant flow.
    """
    def oauth_session(self, provider):
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
        provider.bearer_token = r.json()["access_token"]

        return self.bearer_token_session(provider)

