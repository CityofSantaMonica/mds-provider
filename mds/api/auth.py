"""
Authentication module for MDS API calls.
"""

import requests
from requests import Session


class AuthorizationToken():
    """
    Mixin implementing an Authorization token header of the type specified by
    the provider.
    """
    def auth_token_session(self, provider):
        """
        Establishes a session with the `Authorization: :auth_type: :token:` header.
        """
        session = Session()
        session.headers.update({ "Authorization": f"{provider.auth_type} {provider.token}" })

        return session


class OAuthClientCredentialsAuth(AuthorizationToken):
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
        provider.token = r.json()["access_token"]

        return self.auth_token_session(provider)

