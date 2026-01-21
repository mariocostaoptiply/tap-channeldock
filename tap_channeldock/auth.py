"""Authentication handling for tap-channeldock."""

from __future__ import annotations

from singer_sdk.authenticators import APIKeyAuthenticator


class ChanneldockAuthenticator(APIKeyAuthenticator):
    """Authenticator class for Channeldock API.
    
    Channeldock uses api_key and api_secret in request headers for authentication.
    """

    @classmethod
    def create_for_stream(cls, stream) -> ChanneldockAuthenticator:
        """Create an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            key="api_key",
            value=stream.config["api_key"],
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = super().http_headers
        headers["api_key"] = self._stream.config["api_key"]
        headers["api_secret"] = self._stream.config["api_secret"]
        headers["Content-Type"] = "application/json"
        return headers
