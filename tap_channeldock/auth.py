"""Authentication handling for tap-channeldock."""

from __future__ import annotations

from singer_sdk.authenticators import APIKeyAuthenticator


class ChanneldockAuthenticator(APIKeyAuthenticator):
    """API key + secret in headers for Channeldock."""

    @classmethod
    def create_for_stream(cls, stream) -> ChanneldockAuthenticator:
        return cls(
            stream=stream,
            key="api_key",
            value=stream.config["api_key"],
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        headers = super().http_headers
        headers["api_key"] = self._stream.config["api_key"]
        headers["api_secret"] = self._stream.config["api_secret"]
        headers["Content-Type"] = "application/json"
        return headers
