"""REST client handling, including ChanneldockStream base class."""

from __future__ import annotations

import time
import typing as t

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError

# Rate limiting: 1000 requests/hour = ~3.6 seconds between requests
# We use a small delay to be safe
REQUEST_DELAY_SECONDS = 0.5

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class ChanneldockPaginator(BaseAPIPaginator[int]):
    """Page-based paginator for Channeldock API."""

    PAGE_SIZE = 50  # Channeldock returns 50 items per page

    def __init__(self, start_value: int = 1) -> None:
        """Initialize the paginator.
        
        Args:
            start_value: The starting page number.
        """
        super().__init__(start_value)
        self._page = start_value

    def get_next(self, response: requests.Response) -> int | None:
        """Get the next page number from response.

        Checks the response to determine if there are more pages.

        Args:
            response: The HTTP response object.

        Returns:
            The next page number, or None if no more pages.
        """
        try:
            data = response.json()
        except Exception:
            return None
        
        # Check if response indicates no more data
        # Channeldock returns products_count and products array
        if isinstance(data, dict):
            products_count = data.get("products_count", 0)
            products = data.get("products", [])
            
            # Stop if no products or fewer than page size
            if products_count == 0 or len(products) < self.PAGE_SIZE:
                return None
        
        # More pages available
        self._page += 1
        return self._page


class ChanneldockStream(RESTStream[int]):
    """Base Channeldock stream class with common functionality."""

    # Class-level configuration
    records_jsonpath: str = "$[*]"
    page_size: int = 50  # Channeldock API returns 50 records per page
    
    # Replication settings (can be overridden by subclasses)
    replication_key: str | None = None
    replication_method: str = "FULL_TABLE"

    @property
    def url_base(self) -> str:
        """Return the base URL for the Channeldock API.
        
        Returns:
            The base URL string.
        """
        return "https://channeldock.com"

    @property
    def http_headers(self) -> dict[str, str]:
        """Return headers for HTTP requests including authentication.

        Returns:
            Dictionary of HTTP headers.
        """
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "api_key": self.config["api_key"],
            "api_secret": self.config["api_secret"],
        }

    def get_new_paginator(self) -> ChanneldockPaginator:
        """Return a new paginator instance.

        Returns:
            A ChanneldockPaginator starting at page 1.
        """
        return ChanneldockPaginator(start_value=1)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL query parameters for the request.

        Args:
            context: Stream partition context.
            next_page_token: Page number for pagination.

        Returns:
            Dictionary of URL parameters.
        """
        params: dict[str, t.Any] = {
            "page": next_page_token or 1,
        }
        
        # Add start_date filter if available and stream supports it
        start_date = self.config.get("start_date")
        if start_date and self.replication_key:
            # Extract date part only
            date_part = start_date.split("T")[0] if "T" in start_date else start_date
            params["date_from"] = date_part
            
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the API response.

        Args:
            response: The HTTP response object.

        Yields:
            Parsed records from the response.
        """
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON response: {response.text[:500]}")
            return

        # Check for API error response
        if isinstance(data, dict):
            if data.get("response") == "error":
                self.logger.error(f"API error: {data.get('message', 'Unknown error')}")
                return

        # Extract records using jsonpath
        records = list(extract_jsonpath(self.records_jsonpath, input=data))
        
        # Log pagination info
        products_count = data.get("products_count", len(records)) if isinstance(data, dict) else len(records)
        self.logger.info(f"Page returned {products_count} records")
        
        yield from records

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response and handle rate limiting.

        Args:
            response: The HTTP response object.

        Raises:
            RetriableAPIError: For rate limiting (429) or server errors (5xx).
            FatalAPIError: For client errors (4xx except 429).
        """
        # Small delay between requests to respect rate limits (1000/hour)
        time.sleep(REQUEST_DELAY_SECONDS)
        
        # Log rate limit headers for monitoring
        rate_limit_remaining = response.headers.get("X-RateLimit-Remaining")
        rate_limit_reset = response.headers.get("X-RateLimit-Reset")
        
        if rate_limit_remaining:
            remaining = int(rate_limit_remaining)
            self.logger.debug(
                f"Rate limit: {remaining} remaining, resets at {rate_limit_reset}"
            )
            # If running low on requests, slow down
            if remaining < 100:
                self.logger.warning(f"Rate limit low ({remaining} remaining). Slowing down...")
                time.sleep(5)  # Wait 5 seconds
            elif remaining < 50:
                self.logger.warning(f"Rate limit critical ({remaining} remaining). Waiting longer...")
                time.sleep(30)  # Wait 30 seconds

        # Handle rate limiting (429)
        if response.status_code == 429:
            reset_time = response.headers.get("X-RateLimit-Reset")
            if reset_time:
                try:
                    wait_seconds = int(reset_time) - int(time.time())
                    if wait_seconds > 0:
                        # Cap at 60 minutes max wait
                        actual_wait = min(wait_seconds, 3600)
                        minutes = actual_wait // 60
                        self.logger.warning(
                            f"Rate limit exceeded. Waiting {minutes} minutes ({actual_wait}s)..."
                        )
                        time.sleep(actual_wait)
                except (ValueError, TypeError):
                    time.sleep(60)  # Default wait
            else:
                time.sleep(60)
            
            raise RetriableAPIError(
                f"Rate limit exceeded: {response.text[:200]}"
            )

        # Handle server errors (5xx) - retriable
        if 500 <= response.status_code < 600:
            raise RetriableAPIError(
                f"{response.status_code} Server Error: {response.reason} "
                f"for path: {self.path} - {response.text[:200]}"
            )

        # Handle client errors (4xx except 429) - fatal
        if 400 <= response.status_code < 500:
            raise FatalAPIError(
                f"{response.status_code} Client Error: {response.reason} "
                f"for path: {self.path} - {response.text[:200]}"
            )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process a record.

        Args:
            row: The record to process.
            context: Stream partition context.

        Returns:
            The processed record, or None to skip.
        """
        if not row:
            return None
        return row

    def backoff_wait_generator(self) -> t.Iterator[float]:
        """Return a generator of wait times for exponential backoff.
        
        Yields:
            Wait times in seconds.
        """
        # Exponential backoff: 30s, 60s, 120s, 240s (matching Channeldock webhook retry)
        wait_times = [30, 60, 120, 240]
        for wait_time in wait_times:
            yield wait_time
        # After exhausting the list, keep yielding the last value
        while True:
            yield 240
