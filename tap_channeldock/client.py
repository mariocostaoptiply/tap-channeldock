"""REST client handling, including ChanneldockStream base class."""

from __future__ import annotations

import time
import typing as t

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError

# 1000 req/hour limit; small delay to stay safe
REQUEST_DELAY_SECONDS = 0.5

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class ChanneldockPaginator(BaseAPIPaginator[int]):
    """Page-based paginator for Channeldock API."""

    PAGE_SIZE = 50

    def __init__(self, start_value: int = 1) -> None:
        super().__init__(start_value)
        self._page = start_value

    def get_next(self, response: requests.Response) -> int | None:
        """Resolve next page from response. Works with any endpoint (products, suppliers, etc.)."""
        try:
            data = response.json()
        except Exception:
            return None
        
        if not isinstance(data, dict):
            return None

        # API uses {entity}_count + {entity} array (e.g. products_count/products)
        records_count = 0
        records_list = []
        
        for key in data:
            if key.endswith("_count") and isinstance(data[key], int):
                records_count = data[key]
            elif isinstance(data[key], list) and key not in ("response", "page", "page_size"):
                records_list = data[key]

        if records_count == 0 or len(records_list) < self.PAGE_SIZE:
            return None

        self._page += 1
        return self._page


class ChanneldockStream(RESTStream[int]):
    """Base Channeldock stream class with common functionality."""

    records_jsonpath: str = "$[*]"
    page_size: int = 50
    replication_key: str | None = None
    replication_method: str = "FULL_TABLE"

    @property
    def url_base(self) -> str:
        return "https://channeldock.com"

    @property
    def http_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "api_key": self.config["api_key"],
            "api_secret": self.config["api_secret"],
        }

    def get_new_paginator(self) -> ChanneldockPaginator:
        return ChanneldockPaginator(start_value=1)

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {
            "page": next_page_token or 1,
        }

        start_date = self.config.get("start_date")
        if start_date and self.replication_key:
            date_part = start_date.split("T")[0] if "T" in start_date else start_date
            params["date_from"] = date_part
            
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON response: {response.text[:500]}")
            return

        if isinstance(data, dict):
            if data.get("response") == "error":
                self.logger.error(f"API error: {data.get('message', 'Unknown error')}")
                return

        records = list(extract_jsonpath(self.records_jsonpath, input=data))
        products_count = data.get("products_count", len(records)) if isinstance(data, dict) else len(records)
        self.logger.info(f"Page returned {products_count} records")
        
        yield from records

    def validate_response(self, response: requests.Response) -> None:
        """Validate response, enforce rate limits, and raise RetriableAPIError/FatalAPIError."""
        time.sleep(REQUEST_DELAY_SECONDS)
        rate_limit_remaining = response.headers.get("X-RateLimit-Remaining")
        rate_limit_reset = response.headers.get("X-RateLimit-Reset")
        
        if rate_limit_remaining:
            remaining = int(rate_limit_remaining)
            self.logger.debug(
                f"Rate limit: {remaining} remaining, resets at {rate_limit_reset}"
            )
            if remaining < 100:
                self.logger.warning(f"Rate limit low ({remaining} remaining). Slowing down...")
                time.sleep(5)  # Wait 5 seconds
            elif remaining < 50:
                self.logger.warning(f"Rate limit critical ({remaining} remaining). Waiting longer...")
                time.sleep(30)

        if response.status_code == 429:
            reset_time = response.headers.get("X-RateLimit-Reset")
            if reset_time:
                try:
                    wait_seconds = int(reset_time) - int(time.time())
                    if wait_seconds > 0:
                        actual_wait = min(wait_seconds, 3600)  # cap at 60 min
                        minutes = actual_wait // 60
                        self.logger.warning(
                            f"Rate limit exceeded. Waiting {minutes} minutes ({actual_wait}s)..."
                        )
                        time.sleep(actual_wait)
                except (ValueError, TypeError):
                    time.sleep(60)
            else:
                time.sleep(60)
            
            raise RetriableAPIError(
                f"Rate limit exceeded: {response.text[:200]}"
            )

        if 500 <= response.status_code < 600:
            raise RetriableAPIError(
                f"{response.status_code} Server Error: {response.reason} "
                f"for path: {self.path} - {response.text[:200]}"
            )

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
        if not row:
            return None
        return row

    def backoff_wait_generator(self) -> t.Iterator[float]:
        # 30s, 60s, 120s, 240s (matches Channeldock webhook retry)
        wait_times = [30, 60, 120, 240]
        for wait_time in wait_times:
            yield wait_time
        while True:
            yield 240
