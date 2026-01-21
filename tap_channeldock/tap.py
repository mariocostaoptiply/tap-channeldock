"""Channeldock tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_channeldock.streams import ProductsStream, SuppliersStream

STREAM_TYPES = [
    ProductsStream,
    SuppliersStream,
]


class TapChanneldock(Tap):
    """Channeldock tap class."""

    name = "tap-channeldock"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            description="API key for Channeldock authentication",
        ),
        th.Property(
            "api_secret",
            th.StringType,
            required=True,
            description="API secret for Channeldock authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest date to sync data from (ISO 8601 format)",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapChanneldock.cli()
