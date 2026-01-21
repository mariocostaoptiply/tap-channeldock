"""Channeldock tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

# from tap_channeldock.streams import ()

STREAM_TYPES = []


class TapChanneldock(Tap):
    """Channeldock tap class."""

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0] if config else None
        super().__init__(config, catalog, state, parse_env_config, validate_config)

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
