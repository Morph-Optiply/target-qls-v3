"""QlsV2 target class."""

from __future__ import annotations

from typing import Type

from singer_sdk import typing as th
from singer_sdk.sinks import Sink

from target_hotglue.target import TargetHotglue

from target_qls_v3 import sinks

SINK_TYPES = [sinks.BuyOrdersV2Sink, sinks.UpdateInventorySink]


class TargetQlsV3(TargetHotglue):
    """Singer target for QlsV2, built with the Hotglue SDK."""

    name = "target-qls-v3"

    SINK_TYPES = [sinks.BuyOrdersV2Sink, sinks.UpdateInventorySink]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="QLS v2 API username",
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="QLS v2 API password",
        ),
        th.Property(
            "company_id",
            th.StringType,
            required=True,
            description="QLS v2 company ID used to build the base URL",
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Get sink for a stream."""
        return next(
            (
                sink_class
                for sink_class in SINK_TYPES
                if sink_class.name.lower() == stream_name.lower()
            ),
            None,
        )

    def _process_lines(self, file_input):  # type: ignore[override]
        """Process input and report skipped BuyOrders after all records are handled."""
        sinks.BuyOrdersV2Sink.skipped_records = []
        try:
            return super()._process_lines(file_input)
        finally:
            skipped_records = sinks.BuyOrdersV2Sink.skipped_records
            if skipped_records:
                skipped_ids = ", ".join(record["id"] for record in skipped_records)
                self.logger.error(
                    "Skipped %s BuyOrders because supplier_remoteId is missing: %s",
                    len(skipped_records),
                    skipped_ids,
                )
                for record in skipped_records:
                    self.logger.error(
                        "Skipped BuyOrder %s: %s (supplier_name=%r)",
                        record["id"],
                        record["reason"],
                        record.get("supplier_name"),
                    )


if __name__ == "__main__":
    TargetQlsV3.cli()
