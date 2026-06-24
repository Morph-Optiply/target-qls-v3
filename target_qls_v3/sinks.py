"""QlsV2 stream sink classes."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from target_qls_v3.client import QlsV2Sink


class BuyOrdersV2Sink(QlsV2Sink):
    """Sink for the BuyOrders / purchase-orders stream."""

    name = "BuyOrders"
    endpoint = "purchase-orders"
    skipped_records: list[dict] = []

    @staticmethod
    def _estimated_arrival_date(created_at: datetime, today: date | None = None) -> date:
        """Return a QLS-safe estimated arrival date.

        QLS rejects deliveries with estimated_arrival equal to today or in the
        past. The export ETL sends the Optiply BO delivery date in created_at,
        so normalize any non-future date to tomorrow before applying the
        existing weekend adjustment.
        """
        current_date = today or datetime.now(created_at.tzinfo).date()
        estimated_date = created_at.date()

        if estimated_date <= current_date:
            estimated_date = current_date + timedelta(days=1)

        # Skip weekends: push Saturday → Monday, Sunday → Monday
        if estimated_date.weekday() == 5:   # Saturday
            estimated_date += timedelta(days=2)
        elif estimated_date.weekday() == 6:  # Sunday
            estimated_date += timedelta(days=1)

        return estimated_date

    def preprocess_record(self, record: dict, context: dict) -> dict | None:  # type: ignore[override]
        """Transform an incoming Singer record into the QLS v2 payload shape."""
        dateformatted = self._estimated_arrival_date(record["created_at"]).strftime("%Y-%m-%d")
        deliveries = [{"estimated_arrival": dateformatted}]

        if "line_items" not in record:
            return None

        record["line_items"] = self.parse_stringified_object(record["line_items"])

        purchase_order_products = [
            {
                # QLS purchase-order-product id. New order lines do not have
                # this yet, and the export ETL may omit the key entirely.
                "remoteId": product.get("remoteId"),
                "product_payload": {
                    "amount": product["quantity"],
                    "fulfillment_product_id": product["product_remoteId"],
                },
            }
            for product in record["line_items"]
        ]

        record["id"] = str(record["id"])
        supplier_remote_id = record.get("supplier_remoteId")

        if not supplier_remote_id:
            reason = "Missing required supplier_remoteId"
            skipped_record = {
                "id": record["id"],
                "supplier_name": record.get("supplier_name"),
                "reason": reason,
            }
            self.skipped_records.append(skipped_record)
            self.logger.error(
                f"Skipping BuyOrder {record['id']}: {reason} "
                f"(supplier_name={record.get('supplier_name')!r})"
            )
            return {
                "_skip_record": True,
                "_skip_id": record["id"],
                "_skip_reason": reason,
                "_skip_supplier_name": record.get("supplier_name"),
            }

        payload = {
            "suppliers": [supplier_remote_id],
            "customer_title": str(record["id"]),
            "pre_order": 0,
            "purchase_order_products": purchase_order_products,
            "deliveries": deliveries,
        }

        return {
            "buy_order_remoteId": record.get("remoteId"),
            "payload": payload,
        }

    def upsert_record(self, record: dict, context: dict):  # type: ignore[override]
        """Write the preprocessed record to the QLS v2 API.

        HotglueSink calls upsert_record (not process_record) to persist data.
        Returns (id, True, {}) on success.
        """
        if not record:
            return None, True, {}

        if record.get("_skip_record"):
            return record.get("_skip_id"), False, {
                "skipped": True,
                "error": record.get("_skip_reason"),
                "supplier_name": record.get("_skip_supplier_name"),
            }

        state_updates: dict = {}

        try:
            remoteId = record.get("buy_order_remoteId")

            if remoteId:
                # Check whether the purchase order already exists in QLS
                existing = self.request_api(
                    "GET", endpoint=f"{self.endpoint}/{remoteId}"
                )
                existing_json = existing.json()

                if existing_json.get("data"):
                    # Order exists — add only lines that have no remoteId yet
                    for product in record["payload"]["purchase_order_products"]:
                        if not product["remoteId"]:
                            response = self.request_api(
                                "POST",
                                endpoint=f"{self.endpoint}/{remoteId}/purchase-order-products",
                                request_data=product["product_payload"],
                            )
                            line_id = response.json()["data"]["id"]
                            self.logger.info(f"Order line added with id: {line_id}")
                    return remoteId, True, state_updates

            # Order does not exist — create it from scratch
            new_lines = [
                {
                    "amount": p["product_payload"]["amount"],
                    "fulfillment_product_id": p["product_payload"]["fulfillment_product_id"],
                }
                for p in record["payload"]["purchase_order_products"]
            ]
            record["payload"]["purchase_order_products"] = new_lines

            response = self.request_api(
                "POST",
                endpoint=self.endpoint,
                request_data=record["payload"],
            )
            created_id = response.json()["data"]["id"]
            self.logger.info(f"{self.name} created with id: {created_id}")
            return created_id, True, state_updates

        except Exception as e:
            self.logger.error(f"Error upserting {self.name}: {e}")
            raise


class UpdateInventorySink(QlsV2Sink):
    """Sink for the UpdateInventory stream (no-op for now)."""

    name = "UpdateInventory"
    endpoint = "inventory"

    def upsert_record(self, record: dict, context: dict):  # type: ignore[override]
        """No-op: UpdateInventory records are acknowledged but not written."""
        return None, True, {}
