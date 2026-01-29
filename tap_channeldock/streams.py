"""Stream type classes for tap-channeldock."""

from __future__ import annotations

import json
import typing as t
from datetime import datetime, timezone

from singer_sdk import typing as th

from tap_channeldock.client import ChanneldockStream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class ProductsStream(ChanneldockStream):
    """Products stream with stocking_date incremental sync."""

    name = "products"
    path = "/portal/api/v2/seller/inventory"
    primary_keys = ["id"]
    replication_key = "stocking_date"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$.products[*]"

    _current_end_date: str | None = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("ean", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("title", th.StringType),
        th.Property("img_url", th.StringType),
        th.Property("product_reference", th.StringType),
        th.Property("stock", th.IntegerType),
        th.Property("available_stock", th.IntegerType),
        th.Property("total_lvb_stock", th.IntegerType),
        th.Property("total_fba_stock", th.IntegerType),
        th.Property("total_fbc_stock", th.IntegerType),
        th.Property("x_size", th.IntegerType),
        th.Property("y_size", th.IntegerType),
        th.Property("z_size", th.IntegerType),
        th.Property("weight", th.NumberType),
        th.Property("price", th.NumberType),
        th.Property("purchase_price", th.NumberType),
        th.Property("supplier_id", th.IntegerType),
        th.Property("minimal_supplier_order_quantity", th.IntegerType),
        th.Property("replenishment_time", th.IntegerType),
        th.Property("stock_advice_iron_stock", th.IntegerType),
        th.Property("units_per_box", th.IntegerType),
        th.Property("sold_per_day", th.NumberType),
        th.Property("is_bundle_product", th.IntegerType),
        th.Property("require_serial_number", th.IntegerType),
        th.Property("stocking_date", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("tags", th.StringType),
        th.Property("child_products", th.StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        params["sort_attr"] = "stocking_date"
        params["sort_dir"] = "ASC"

        bookmark_value = self.get_starting_replication_key_value(context)
        if bookmark_value:
            params["start_date"] = bookmark_value
            self.logger.info(f"[{self.name}] start_date: {bookmark_value}")

        self._current_end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        params["end_date"] = self._current_end_date
        self.logger.info(f"[{self.name}] end_date: {self._current_end_date}")

        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        if not row:
            return None

        for field in ["tags", "child_products"]:
            if field in row and isinstance(row[field], (list, dict)):
                row[field] = json.dumps(row[field])
            elif field not in row:
                row[field] = None

        return row

    def get_replication_key_signpost(
        self,
        context: Context | None = None,
    ) -> str | None:
        return self._current_end_date


class SuppliersStream(ChanneldockStream):
    """Suppliers stream (full table sync)."""

    name = "suppliers"
    path = "/portal/api/v2/seller/suppliers"
    primary_keys = ["id"]
    replication_key = None
    replication_method = "FULL_TABLE"
    records_jsonpath = "$.suppliers[*]"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("company", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("email", th.StringType),
        th.Property("address1", th.StringType),
        th.Property("address2", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("zipcode", th.StringType),
        th.Property("country", th.StringType),
        th.Property("payment_term", th.IntegerType),
        th.Property("website", th.StringType),
        th.Property("vat", th.IntegerType),
        th.Property("vat_number", th.StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return pagination params."""
        return {"page": next_page_token or 1}


class OrdersStream(ChanneldockStream):
    """Orders stream with updated_at incremental sync."""

    name = "orders"
    path = "/portal/api/v2/seller/orders"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$.orders[*]"

    _current_end_date: str | None = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("seller_id", th.IntegerType),
        th.Property("order_id", th.StringType),
        th.Property("seq_order_id", th.StringType),
        th.Property("channel_name", th.StringType),
        th.Property("channel_id", th.IntegerType),
        th.Property("api_id", th.IntegerType),
        th.Property("payment_id", th.StringType),
        th.Property("price_total", th.NumberType),
        th.Property("price_currency", th.StringType),
        th.Property("discount_total", th.NumberType),
        th.Property("vat_number", th.StringType),
        th.Property("total_weight", th.NumberType),
        th.Property("billing_first_name", th.StringType),
        th.Property("billing_middle_name", th.StringType),
        th.Property("billing_last_name", th.StringType),
        th.Property("billing_company", th.StringType),
        th.Property("billing_street", th.StringType),
        th.Property("billing_address1", th.StringType),
        th.Property("billing_address2", th.StringType),
        th.Property("billing_address_supplement", th.StringType),
        th.Property("billing_house_number", th.StringType),
        th.Property("billing_house_number_ext", th.StringType),
        th.Property("billing_city", th.StringType),
        th.Property("billing_region", th.StringType),
        th.Property("billing_zip_code", th.StringType),
        th.Property("billing_country_code", th.StringType),
        th.Property("billing_email", th.StringType),
        th.Property("shipping_first_name", th.StringType),
        th.Property("shipping_last_name", th.StringType),
        th.Property("shipping_company", th.StringType),
        th.Property("shipping_street", th.StringType),
        th.Property("shipping_address1", th.StringType),
        th.Property("shipping_address2", th.StringType),
        th.Property("shipping_house_number", th.StringType),
        th.Property("shipping_house_number_ext", th.StringType),
        th.Property("shipping_city", th.StringType),
        th.Property("shipping_region", th.StringType),
        th.Property("shipping_zip_code", th.StringType),
        th.Property("shipping_country_code", th.StringType),
        th.Property("shipping_email", th.StringType),
        th.Property("shipping_phone_number", th.StringType),
        th.Property("shipping_service", th.StringType),
        th.Property("order_status", th.StringType),
        th.Property("order_date", th.DateTimeType),
        th.Property("ship_on_date", th.BooleanType),
        th.Property("sync_date", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("batch_id", th.IntegerType),
        th.Property("batch_title", th.StringType),
        th.Property("packaging_id", th.IntegerType),
        th.Property("order_products", th.StringType),
        th.Property("extra_comment", th.StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {
            "page": next_page_token or 1,
            "order_status": "ALL",
            "sort_attr": "updated_at",
            "sort_dir": "asc",
        }

        bookmark_value = self.get_starting_replication_key_value(context)
        if bookmark_value:
            params["updated_at_from"] = bookmark_value
            self.logger.info(f"[{self.name}] updated_at_from: {bookmark_value}")

        self._current_end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        params["updated_at_to"] = self._current_end_date
        self.logger.info(f"[{self.name}] updated_at_to: {self._current_end_date}")

        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        if not row:
            return None

        for field in ["order_products"]:
            if field in row and isinstance(row[field], (list, dict)):
                row[field] = json.dumps(row[field])
            elif field not in row:
                row[field] = None

        return row

    def get_replication_key_signpost(
        self,
        context: Context | None = None,
    ) -> str | None:
        return self._current_end_date


class DeliveriesStream(ChanneldockStream):
    """Deliveries stream with updated_at incremental sync."""

    name = "deliveries"
    path = "/portal/api/v2/seller/delivery"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$.deliveries[*]"

    _current_end_date: str | None = None

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType, required=True),
        th.Property("delivery_type", th.StringType),
        th.Property("ref", th.StringType),
        th.Property("status", th.StringType),
        th.Property("delivery_date", th.DateType),
        th.Property("stocked_at", th.DateTimeType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("pallets", th.IntegerType),
        th.Property("boxes", th.IntegerType),
        th.Property("supplier_id", th.IntegerType),
        th.Property("supplier", th.StringType),
        th.Property("center_id", th.IntegerType),
        th.Property("extra_description", th.StringType),
        th.Property("items", th.StringType),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any] = {
            "page": next_page_token or 1,
            "sort_attr": "updated_at",
            "sort_dir": "ASC",
        }

        bookmark_value = self.get_starting_replication_key_value(context)
        if bookmark_value:
            params["updated_at"] = bookmark_value
            self.logger.info(f"[{self.name}] updated_at: {bookmark_value}")

        delivery_type = self.config.get("delivery_type")
        if delivery_type:
            params["delivery_type"] = delivery_type
            self.logger.info(f"[{self.name}] delivery_type: {delivery_type}")

        self._current_end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"[{self.name}] end_date: {self._current_end_date}")

        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        if not row:
            return None

        for field in ["supplier", "items"]:
            if field in row and isinstance(row[field], (list, dict)):
                row[field] = json.dumps(row[field])
            elif field not in row:
                row[field] = None

        return row

    def get_replication_key_signpost(
        self,
        context: Context | None = None,
    ) -> str | None:
        return self._current_end_date
