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
    """Products stream from Channeldock Seller Inventory API.
    
    Endpoint: GET /portal/api/v2/seller/inventory
    Documentation: Returns all main products from the seller account,
    including stock levels and multiple product attributes.
    
    Uses stocking_date for incremental sync with API-side filtering.
    The API supports start_date/end_date filters when using sort_attr=stocking_date.
    """

    name = "products"
    path = "/portal/api/v2/seller/inventory"
    primary_keys = ["id"]
    replication_key = "stocking_date"
    replication_method = "INCREMENTAL"

    records_jsonpath = "$.products[*]"
    
    # Store the end_date used in the request to save as bookmark
    _current_end_date: str | None = None

    schema = th.PropertiesList(
        # Primary key
        th.Property("id", th.IntegerType, required=True, description="Internal product ID"),
        
        # String identifiers
        th.Property("ean", th.StringType, description="European Article Number"),
        th.Property("sku", th.StringType, description="Stock Keeping Unit"),
        th.Property("title", th.StringType, description="Product name"),
        th.Property("img_url", th.StringType, description="Product image URL"),
        th.Property("product_reference", th.StringType, description="Product reference"),
        
        # Stock levels
        th.Property("stock", th.IntegerType, description="Current stock level"),
        th.Property("available_stock", th.IntegerType, description="Available stock after reservations"),
        th.Property("total_lvb_stock", th.IntegerType, description="Total LVB stock"),
        th.Property("total_fba_stock", th.IntegerType, description="Total FBA stock"),
        th.Property("total_fbc_stock", th.IntegerType, description="Total FBC stock"),
        
        # Dimensions
        th.Property("x_size", th.IntegerType, description="Product dimension X"),
        th.Property("y_size", th.IntegerType, description="Product dimension Y"),
        th.Property("z_size", th.IntegerType, description="Product dimension Z"),
        th.Property("weight", th.NumberType, description="Product weight"),
        
        # Pricing
        th.Property("price", th.NumberType, description="Selling price"),
        th.Property("purchase_price", th.NumberType, description="Purchase price from supplier"),
        
        # Supplier info
        th.Property("supplier_id", th.IntegerType, description="Supplier identifier"),
        th.Property("minimal_supplier_order_quantity", th.IntegerType, description="MOQ from supplier"),
        th.Property("replenishment_time", th.IntegerType, description="Replenishment time in days"),
        
        # Stock management
        th.Property("stock_advice_iron_stock", th.IntegerType, description="Recommended safety stock"),
        th.Property("units_per_box", th.IntegerType, description="Units per box"),
        th.Property("sold_per_day", th.NumberType, description="Average daily sales"),
        
        # Flags
        th.Property("is_bundle_product", th.IntegerType, description="Indicates if product is a bundle (0/1)"),
        th.Property("require_serial_number", th.IntegerType, description="Requires serial number (0/1)"),
        
        # Timestamps
        th.Property("stocking_date", th.DateTimeType, description="Last stocking date (UTC)"),
        th.Property("updated_at", th.DateTimeType, description="Last update timestamp - replication key"),
        
        # Arrays (stored as JSON strings)
        th.Property("tags", th.StringType, description="Product tags (JSON array)"),
        th.Property("child_products", th.StringType, description="Child products for bundles (JSON array)"),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL query parameters for the request.

        Uses stocking_date sorting with API-side date filtering.
        The API supports start_date/end_date when sort_attr=stocking_date.

        Args:
            context: Stream partition context.
            next_page_token: Page number for pagination.

        Returns:
            Dictionary of URL parameters.
        """
        params = super().get_url_params(context, next_page_token)
        
        # Use stocking_date sorting (required for date filtering)
        params["sort_attr"] = "stocking_date"
        params["sort_dir"] = "ASC"  # Oldest first to process chronologically
        
        # Get bookmark value for start_date
        bookmark_value = self.get_starting_replication_key_value(context)
        if bookmark_value:
            # Use bookmark as start_date (API format: "YYYY-MM-DD HH:MM:SS")
            params["start_date"] = bookmark_value
            self.logger.info(f"[{self.name}] Using start_date from bookmark: {bookmark_value}")
        
        # Set end_date to current UTC time
        self._current_end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        params["end_date"] = self._current_end_date
        self.logger.info(f"[{self.name}] Using end_date: {self._current_end_date}")
        
        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process a record.

        Convert array fields to JSON strings for compatibility.
        Set stocking_date to end_date for proper bookmark tracking.

        Args:
            row: The record to process.
            context: Stream partition context.

        Returns:
            The processed record, or None to skip.
        """
        if not row:
            return None

        # Convert array fields to JSON strings
        array_fields = ["tags", "child_products"]
        for field in array_fields:
            if field in row and row[field] is not None:
                if isinstance(row[field], (list, dict)):
                    row[field] = json.dumps(row[field])
            else:
                row[field] = None

        return row

    def get_replication_key_signpost(
        self,
        context: Context | None = None,
    ) -> str | None:
        """Return the signpost (end_date) for replication key.

        The signpost is the maximum value we want to sync up to.
        We use the end_date from our API request so the bookmark
        is set to the time window we requested.

        Args:
            context: Stream partition context.

        Returns:
            The end_date used in the request.
        """
        return self._current_end_date


class SuppliersStream(ChanneldockStream):
    """Suppliers stream from Channeldock Seller API.
    
    Endpoint: GET /portal/api/v2/seller/suppliers
    Documentation: Returns all suppliers associated with the seller account.
    Suppliers can be assigned to inbound deliveries or directly to products.
    
    This is a FULL_TABLE stream as the API does not support date filtering.
    """

    name = "suppliers"
    path = "/portal/api/v2/seller/suppliers"
    primary_keys = ["id"]
    replication_key = None
    replication_method = "FULL_TABLE"

    records_jsonpath = "$.suppliers[*]"

    schema = th.PropertiesList(
        # Primary key
        th.Property("id", th.IntegerType, required=True, description="Internal supplier ID"),
        
        # Contact info
        th.Property("firstname", th.StringType, description="Contact first name"),
        th.Property("lastname", th.StringType, description="Contact last name"),
        th.Property("company", th.StringType, description="Company name"),
        th.Property("phone", th.StringType, description="Phone number"),
        th.Property("email", th.StringType, description="Email address"),
        
        # Address
        th.Property("address1", th.StringType, description="Address line 1"),
        th.Property("address2", th.StringType, description="Address line 2"),
        th.Property("city", th.StringType, description="City"),
        th.Property("state", th.StringType, description="State / region"),
        th.Property("zipcode", th.StringType, description="Postal code"),
        th.Property("country", th.StringType, description="Country code (ISO)"),
        
        # Business info
        th.Property("payment_term", th.IntegerType, description="Payment term in days"),
        th.Property("website", th.StringType, description="Company website"),
        th.Property("vat", th.IntegerType, description="Indicates if VAT applies (0/1)"),
        th.Property("vat_number", th.StringType, description="VAT number"),
    ).to_dict()

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
        return params


class OrdersStream(ChanneldockStream):
    """Orders stream from Channeldock Seller API.
    
    Endpoint: GET /portal/api/v2/seller/orders
    Documentation: Returns paginated orders belonging to the seller account.
    Supports filtering by updated_at for incremental sync.
    
    Uses updated_at for incremental sync with API-side filtering via
    updated_at_from and updated_at_to parameters.
    """

    name = "orders"
    path = "/portal/api/v2/seller/orders"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"

    records_jsonpath = "$.orders[*]"
    
    # Store the end_date used in the request to save as bookmark
    _current_end_date: str | None = None

    schema = th.PropertiesList(
        # Primary key
        th.Property("id", th.IntegerType, required=True, description="ChannelDock order ID"),
        
        # Order identifiers
        th.Property("seller_id", th.IntegerType, description="Seller ID"),
        th.Property("order_id", th.StringType, description="Remote channel order ID"),
        th.Property("seq_order_id", th.StringType, description="Sequential ChannelDock order ID"),
        th.Property("channel_name", th.StringType, description="Sales channel name"),
        th.Property("channel_id", th.IntegerType, description="Channel ID"),
        th.Property("api_id", th.IntegerType, description="API ID"),
        th.Property("payment_id", th.StringType, description="Payment ID"),
        
        # Pricing
        th.Property("price_total", th.NumberType, description="Total order price"),
        th.Property("price_shipping", th.NumberType, description="Shipping cost"),
        th.Property("price_discount", th.NumberType, description="Discount amount"),
        th.Property("price_currency", th.StringType, description="Price currency code"),
        th.Property("currency", th.StringType, description="Currency code"),
        th.Property("discount_total", th.NumberType, description="Total discount amount"),
        th.Property("vat_number", th.StringType, description="VAT number"),
        th.Property("total_weight", th.NumberType, description="Total order weight"),
        
        # Billing info
        th.Property("billing_first_name", th.StringType, description="Billing first name"),
        th.Property("billing_middle_name", th.StringType, description="Billing middle name"),
        th.Property("billing_last_name", th.StringType, description="Billing last name"),
        th.Property("billing_company", th.StringType, description="Billing company"),
        th.Property("billing_street", th.StringType, description="Billing street"),
        th.Property("billing_address1", th.StringType, description="Billing address line 1"),
        th.Property("billing_address2", th.StringType, description="Billing address line 2"),
        th.Property("billing_address_supplement", th.StringType, description="Billing address supplement"),
        th.Property("billing_house_number", th.StringType, description="Billing house number"),
        th.Property("billing_house_number_ext", th.StringType, description="Billing house number extension"),
        th.Property("billing_city", th.StringType, description="Billing city"),
        th.Property("billing_region", th.StringType, description="Billing region/state"),
        th.Property("billing_zipcode", th.StringType, description="Billing postal code"),
        th.Property("billing_zip_code", th.StringType, description="Billing zip code (alternative)"),
        th.Property("billing_country_code", th.StringType, description="Billing country code (ISO)"),
        th.Property("billing_email", th.StringType, description="Billing email"),
        th.Property("billing_phone", th.StringType, description="Billing phone"),
        
        # Shipping info
        th.Property("shipping_first_name", th.StringType, description="Shipping first name"),
        th.Property("shipping_last_name", th.StringType, description="Shipping last name"),
        th.Property("shipping_company", th.StringType, description="Shipping company"),
        th.Property("shipping_street", th.StringType, description="Shipping street"),
        th.Property("shipping_address1", th.StringType, description="Shipping address line 1"),
        th.Property("shipping_address2", th.StringType, description="Shipping address line 2"),
        th.Property("shipping_house_number", th.StringType, description="Shipping house number"),
        th.Property("shipping_house_number_ext", th.StringType, description="Shipping house number extension"),
        th.Property("shipping_city", th.StringType, description="Shipping city"),
        th.Property("shipping_region", th.StringType, description="Shipping region/state"),
        th.Property("shipping_zipcode", th.StringType, description="Shipping postal code"),
        th.Property("shipping_zip_code", th.StringType, description="Shipping zip code (alternative)"),
        th.Property("shipping_country_code", th.StringType, description="Shipping country code (ISO)"),
        th.Property("shipping_email", th.StringType, description="Shipping email"),
        th.Property("shipping_phone", th.StringType, description="Shipping phone"),
        th.Property("shipping_phone_number", th.StringType, description="Shipping phone number (alternative)"),
        th.Property("shipping_service", th.StringType, description="Shipping service/carrier"),
        
        # Order status and dates
        th.Property("order_status", th.StringType, description="Order status"),
        th.Property("order_date", th.DateTimeType, description="Order creation date"),
        th.Property("ship_on_date", th.DateTimeType, description="Scheduled ship date"),
        th.Property("sync_date", th.DateTimeType, description="Sync date"),
        th.Property("updated_at", th.DateTimeType, description="Last update timestamp - replication key"),
        
        # Batch info
        th.Property("batch_id", th.IntegerType, description="Batch ID"),
        th.Property("batch_title", th.StringType, description="Batch title"),
        th.Property("packaging_id", th.IntegerType, description="Packaging ID"),
        
        # Order products (stored as JSON string)
        th.Property("order_products", th.StringType, description="Order line items (JSON array)"),
        
        # Optional data
        th.Property("notes", th.StringType, description="Order notes"),
        th.Property("extra_comment", th.StringType, description="Extra comments"),
        th.Property("raw_order_data", th.StringType, description="Raw channel payload (JSON)"),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL query parameters for the request.

        Uses updated_at sorting with API-side date filtering.
        The API supports updated_at_from/updated_at_to for incremental sync.

        Args:
            context: Stream partition context.
            next_page_token: Page number for pagination.

        Returns:
            Dictionary of URL parameters.
        """
        params: dict[str, t.Any] = {
            "page": next_page_token or 1,
            "order_status": "ALL",  # Get all statuses for complete sync
        }
        
        # Use updated_at sorting for incremental sync
        params["sort_attr"] = "updated_at"
        params["sort_dir"] = "asc"  # Oldest first to process chronologically
        
        # Get bookmark value for updated_at_from
        bookmark_value = self.get_starting_replication_key_value(context)
        if bookmark_value:
            params["updated_at_from"] = bookmark_value
            self.logger.info(f"[{self.name}] Using updated_at_from from bookmark: {bookmark_value}")
        
        # Set updated_at_to to current UTC time
        self._current_end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        params["updated_at_to"] = self._current_end_date
        self.logger.info(f"[{self.name}] Using updated_at_to: {self._current_end_date}")
        
        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process a record.

        Convert array/object fields to JSON strings for compatibility.

        Args:
            row: The record to process.
            context: Stream partition context.

        Returns:
            The processed record, or None to skip.
        """
        if not row:
            return None

        # Convert complex fields to JSON strings
        json_fields = ["order_products", "raw_order_data"]
        for field in json_fields:
            if field in row and row[field] is not None:
                if isinstance(row[field], (list, dict)):
                    row[field] = json.dumps(row[field])
            else:
                row[field] = None

        return row

    def get_replication_key_signpost(
        self,
        context: Context | None = None,
    ) -> str | None:
        """Return the signpost (end_date) for replication key.

        The signpost is the maximum value we want to sync up to.
        We use the updated_at_to from our API request so the bookmark
        is set to the time window we requested.

        Args:
            context: Stream partition context.

        Returns:
            The updated_at_to used in the request.
        """
        return self._current_end_date


class DeliveriesStream(ChanneldockStream):
    """Deliveries stream from Channeldock Seller API.
    
    Endpoint: GET /portal/api/v2/seller/deliveries
    Documentation: Returns pending and historical deliveries including
    supplier information, delivery status, dates, and line items.
    
    Delivery types: inbound, outbound, bol_outbound, amazon_outbound
    
    Uses updated_at for incremental sync. Supports filtering by delivery_type
    via the tap config property 'delivery_types'.
    """

    name = "deliveries"
    path = "/portal/api/v2/seller/delivery"
    primary_keys = ["id"]
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"

    records_jsonpath = "$.deliveries[*]"
    
    # Store the end_date used in the request to save as bookmark
    _current_end_date: str | None = None

    schema = th.PropertiesList(
        # Primary key
        th.Property("id", th.IntegerType, required=True, description="Delivery ID"),
        
        # Delivery info
        th.Property("delivery_type", th.StringType, description="Type: inbound, outbound, bol_outbound, amazon_outbound"),
        th.Property("ref", th.StringType, description="Reference code"),
        th.Property("status", th.StringType, description="Status: new, confirmed, delivered, stocked, shipped, cancelled"),
        
        # Dates
        th.Property("delivery_date", th.DateType, description="Planned delivery date"),
        th.Property("stocked_at", th.DateTimeType, description="Stocked timestamp"),
        th.Property("created_at", th.DateTimeType, description="Creation timestamp"),
        th.Property("updated_at", th.DateTimeType, description="Last update timestamp - replication key"),
        
        # Quantities
        th.Property("pallets", th.IntegerType, description="Number of pallets"),
        th.Property("boxes", th.IntegerType, description="Number of boxes"),
        
        # Supplier
        th.Property("supplier_id", th.IntegerType, description="Supplier ID"),
        th.Property("supplier", th.StringType, description="Supplier details (JSON object)"),
        
        # Center
        th.Property("center_id", th.IntegerType, description="Fulfillment center ID"),
        
        # Additional info
        th.Property("extra_description", th.StringType, description="Extra description/notes"),
        
        # Items
        th.Property("items", th.StringType, description="Delivery line items (JSON array)"),
    ).to_dict()

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        """Return URL query parameters for the request.

        Uses updated_at for incremental sync filtering.
        Applies delivery_type filter from config if single type specified.

        Args:
            context: Stream partition context.
            next_page_token: Page number for pagination.

        Returns:
            Dictionary of URL parameters.
        """
        params: dict[str, t.Any] = {
            "page": next_page_token or 1,
        }
        
        # Use updated_at sorting for incremental sync
        params["sort_attr"] = "updated_at"
        params["sort_dir"] = "ASC"  # Oldest first to process chronologically
        
        # Get bookmark value for updated_at filter
        bookmark_value = self.get_starting_replication_key_value(context)
        if bookmark_value:
            # API returns deliveries with updated_at > value
            params["updated_at"] = bookmark_value
            self.logger.info(f"[{self.name}] Using updated_at from bookmark: {bookmark_value}")
        
        # Apply delivery_type filter if specified in config
        delivery_type = self.config.get("delivery_type")
        if delivery_type:
            params["delivery_type"] = delivery_type
            self.logger.info(f"[{self.name}] Filtering by delivery_type: {delivery_type}")
        
        # Set current end date for bookmark
        self._current_end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.logger.info(f"[{self.name}] Sync end_date: {self._current_end_date}")
        
        return params

    def post_process(
        self,
        row: dict,
        context: Context | None = None,
    ) -> dict | None:
        """Post-process a record.

        Convert complex fields to JSON strings.

        Args:
            row: The record to process.
            context: Stream partition context.

        Returns:
            The processed record, or None to skip.
        """
        if not row:
            return None

        # Convert complex fields to JSON strings
        json_fields = ["supplier", "items"]
        for field in json_fields:
            if field in row and row[field] is not None:
                if isinstance(row[field], (list, dict)):
                    row[field] = json.dumps(row[field])
            else:
                row[field] = None

        return row

    def get_replication_key_signpost(
        self,
        context: Context | None = None,
    ) -> str | None:
        """Return the signpost for replication key.

        Args:
            context: Stream partition context.

        Returns:
            The end_date used in the request.
        """
        return self._current_end_date
