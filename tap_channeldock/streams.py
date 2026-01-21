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
