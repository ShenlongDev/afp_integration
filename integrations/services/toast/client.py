import requests
import logging
from datetime import datetime
from .auth import ToastAuthService

logger = logging.getLogger(__name__)

class ToastIntegrationService:
    """
    Provides utility methods for interacting with the Toast API.
    """
    def __init__(self, integration):
        self.integration = integration
        self.hostname = integration.toast_api_url  # e.g. "https://ws-api.toasttab.com"
        self.client_id = integration.toast_client_id
        self.client_secret = integration.toast_client_secret
        self.auth_service = ToastAuthService(self.hostname, self.client_id, self.client_secret)
        self.access_token = self.auth_service.login()

    def get_restaurant_guid(self):
        """
        Retrieves the restaurant GUID for this Toast integration.
        """
        url = f"{self.hostname}/partners/v1/restaurants"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        payload = {
            "clientId": self.client_id,
            "clientSecret": self.client_secret,
            "userAccessType": "TOAST_MACHINE_CLIENT"
        }
        try:
            response = requests.get(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and data:
                restaurant_guid = data[0].get("guid")
                return restaurant_guid
            else:
                logger.error("No restaurant GUID found in Toast response.")
                return None
        except requests.RequestException as e:
            logger.error("Failed to get restaurant GUID: %s", e)
            return None

    def format_date_for_toast(self, date_obj):
        """
        Formats a datetime object to the Toast required date string.
        Example: "2025-02-04T23:00:00.000+0000"
        """
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    def import_orders(self, start_date, end_date):
        """
        Imports orders from Toast between start_date and end_date.
        Paginate requests and process each order to calculate net sales.
        """
        start_date_str = self.format_date_for_toast(start_date)
        end_date_str = self.format_date_for_toast(end_date)
        restaurant_guid = self.get_restaurant_guid()
        if not restaurant_guid:
            raise Exception("Restaurant GUID not found.")

        url = f"{self.hostname}/orders/v2/ordersBulk"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Toast-Restaurant-External-ID": restaurant_guid
        }
        params = {
            "startDate": start_date_str,
            "endDate": end_date_str
        }
        orders = []
        page = 1
        while True:
            params["page"] = page
            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                orders_batch = data.get("orders", [])
                if not orders_batch:
                    break
                orders.extend(orders_batch)
                page += 1
            except requests.RequestException as e:
                logger.error("Error fetching orders on page %s: %s", page, e)
                break

        self.process_orders(orders)
        return orders

    def process_orders(self, orders):
        """
        Processes each Toast order to calculate net sales.
        
        Order Structure:
          - **Selection Level:** For each selection (menu item), calculate:
            selectionNetSales = preDiscountPrice minus total nonTaxableDiscountAmount.
            Skip voided selections or selections where displayName equals "Gift Card".
          - **Check Level:** Sum the selectionNetSales, add non-gratuity service charges, and subtract check-level discounts.
          - **Order Level:** Sum the check net sales values.
        
        Each order is annotated with import metadata (importID, OrgID, WSImportDate)
        to ensure data integrity and traceability.
        """
        for order in orders:
            order_net_sales = 0
            checks = order.get("checks", [])
            for check in checks:
                check_net_sales = 0
                selections = check.get("selections", [])
                for selection in selections:
                    # Skip if voided or a gift card.
                    if selection.get("voided") or selection.get("displayName", "").lower() == "gift card":
                        continue
                    pre_discount = selection.get("preDiscountPrice", 0)
                    discount_total = sum(
                        disc.get("nonTaxableDiscountAmount", 0)
                        for disc in selection.get("appliedDiscounts", [])
                    )
                    selection_net_sales = pre_discount - discount_total
                    check_net_sales += selection_net_sales

                # Process check-level adjustments.
                service_charge = sum(
                    sc.get("chargeAmount", 0)
                    for sc in check.get("appliedServiceCharges", [])
                    if not sc.get("gratuity", False)
                )
                check_discounts = sum(
                    disc.get("nonTaxableDiscountAmount", 0)
                    for disc in check.get("appliedDiscounts", [])
                )
                check_net_sales = check_net_sales + service_charge - check_discounts
                order_net_sales += check_net_sales

            order["orderNetSales"] = order_net_sales
            # Add metadata for data integrity.
            order["importID"] = self.integration.id
            order["orgID"] = self.integration.org.id
            order["WSImportDate"] = datetime.utcnow().isoformat()
