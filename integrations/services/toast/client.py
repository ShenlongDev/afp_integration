import requests
import logging
from datetime import datetime
from decimal import Decimal
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from .auth import ToastAuthService
from integrations.models.toast.raw import ToastOrder, ToastCheck, ToastSelection
from django.db import transaction

logger = logging.getLogger(__name__)

class ToastIntegrationService:
    """
    Provides utility methods for interacting with the Toast API.
    """
    def __init__(self, integration):
        self.integration = integration
        self.hostname = integration.toast_api_url 
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
                restaurant_guid = data[0].get("restaurantGuid")
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
        start_date_str = self.format_date_for_toast(start_date)
        # Ensure end_date is provided; if not, default to current time.
        if not end_date:
            end_date = timezone.now()
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
                orders_batch = data
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
        Decomposes the Toast orders into the database tables (order, check, and selection)
        and calculates net sales.

        This implementation batches bulk inserts (using a batch_size of 5000) and wraps
        each order's processing in its own transaction to ensure that even if one order fails,
        none of the others are lost.
        """
        for order_data in orders:
            print(order_data)
            order_guid = order_data.get("guid")
            try:
                with transaction.atomic():
                    # Create or update the ToastOrder record
                    order, created = ToastOrder.objects.update_or_create(
                        order_guid=order_guid,
                        tenant_id=self.integration.org.id,
                        defaults={
                            "integration": self.integration,
                            "payload": order_data,
                            "order_net_sales": Decimal("0.00"),  # Placeholder; will update after processing checks
                            "import_id": self.integration.id,
                            "ws_import_date": timezone.now(),
                            "created_date": parse_datetime(order_data.get("createdDate")) if order_data.get("createdDate") else None,
                            "closed_date": parse_datetime(order_data.get("closedDate")) if order_data.get("closedDate") else None,
                            "modified_date": parse_datetime(order_data.get("modifiedDate")) if order_data.get("modifiedDate") else None,
                        }
                    )
                    order_net_sales = Decimal("0.00")

                    # If updating, clear previous checks (and cascaded selections)
                    order.checks.all().delete()

                    for check_data in order_data.get("checks", []):
                        check_guid = check_data.get("guid")
                        selection_net_total = Decimal("0.00")
                        selection_instances = []

                        for selection_data in check_data.get("selections", []):
                            if selection_data.get("voided") or selection_data.get("displayName", "").lower() == "gift card":
                                continue
                            pre_discount_price = Decimal(str(selection_data.get("preDiscountPrice", 0)))
                            discount_total = sum(
                                Decimal(str(d.get("nonTaxableDiscountAmount", 0)))
                                for d in selection_data.get("appliedDiscounts", [])
                            )
                            selection_net = pre_discount_price - discount_total
                            selection_net_total += selection_net
                            selection_instances.append(ToastSelection(
                                tenant_id=self.integration.org.id,
                                selection_guid=selection_data.get("guid"),
                                display_name=selection_data.get("displayName"),
                                pre_discount_price=pre_discount_price,
                                discount_total=discount_total,
                                net_sales=selection_net,
                                quantity=Decimal(str(selection_data.get("quantity", 1))),
                                voided=selection_data.get("voided", False)
                            ))

                        service_charge_total = sum(
                            Decimal(str(sc.get("chargeAmount", 0)))
                            for sc in check_data.get("appliedServiceCharges", [])
                            if not sc.get("gratuity", False)
                        )
                        check_discount_total = sum(
                            Decimal(str(d.get("nonTaxDiscountAmount", 0)))
                            for d in check_data.get("appliedDiscounts", [])
                        )
                        check_net_sales = selection_net_total + service_charge_total - check_discount_total
                        order_net_sales += check_net_sales

                        # Create the ToastCheck record
                        check_record = ToastCheck.objects.create(
                            tenant_id=self.integration.org.id,
                            order=order,
                            check_guid=check_guid,
                            display_number=check_data.get("displayNumber"),
                            net_sales=check_net_sales,
                            service_charge_total=service_charge_total,
                            discount_total=check_discount_total,
                            opened_date=parse_datetime(check_data.get("openedDate")) if check_data.get("openedDate") else None,
                            closed_date=parse_datetime(check_data.get("closedDate")) if check_data.get("closedDate") else None,
                        )
                        # Assign the foreign key for each selection record; note the model field is 'toast_check'
                        for selection in selection_instances:
                            selection.toast_check = check_record
                        # Bulk create selections using a batch size of 5000
                        ToastSelection.objects.bulk_create(selection_instances, batch_size=5000)

                    # Update the order's net sales with the calculated total
                    order.order_net_sales = order_net_sales
                    order.save()
            except Exception as e:
                logger.error("Error processing order %s: %s", order_guid, e)
