import requests
import logging
from datetime import datetime
from decimal import Decimal
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.db import transaction
from .auth import ToastAuthService
from integrations.models.toast.raw import ToastOrder, ToastCheck, ToastSelection

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
                # Adjust key as required by your API (here we assume restaurantGuid)
                restaurant_guid = data[0].get("restaurantGuid")
                return restaurant_guid
            else:
                logger.error("No restaurant GUID found in Toast response.")
                return None
        except requests.RequestException as e:
            logger.error("Failed to get restaurant GUID: %s", e)
            return None

    def format_date_for_toast(self, date_obj):
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    def import_orders(self, start_date, end_date):
        start_date_str = self.format_date_for_toast(start_date)
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
                orders_batch = data  # assuming the API returns a list at the top level
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
        capturing all fields from the API response.
        """
        for order_data in orders:
            order_guid = order_data.get("guid")
            try:
                with transaction.atomic():
                    # Build a dictionary of order-level fields from the response.
                    order_defaults = {
                        "integration": self.integration,
                        "payload": order_data,
                        "order_net_sales": Decimal("0.00"),  # Placeholder to be updated after checks are processed.
                        "import_id": self.integration.id,
                        "ws_import_date": timezone.now(),
                        "created_date": parse_datetime(order_data.get("createdDate")) if order_data.get("createdDate") else None,
                        "closed_date": parse_datetime(order_data.get("closedDate")) if order_data.get("closedDate") else None,
                        "modified_date": parse_datetime(order_data.get("modifiedDate")) if order_data.get("modifiedDate") else None,
                        # Additional fields:
                        "external_id": order_data.get("externalId"),
                        "entity_type": order_data.get("entityType"),
                        "revenue_center_guid": order_data.get("revenueCenter", {}).get("guid"),
                        "server_guid": order_data.get("server", {}).get("guid"),
                        "created_in_test_mode": order_data.get("createdInTestMode"),
                        "display_number": order_data.get("displayNumber"),
                        "last_modified_device_id": order_data.get("lastModifiedDevice", {}).get("id"),
                        "source": order_data.get("source"),
                        "void_date": parse_datetime(order_data.get("voidDate")) if order_data.get("voidDate") else None,
                        "duration": order_data.get("duration"),
                        "business_date": order_data.get("businessDate"),
                        "paid_date": parse_datetime(order_data.get("paidDate")) if order_data.get("paidDate") else None,
                        "restaurant_service_guid": order_data.get("restaurantService", {}).get("guid"),
                        "excess_food": order_data.get("excessFood"),
                        "voided": order_data.get("voided"),
                        "estimated_fulfillment_date": parse_datetime(order_data.get("estimatedFulfillmentDate")) if order_data.get("estimatedFulfillmentDate") else None,
                        "table_guid": order_data.get("table", {}).get("guid"),
                        "required_prep_time": order_data.get("requiredPrepTime"),
                        "approval_status": order_data.get("approvalStatus"),
                        "delivery_info": order_data.get("deliveryInfo"),
                        "service_area_guid": order_data.get("serviceArea", {}).get("guid"),
                        "curbside_pickup_info": order_data.get("curbsidePickupInfo"),
                        "number_of_guests": order_data.get("numberOfGuests"),
                        "dining_option": order_data.get("diningOption"),
                        "applied_packaging_info": order_data.get("appliedPackagingInfo"),
                        "opened_date": parse_datetime(order_data.get("openedDate")) if order_data.get("openedDate") else None,
                        "void_business_date": order_data.get("voidBusinessDate")  # conversion as needed
                    }
                    order, created = ToastOrder.objects.update_or_create(
                        order_guid=order_guid,
                        tenant_id=self.integration.org.id,
                        defaults=order_defaults
                    )
                    order_net_sales = Decimal("0.00")
                    # Clear existing related checks (and cascading selections) to avoid duplicates.
                    order.checks.all().delete()

                    for check_data in order_data.get("checks", []):
                        # Build check-level defaults:
                        check_defaults = {
                            "external_id": check_data.get("externalId"),
                            "entity_type": check_data.get("entityType"),
                            "last_modified_device_id": check_data.get("lastModifiedDevice", {}).get("id"),
                            "void_date": parse_datetime(check_data.get("voidDate")) if check_data.get("voidDate") else None,
                            "duration": check_data.get("duration"),
                            "opened_by": check_data.get("openedBy"),
                            "paid_date": parse_datetime(check_data.get("paidDate")) if check_data.get("paidDate") else None,
                            "applied_loyalty_info": check_data.get("appliedLoyaltyInfo"),
                            "voided": check_data.get("voided"),
                            "payment_status": check_data.get("paymentStatus"),
                            "amount": check_data.get("amount"),
                            "tab_name": check_data.get("tabName"),
                            "tax_exempt": check_data.get("taxExempt"),
                            "tax_exemption_account": check_data.get("taxExemptionAccount"),
                            "total_amount": check_data.get("totalAmount"),
                        }
                        selection_net_total = Decimal("0.00")
                        selection_instances = []

                        for selection_data in check_data.get("selections", []):
                            if selection_data.get("voided") or selection_data.get("displayName", "").lower() == "gift card":
                                continue
                            pre_discount_price = Decimal(str(selection_data.get("preDiscountPrice", 0)))
                            discount_total = sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", 0)))
                                for d in selection_data.get("appliedDiscounts", [])
                            )
                            selection_net = pre_discount_price - discount_total
                            selection_net_total += selection_net

                            # Build a dictionary of selection-level extra fields.
                            selection_defaults = {
                                "external_id": selection_data.get("externalId"),
                                "entity_type": selection_data.get("entityType"),
                                "deferred": selection_data.get("deferred"),
                                "void_reason": selection_data.get("voidReason"),
                                "option_group": selection_data.get("optionGroup"),
                                "modifiers": selection_data.get("modifiers"),
                                "seat_number": selection_data.get("seatNumber"),
                                "fulfillment_status": selection_data.get("fulfillmentStatus"),
                                "option_group_pricing_mode": selection_data.get("optionGroupPricingMode"),
                                "gift_card_selection_info": selection_data.get("giftCardSelectionInfo"),
                                "sales_category_guid": selection_data.get("salesCategory", {}).get("guid"),
                                "split_origin": selection_data.get("splitOrigin"),
                                "selection_type": selection_data.get("selectionType"),
                                "price": selection_data.get("price"),
                                "applied_taxes": selection_data.get("appliedTaxes"),
                                "stored_value_transaction_id": selection_data.get("storedValueTransactionId"),
                                "item_group": selection_data.get("itemGroup"),
                                "item": selection_data.get("item"),
                                "tax_inclusion": selection_data.get("taxInclusion"),
                                "receipt_line_price": selection_data.get("receiptLinePrice"),
                                "unit_of_measure": selection_data.get("unitOfMeasure"),
                                "refund_details": selection_data.get("refundDetails"),
                                "toast_gift_card": selection_data.get("toastGiftCard"),
                                "tax": selection_data.get("tax"),
                                "dining_option": selection_data.get("diningOption"),
                                "void_business_date": selection_data.get("voidBusinessDate"),
                                "created_date": parse_datetime(selection_data.get("createdDate")) if selection_data.get("createdDate") else None,
                                "pre_modifier": selection_data.get("preModifier"),
                                "modified_date": parse_datetime(selection_data.get("modifiedDate")) if selection_data.get("modifiedDate") else None,
                            }
                            # Create an instance; note that we later assign the check's foreign key.
                            selection_instance = ToastSelection(
                                tenant_id=self.integration.org.id,
                                selection_guid=selection_data.get("guid"),
                                display_name=selection_data.get("displayName"),
                                pre_discount_price=pre_discount_price,
                                discount_total=discount_total,
                                net_sales=selection_net,
                                quantity=Decimal(str(selection_data.get("quantity", 1))),
                                voided=selection_data.get("voided", False),
                                **selection_defaults
                            )
                            selection_instances.append(selection_instance)

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

                        # Update check defaults with basic values.
                        check_defaults.update({
                            "display_number": check_data.get("displayNumber"),
                            "net_sales": check_net_sales,
                            "service_charge_total": service_charge_total,
                            "discount_total": check_discount_total,
                            "opened_date": parse_datetime(check_data.get("openedDate")) if check_data.get("openedDate") else None,
                            "closed_date": parse_datetime(check_data.get("closedDate")) if check_data.get("closedDate") else None,
                        })
                        check_record = ToastCheck.objects.create(
                            tenant_id=self.integration.org.id,
                            order=order,
                            check_guid=check_data.get("guid"),
                            **check_defaults
                        )
                        for selection in selection_instances:
                            selection.toast_check = check_record
                        # Bulk create all selections for this check.
                        ToastSelection.objects.bulk_create(selection_instances, batch_size=5000)
                    order.order_net_sales = order_net_sales
                    order.save()
            except Exception as e:
                logger.error("Error processing order %s: %s", order_guid, e)
