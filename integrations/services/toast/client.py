import requests
import logging
from datetime import datetime, time  # import time for our conversion
from decimal import Decimal
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.db import transaction
from .auth import ToastAuthService
from integrations.models.toast.raw import (
    ToastOrder, ToastCheck, ToastSelection,
    ToastGeneralLocation, ToastDaySchedule, 
    ToastWeeklySchedule, ToastJoinedOpeningHours
)
from integrations.models.models import SyncTableLogs

logger = logging.getLogger(__name__)

class ToastIntegrationService:
    """
    Provides utility methods for interacting with the Toast API.
    """
    def __init__(self, integration, start_date=None, end_date=None):
        self.integration = integration
        self.hostname = integration.toast_api_url 
        self.client_id = integration.toast_client_id
        self.client_secret = integration.toast_client_secret
        self.auth_service = ToastAuthService(self.hostname, self.client_id, self.client_secret)
        self.access_token = self.auth_service.login()
        self.start_date = start_date
        self.end_date = end_date

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
            logger.info("Restaurant GUID response: %s", data)
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
        if isinstance(date_obj, str):
            try:
                # Attempt to parse the string as an ISO8601 datetime.
                date_obj_parsed = parse_datetime(date_obj)
                if date_obj_parsed is None:
                    # Fall back to parsing as a plain date (YYYY-MM-DD)
                    date_obj_parsed = datetime.strptime(date_obj, "%Y-%m-%d")
                date_obj = date_obj_parsed
            except Exception as e:
                logger.error("Error converting date string %s: %s", date_obj, e)
                raise e
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S.000+0000")

    def convert_to_time(self, time_str):
        """
        Converts a time string such as '08:00:00.000' to a Python time object.
        """
        if not time_str:
            return None
        try:
            dt = datetime.strptime(time_str, "%H:%M:%S.000")
            return dt.time()
        except Exception as e:
            logger.error("Error converting time string %s: %s", time_str, e)
            return None
        

    def convert_int_to_time(self, hour_value):
        """
        Converts an integer hour (or a value that can be cast to int) into a Python time object.
        For example, 4 becomes time(4, 0) i.e. "04:00:00".
        """
        if hour_value is None:
            return None
        try:
            hour = int(hour_value)
            return time(hour=hour)
        except Exception as e:
            logger.error("Error converting closeout hour %s: %s", hour_value, e)
            return None

    def convert_to_date(self, date_val):
        """
        Converts a value in YYYYMMDD format (e.g. 20231130) to a date object.
        """
        if not date_val:
            return None
        try:
            s = str(date_val)
            return datetime.strptime(s, "%Y%m%d").date()
        except Exception as e:
            logger.error("Error converting date value %s: %s", date_val, e)
            return None

    def import_restaurant_and_schedule_data(self):
        """
        Fetches restaurant info (including schedules) from Toast and updates/creates:
         - ToastGeneralLocation,
         - ToastDaySchedule,
         - ToastWeeklySchedule, and
         - ToastJoinedOpeningHours.
         
        Assumes a GET request to:
           {{hostname}}/restaurants/v1/restaurants/{restaurantGUID}?includeArchived=false
        returns a JSON response that includes keys "general", "urls", "location", and "schedules".
        """
        restaurant_guid = self.get_restaurant_guid()
        if not restaurant_guid:
            raise Exception("Restaurant GUID not found.")

        url = f"{self.hostname}/restaurants/v1/restaurants/{restaurant_guid}?includeArchived=false"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Toast-Restaurant-External-ID": restaurant_guid
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error("Failed to import restaurant and schedule data: %s", e)
            raise

        general = data.get("general", {})
        urls = data.get("urls", {})
        location_data = data.get("location", {})

        general_location_defaults = {
            "tenant_id": self.integration.org.id,
            "guid": data.get("guid"),
            "general_name": general.get("name"),
            "location_name": general.get("locationName"),
            "location_code": general.get("locationCode"),
            "description": general.get("description"),
            "timezone": general.get("timeZone"),
            # Convert the closeout hour (which is an integer) to a time object.
            "closeout_hour": self.convert_int_to_time(general.get("closeoutHour")),
            "management_group_guid": general.get("managementGroupGuid"),
            "currency_code": general.get("currencyCode"),
            "first_business_date": self.convert_to_date(general.get("firstBusinessDate")),
            "archived": general.get("archived", False),
            "url_website": urls.get("website"),
            "url_facebook": urls.get("facebook"),
            "url_twitter": urls.get("twitter"),
            "url_order_online": urls.get("orderOnline"),
            "address_line1": location_data.get("address1"),
            "address_line2": location_data.get("address2"),
            "city": location_data.get("city"),
            "state_code": location_data.get("stateCode"),
            "zip_code": location_data.get("zipCode"),
            "country": location_data.get("country"),
            "phone": location_data.get("phone"),
            "latitude": location_data.get("latitude"),
            "longitude": location_data.get("longitude"),
        }
        location_obj, _ = ToastGeneralLocation.objects.update_or_create(
            guid=general_location_defaults["guid"],
            tenant_id=self.integration.org.id,
            defaults=general_location_defaults
        )
        logger.info("Imported restaurant location: %s", location_obj)

        # Process schedule data:
        schedules = data.get("schedules", {})
        day_schedules_data = schedules.get("daySchedules", {})
        week_schedule_data = schedules.get("weekSchedule", {})

        # Update/insert ToastDaySchedule records.
        day_schedule_map = {}
        for ds_guid, ds in day_schedules_data.items():
            ds_defaults = {
                "tenant_id": self.integration.org.id,
                "integration": self.integration,
                "restaurant": location_obj,
                "guid": ds_guid,
                "property_name": ds.get("scheduleName"),
                "open_time": self.convert_to_time(ds.get("openTime")),
                "close_time": self.convert_to_time(ds.get("closeTime")),
                "schedule_name": ds.get("scheduleName"),
            }
            day_obj, _ = ToastDaySchedule.objects.update_or_create(
                guid=ds_guid,
                tenant_id=self.integration.org.id,
                defaults=ds_defaults
            )
            day_schedule_map[ds_guid] = day_obj
        logger.info("Imported %d day schedules.", len(day_schedule_map))

        # Update/insert ToastWeeklySchedule record.
        weekly_defaults = {
            "tenant_id": self.integration.org.id,
            "integration": self.integration,
            "restaurant": location_obj,
            "monday": week_schedule_data.get("monday"),
            "tuesday": week_schedule_data.get("tuesday"),
            "wednesday": week_schedule_data.get("wednesday"),
            "thursday": week_schedule_data.get("thursday"),
            "friday": week_schedule_data.get("friday"),
            "saturday": week_schedule_data.get("saturday"),
            "sunday": week_schedule_data.get("sunday"),
        }
        weekly_obj, _ = ToastWeeklySchedule.objects.update_or_create(
            integration=self.integration,
            restaurant=location_obj,
            tenant_id=self.integration.org.id,
            defaults=weekly_defaults
        )
        logger.info("Imported weekly schedule for restaurant: %s", location_obj)

        # Build joined opening hours (ToastJoinedOpeningHours)
        def get_day_schedule_info(schedule_id):
            ds = day_schedule_map.get(schedule_id)
            if ds:
                overnight = False
                if ds.open_time and ds.close_time and ds.close_time < ds.open_time:
                    overnight = True
                return ds.open_time, ds.close_time, overnight, ds.schedule_name
            return None, None, False, None

        monday_start, monday_end, monday_overnight, monday_related = get_day_schedule_info(week_schedule_data.get("monday"))
        tuesday_start, tuesday_end, tuesday_overnight, tuesday_related = get_day_schedule_info(week_schedule_data.get("tuesday"))
        wednesday_start, wednesday_end, wednesday_overnight, wednesday_related = get_day_schedule_info(week_schedule_data.get("wednesday"))
        thursday_start, thursday_end, thursday_overnight, thursday_related = get_day_schedule_info(week_schedule_data.get("thursday"))
        friday_start, friday_end, friday_overnight, friday_related = get_day_schedule_info(week_schedule_data.get("friday"))
        saturday_start, saturday_end, saturday_overnight, saturday_related = get_day_schedule_info(week_schedule_data.get("saturday"))
        sunday_start, sunday_end, sunday_overnight, sunday_related = get_day_schedule_info(week_schedule_data.get("sunday"))

        joined_defaults = {
            "tenant_id": self.integration.org.id,
            "integration": self.integration,
            "restaurant": location_obj,
            "monday_start_time": monday_start,
            "monday_end_time": monday_end,
            "monday_overnight": monday_overnight,
            "monday_related_day_schedule": monday_related,
            "tuesday_start_time": tuesday_start,
            "tuesday_end_time": tuesday_end,
            "tuesday_overnight": tuesday_overnight,
            "tuesday_related_day_schedule": tuesday_related,
            "wednesday_start_time": wednesday_start,
            "wednesday_end_time": wednesday_end,
            "wednesday_overnight": wednesday_overnight,
            "wednesday_related_day_schedule": wednesday_related,
            "thursday_start_time": thursday_start,
            "thursday_end_time": thursday_end,
            "thursday_overnight": thursday_overnight,
            "thursday_related_day_schedule": thursday_related,
            "friday_start_time": friday_start,
            "friday_end_time": friday_end,
            "friday_overnight": friday_overnight,
            "friday_related_day_schedule": friday_related,
            "saturday_start_time": saturday_start,
            "saturday_end_time": saturday_end,
            "saturday_overnight": saturday_overnight,
            "saturday_related_day_schedule": saturday_related,
            "sunday_start_time": sunday_start,
            "sunday_end_time": sunday_end,
            "sunday_overnight": sunday_overnight,
            "sunday_related_day_schedule": sunday_related,
        }
        joined_obj, _ = ToastJoinedOpeningHours.objects.update_or_create(
            integration=self.integration,
            restaurant=location_obj,
            tenant_id=self.integration.org.id,
            defaults=joined_defaults
        )
        logger.info("Imported joined opening hours for restaurant: %s", location_obj)
        return {
            "location": location_obj,
            "day_schedules": list(day_schedule_map.values()),
            "weekly_schedule": weekly_obj,
            "joined_opening_hours": joined_obj,
        }

    def import_orders(self):
        if self.start_date is None:
            self.start_date = timezone.now().date()
        start_date_str = self.format_date_for_toast(self.start_date)
        if not self.end_date:
            self.end_date = timezone.now()
        end_date_str = self.format_date_for_toast(self.end_date)

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
            # "businessDate": '20250302'
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

    def log_import_event(self, module_name: str, fetched_records: int):
        SyncTableLogs.objects.create(
            module_name=module_name,
            integration='TOAST',
            organization=self.integration.org,
            fetched_records=fetched_records,
            last_updated_time=timezone.now(),
            last_updated_date=timezone.now().date()
        )

    def process_orders(self, orders):
        for order_data in orders:
            order_guid = order_data.get("guid")
            # Skip the order if it is voided, deleted, or marked as a refund.
            if order_data.get("voided") or order_data.get("deleted") or order_data.get("refund"):
                continue

            try:
                with transaction.atomic():
                    order_defaults = {
                        "integration": self.integration,
                        "payload": order_data,
                        "order_net_sales": Decimal("0.00"),  # will update below
                        "import_id": self.integration.id,
                        "ws_import_date": timezone.now(),
                        "created_date": parse_datetime(order_data.get("createdDate")) if order_data.get("createdDate") else None,
                        "closed_date": parse_datetime(order_data.get("closedDate")) if order_data.get("closedDate") else None,
                        "modified_date": parse_datetime(order_data.get("modifiedDate")) if order_data.get("modifiedDate") else None,
                        "external_id": order_data.get("externalId"),
                        "entity_type": order_data.get("entityType"),
                        "revenue_center_guid": order_data.get("revenueCenter", {}).get("guid") if order_data.get("revenueCenter") else None,
                        "server_guid": order_data.get("server", {}).get("guid") if order_data.get("server") else None,
                        "created_in_test_mode": order_data.get("createdInTestMode"),
                        "display_number": order_data.get("displayNumber"),
                        "last_modified_device_id": order_data.get("lastModifiedDevice", {}).get("id") if order_data.get("lastModifiedDevice") else None,
                        "source": order_data.get("source"),
                        "void_date": parse_datetime(order_data.get("voidDate")) if order_data.get("voidDate") else None,
                        "duration": order_data.get("duration"),
                        "business_date": order_data.get("businessDate"),
                        "paid_date": parse_datetime(order_data.get("paidDate")) if order_data.get("paidDate") else None,
                        "restaurant_service_guid": order_data.get("restaurantService", {}).get("guid") if order_data.get("restaurantService") else None,
                        "excess_food": order_data.get("excessFood"),
                        "voided": order_data.get("voided"),
                        "estimated_fulfillment_date": parse_datetime(order_data.get("estimatedFulfillmentDate")) if order_data.get("estimatedFulfillmentDate") else None,
                        "table_guid": order_data.get("table", {}).get("guid") if order_data.get("table", {}) else None,
                        "required_prep_time": order_data.get("requiredPrepTime"),
                        "approval_status": order_data.get("approvalStatus"),
                        "delivery_info": order_data.get("deliveryInfo"),
                        "service_area_guid": order_data.get("serviceArea", {}).get("guid") if order_data.get("serviceArea") else None,
                        "curbside_pickup_info": order_data.get("curbsidePickupInfo") if order_data.get("curbsidePickupInfo") else None,
                        "number_of_guests": order_data.get("numberOfGuests") if order_data.get("numberOfGuests") else None,
                        "dining_option": order_data.get("diningOption") if order_data.get("diningOption") else None,
                        "applied_packaging_info": order_data.get("appliedPackagingInfo") if order_data.get("appliedPackagingInfo") else None,
                        "opened_date": parse_datetime(order_data.get("openedDate")) if order_data.get("openedDate") else None,
                        "void_business_date": order_data.get("voidBusinessDate")
                    }
                    order, created = ToastOrder.objects.update_or_create(
                        order_guid=order_guid,
                        tenant_id=self.integration.org.id,
                        defaults=order_defaults
                    )

                    # Initialize totals.
                    total_revenue = Decimal("0.00")     # This will become toast_sales
                    total_net_sales = Decimal("0.00")     # This will become order_net_sales
                    total_refund_amount = Decimal("0.00")
                    total_tip_total = Decimal("0.00")     # New accumulator for tips
                    total_service_charge_total = Decimal("0.00")  # New accumulator for service charges
                    refund_business_date = None

                    # Process each check in the order.
                    for check_data in order_data.get("checks", []):
                        # Skip check if it is voided, deleted, or marked as a refund.
                        if check_data.get("voided") or check_data.get("deleted") or check_data.get("refund"):
                            continue

                        check_guid = check_data.get("guid")
                        # Assume check_subtotal is the base amount (which already reflects discounts).
                        check_subtotal = Decimal(str(check_data.get("amount", "0.00")))
                        tax_amount = Decimal(str(check_data.get("taxAmount", "0.00")))
                        tip_total = sum(
                            Decimal(str(p.get("tipAmount", "0.00")))
                            for p in check_data.get("payments", [])
                        )
                        service_charge_total = sum(
                            Decimal(str(sc.get("chargeAmount", "0.00")))
                            for sc in check_data.get("appliedServiceCharges", [])
                        )
                        # Accumulate tip and service charge totals for the order.
                        total_tip_total += tip_total
                        total_service_charge_total += service_charge_total

                        # For revenue, add everything.
                        check_revenue = check_subtotal + tax_amount + tip_total + service_charge_total
                        total_revenue += check_revenue
                        # For net sales, we use only the check_subtotal.
                        total_net_sales += check_subtotal

                        # Process refunds on this check.
                        check_refund = Decimal("0.00")
                        for payment in check_data.get("payments", []):
                            if payment.get("refund"):
                                refund_amt = Decimal(str(payment.get("refund", {}).get("refundAmount", "0.00")))
                                check_refund += refund_amt
                                total_refund_amount += refund_amt
                                # Capture refund business date (assuming all refunds in this check share the same date)
                                rbd = payment.get("refund", {}).get("refundBusinessDate")
                                if rbd:
                                    refund_business_date = rbd
                        # Update the check record with its computed refund.
                        check_defaults = {
                            "external_id": check_data.get("externalId"),
                            "entity_type": check_data.get("entityType"),
                            "last_modified_device_id": check_data.get("lastModifiedDevice", {}).get("id") if check_data.get("lastModifiedDevice") else None,
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
                        check_defaults.update({
                            "display_number": check_data.get("displayNumber"),
                            "net_sales": check_subtotal,
                            "service_charge_total": service_charge_total,
                            "discount_total": sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                                for d in check_data.get("appliedDiscounts", [])
                            ),
                            "opened_date": parse_datetime(check_data.get("openedDate")) if check_data.get("openedDate") else None,
                            "closed_date": parse_datetime(check_data.get("closedDate")) if check_data.get("closedDate") else None,
                            "check_refund": check_refund
                        })
                        check_obj, _ = ToastCheck.objects.update_or_create(
                            check_guid=check_guid,
                            order=order,
                            tenant_id=self.integration.org.id,
                            defaults=check_defaults
                        )

                        # Process each selection within the check.
                        for selection_data in check_data.get("selections", []):
                            # Skip selection if voided, a gift card, or marked as refunded.
                            if (selection_data.get("voided") or 
                                selection_data.get("displayName", "").strip().lower() == "gift card" or 
                                selection_data.get("refund")):
                                continue
                            selection_guid = selection_data.get("guid")
                            pre_discount_price = Decimal(str(selection_data.get("preDiscountPrice", "0.00")))
                            discount_total = sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                                for d in selection_data.get("appliedDiscounts", [])
                            )
                            quantity = Decimal(str(selection_data.get("quantity", "1")))
                            selection_net = (pre_discount_price - discount_total) * quantity

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
                                "sales_category_guid": selection_data.get("salesCategory", {}).get("guid") if selection_data.get("salesCategory") else None,
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
                            ToastSelection.objects.update_or_create(
                                selection_guid=selection_guid,
                                toast_check=check_obj,
                                tenant_id=self.integration.org.id,
                                defaults={
                                    "order_guid": order_guid,
                                    "display_name": selection_data.get("displayName"),
                                    "pre_discount_price": pre_discount_price,
                                    "discount_total": discount_total,
                                    "net_sales": selection_net,
                                    "quantity": quantity,
                                    **selection_defaults,
                                }
                            )

                    # Now, determine if refunds should be applied to this order's totals.
                    business_date = order_data.get("businessDate")
                    if refund_business_date and business_date and str(refund_business_date) == str(business_date):
                        total_revenue -= total_refund_amount
                        total_net_sales -= total_refund_amount
                        # Prevent net sales from going negative.
                        if total_net_sales < Decimal("0.00"):
                            total_net_sales = Decimal("0.00")
                    # Otherwise, if the refund business date does not match,
                    # leave these day's totals unchanged (refund will be recorded on its own day).

                    # Save the accumulated tip and service charge totals on the order level.
                    order.tip = total_tip_total
                    order.service_charges = total_service_charge_total
                    order.toast_sales = total_revenue
                    order.order_net_sales = total_net_sales
                    order.total_refunds = total_refund_amount
                    if refund_business_date:
                        order.refund_business_date = refund_business_date
                    order.save()
            except Exception as e:
                logger.error("Error processing order %s: %s", order_guid, e)

        logger.info("Imported %s orders", len(orders))
        self.log_import_event(module_name="toast_orders", fetched_records=len(orders))



