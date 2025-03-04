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
    def __init__(self, integration):
        self.integration = integration
        self.hostname = integration.toast_api_url 
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

    def import_orders(self, start_date, end_date):
        if start_date is None:
            start_date = timezone.now().date()
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
        """
        Decomposes the Toast orders into the database tables (order, check, and selection)
        capturing all fields from the API response and calculating net sales.

        New net sales calculation (per check):
        final_check_net = (check.amount) + (check.taxAmount)
                            + (sum of tip amounts from payments)
                            + (sum of applied service charge amounts for entries where name is 
                                "Discretionary Service Charge")
        
        The order's net sales is the sum of final_check_net for all checks.
        
        In addition, we now calculate total_sales_without_deduction (stored in order.toast_sales)
        which is the sum of the raw check amounts (i.e. without adding taxes, tips, or service charges).
        """
        for order_data in orders:
                order_guid = order_data.get("guid")
            # try:
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
                        "table_guid": order_data.get("table", {}).get("guid") if order_data.get("table") else None,
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
                    # Clear previous checks to avoid duplicates.
                    order.checks.all().delete()

                    total_order_net_sales = Decimal("0.00")
                    total_sales_without_deduction = Decimal("0.00")  # new accumulator for raw total
                    for check_data in order_data.get("checks", []):
                        # Calculate the raw check amount.
                        check_amount = Decimal(str(check_data.get("amount", "0.00")))
                        total_sales_without_deduction += check_amount  # add raw amount for toast_sales

                        tax_amount = Decimal(str(check_data.get("taxAmount", "0.00")))
                        tip_total = sum(Decimal(str(p.get("tipAmount", "0.00"))) for p in check_data.get("payments", []))
                        discretionary_total = sum(
                            Decimal(str(sc.get("chargeAmount", "0.00")))
                            for sc in check_data.get("appliedServiceCharges", [])
                            if sc.get("name", "").strip().lower() == "discretionary service charge"
                        )
                        final_check_net = check_amount + tax_amount + tip_total + discretionary_total
                        total_order_net_sales += final_check_net

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
                        check_defaults.update({
                            "display_number": check_data.get("displayNumber"),
                            "net_sales": final_check_net,
                            "service_charge_total": discretionary_total,
                            "discount_total": sum(Decimal(str(d.get("nonTaxDiscountAmount", "0.00"))) for d in check_data.get("appliedDiscounts", [])),
                            "opened_date": parse_datetime(check_data.get("openedDate")) if check_data.get("openedDate") else None,
                            "closed_date": parse_datetime(check_data.get("closedDate")) if check_data.get("closedDate") else None,
                        })
                        check_record = ToastCheck.objects.create(
                            tenant_id=self.integration.org.id,
                            order=order,
                            check_guid=check_data.get("guid"),
                            **check_defaults
                        )
                        
                        selection_instances = []
                        for selection_data in check_data.get("selections", []):
                            if selection_data.get("voided") or selection_data.get("displayName", "").strip().lower() == "gift card":
                                continue
                            pre_discount_price = Decimal(str(selection_data.get("preDiscountPrice", 0)))
                            discount_total = sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", 0)))
                                for d in selection_data.get("appliedDiscounts", [])
                            )
                            quantity = Decimal(str(selection_data.get("quantity", 1)))
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
                            selection_instance = ToastSelection(
                                tenant_id=self.integration.org.id,
                                selection_guid=selection_data.get("guid"),
                                display_name=selection_data.get("displayName"),
                                pre_discount_price=pre_discount_price,
                                discount_total=discount_total,
                                net_sales=selection_net,
                                quantity=quantity,
                                voided=selection_data.get("voided", False),
                                **selection_defaults
                            )
                            selection_instances.append(selection_instance)
                        
                        if selection_instances:
                            for selection in selection_instances:
                                selection.toast_check = check_record
                            ToastSelection.objects.bulk_create(selection_instances, batch_size=5000)
                    
                    # Save both the calculated net sales and the raw (undeducted) sales.
                    order.order_net_sales = total_order_net_sales
                    order.toast_sales = total_sales_without_deduction
                    order.save()
                    self.log_import_event(module_name="toast_orders", fetched_records=len(orders))
            # except Exception as e:
            #     logger.error("Error processing order %s: %s", order_guid, e)

    