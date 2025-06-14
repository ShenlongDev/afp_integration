import requests
import logging
from datetime import datetime, time , timedelta
from decimal import Decimal
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.db import transaction
from .auth import ToastAuthService
from integrations.models.toast.raw import (
    ToastOrder, ToastCheck, ToastSelection,
    ToastDaySchedule, ToastWeeklySchedule, 
    ToastJoinedOpeningHours, ToastRevenueCenter,
    ToastRestaurantService, ToastSalesCategory, ToastDiningOption, ToastServiceArea,
    ToastPayment, ToastRefund
)
from integrations.models.models import SyncTableLogs
from core.models import Site, IntegrationSiteMapping
import time as timeclock  

logger = logging.getLogger(__name__)

class ToastIntegrationService:
    """
    Provides utility methods for interacting with the Toast API.
    """
    def __init__(self, integration, start_date=None, end_date=None):
        self.integration = integration
        self.hostname = integration.settings.get('api_url')
        self.client_id = integration.settings.get('client_id')
        self.client_secret = integration.settings.get('client_secret')
        self.auth_service = ToastAuthService(self.hostname, self.client_id, self.client_secret)
        self.access_token = self.auth_service.login()
        self.start_date = start_date
        self.end_date = end_date

    def get_restaurant_guid(self):
        """Get all restaurant GUIDs associated with the current integration"""
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
                # Return all restaurant GUIDs instead of just the first one
                restaurant_guids = [restaurant.get("restaurantGuid") for restaurant in data if restaurant.get("restaurantGuid")]
                if restaurant_guids:
                    return restaurant_guids
                else:
                    logger.error("No restaurant GUIDs found in Toast response.")
                    return []
            else:
                logger.error("No restaurant data found in Toast response.")
                return []
        except requests.RequestException as e:
            logger.error("Failed to get restaurant GUIDs: %s", e)
            return []

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
        Fetches restaurant info (including schedules) for all restaurants from Toast and updates/creates:
         - Site,
         - IntegrationSiteMapping,
         - ToastDaySchedule,
         - ToastWeeklySchedule, and
         - ToastJoinedOpeningHours.
        """
        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")
        
        results = []
        for restaurant_guid in restaurant_guids:
            logger.info(f"Processing restaurant with GUID: {restaurant_guid}")
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

            # Create or update Site
            site_defaults = {
                "name": general.get("name"),
                "code": general.get("locationCode"),
                "description": general.get("description"),
                "postcode": location_data.get("zipCode"),
                "region": location_data.get("stateCode"),
                "address_line1": location_data.get("address1"),
                "address_line2": location_data.get("address2"),
                "city": location_data.get("city"),
                "state_code": location_data.get("stateCode"),
                "zip_code": location_data.get("zipCode"),
                "country": location_data.get("country"),
                "phone": location_data.get("phone"),
                "timezone": general.get("timeZone"),
                "currency_code": general.get("currencyCode"),
                "opened_date": self.convert_to_date(general.get("firstBusinessDate")),
                "status": "inactive" if general.get("archived", False) else "active",
            }
            
            # First try to find an existing site by name and organisation
            site = Site.objects.filter(
                organisation=self.integration.organisation,
                name=site_defaults["name"]
            ).first()
            
            if site:
                # Update existing site
                for key, value in site_defaults.items():
                    setattr(site, key, value)
                site.save()
            else:
                # Create new site
                site = Site.objects.create(
                    organisation=self.integration.organisation,
                    **site_defaults
                )

            # Create or update IntegrationSiteMapping
            mapping_defaults = {
                "site": site,
                "integration": self.integration,
                "external_id": restaurant_guid,
                "external_name": general.get("name"),
                "settings": {
                    "closeout_hour": general.get("closeoutHour"),
                    "management_group_guid": general.get("managementGroupGuid"),
                    "website": urls.get("website"),
                    "facebook": urls.get("facebook"),
                    "twitter": urls.get("twitter"),
                    "order_online": urls.get("orderOnline"),
                    "first_business_date": general.get("firstBusinessDate"),
                    "latitude": location_data.get("latitude"),
                    "longitude": location_data.get("longitude"),
                }
            }
            mapping, _ = IntegrationSiteMapping.objects.update_or_create(
                site=site,
                integration=self.integration,
                defaults=mapping_defaults
            )

            logger.info("Imported restaurant location: %s", site)

            # Process schedule data:
            schedules = data.get("schedules", {})
            day_schedules_data = schedules.get("daySchedules", {})
            week_schedule_data = schedules.get("weekSchedule", {})

            # Update/insert ToastDaySchedule records.
            day_schedule_map = {}
            for ds_guid, ds in day_schedules_data.items():
                ds_defaults = {
                    "tenant_id": self.integration.organisation.id,
                    "integration": self.integration,
                    "restaurant": site,  # Changed from location_obj to site
                    "guid": ds_guid,
                    "property_name": ds.get("scheduleName"),
                    "open_time": self.convert_to_time(ds.get("openTime")),
                    "close_time": self.convert_to_time(ds.get("closeTime")),
                    "schedule_name": ds.get("scheduleName"),
                }
                day_obj, _ = ToastDaySchedule.objects.update_or_create(
                    guid=ds_guid,
                    tenant_id=self.integration.organisation.id,
                    defaults=ds_defaults
                )
                day_schedule_map[ds_guid] = day_obj
            logger.info("Imported %d day schedules.", len(day_schedule_map))

            # Update/insert ToastWeeklySchedule record.
            weekly_defaults = {
                "tenant_id": self.integration.organisation.id,
                "integration": self.integration,
                "restaurant": site,  # Changed from location_obj to site
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
                restaurant=site,  # Changed from location_obj to site
                tenant_id=self.integration.organisation.id,
                defaults=weekly_defaults
            )
            logger.info("Imported weekly schedule for restaurant: %s", site)

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
                "tenant_id": self.integration.organisation.id,
                "integration": self.integration,
                "restaurant": site,  # Changed from location_obj to site
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
                restaurant=site,  # Changed from location_obj to site
                tenant_id=self.integration.organisation.id,
                defaults=joined_defaults
            )
            logger.info("Imported joined opening hours for restaurant: %s", site)
            results.append({
                "site": site,
                "mapping": mapping,
                "day_schedules": list(day_schedule_map.values()),
                "weekly_schedule": weekly_obj,
                "joined_opening_hours": joined_obj,
            })
        
        logger.info(f"Imported data for {len(results)} restaurants")
        return results

    def import_orders(self):
        """Import orders for all restaurants"""
        if self.start_date is None:
            self.start_date = timezone.now().date()
        start_date_str = self.format_date_for_toast(self.start_date)
        if not self.end_date:
            self.end_date = timezone.now()
        end_date_str = self.format_date_for_toast(self.end_date)

        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")

        all_orders = []
        for restaurant_guid in restaurant_guids:
            logger.info(f"Importing orders for restaurant with GUID: {restaurant_guid}")
            
            url = f"{self.hostname}/orders/v2/ordersBulk"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            }
            params = {
                "startDate": start_date_str ,
                "endDate": end_date_str
            }
            orders = []
            page = 1
            while True:
                params["page"] = page
                max_retries = 1
                retry_count = 0
                
                while retry_count <= max_retries:
                    try:
                        response = requests.get(url, headers=headers, params=params)
                        response.raise_for_status()
                        data = response.json()
                        orders_batch = data 
                        if not orders_batch:
                            break
                        
                        # Add restaurant_guid to each order
                        for order in orders_batch:
                            order["restaurant_guid"] = restaurant_guid
                            
                        orders.extend(orders_batch)
                        page += 1
                        # Success - break retry loop
                        break
                        
                    except requests.RequestException as e:
                        # Check specifically for 401 Unauthorized errors
                        if hasattr(e, 'response') and e.response and e.response.status_code == 401:
                            if retry_count < max_retries:
                                logger.warning(f"Received 401 Unauthorized error. Refreshing access token and retrying...")
                                # Refresh the token
                                self.access_token = self.auth_service.login()
                                # Update headers with new token
                                headers["Authorization"] = f"Bearer {self.access_token}"
                                retry_count += 1
                                continue
                        
                        # Either not a 401 error or retry failed
                        logger.error(f"Error fetching orders on page {page}: {e}")
                        break
                
                # If we exhausted our retries or hit a different error, break the pagination loop
                if retry_count > max_retries:
                    break
                
                # If orders_batch was empty, we've reached the end
                if not orders_batch:
                    break
            
            all_orders.extend(orders)
        
        self.process_orders(all_orders)
        return all_orders

    def log_import_event(self, module_name: str, fetched_records: int):
        SyncTableLogs.objects.create(
            module_name=module_name,
            integration='TOAST',
            organisation=self.integration.organisation,
            fetched_records=fetched_records,
            last_updated_time=timezone.now(),
            last_updated_date=timezone.now().date()
        )

    def process_orders(self, orders):
        
        for index, order_data in enumerate(orders):
            order_guid = order_data.get("guid", "unknown")            
            if order_data.get("refund"):
                print(f"Skipping refund order {order_guid}")
                continue
            
            try:
                all_payments = []
                for check_data in order_data.get("checks", []):
                    if check_data.get("payments"):
                        all_payments.extend(check_data.get("payments", []))

                order_defaults = {
                    "integration": self.integration,
                    "payload": {},
                    "order_net_sales": Decimal("0.00"),
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
                    "deleted": order_data.get("deleted", False),
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
                    "void_business_date": order_data.get("voidBusinessDate"),
                    "restaurant_guid": order_data.get("restaurant_guid"),
                    "payments": all_payments if all_payments else None,
                    "site": self.integration.organisation.sites.filter(integration_mappings__external_id=order_data.get("restaurant_guid")).first(),
                }
                    
                order, created = ToastOrder.objects.update_or_create(
                    order_guid=order_guid,
                    tenant_id=self.integration.organisation.id,
                    defaults=order_defaults
                )
                
                total_revenue = Decimal("0.00")  
                total_net_sales = Decimal("0.00")
                total_refund_amount = Decimal("0.00")
                total_tip_total = Decimal("0.00")    
                total_service_charge_total = Decimal("0.00")
                total_discount_amount = Decimal("0.00")
                discount_count = 0
                refund_business_date = None

                for check_index, check_data in enumerate(order_data.get("checks", [])):
                    if check_data.get("voided") or check_data.get("deleted") or check_data.get("refund"):
                        continue

                    check_discount_total = Decimal("0.00")
                    check_discount_count = 0
                    applied_discounts = check_data.get("appliedDiscounts", [])

                    if applied_discounts:
                        valid_discounts = [d for d in applied_discounts if d.get("processingState") != "VOID"]
                        check_discount_count = len(valid_discounts)
                        check_discount_total = sum(
                            Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                            for d in valid_discounts
                        )
                    else:
                        for selection_data in check_data.get("selections", []):
                            if selection_data.get("voided"):
                                continue
                                
                            selection_discounts = selection_data.get("appliedDiscounts", [])
                            valid_selection_discounts = [d for d in selection_discounts if d.get("processingState") != "VOID"]
                            
                            check_discount_count += len(valid_selection_discounts)
                            check_discount_total += sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                                for d in valid_selection_discounts
                            )
                    
                    total_discount_amount += check_discount_total
                    discount_count += check_discount_count

                    check_guid = check_data.get("guid")
                    check_subtotal = Decimal(str(check_data.get("amount", "0.00")))
                    tax_amount = Decimal(str(check_data.get("taxAmount", "0.00")))
                    tip_total = sum(
                        Decimal(str(p.get("tipAmount", "0.00")))
                        for p in check_data.get("payments", [])
                    )

                    service_charge_total = Decimal("0.00")
                    service_charge_total_exclude_refunds = Decimal("0.00")
                    
                    for sc in check_data.get("appliedServiceCharges", []):
                        charge_amount = Decimal(str(sc.get("chargeAmount", "0.00")))
                        service_charge_total += charge_amount
                        if not sc.get("refundDetails"):
                            service_charge_total_exclude_refunds += charge_amount
                    
                    total_tip_total += tip_total
                    total_service_charge_total += service_charge_total_exclude_refunds

                    check_revenue = check_subtotal + tax_amount + tip_total + service_charge_total
                    total_revenue += check_revenue
                    total_net_sales += check_subtotal

                    check_refund = Decimal("0.00")
                    for payment in check_data.get("payments", []):
                        if payment.get("refund"):
                            refund_amt = Decimal(str(payment.get("refund", {}).get("refundAmount", "0.00")))
                            tip_refund_amt = Decimal(str(payment.get("refund", {}).get("tipRefundAmount", "0.00")))
                            
                            check_refund += refund_amt
                            total_refund_amount += refund_amt 
                            rbd = payment.get("refund", {}).get("refundBusinessDate")
                            if rbd:
                                refund_business_date = rbd
                    
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
                        "discount_total": check_discount_total, 
                        "opened_date": parse_datetime(check_data.get("openedDate")) if check_data.get("openedDate") else None,
                        "closed_date": parse_datetime(check_data.get("closedDate")) if check_data.get("closedDate") else None,
                        "check_refund": check_refund
                    })

                    check_obj, _ = ToastCheck.objects.update_or_create(
                        check_guid=check_guid,
                        order=order,
                        tenant_id=self.integration.organisation.id,
                        defaults=check_defaults
                    )

                    selection_count = len(check_data.get("selections", []))
                    
                    selection_index = 0
                    for selection_data in check_data.get("selections", []):
                        try:
                            selection_index += 1
                            if (selection_data.get("voided") or 
                                selection_data.get("displayName", "").strip().lower() == "gift card" or 
                                selection_data.get("refund")):
                                continue
                            
                            selection_guid = selection_data.get("guid")
                            pre_discount_price = Decimal(str(selection_data.get("preDiscountPrice", "0.00")))
                            selection_discount_total = sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                                for d in selection_data.get("appliedDiscounts", [])
                            )
                            quantity = Decimal(str(selection_data.get("quantity", "1")))
                            selection_net = (pre_discount_price - selection_discount_total) * quantity

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
                         

                            try:
                                selection_obj = ToastSelection.objects.get(
                                    selection_guid=selection_guid,
                                    tenant_id=self.integration.organisation.id
                                )
                                for key, value in selection_defaults.items():
                                    setattr(selection_obj, key, value)
                                selection_obj.order_guid = order_guid
                                selection_obj.toast_check = check_obj 
                                selection_obj.display_name = selection_data.get("displayName")
                                selection_obj.pre_discount_price = pre_discount_price
                                selection_obj.discount_total = selection_discount_total 
                                selection_obj.net_sales = selection_net 
                                selection_obj.quantity = quantity
                                selection_obj.business_date = order_data["businessDate"]
                                selection_obj.save()
                            except ToastSelection.DoesNotExist:
                                ToastSelection.objects.create(
                                    selection_guid=selection_guid,
                                    toast_check=check_obj,
                                    tenant_id=self.integration.organisation.id,
                                    order_guid=order_guid,
                                    display_name=selection_data.get("displayName"),
                                    pre_discount_price=pre_discount_price,
                                    discount_total=selection_discount_total,
                                    net_sales=selection_net,
                                    quantity=quantity,
                                    business_date=order_data["businessDate"],
                                    **selection_defaults
                                )
                            

                        except Exception as e:
                            print(f"Error processing selection {selection_index} in check {check_index+1}: {str(e)}")
                            logger.error(f"Error processing selection in order {order_guid}: {e}", exc_info=True)
                
                business_date = order_data.get("businessDate")
                if refund_business_date and business_date and str(refund_business_date) == str(business_date):
                    total_revenue -= total_refund_amount
                    
                    total_net_sales -= total_refund_amount
                    
                    if total_net_sales < Decimal("0.00"):
                        total_net_sales = Decimal("0.00")
                
                order.tip = total_tip_total 
                order.service_charges = total_service_charge_total
                order.toast_sales = total_revenue 
                order.total_amount = total_revenue 
                order.order_net_sales = total_net_sales
                order.total_refunds = total_refund_amount 
                order.total_discount_amount = total_discount_amount 
                order.discount_count = discount_count
                if refund_business_date:
                    order.refund_business_date = refund_business_date
                order.save()

                order.payload = order_data
                order.save(update_fields=['payload'])

            except Exception as e:
                print(f"ERROR processing order {order_guid}: {str(e)}")
                logger.error(f"Error processing order {order_guid}: {e}", exc_info=True)
                continue
        
        print(f"Finished processing all {len(orders)} orders")
        logger.info(f"Imported {len(orders)} orders")
        self.log_import_event(module_name="toast_orders", fetched_records=len(orders))


    def import_revenue_centers(self):
        """
        Import revenue centers for all restaurants from Toast API
        """
        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")
        
        total_centers = 0
        
        for restaurant_guid in restaurant_guids:
            logger.info(f"Importing revenue centers for restaurant with GUID: {restaurant_guid}")
            
            url = f"{self.hostname}/config/v2/revenueCenters/"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            }
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                centers = response.json()
                
                for center in centers:
                    center_guid = center.get("guid")
                    if not center_guid:
                        continue
                        
                    ToastRevenueCenter.objects.update_or_create(
                        revenue_center_guid=center_guid,
                        tenant_id=self.integration.organisation.id,
                        defaults={
                            "integration": self.integration,
                            "restaurant_guid": restaurant_guid,
                            "name": center.get("name"),
                            "description": center.get("description"),
                            "entity_type": center.get("entityType")
                        }
                    )
                    
                total_centers += len(centers)
                logger.info(f"Imported {len(centers)} revenue centers for restaurant {restaurant_guid}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching revenue centers for restaurant {restaurant_guid}: {e}")
        
        self.log_import_event(module_name="toast_revenue_centers", fetched_records=total_centers)
        return total_centers

    def import_restaurant_services(self):
        """
        Import restaurant services for all restaurants from Toast API
        """
        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")
        
        total_services = 0
        
        for restaurant_guid in restaurant_guids:
            logger.info(f"Importing restaurant services for restaurant with GUID: {restaurant_guid}")
            
            url = f"{self.hostname}/config/v2/restaurantServices/"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            }
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                services = response.json()
                
                for service in services:
                    service_guid = service.get("guid")
                    if not service_guid:
                        continue
                        
                    ToastRestaurantService.objects.update_or_create(
                        service_guid=service_guid,
                        tenant_id=self.integration.organisation.id,
                        defaults={
                            "integration": self.integration,
                            "restaurant_guid": restaurant_guid,
                            "name": service.get("name"),
                            "entity_type": service.get("entityType")
                        }
                    )
                    
                total_services += len(services)
                logger.info(f"Imported {len(services)} restaurant services for restaurant {restaurant_guid}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching restaurant services for restaurant {restaurant_guid}: {e}")
        
        self.log_import_event(module_name="toast_restaurant_services", fetched_records=total_services)
        return total_services

    def import_sales_categories(self):
        """
        Import sales categories for all restaurants from Toast API
        """
        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")
        
        total_categories = 0
        
        for restaurant_guid in restaurant_guids:
            logger.info(f"Importing sales categories for restaurant with GUID: {restaurant_guid}")
            
            url = f"{self.hostname}/config/v2/salesCategories"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            }
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                categories = response.json()
                
                for category in categories:
                    category_guid = category.get("guid")
                    if not category_guid:
                        continue
                        
                    ToastSalesCategory.objects.update_or_create(
                        category_guid=category_guid,
                        tenant_id=self.integration.organisation.id,
                        defaults={
                            "integration": self.integration,
                            "restaurant_guid": restaurant_guid,
                            "name": category.get("name"),
                            "entity_type": category.get("entityType")
                        }
                    )
                    
                total_categories += len(categories)
                logger.info(f"Imported {len(categories)} sales categories for restaurant {restaurant_guid}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching sales categories for restaurant {restaurant_guid}: {e}")
        
        self.log_import_event(module_name="toast_sales_categories", fetched_records=total_categories)
        return total_categories

    def import_dining_options(self):
        """
        Import dining options for all restaurants from Toast API
        """
        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")
        
        total_options = 0
        
        for restaurant_guid in restaurant_guids:
            logger.info(f"Importing dining options for restaurant with GUID: {restaurant_guid}")
            
            url = f"{self.hostname}/config/v2/diningOptions"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            }
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                options = response.json()
                
                for option in options:
                    option_guid = option.get("guid")
                    if not option_guid:
                        continue
                        
                    ToastDiningOption.objects.update_or_create(
                        option_guid=option_guid,
                        tenant_id=self.integration.organisation.id,
                        defaults={
                            "integration": self.integration,
                            "restaurant_guid": restaurant_guid,
                            "name": option.get("name"),
                            "entity_type": option.get("entityType"),
                            "external_id": option.get("externalId"),
                            "behavior": option.get("behavior"),
                            "curbside": option.get("curbside", False)
                        }
                    )
                    
                total_options += len(options)
                logger.info(f"Imported {len(options)} dining options for restaurant {restaurant_guid}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching dining options for restaurant {restaurant_guid}: {e}")
        
        self.log_import_event(module_name="toast_dining_options", fetched_records=total_options)
        return total_options

    def import_service_areas(self):
        """
        Import service areas for all restaurants from Toast API and update revenue centers
        """
        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")
        
        total_areas = 0
        
        for restaurant_guid in restaurant_guids:
            logger.info(f"Importing service areas for restaurant with GUID: {restaurant_guid}")
            
            url = f"{self.hostname}/config/v2/serviceAreas"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Toast-Restaurant-External-ID": restaurant_guid
            }
            
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                areas = response.json()
                
                for area in areas:
                    area_guid = area.get("guid")
                    if not area_guid:
                        continue
                    
                    # Extract revenue center info if present
                    revenue_center_guid = None
                    if area.get("revenueCenter") and area["revenueCenter"].get("guid"):
                        revenue_center_guid = area["revenueCenter"]["guid"]
                        
                        # Update or create the revenue center
                        ToastRevenueCenter.objects.update_or_create(
                            revenue_center_guid=revenue_center_guid,
                            tenant_id=self.integration.organisation.id,
                            defaults={
                                "integration": self.integration,
                                "restaurant_guid": restaurant_guid,
                                "entity_type": area["revenueCenter"].get("entityType"),
                                "name": area["revenueCenter"].get("name", "Unknown")
                            }
                        )
                        
                    # Create or update the service area
                    ToastServiceArea.objects.update_or_create(
                        area_guid=area_guid,
                        tenant_id=self.integration.organisation.id,
                        defaults={
                            "integration": self.integration,
                            "restaurant_guid": restaurant_guid,
                            "name": area.get("name"),
                            "entity_type": area.get("entityType"),
                            "revenue_center_guid": revenue_center_guid
                        }
                    )
                    
                total_areas += len(areas)
                logger.info(f"Imported {len(areas)} service areas for restaurant {restaurant_guid}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching service areas for restaurant {restaurant_guid}: {e}")
        
        self.log_import_event(module_name="toast_service_areas", fetched_records=total_areas)
        return total_areas

    def import_payment_details(self):
        """
        Import payment details for orders that have payment data
        """
        query_filters = {
            'integration': self.integration,
            'tenant_id': self.integration.organisation.id,
            'payments__isnull': False,
            'business_date': self.start_date.strftime("%Y%m%d")
        }
        orders_with_payments = ToastOrder.objects.filter(**query_filters)
        
        total_payments = 0
        
        for order in orders_with_payments:
            restaurant_guid = order.restaurant_guid
            if not restaurant_guid:
                logger.warning(f"Order {order.order_guid} has no restaurant GUID, skipping payment import")
                continue
            
            payments = order.payments   
            if not payments:
                continue
            
            for payment_data in payments:
                payment_guid = payment_data.get("guid")
                if not payment_guid:
                    continue
                
                # Basic data from order payment data
                check_guid = payment_data.get("checkGuid")
                amount = Decimal(str(payment_data.get("amount", "0.00")))
                tip_amount = Decimal(str(payment_data.get("tipAmount", "0.00")))
                
                # Try to get detailed payment info from API
                detailed_payment = self.get_payment_details(restaurant_guid, payment_guid)
                
                # Use the most detailed data available (API response or embedded data)
                payment_info = detailed_payment if detailed_payment else payment_data
                
                payment_defaults = {
                    "integration": self.integration,
                    "restaurant_guid": restaurant_guid,
                    "order_guid": order.order_guid,
                    "check_guid": check_guid,
                    "type": payment_info.get("type"),
                    "amount": amount,
                    "tip_amount": tip_amount,
                    "card_type": payment_info.get("cardType"),
                    "last4_digits": payment_info.get("last4Digits"),
                    "paid_date": parse_datetime(payment_info.get("paidDate")) if payment_info.get("paidDate") else None,
                    "paid_business_date": payment_info.get("paidBusinessDate"),
                    "refund_status": payment_info.get("refundStatus"),
                    "payment_status": payment_info.get("paymentStatus"),
                    "card_entry_mode": payment_info.get("cardEntryMode"),
                    "server_guid": payment_info.get("server", {}).get("guid") if payment_info.get("server") else None,
                    "created_device_id": payment_info.get("createdDevice", {}).get("id") if payment_info.get("createdDevice") else None,
                    "last_modified_device_id": payment_info.get("lastModifiedDevice", {}).get("id") if payment_info.get("lastModifiedDevice") else None,
                    "raw_payload": payment_info
                }
                
                ToastPayment.objects.update_or_create(
                    payment_guid=payment_guid,
                    tenant_id=self.integration.organisation.id,
                    defaults=payment_defaults
                )
                
                total_payments += 1
            
        self.log_import_event(module_name="toast_payments", fetched_records=total_payments)
        return total_payments

    def get_payment_details(self, restaurant_guid, payment_guid):
        """
        Get detailed payment information from Toast API
        """
        url = f"{self.hostname}/orders/v2/payments/{payment_guid}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Toast-Restaurant-External-ID": restaurant_guid
        }
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching payment details for {payment_guid}: {e}")
            return None



    def import_orders_v2(self):
        start_date = datetime.now() - timedelta(days=1)
        end_date = datetime.now()
        business_date = self.start_date.strftime("%Y%m%d")


        if self.start_date is None:
            self.start_date = start_date
        if self.end_date is None:
            self.end_date = end_date
        start_date_str = self.format_date_for_toast(self.start_date - timedelta(hours=12))
        end_date_str = self.format_date_for_toast(self.end_date + timedelta(days=1))

        restaurant_guids = self.get_restaurant_guid()
        if not restaurant_guids:
            raise Exception("No restaurant GUIDs found.")




        print(f"Fetching orders from {start_date_str} to {end_date_str}")

        print(f"Business date: {business_date}")

        
        for restaurant_guid in restaurant_guids:
            page_number = 1
            orders = []
            while True:
                print(f"Fetching orders for restaurant with GUID: {restaurant_guid}")
                response = requests.get(
                    f"{self.hostname}/orders/v2/ordersBulk",
                    headers={
                        "Authorization": f"Bearer {self.access_token}",
                        "Toast-Restaurant-External-ID": restaurant_guid
                    },
                    params={
                        # "businessDate": "20250504",
                        "startDate": start_date_str,
                        "endDate": end_date_str,
                        "pageSize": 100,
                        "page": page_number,
                    }
                )

                # print(f"Response: {response.status_code}")



                if response.status_code != 200:
                    print(f"Error fetching orders: {response.text}")
                    break

                resp_orders = response.json()

                if not resp_orders:
                    print("No more orders to fetch.")
                    break

                orders.extend(resp_orders)
                page_number += 1
                print(f"Fetched {len(resp_orders)} orders for page {page_number}. Total Orders: {len(orders)}")

            
            orders_by_date = {}

            for order in orders:

                if  order.get("voided") == False and order.get("deleted") == False:

                    order_business_date = order.get("businessDate")
                    net_amount = Decimal( "0.00")

                    if order_business_date not in orders_by_date:
                        orders_by_date[order_business_date] = {"Covers": order.get("numberOfGuests"), 'NetAmount': 0 }
                    else:
                        orders_by_date[order_business_date]['Covers'] += order.get("numberOfGuests")
                
                    
                    for check_data in order.get("checks", []):
                        net_amount += Decimal(str(check_data.get("amount", "0.00")))

                    
                    orders_by_date[order_business_date]["NetAmount"] += net_amount

            
                print(order.get("displayNumber"))

                if order.get("displayNumber") == '124':
                    #save the order to json
                    with open('order.json', 'w') as f:
                        import json
                        json.dump(order, f, indent=4)
                        print(f"Order {order.get('displayNumber')} saved to order.json")




            print(f"Orders by date: {orders_by_date}")

            self.process_orders_v2(orders,restaurant_guid=restaurant_guid)

            return orders


    def process_orders_v2(self,orders,restaurant_guid):
        
        process_count = 1

        for order_data in orders:
            order_guid = order_data.get("guid")
            order_data['restaurant_guid'] = restaurant_guid
            if not order_guid:
                continue

            net_sales = Decimal("0.00")
            total_discount_amount = Decimal("0.00")
            discount_count = 0
            total_tip_total = Decimal("0.00")
            total_service_charge_total = Decimal("0.00")
            total_refund_amount = Decimal("0.00")
            total_revenue = Decimal("0.00")



            for check_data in order_data.get("checks", []):
                net_sales += Decimal(str(check_data.get("amount", "0.00"))) 

                check_discount_total = Decimal("0.00")
                check_discount_count = 0

                for selection_data in check_data.get("selections", []):
                    if selection_data.get("voided"):
                        continue
                    

                    selection_discounts = selection_data.get("appliedDiscounts", [])
                    valid_selection_discounts = [d for d in selection_discounts if d.get("processingState") != "VOID"]
                            
                    check_discount_count += len(valid_selection_discounts)
                    check_discount_total += sum(
                        Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                        for d in valid_selection_discounts
                    )
                
                total_discount_amount += check_discount_total
                discount_count += check_discount_count
                check_subtotal = Decimal(str(check_data.get("amount", "0.00")))
                tax_amount = Decimal(str(check_data.get("taxAmount", "0.00")))
                tip_total = sum(
                    Decimal(str(p.get("tipAmount", "0.00")))
                    for p in check_data.get("payments", [])
                )

                service_charge_total = Decimal("0.00")
                service_charge_total_exclude_refunds = Decimal("0.00")
                
                for sc in check_data.get("appliedServiceCharges", []):
                    charge_amount = Decimal(str(sc.get("chargeAmount", "0.00")))
                    service_charge_total += charge_amount
                    if not sc.get("refundDetails"):
                        service_charge_total_exclude_refunds += charge_amount
                
                total_tip_total += tip_total
                total_service_charge_total += service_charge_total_exclude_refunds

                check_revenue = check_subtotal + tax_amount + tip_total + service_charge_total
                total_revenue += check_revenue

                check_refund = Decimal("0.00")
                for payment in check_data.get("payments", []):
                    if payment.get("refund"):
                        refund_amt = Decimal(str(payment.get("refund", {}).get("refundAmount", "0.00")))
                        tip_refund_amt = Decimal(str(payment.get("refund", {}).get("tipRefundAmount", "0.00")))
                        
                        check_refund += refund_amt
                        total_refund_amount += refund_amt 
                        rbd = payment.get("refund", {}).get("refundBusinessDate")
                        if rbd:
                            refund_business_date = rbd
                


            order_defaults = {
                    "integration": self.integration, 
                    "payload": {},
                    "order_net_sales":net_sales,
                    "import_id": self.integration.id,
                    'business_date': order_data.get("businessDate"),
                    "order_guid": order_guid,
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
                    "paid_date": parse_datetime(order_data.get("paidDate")) if order_data.get("paidDate") else None,
                    "restaurant_service_guid": order_data.get("restaurantService", {}).get("guid") if order_data.get("restaurantService") else None,
                    "excess_food": order_data.get("excessFood"),
                    "voided": order_data.get("voided"),
                    "deleted": order_data.get("deleted", False),
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
                    "void_business_date": order_data.get("voidBusinessDate"),
                    "restaurant_guid": order_data.get("restaurant_guid"),
                    "payments": None,
                    "site": self.integration.organisation.sites.filter(integration_mappings__external_id=order_data.get("restaurant_guid")).first(),
                    "tip": total_tip_total,
                    "service_charges": total_service_charge_total,
                    "toast_sales": total_revenue,
                    "total_amount": total_revenue,
                    "total_refunds": total_refund_amount,
                    "total_discount_amount": total_discount_amount,
                    "discount_count": discount_count,
                    "refund_business_date": refund_business_date if 'refund_business_date' in locals() else None,
                }
            
            # print(f"restaurant guid: {order_data.get('restaurant_guid')}")
            try:
                order_update, created = ToastOrder.objects.update_or_create(
                    order_guid=order_guid,
                    tenant_id=self.integration.organisation.id,
                    defaults=order_defaults
                )

                if created:
                   print(f"Created {process_count} order with GUID: {order_guid} and net sales: {net_sales} business date: {order_data.get('businessDate')}")
                else:
                    # print(order_update)
                    print(f"Updated {process_count} order with GUID: {order_guid} and net sales: {net_sales} business date: {order_data.get('businessDate')}")


                self.process_checks_v2(order_data, order_update, restaurant_guid)

                process_count += 1
            except Exception as e:
                print(f"Error processing order {order_guid}: {str(e)}")
                logger.error(f"Error processing order {order_guid}: {e}", exc_info=True)
                continue







    def process_checks_v2(self,order_data,order,restaurant_guid):
        
        order_guid = order_data.get("guid")

        total_revenue = Decimal("0.00")  
        total_net_sales = Decimal("0.00")
        total_refund_amount = Decimal("0.00")
        total_tip_total = Decimal("0.00")    
        total_service_charge_total = Decimal("0.00")
        total_discount_amount = Decimal("0.00")
        discount_count = 0

        for check_index, check_data in enumerate(order_data.get("checks", [])):
                    if check_data.get("voided") or check_data.get("deleted") or check_data.get("refund"):
                        continue

                    check_discount_total = Decimal("0.00")
                    check_discount_count = 0
                    applied_discounts = check_data.get("appliedDiscounts", [])

                    if applied_discounts:
                        valid_discounts = [d for d in applied_discounts if d.get("processingState") != "VOID"]
                        check_discount_count = len(valid_discounts)
                        check_discount_total = sum(
                            Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                            for d in valid_discounts
                        )
                    else:
                        for selection_data in check_data.get("selections", []):
                            if selection_data.get("voided"):
                                continue
                                
                            selection_discounts = selection_data.get("appliedDiscounts", [])
                            valid_selection_discounts = [d for d in selection_discounts if d.get("processingState") != "VOID"]
                            
                            check_discount_count += len(valid_selection_discounts)
                            check_discount_total += sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                                for d in valid_selection_discounts
                            )
                    
                    total_discount_amount += check_discount_total
                    discount_count += check_discount_count

                    check_guid = check_data.get("guid")
                    check_subtotal = Decimal(str(check_data.get("amount", "0.00")))
                    tax_amount = Decimal(str(check_data.get("taxAmount", "0.00")))
                    tip_total = sum(
                        Decimal(str(p.get("tipAmount", "0.00")))
                        for p in check_data.get("payments", [])
                    )

                    service_charge_total = Decimal("0.00")
                    service_charge_total_exclude_refunds = Decimal("0.00")
                    
                    for sc in check_data.get("appliedServiceCharges", []):
                        charge_amount = Decimal(str(sc.get("chargeAmount", "0.00")))
                        service_charge_total += charge_amount
                        if not sc.get("refundDetails"):
                            service_charge_total_exclude_refunds += charge_amount
                    
                    total_tip_total += tip_total
                    total_service_charge_total += service_charge_total_exclude_refunds

                    check_revenue = check_subtotal + tax_amount + tip_total + service_charge_total
                    total_revenue += check_revenue
                    total_net_sales += check_subtotal

                    check_refund = Decimal("0.00")
                    for payment in check_data.get("payments", []):
                        if payment.get("refund"):

                            refund_defaults = {
                                "order_guid": order_guid,
                                "refund_amount": Decimal(str(payment.get("refund", {}).get("refundAmount", "0.00"))),
                                "tip_refund_amount": Decimal(str(payment.get("refund", {}).get("tipRefundAmount", "0.00"))),
                                "refund_business_date": payment.get("refund", {}).get("refundBusinessDate"),
                                "refund_date": parse_datetime(payment.get("refund", {}).get("refundDate")) if payment.get("refund", {}).get("refundDate") else None,
                            }

                            refund = ToastRefund.objects.update_or_create(
                                order_guid=order_guid,
                                check_guid=check_guid,
                                refund_transaction_guid=payment.get("refund", {}).get("refundTransaction", {}).get("guid"),
                                refund_transaction_entity_type = payment.get("refund", {}).get("refundTransaction", {}).get("entityType"),
                                payment_guid=payment.get("guid"),
                                tenant_id=self.integration.organisation.id,
                                restaurant_guid=restaurant_guid,
                                
                                defaults=refund_defaults
                            )

                            refund_amt = Decimal(str(payment.get("refund", {}).get("refundAmount", "0.00")))
                            tip_refund_amt = Decimal(str(payment.get("refund", {}).get("tipRefundAmount", "0.00")))
                            
                            check_refund += refund_amt
                            total_refund_amount += refund_amt 
                            rbd = payment.get("refund", {}).get("refundBusinessDate")
                            if rbd:
                                refund_business_date = rbd
                    
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
                        "discount_total": check_discount_total, 
                        "opened_date": parse_datetime(check_data.get("openedDate")) if check_data.get("openedDate") else None,
                        "closed_date": parse_datetime(check_data.get("closedDate")) if check_data.get("closedDate") else None,
                        "check_refund": check_refund
                    })

                    check_obj, _ = ToastCheck.objects.update_or_create(
                        check_guid=check_guid,
                        order=order,
                        tenant_id=self.integration.organisation.id,
                        defaults=check_defaults
                    )

                    selection_count = len(check_data.get("selections", []))
                    
                    selection_index = 0
                    for selection_data in check_data.get("selections", []):
                        try:
                            selection_index += 1
                            if (selection_data.get("voided") or 
                                selection_data.get("displayName", "").strip().lower() == "gift card" or 
                                selection_data.get("refund")):
                                continue
                            
                            selection_guid = selection_data.get("guid")
                            pre_discount_price = Decimal(str(selection_data.get("preDiscountPrice", "0.00")))
                            selection_discount_total = sum(
                                Decimal(str(d.get("nonTaxDiscountAmount", "0.00")))
                                for d in selection_data.get("appliedDiscounts", [])
                            )
                            quantity = Decimal(str(selection_data.get("quantity", "1")))
                            selection_net = (pre_discount_price - selection_discount_total) * quantity

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
                         

                            try:
                                selection_obj = ToastSelection.objects.get(
                                    selection_guid=selection_guid,
                                    tenant_id=self.integration.organisation.id
                                )
                                for key, value in selection_defaults.items():
                                    setattr(selection_obj, key, value)
                                selection_obj.order_guid = order_guid
                                selection_obj.toast_check = check_obj 
                                selection_obj.display_name = selection_data.get("displayName")
                                selection_obj.pre_discount_price = pre_discount_price
                                selection_obj.discount_total = selection_discount_total 
                                selection_obj.net_sales = selection_net 
                                selection_obj.quantity = quantity
                                selection_obj.business_date = order_data["businessDate"]
                                selection_obj.save()
                            except ToastSelection.DoesNotExist:
                                ToastSelection.objects.create(
                                    selection_guid=selection_guid,
                                    toast_check=check_obj,
                                    tenant_id=self.integration.organisation.id,
                                    order_guid=order_guid,
                                    display_name=selection_data.get("displayName"),
                                    pre_discount_price=pre_discount_price,
                                    discount_total=selection_discount_total,
                                    net_sales=selection_net,
                                    quantity=quantity,
                                    business_date=order_data["businessDate"],
                                    **selection_defaults
                                )
                            

                        except Exception as e:
                            print(f"Error processing selection {selection_index} in check {check_index+1}: {str(e)}")
                            logger.error(f"Error processing selection in order {order_guid}: {e}", exc_info=True)
                
