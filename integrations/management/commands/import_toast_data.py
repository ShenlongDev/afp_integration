from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime, date
import logging
from integrations.models.models import Integration
from integrations.services.toast.client import ToastIntegrationService

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import Toast data for a specific Toast integration.'

    def add_arguments(self, parser):
        parser.add_argument(
            'integration_id',
            type=int,
            nargs='?',
            help='The ID of the Toast integration to import data for'
        )
        parser.add_argument(
            '--since',
            type=str,
            help='Optional start date (YYYY-MM-DD) to import orders from'
        )
        parser.add_argument(
            '--until',
            type=str,
            help='Optional end date (YYYY-MM-DD) to import orders until',
            default=None
        )
        # Add module selection arguments
        parser.add_argument(
            '--orders',
            action='store_true',
            help='Import orders data'
        )
        parser.add_argument(
            '--restaurants',
            action='store_true',
            help='Import restaurant and schedule data'
        )
        parser.add_argument(
            '--revenue-centers',
            action='store_true',
            help='Import revenue centers data'
        )
        parser.add_argument(
            '--restaurant-services',
            action='store_true',
            help='Import restaurant services data'
        )
        parser.add_argument(
            '--sales-categories',
            action='store_true',
            help='Import sales categories data'
        )
        parser.add_argument(
            '--dining-options',
            action='store_true',
            help='Import dining options data'
        )
        parser.add_argument(
            '--service-areas',
            action='store_true',
            help='Import service areas data'
        )
        parser.add_argument(
            '--payments',
            action='store_true',
            help='Import payments data'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Import all available data types (default if no specific modules are selected)'
        )

    def handle(self, *args, **options):
        integration_id = options.get('integration_id')
        since_date_str = options.get('since')
        until_date_str = options.get('until')

        if since_date_str:
            try:
                since_date = datetime.strptime(since_date_str, "%Y-%m-%d")
            except ValueError:
                self.stdout.write(self.style.ERROR(f"Invalid --since date format: {since_date_str}"))
                return
        else:
            since_date = datetime.combine(date.today(), datetime.min.time())

        until_date = None
        if until_date_str:
            try:
                until_date = datetime.strptime(until_date_str, "%Y-%m-%d")
            except ValueError:
                self.stdout.write(self.style.ERROR(f"Invalid --until date format: {until_date_str}"))
                return

        # Determine which modules to run
        run_orders = options.get('orders')
        run_restaurants = options.get('restaurants')
        run_revenue_centers = options.get('revenue_centers')
        run_restaurant_services = options.get('restaurant_services')
        run_sales_categories = options.get('sales_categories')
        run_dining_options = options.get('dining_options')
        run_service_areas = options.get('service_areas')
        run_payments = options.get('payments')
        run_all = options.get('all')

        # If no specific modules are selected, run all
        if not (run_orders or run_restaurants or run_revenue_centers or 
                run_restaurant_services or run_sales_categories or 
                run_dining_options or run_service_areas or run_payments):
            run_all = True

        integrations = []
        if integration_id:
            try:
                integration = Integration.objects.get(pk=integration_id)
                integrations.append(integration)
            except Integration.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Integration with ID {integration_id} does not exist."))
                return
        else:
            integrations = Integration.objects.filter(
                toast_client_id__isnull=False,
                toast_client_secret__isnull=False
            )
            if not integrations.exists():
                self.stdout.write(self.style.WARNING("No Toast integrations found."))
                return

        for integration in integrations:
            self.stdout.write(f"Processing Toast Integration ID: {integration.id}")
            try:
                service = ToastIntegrationService(integration, since_date, until_date)
                
                # Import orders if selected
                if run_all or run_orders:
                    self.stdout.write(f"Importing orders for integration ID {integration.id}...")
                    orders = service.import_orders()
                    self.stdout.write(self.style.SUCCESS(f"Imported {len(orders)} orders for integration ID {integration.id}"))
                
                # Import restaurant data if selected
                if run_all or run_restaurants:
                    self.stdout.write(f"Importing restaurant data for integration ID {integration.id}...")
                    restaurant_info = service.import_restaurant_and_schedule_data()
                    self.stdout.write(self.style.SUCCESS(f"Imported restaurant info, day schedules, weekly schedule, and opening hours for integration ID {integration.id}"))
                
                # Import revenue centers if selected
                if run_all or run_revenue_centers:
                    self.stdout.write(f"Importing revenue centers for integration ID {integration.id}...")
                    revenue_centers = service.import_revenue_centers()
                    self.stdout.write(self.style.SUCCESS(f"Imported revenue centers for integration ID {integration.id}"))
                
                # Import restaurant services if selected
                if run_all or run_restaurant_services:
                    self.stdout.write(f"Importing restaurant services for integration ID {integration.id}...")
                    services_count = service.import_restaurant_services()
                    self.stdout.write(self.style.SUCCESS(f"Imported {services_count} restaurant services for integration ID {integration.id}"))
                
                # Import sales categories if selected
                if run_all or run_sales_categories:
                    self.stdout.write(f"Importing sales categories for integration ID {integration.id}...")
                    categories_count = service.import_sales_categories()
                    self.stdout.write(self.style.SUCCESS(f"Imported {categories_count} sales categories for integration ID {integration.id}"))
                
                # Import dining options if selected
                if run_all or run_dining_options:
                    self.stdout.write(f"Importing dining options for integration ID {integration.id}...")
                    options_count = service.import_dining_options()
                    self.stdout.write(self.style.SUCCESS(f"Imported {options_count} dining options for integration ID {integration.id}"))
                
                # Import service areas if selected
                if run_all or run_service_areas:
                    self.stdout.write(f"Importing service areas for integration ID {integration.id}...")
                    areas_count = service.import_service_areas()
                    self.stdout.write(self.style.SUCCESS(f"Imported {areas_count} service areas for integration ID {integration.id}"))
                
                if run_all or run_payments:
                    self.stdout.write(f"Importing payments for integration ID {integration.id}...")
                    payments_count = service.import_payment_details()
                    self.stdout.write(self.style.SUCCESS(f"Imported {payments_count} payments for integration ID {integration.id}"))
            except Exception as e:
                logger.error("Error importing Toast data for integration ID %s: %s", integration.id, e, exc_info=True)
                self.stdout.write(self.style.ERROR(f"Error importing data for integration ID {integration.id}: {str(e)}"))
