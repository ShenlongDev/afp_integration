from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import datetime, date
import logging
from integrations.models.models import Integration
from integrations.services.toast.client import ToastIntegrationService

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import Toast orders for a specific Toast integration.'

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
                service = ToastIntegrationService(integration)
                orders = service.import_orders(since_date, until_date)
                restaurant_info = service.import_restaurant_and_schedule_data()
                self.stdout.write(self.style.SUCCESS(f"Imported restaurant info for integration ID {integration.id}"))
                self.stdout.write(self.style.SUCCESS(f"Imported day schedules for integration ID {integration.id}"))
                self.stdout.write(self.style.SUCCESS(f"Imported weekly schedule for integration ID {integration.id}"))
                self.stdout.write(self.style.SUCCESS(f"Imported joined opening hours for integration ID {integration.id}"))
                self.stdout.write(self.style.SUCCESS(f"Imported {len(orders)} orders for integration ID {integration.id}"))
            except Exception as e:
                logger.error("Error importing Toast orders for integration ID %s: %s", integration.id, e, exc_info=True)
                self.stdout.write(self.style.ERROR(f"Error importing orders for integration ID {integration.id}: {str(e)}"))
