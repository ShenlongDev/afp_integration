import logging
import os
from django.core.management.base import BaseCommand
from integrations.models.models import Integration
from integrations.services.xero.xero_client import import_xero_data
from datetime import datetime

class Command(BaseCommand):
    help = 'Import Xero data for a specific integration'

    def add_arguments(self, parser):
        parser.add_argument('integration_id', type=int, help='The ID of the integration to import data for')
        parser.add_argument('--since', type=str, help='Optional date to filter data since (format: YYYY-MM-DD)')

    def handle(self, *args, **options):
        integration_id = options['integration_id']
        since_date = options.get('since')

        # Fetch the integration
        integration = Integration.objects.get(pk=integration_id)

        if not (integration.xero_client_id and integration.xero_client_secret):
            self.stdout.write(self.style.ERROR("Xero credentials not fully set."))
            return

        # Parse the since_date if provided
        if since_date:
            try:
                since_date = datetime.strptime(since_date, "%Y-%m-%d")
            except ValueError:
                self.stdout.write(self.style.ERROR(f"Invalid date: {since_date}"))
                return

        # Call the import function
        try:
            import_xero_data(integration, since_date)
            self.stdout.write(self.style.SUCCESS("Xero data imported successfully."))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error importing Xero data: {str(e)}"))
