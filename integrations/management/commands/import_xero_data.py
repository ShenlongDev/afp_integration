import logging
import os
from django.core.management.base import BaseCommand
from integrations.models.models import Integration
from integrations.services.xero.xero_client import import_xero_data
from datetime import datetime, date

class Command(BaseCommand):
    help = 'Import Xero data for specific integrations'

    def add_arguments(self, parser):
        parser.add_argument(
            'integration_id',
            type=int,
            nargs='?',
            help='The ID of the integration to import data for'
        )
        parser.add_argument(
            '--since',
            type=str,
            help='Optional date to filter data since (format: YYYY-MM-DD)'
        )

    def handle(self, *args, **options):
        integration_id = options.get('integration_id')
        since_date_str = options.get('since')

        # Set default since_date to today's date if not provided
        if since_date_str:
            try:
                since_date = datetime.strptime(since_date_str, "%Y-%m-%d")
            except ValueError:
                self.stdout.write(self.style.ERROR(f"Invalid date format: {since_date_str}. Expected YYYY-MM-DD."))
                return
        else:
            since_date = datetime.combine(date.today(), datetime.min.time())

        integrations = []
        
        if integration_id:
            try:
                integration = Integration.objects.get(pk=integration_id)
                if not (integration.xero_client_id and integration.xero_client_secret):
                    self.stdout.write(self.style.ERROR(f"Integration ID {integration_id} does not have Xero credentials set."))
                    return
                integrations.append(integration)
            except Integration.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Integration with ID {integration_id} does not exist."))
                return
        else:
            integrations = Integration.objects.filter(
                xero_client_id__isnull=False,
                xero_client_secret__isnull=False
            )
            if not integrations.exists():
                self.stdout.write(self.style.WARNING("No integrations found with Xero credentials."))

        for integration in integrations:
            self.stdout.write(f"Importing data for Integration ID: {integration.id}")
            try:
                import_xero_data(integration, since_date)
                self.stdout.write(self.style.SUCCESS(f"Xero data imported successfully for Integration ID: {integration.id}."))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error importing Xero data for Integration ID {integration.id}: {str(e)}"))
