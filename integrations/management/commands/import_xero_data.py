import logging
import os
from django.core.management.base import BaseCommand
from integrations.models.models import Integration
from integrations.services.xero.xero_client import XeroDataImporter
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
        parser.add_argument(
            '--components',
            nargs='+',
            type=str,
            help='Specific components to import (e.g., accounts journal_lines contacts invoices bank_transactions budgets general_ledger)',
            required=False
        )

    def handle(self, *args, **options):
        integration_id = options.get('integration_id')
        since_date_str = options.get('since')
        components = options.get('components')

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
                return

        for integration in integrations:
            self.stdout.write(f"Importing data for Integration ID: {integration.id}")
            try:
                xero = XeroDataImporter(integration, since_date)
                # If components are specified, run only those, else run full import
                if components:
                    import_methods = {
                        'accounts': xero.sync_xero_chart_of_accounts,
                        'journal_lines': xero.import_xero_journal_lines,
                        'contacts': xero.import_xero_contacts,
                        'invoices': xero.import_xero_invoices,
                        'bank_transactions': xero.import_xero_bank_transactions,
                        'budgets': xero.import_xero_budgets,
                        # 'general_ledger': xero.map_xero_general_ledger,
                        # 'general_ledger_1': xero.map_xero_general_ledger_1,
                        # 'general_ledger_2': xero.map_xero_general_ledger_2,
                        # 'general_ledger_3': xero.map_xero_general_ledger_3,
                    }
                    for component in components:
                        if component in import_methods:
                            self.stdout.write(f"Importing {component}...")
                            import_methods[component]()
                            self.stdout.write(self.style.SUCCESS(f"Successfully imported {component}."))
                        else:
                            self.stdout.write(self.style.WARNING(f"Unknown component: {component}. Skipping."))
                else:
                    xero.import_xero_data()
                    self.stdout.write(self.style.SUCCESS(f"Xero data imported successfully for Integration ID: {integration.id}."))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error importing Xero data for Integration ID {integration.id}: {str(e)}"))
