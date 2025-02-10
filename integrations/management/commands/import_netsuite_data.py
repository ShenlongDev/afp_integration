from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from integrations.models.models import Integration
from integrations.services.netsuite.importer import NetSuiteImporter
from integrations.services.netsuite_transformer import NetSuiteTransformer
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import and transform NetSuite data for specific integrations'

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
            help='Specific components to import (e.g., vendors accounts transactions)',
            required=False
        )
        parser.add_argument(
            '--transform-only',
            action='store_true',
            help='Skip import and only run transformations'
        )

    def handle(self, *args, **options):
        integration_id = options.get('integration_id')
        since_date_str = options.get('since')
        components = options.get('components', [])
        transform_only = options.get('transform_only', False)

        # Set default since_date to today's date if not provided
        if since_date_str:
            try:
                since_date = timezone.datetime.strptime(since_date_str, "%Y-%m-%d")
            except ValueError:
                self.stdout.write(self.style.ERROR(f"Invalid date format: {since_date_str}. Expected YYYY-MM-DD."))
                return
        else:
            since_date = timezone.datetime.combine(date.today(), datetime.min.time())

        integrations = []

        if integration_id:
            try:
                integration = Integration.objects.get(pk=integration_id)
                if not (integration.netsuite_client_id and integration.netsuite_client_secret):
                    self.stdout.write(self.style.ERROR(f"Integration ID {integration_id} does not have NetSuite credentials set."))
                    return
                integrations.append(integration)
            except Integration.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Integration with ID {integration_id} does not exist."))
                return
        else:
            integrations = Integration.objects.filter(
                netsuite_client_id__isnull=False,
                netsuite_client_secret__isnull=False
            )
            if not integrations.exists():
                self.stdout.write(self.style.WARNING("No integrations found with NetSuite credentials."))

        for integration in integrations:
            self.stdout.write(f"Processing Integration ID: {integration.id}")

            try:
                if not transform_only:
                    self.stdout.write('Starting NetSuite data import...')
                    
                    # Initialize importer and transformer for each integration
                    importer = NetSuiteImporter(integration)
                    transformer = NetSuiteTransformer()

                    # Import data based on specified components or all if none specified
                    import_methods = {
                        'vendors': lambda: importer.import_vendors(load_type="drop_and_reload"),
                        'accounts': importer.import_accounts,
                        'transactions': importer.import_transactions,
                        'transaction_accounting_lines': importer.import_transaction_accounting_lines,
                        'transaction_lines': importer.import_transaction_lines,
                        'subsidiaries': importer.import_subsidiaries,
                        'departments': importer.import_departments,
                        'entities': importer.import_entities,
                        'accounting_periods': importer.import_accounting_periods,
                        # 'general_ledger': importer.map_net_suite_general_ledger,
                    }

                    if not components:
                        # If no specific components specified, import all
                        components_to_import = import_methods.keys()
                    else:
                        components_to_import = components

                    for component in components_to_import:
                        if component in import_methods:
                            try:
                                self.stdout.write(f'Importing {component}...')
                                import_methods[component]()
                                self.stdout.write(self.style.SUCCESS(f'Successfully imported {component}'))
                            except Exception as e:
                                self.stdout.write(
                                    self.style.ERROR(f'Error importing {component}: {str(e)}')
                                )
                                logger.error(f'Error importing {component}', exc_info=True)
                        else:
                            self.stdout.write(
                                self.style.WARNING(f'Unknown component: {component}')
                            )

                # Transform data
                # self.stdout.write('Starting data transformation...')
                # try:
                #     transformer.transform_general_ledger(integration)
                #     self.stdout.write(
                #         self.style.SUCCESS('Successfully transformed general ledger data')
                #     )
                # except Exception as e:
                #     self.stdout.write(
                #         self.style.ERROR(f'Error transforming general ledger: {str(e)}')
                #     )
                #     logger.error('Error transforming general ledger', exc_info=True)

                # self.stdout.write(self.style.SUCCESS(f'NetSuite data import and transform completed for Integration ID: {integration.id}'))

            except Exception as e:
                logger.error('Unexpected error during import/transform process', exc_info=True)
                self.stdout.write(self.style.ERROR(f'Failed to import/transform NetSuite data for Integration ID {integration.id}: {str(e)}'))
