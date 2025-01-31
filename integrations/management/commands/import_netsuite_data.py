from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from integrations.models.models import Integration
from integrations.services.netsuite.importer import NetSuiteImporter
from integrations.services.netsuite_transformer import NetSuiteTransformer
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Import and transform NetSuite data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--integration-id',
            type=int,
            help='Integration ID to import data from',
            required=True
        )
        parser.add_argument(
            '--since',
            type=str,
            help='Import data since date (YYYY-MM-DD)',
            required=False
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
        try:
            integration_id = options['integration_id']
            since_date = None
            components = options.get('components', [])
            transform_only = options.get('transform_only', False)

            # Validate integration
            try:
                integration = Integration.objects.get(pk=integration_id)
            except Integration.DoesNotExist:
                raise CommandError(f'Integration with ID {integration_id} does not exist')

            # Parse since date if provided
            if options['since']:
                try:
                    since_date = timezone.datetime.strptime(options['since'], "%Y-%m-%d")
                except ValueError:
                    raise CommandError('Invalid date format. Use YYYY-MM-DD')

            # Initialize importer and transformer
            importer = NetSuiteImporter(integration)
            transformer = NetSuiteTransformer()

            if not transform_only:
                self.stdout.write('Starting NetSuite data import...')
                
                # Import data based on specified components or all if none specified
                import_methods = {
                    'vendors': lambda: importer.import_vendors(load_type="drop_and_reload"),
                    'accounts': importer.import_accounts,
                    'transactions': importer.import_transactions,
                    'subsidiaries': importer.import_subsidiaries,
                    'departments': importer.import_departments,
                    'entities': importer.import_entities,
                    'accounting_periods': importer.import_accounting_periods,
                    'general_ledger': importer.import_general_ledger,
                }

                if not components:
                    # If no specific components specified, import all
                    components = import_methods.keys()

                for component in components:
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
            self.stdout.write('Starting data transformation...')
            try:
                transformer.transform_general_ledger(integration)
                self.stdout.write(
                    self.style.SUCCESS('Successfully transformed general ledger data')
                )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error transforming general ledger: {str(e)}')
                )
                logger.error('Error transforming general ledger', exc_info=True)

            self.stdout.write(self.style.SUCCESS('NetSuite data import and transform completed'))

        except Exception as e:
            logger.error('Unexpected error during import/transform process', exc_info=True)
            raise CommandError(f'Failed to import/transform NetSuite data: {str(e)}')
