import logging
import os
from django.core.management.base import BaseCommand
from config.integrations.models.models import Integration
from config.integrations.services.xero.xero_client import (
    authorize_xero,
    import_xero_journal_lines,
    sync_xero_chart_of_accounts
)
from django.db import transaction
from datetime import datetime

class Command(BaseCommand):
    help = 'Synchronize Xero integrations for all organizations by authenticating, importing journals, and syncing chart of accounts.'

    def handle(self, *args, **options):
        # Setup logging
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f'data_sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

        logger = logging.getLogger(__name__)

        logger.info('Starting synchronization of all Xero integrations.')

        # Retrieve all integrations with Xero credentials
        integrations = Integration.objects.filter(xero_client_id__isnull=False, xero_client_secret__isnull=False)

        if not integrations.exists():
            logger.warning('No integrations found with Xero credentials.')
            return

        for integration in integrations:
            org_name = integration.org.name
            integration_id = integration.id
            logger.info(f'----- Processing Integration ID: {integration_id} for Organization: {org_name} -----')

            try:
                # Step 1: Authenticate with Xero
                logger.info('Authenticating with Xero...')
                authorize_xero(integration)
                logger.info('Xero authentication successful.')

                # Step 2: Import Journal Lines
                logger.info('Importing Xero Journal Lines...')
                import_xero_journal_lines(integration)
                logger.info('Xero Journal Lines imported successfully.')

                # Step 3: Synchronize Chart of Accounts
                logger.info('Synchronizing Chart of Accounts...')
                sync_xero_chart_of_accounts(integration)
                logger.info('Chart of Accounts synchronized successfully.')

                logger.info(f'----- Completed Integration ID: {integration_id} for Organization: {org_name} -----\n')

            except Exception as e:
                logger.error(f'Error processing Integration ID: {integration_id} for Organization: {org_name}: {e}\n')
                continue  # Proceed to the next integration

        logger.info('Synchronization of all Xero integrations completed.')
