from celery import Celery, shared_task
from integrations.services.netsuite_transformer import NetSuiteTransformer
from integrations.services.netsuite.importer import NetSuiteImporter
from integrations.services.netsuite.auth import NetSuiteAuthService
from integrations.services.xero.xero_client import (
    authorize_xero,
    import_xero_journal_lines,
    sync_xero_chart_of_accounts
)
import logging
from celery.schedules import crontab


logger = logging.getLogger(__name__)

# define celery app
app = Celery('core')


# ------------------- NetSuite Tasks -------------------

@shared_task
def transform_netsuite_data():
    """Transform all NetSuite data"""
    try:
        transformer = NetSuiteTransformer()
        transformer.transform_all()
        logger.info("NetSuite data transformed successfully.")
    except Exception as e:
        logger.error(f"Error in transforming NetSuite data: {str(e)}")
        raise e

@shared_task
def sync_netsuite_data(consolidation_key: str):
    """Import and transform NetSuite data with budgets"""
    try:
        importer = NetSuiteImporter(consolidation_key)
       
        # Import reference data
        importer.import_accounts()
        importer.import_accounting_periods()
        importer.import_entity()
        importer.import_vendors()
        importer.import_subsidiary()
        importer.import_departments()
        importer.import_budgets()
        importer.import_budget_lines()
       
        # Import transactions and related data
        importer.import_transactions()
        importer.import_transaction_lines()
        importer.import_transaction_accounting_lines()
       
        # Transform data
        transformer = NetSuiteTransformer()
       
        # Transform reference data first
        transformer.transform_subsidiaries()
        transformer.transform_departments()
        transformer.transform_accounting_periods()
        transformer.transform_accounts()
        transformer.transform_vendors()
        transformer.transform_entity()
       
        # Transform budgets
        transformer.transform_budget_period_balances()
       
        # Transform transactions
        transformer.transform_transactions()
        transformer.transform_transaction_lines()
        transformer.transform_transaction_accounting_lines()
       
        # Finally transform general ledger
        transformer.transform_general_ledger()
       
        logger.info(f"NetSuite synchronization completed for consolidation_key: {consolidation_key}")
    except Exception as e:
        logger.error(f"Error in syncing NetSuite data for {consolidation_key}: {str(e)}")
        raise e

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    autoretry_for=(Exception,)
)
def sync_netsuite_data_with_retry(self, consolidation_key: str):
    """Import and transform NetSuite data with budgets with retry logic"""
    try:
        sync_netsuite_data(consolidation_key)
    except Exception as e:
        logger.error(f"Retrying sync_netsuite_data for {consolidation_key} due to error: {str(e)}")
        raise self.retry(exc=e)

# --------------------- Xero Tasks ---------------------

@shared_task
def sync_xero_integrations():
    """
    Synchronize all Xero integrations by authenticating, importing journals,
    and syncing chart of accounts.
    """
    from integrations.models.models import Integration

    try:
        logger.info('Starting synchronization of all Xero integrations.')

        # Retrieve all integrations with Xero credentials
        integrations = Integration.objects.filter(
            xero_client_id__isnull=False,
            xero_client_secret__isnull=False
        )

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

    except Exception as e:
        logger.error(f"Error in sync_xero_integrations: {str(e)}")
        raise e

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    autoretry_for=(Exception,)
)
def sync_xero_integrations_with_retry(self):
    """Synchronize all Xero integrations with retry logic"""
    try:
        sync_xero_integrations()
    except Exception as e:
        logger.error(f"Retrying sync_xero_integrations due to error: {str(e)}")
        raise self.retry(exc=e)

# --------------------- Combined Data Sync Task ---------------------

@shared_task
def run_data_sync():
    """
    Executes synchronization tasks for both NetSuite and Xero integrations.
    """
    try:
        # Synchronize NetSuite Integrations
        from integrations.models.models import Integration

        netsuite_integrations = Integration.objects.filter(
            netsuite_client_id__isnull=False,
            netsuite_client_secret__isnull=False
        )

        for integration in netsuite_integrations:
            consolidation_key = integration.consolidation_key
            logger.info(f"Starting NetSuite sync for consolidation_key: {consolidation_key}")
            sync_netsuite_data_with_retry.delay(consolidation_key)

        # Synchronize Xero Integrations
        logger.info("Starting Xero integrations synchronization.")
        sync_xero_integrations_with_retry.delay()

        logger.info("Data synchronization tasks have been dispatched.")

    except Exception as e:
        logger.error(f"Error in run_data_sync: {str(e)}")
        raise e

app.conf.beat_schedule = {
    'run-data-sync-every-day-at-midnight': {
        'task': 'core.tasks.run_data_sync',
        'schedule': crontab(hour=0, minute=0),  
    },
}
