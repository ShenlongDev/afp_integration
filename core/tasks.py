from celery import Celery, shared_task, chain
from integrations.services.netsuite.importer import NetSuiteImporter
from datetime import datetime
import logging



logger = logging.getLogger(__name__)

app = Celery('core')


# ------------------- NetSuite Tasks -------------------

@shared_task
def netsuite_import_accounts(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_accounts()
    logger.info("NetSuite accounts imported.")

@shared_task
def netsuite_import_accounting_periods(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_accounting_periods()
    logger.info("NetSuite accounting periods imported.")

@shared_task
def netsuite_import_entity(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_entities()
    logger.info("NetSuite entity imported.")

@shared_task
def netsuite_import_vendors(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_vendors()
    logger.info("NetSuite vendors imported.")

@shared_task
def netsuite_import_subsidiary(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_subsidiaries()
    logger.info("NetSuite subsidiary imported.")

@shared_task
def netsuite_import_departments(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_departments()
    logger.info("NetSuite departments imported.")

@shared_task
def netsuite_import_transactions(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_transactions()
    logger.info("NetSuite transactions imported.")

@shared_task
def netsuite_import_transaction_lines(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_transaction_lines()
    logger.info("NetSuite transaction lines imported.")

@shared_task
def netsuite_import_transaction_accounting_lines(consolidation_key: str):
    importer = NetSuiteImporter(consolidation_key)
    importer.import_transaction_accounting_lines()
    logger.info("NetSuite transaction accounting lines imported.")


@shared_task
def sync_netsuite_data(consolidation_key: str):
    """
    Chain all the NetSuite import and transformation tasks.
    """
    task_chain = chain(
        netsuite_import_accounts.s(consolidation_key),
        netsuite_import_accounting_periods.s(consolidation_key),
        netsuite_import_entity.s(consolidation_key),
        netsuite_import_vendors.s(consolidation_key),
        netsuite_import_subsidiary.s(consolidation_key),
        netsuite_import_departments.s(consolidation_key),
        netsuite_import_transactions.s(consolidation_key),
        netsuite_import_transaction_lines.s(consolidation_key),
        netsuite_import_transaction_accounting_lines.s(consolidation_key),
    )
    task_chain.apply_async()
    logger.info(f"Dispatched NetSuite sync tasks for consolidation_key: {consolidation_key}")


# --------------------- Xero Tasks ---------------------

def get_xero_importer(integration_id: int, since_str: str = None):
    """
    Helper to instantiate XeroDataImporter for the given integration.
    Optionally parses a since date, if provided.
    """
    from integrations.models.models import Integration
    from integrations.services.xero.xero_client import XeroDataImporter
    integration = Integration.objects.get(id=integration_id)
    since_date = datetime.strptime(since_str, '%Y-%m-%d') if since_str else None
    return XeroDataImporter(integration, since_date)

@shared_task
def xero_sync_accounts_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.sync_xero_chart_of_accounts()
    logger.info(f"Xero accounts synced for integration id: {integration_id}")

@shared_task
def xero_import_journal_lines_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_journal_lines()
    logger.info(f"Xero journal lines imported for integration id: {integration_id}")

@shared_task
def xero_import_contacts_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_contacts()
    logger.info(f"Xero contacts imported for integration id: {integration_id}")

@shared_task
def xero_import_invoices_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_invoices()
    logger.info(f"Xero invoices imported for integration id: {integration_id}")

@shared_task
def xero_import_bank_transactions_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_bank_transactions()
    logger.info(f"Xero bank transactions imported for integration id: {integration_id}")

@shared_task
def xero_import_budgets_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_budgets()
    logger.info(f"Xero budgets imported for integration id: {integration_id}")

@shared_task
def xero_map_general_ledger_task(integration_id: int, since_str: str = None):
    importer = get_xero_importer(integration_id, since_str)
    importer.map_xero_general_ledger()
    logger.info(f"Xero general ledger mapped for integration id: {integration_id}")

@shared_task
def sync_xero_data_for_integration(integration_id: int, since_str: str = None):
    """
    Chain together the Xero import tasks for a single integration.
    """
    task_chain = chain(
        xero_sync_accounts_task.s(integration_id, since_str),
        xero_import_journal_lines_task.s(integration_id, since_str),
        xero_import_contacts_task.s(integration_id, since_str),
        xero_import_invoices_task.s(integration_id, since_str),
        xero_import_bank_transactions_task.s(integration_id, since_str),
        xero_import_budgets_task.s(integration_id, since_str),
        xero_map_general_ledger_task.s(integration_id, since_str)
    )
    task_chain.apply_async()
    logger.info(f"Dispatched full Xero sync tasks for integration id: {integration_id}")

@shared_task
def sync_xero_data(since_str: str = None):
    """
    Finds all integrations with Xero credentials and begins the sync chain for each.
    Optionally accepts a since date string (YYYY-MM-DD) to pass along.
    """
    from integrations.models.models import Integration
    integrations = Integration.objects.filter(
        xero_client_id__isnull=False,
        xero_client_secret__isnull=False
    )
    if not integrations.exists():
        logger.warning("No integrations found with Xero credentials.")
        return
    for integration in integrations:
        logger.info(f"Starting full Xero sync for Integration ID: {integration.id}")
        sync_xero_data_for_integration.delay(integration.id, since_str)
    logger.info("All Xero sync tasks have been dispatched.")


# --------------------- NETSUITE TOKEN REFRESH TASK ---------------------

@shared_task
def refresh_netsuite_token_task():
    """
    Refresh the NetSuite token.
    """
    try:
        from integrations.models.models import Integration, IntegrationAccessToken
        from integrations.services.netsuite.auth import NetSuiteAuthService
        netsuite_integrations = Integration.objects.filter(
            netsuite_client_id__isnull=False,
        )
        for integration in netsuite_integrations:
            auth_service = NetSuiteAuthService(integration)
            token_obj = IntegrationAccessToken.objects.get(
                integration=integration,
                integration_type="NETSUITE"
            )
            auth_service._refresh_token(token_obj)
        logger.info("NetSuite token refreshed successfully.")
    except Exception as e:
        logger.error(f"Error refreshing NetSuite token: {e}")
        raise e


# --------------------- COMBINED DATA SYNC TASK ---------------------

@shared_task
def run_data_sync():
    """
    Dispatches both NetSuite and Xero sync tasks.
    """
    try:
        from integrations.models.models import Integration
        # Dispatch NetSuite tasks.
        netsuite_integrations = Integration.objects.filter(
            netsuite_client_id__isnull=False,
            netsuite_client_secret__isnull=False
        )
        for integration in netsuite_integrations:
            consolidation_key = integration.consolidation_key
            logger.info(f"Dispatching NetSuite sync for consolidation_key: {consolidation_key}")
            sync_netsuite_data.delay(consolidation_key)

        # Dispatch Xero tasks.
        logger.info("Dispatching Xero sync tasks.")
        sync_xero_data.delay()

        # Optionally, refresh the NetSuite token.
        refresh_netsuite_token_task.delay()

        logger.info("All data sync tasks have been dispatched.")
    except Exception as e:
        logger.error(f"Error in run_data_sync: {e}")
        raise e

app.conf.beat_schedule = {
    'run-data-sync-every-day-at-midnight': {
        'task': 'core.tasks.run_data_sync',
        'schedule': 60 * 60 * 24,
    },
}
