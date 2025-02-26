"""
Tasks for handling NetSuite integrations including data import and token refresh.
"""

import logging
from celery import shared_task, chain
import time

logger = logging.getLogger(__name__)

from integrations.models.models import Integration, IntegrationAccessToken
from integrations.services.netsuite.auth import NetSuiteAuthService
from integrations.services.netsuite.importer import NetSuiteImporter 


@shared_task
def netsuite_import_accounts(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_accounts()
    logger.info(f"NetSuite accounts imported for integration: {integration}")

@shared_task
def netsuite_import_accounting_periods(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_accounting_periods()
    logger.info(f"NetSuite accounting periods imported for integration: {integration}")

@shared_task
def netsuite_import_entity(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_entities()
    logger.info(f"NetSuite entity imported for integration: {integration}")

@shared_task
def netsuite_import_vendors(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_vendors()
    logger.info(f"NetSuite vendors imported for integration: {integration}")

@shared_task
def netsuite_import_subsidiary(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_subsidiaries()
    logger.info(f"NetSuite subsidiary imported for integration: {integration}")

@shared_task
def netsuite_import_departments(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_departments()
    logger.info(f"NetSuite departments imported for integration: {integration}")

@shared_task
def netsuite_import_transactions(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_transactions()
    logger.info(f"NetSuite transactions imported for integration: {integration}")

@shared_task
def netsuite_import_transaction_lines(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_transaction_lines()
    logger.info(f"NetSuite transaction lines imported for integration: {integration}")

@shared_task
def netsuite_import_transaction_accounting_lines(integration_id):
    integration = Integration.objects.get(pk=integration_id)
    importer = NetSuiteImporter(integration)
    importer.import_transaction_accounting_lines()
    logger.info(f"NetSuite transaction accounting lines imported for integration: {integration}")

@shared_task
def wait_60_seconds(integration_id):
    """
    Waits for 20 seconds before returning.
    This task ensures at least a 20-second delay after the previous task.
    """
    time.sleep(20)
    return integration_id

@shared_task
def wait_and_reschedule(integration_id):
    """
    (Deprecated) Once the chain is complete, waits and then reschedules the entire sync.
    """
    logger.info(f"Final wait complete for integration {integration_id}. Rescheduling next sync iteration...")
    sync_netsuite_data.apply_async(args=[integration_id], countdown=10)

@shared_task
def sync_netsuite_data(integration_id):
    """
    Original task: Dispatches a chain of NetSuite import tasks for a given integration,
    ending with a reschedule.
    """
    task_chain = chain(
        netsuite_import_accounts.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_accounting_periods.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_entity.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_vendors.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_subsidiary.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_departments.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_transactions.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_lines.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_accounting_lines.si(integration_id),
        wait_and_reschedule.si(integration_id)
    )
    task_chain.apply_async()
    logger.info(f"Dispatched NetSuite sync tasks for integration: {integration_id}")

@shared_task
def sync_single_netsuite_data(integration_id):
    """
    Sync tasks for a single NetSuite integration executed sequentially.
    This version builds the same chain but does not automatically reschedule continuous sync.
    """
    task_chain = chain(
        netsuite_import_accounts.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_accounting_periods.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_entity.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_vendors.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_subsidiary.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_departments.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_transactions.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_lines.si(integration_id),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_accounting_lines.si(integration_id),
    )
    result = task_chain.apply_async()
    return result.get()  # wait for chain to complete and return result

@shared_task
def refresh_netsuite_token_task():
    """
    Refresh the NetSuite token for all available integrations.
    """
    try:
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