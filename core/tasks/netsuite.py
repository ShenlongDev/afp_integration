"""
Tasks for handling NetSuite integrations including data import and token refresh.
"""

import logging
from celery import shared_task, chain
from django.utils import timezone
from datetime import datetime
import time
from django.core.cache import cache

logger = logging.getLogger(__name__)

from integrations.models.models import Integration, IntegrationAccessToken
from integrations.services.netsuite.auth import NetSuiteAuthService
from integrations.services.netsuite.importer import NetSuiteImporter 
from core.tasks.general import SYSTEM_TASK_ACTIVE_KEY  # Import the constant
from core.tasks.general import log_task_event  # Import log_task_event

def get_netsuite_importer(integration_id, since_str=None):
    """
    Instantiate and return a NetSuiteImporter.
    If since_str is not provided, compute today's date at task execution time.
    """
    integration = Integration.objects.get(pk=integration_id)
    
    # Validate that we have the required NetSuite settings
    settings = integration.settings or {}
    if not settings.get("account_id"):
        logger.error(f"Integration {integration_id} missing 'account_id' in settings")
        raise ValueError(f"Integration {integration_id} missing required NetSuite settings")
    
    if since_str is None:
        since_str = timezone.now().strftime('%Y-%m-%d')
    since_date = datetime.strptime(since_str, '%Y-%m-%d')
    return NetSuiteImporter(integration, since_date)

@shared_task
def netsuite_import_accounts(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_accounts()
    logger.info(f"NetSuite accounts imported for integration: {integration_id}")

@shared_task
def netsuite_import_accounting_periods(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_accounting_periods()
    logger.info(f"NetSuite accounting periods imported for integration: {integration_id}")

@shared_task
def netsuite_import_entity(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_entities()
    logger.info(f"NetSuite entity imported for integration: {integration_id}")

@shared_task
def netsuite_import_vendors(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_vendors()
    logger.info(f"NetSuite vendors imported for integration: {integration_id}")

@shared_task
def netsuite_import_subsidiary(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_subsidiaries()
    logger.info(f"NetSuite subsidiary imported for integration: {integration_id}")

@shared_task
def netsuite_import_departments(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_departments()
    logger.info(f"NetSuite departments imported for integration: {integration_id}")

@shared_task
def netsuite_import_transactions(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_transactions()
    logger.info(f"NetSuite transactions imported for integration: {integration_id}")

@shared_task
def netsuite_import_transaction_lines(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_transaction_lines()
    logger.info(f"NetSuite transaction lines imported for integration: {integration_id}")

@shared_task
def netsuite_import_transaction_accounting_lines(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_transaction_accounting_lines()
    logger.info(f"NetSuite transaction accounting lines imported for integration: {integration_id}")

@shared_task
def netsuite_import_locations(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_locations()
    logger.info(f"NetSuite locations imported for integration: {integration_id}")
    
@shared_task
def netsuite_import_budgets(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_budgets()
    logger.info(f"NetSuite budgets imported for integration: {integration_id}")


@shared_task
def netsuite_import_general_ledger(integration_id, since_str=None):
    importer = get_netsuite_importer(integration_id, since_str)
    importer.import_general_ledger()
    logger.info(f"NetSuite general ledger imported for integration: {integration_id}")

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
def sync_netsuite_data(integration_id, since_str: str = None):
    """
    Original task: Dispatches a chain of NetSuite import tasks for a given integration,
    ending with a reschedule.
    Each individual task will determine the current date if since_str is not provided.
    """
    task_chain = chain(
        netsuite_import_accounts.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_accounting_periods.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_entity.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_vendors.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_subsidiary.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_departments.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_transactions.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_lines.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_accounting_lines.si(integration_id, since_str),
        wait_and_reschedule.si(integration_id),
        netsuite_import_locations.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_budgets.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_general_ledger.si(integration_id, since_str),
    )
    task_chain.apply_async()
    logger.info(f"Dispatched NetSuite sync tasks for integration: {integration_id}")

@shared_task
def sync_single_netsuite_data(integration_id, since_str: str = None):
    """
    Sync tasks for a single NetSuite integration executed sequentially.
    Builds the chain with the since_str computed at runtime if not provided.
    """
    task_chain = chain(
        netsuite_import_accounts.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_accounting_periods.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_entity.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_vendors.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_subsidiary.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_departments.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_transactions.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_lines.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_transaction_accounting_lines.si(integration_id, since_str),
        wait_and_reschedule.si(integration_id),
        netsuite_import_locations.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_budgets.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        netsuite_import_general_ledger.si(integration_id, since_str),
    )
    task_chain.apply_async()

@shared_task(queue="high_priority")
def refresh_netsuite_token_task():
    """Refresh the NetSuite token for all available integrations."""
    from django.db import close_old_connections
    from integrations.models.models import Integration
    from integrations.services.netsuite.auth import NetSuiteAuthService
    from django.core.cache import cache 
    
    active_system_task = cache.get(SYSTEM_TASK_ACTIVE_KEY)
    if active_system_task:
        logger.info(f"Another system task {active_system_task} is already running. Skipping token refresh.")
        return
    
    # Set the active system task flag
    cache.set(SYSTEM_TASK_ACTIVE_KEY, "netsuite_token_refresh", timeout=229600) 
    
    try:
        close_old_connections()
        
        # Find all integrations with NetSuite settings
        netsuite_integrations = Integration.objects.filter(
            integration_type='netsuite',
            settings__account_id__isnull=False,
            is_active=True
        )
        
        refresh_count = 0
        error_count = 0
        for integration in netsuite_integrations:
            try:
                auth_service = NetSuiteAuthService(integration)
                auth_service.obtain_access_token()
                refresh_count += 1
                logger.info(f"NetSuite token refreshed for integration {integration.id}")
                
                log_task_event(
                    "netsuite_token_refresh",
                    "success",
                    f"Refreshed NetSuite token for integration {integration.id}"
                )
                
            except Exception as e:
                error_count += 1
                error_msg = f"Error refreshing token for integration {integration.id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                
                log_task_event(
                    "netsuite_token_refresh",
                    "error",
                    error_msg
                )
            
            close_old_connections()
        
        summary_msg = f"Completed NetSuite token refresh: {refresh_count} successful, {error_count} failed"
        logger.info(summary_msg)
        log_task_event("netsuite_token_refresh", "completed", summary_msg)
        
    except Exception as e:
        logger.error(f"Error in NetSuite token refresh task: {e}", exc_info=True)
        log_task_event("netsuite_token_refresh", "failed", f"General error: {str(e)}")
    finally:
        cache.delete(SYSTEM_TASK_ACTIVE_KEY)
        close_old_connections() 