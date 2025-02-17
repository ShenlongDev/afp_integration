from celery import Celery, shared_task, chain
from integrations.services.netsuite.importer import NetSuiteImporter
from datetime import datetime
import logging
from django.utils import timezone
from integrations.models.models import HighPriorityTask
from django.core.cache import cache
from integrations.services.utils import (
    log_task_event, get_integrations_by_integration_type
)

logger = logging.getLogger(__name__)

app = Celery('core')

GLOBAL_TASK_LOCK_KEY = "global_task_lock"

def acquire_global_lock(timeout=600):
    """
    Tries to acquire the lock.
    Returns True if the lock was acquired, False otherwise.
    """
    # cache.add will only add if the key does not exist.
    return cache.add(GLOBAL_TASK_LOCK_KEY, "locked", timeout)

def release_global_lock():
    """
    Releases the lock.
    """
    cache.delete(GLOBAL_TASK_LOCK_KEY)

# ------------------- NetSuite Tasks -------------------

@shared_task
def netsuite_import_accounts(integration):
    importer = NetSuiteImporter(integration)
    importer.import_accounts()
    logger.info(f"NetSuite accounts imported for integration: {integration}")


@shared_task
def netsuite_import_accounting_periods(integration):
    importer = NetSuiteImporter(integration)
    importer.import_accounting_periods()
    logger.info(f"NetSuite accounting periods imported for integration: {integration}")


@shared_task
def netsuite_import_entity(integration):
    importer = NetSuiteImporter(integration)
    importer.import_entities()
    logger.info(f"NetSuite entity imported for integration: {integration}")


@shared_task
def netsuite_import_vendors(integration):
    importer = NetSuiteImporter(integration)
    importer.import_vendors()
    logger.info(f"NetSuite vendors imported for integration: {integration}")


@shared_task
def netsuite_import_subsidiary(integration):
    importer = NetSuiteImporter(integration)
    importer.import_subsidiaries()
    logger.info(f"NetSuite subsidiary imported for integration: {integration}")


@shared_task
def netsuite_import_departments(integration):
    importer = NetSuiteImporter(integration)
    importer.import_departments()
    logger.info(f"NetSuite departments imported for integration: {integration}")


@shared_task
def netsuite_import_transactions(integration):
    importer = NetSuiteImporter(integration)
    importer.import_transactions()
    logger.info(f"NetSuite transactions imported for integration: {integration}")


@shared_task
def netsuite_import_transaction_lines(integration):
    importer = NetSuiteImporter(integration)
    importer.import_transaction_lines()
    logger.info(f"NetSuite transaction lines imported for integration: {integration}")


@shared_task
def netsuite_import_transaction_accounting_lines(integration):
    importer = NetSuiteImporter(integration)
    importer.import_transaction_accounting_lines()
    logger.info(f"NetSuite transaction accounting lines imported for integration: {integration}")


@shared_task
def sync_netsuite_data(integration):
    """
    Chain all the NetSuite import and transformation tasks.
    """
    task_chain = chain(
        netsuite_import_accounts.s(integration),
        netsuite_import_accounting_periods.s(integration),
        netsuite_import_entity.s(integration),
        netsuite_import_vendors.s(integration),
        netsuite_import_subsidiary.s(integration),
        netsuite_import_departments.s(integration),
        netsuite_import_transactions.s(integration),
        netsuite_import_transaction_lines.s(integration),
        netsuite_import_transaction_accounting_lines.s(integration),
    )
    task_chain.apply_async()
    logger.info(f"Dispatched NetSuite sync tasks for integration: {integration}")

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


@shared_task(bind=True, max_retries=3)
def run_data_sync(self):
    """
    This task is responsible for scheduling Netsuite and Xero sync subtasks.
    It acquires a global lock so that no new top-level scheduling starts until the current one is done.
    It uses get_organisations_by_integration_type to filter the eligible Organisations, then
    for each Organisation, it iterates through the related, eligible integrations.
    Note: The dispatched subtasks (e.g. netsuite_import_accounts) run asynchronously.
    """
    if not acquire_global_lock():
        logger.info("run_data_sync: Another top-level sync is in progress. Retrying in 10 seconds...")
        raise self.retry(countdown=10)
    
    try:
        # Optionally, refresh the NetSuite token.
        refresh_netsuite_token_task.delay()

        eligible_integrations = get_integrations_by_integration_type("netsuite")
        if eligible_integrations.exists():
            for integration in eligible_integrations:
                logger.info(f"Dispatching Netsuite sync for integration: {integration}")
                sync_netsuite_data.delay(integration.id)
        else:
            logger.warning("No eligible Netsuite integrations found.")
        # Dispatch Xero tasks.
        logger.info("Dispatching Xero sync tasks.")
        sync_xero_data.delay()

        logger.info("All Netsuite and Xero sync subtasks have been dispatched.")
    except Exception as e:
        logger.error("Error in run_data_sync: %s", e)
        raise e
    finally:
        release_global_lock()


@shared_task
def sync_xero_data(since_str: str = None):
    """
    Finds all integrations with Xero credentials and begins the sync chain for each.
    Optionally accepts a since date string (YYYY-MM-DD) to pass along.
    """
    from datetime import datetime
    from integrations.services.utils import get_organisations_by_integration_type
    from celery import chain

    since_str = since_str or datetime.now().strftime('%Y-%m-%d')
    organisations = get_organisations_by_integration_type("xero")
    if not organisations.exists():
        logger.warning("No organisations found with Xero eligible integrations.")
        return

    for org in organisations:
        integrations = org.integrations.filter(
            xero_client_id__isnull=False,
            xero_client_secret__isnull=False
        )
        for integration in integrations:
            task_chain = chain(
                xero_sync_accounts_task.s(integration.id, since_str),
                xero_import_journal_lines_task.s(integration.id, since_str),
                xero_import_contacts_task.s(integration.id, since_str),
                xero_import_invoices_task.s(integration.id, since_str),
                xero_import_bank_transactions_task.s(integration.id, since_str),
            )
            task_chain.apply_async()
            logger.info(f"Dispatched Xero sync tasks for integration: {integration}")


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



@shared_task(bind=True, max_retries=5)
def process_data_import_task(self, integration_id, integration_type, since_date_str, selected_modules):
    if not acquire_global_lock():
        logger.info("process_data_import_task: Another task is in progress. Retrying in 10 seconds...")
        raise self.retry(countdown=10)
    
    try:
        # Retrieve the Integration object
        from integrations.models.models import Integration
        integration = Integration.objects.get(pk=integration_id)
        
        # Convert since_date from string to datetime (assuming YYYY-MM-DD format)
        since_date = datetime.strptime(since_date_str, "%Y-%m-%d")
        
        # Load the module configuration for the selected integration type
        from integrations.modules import MODULES
        module_config = MODULES[integration_type]
        ImporterClass = module_config['client']
        importer = ImporterClass(integration, since_date)
        
        if selected_modules:
            for module in selected_modules:
                import_func = module_config['import_methods'].get(module)
                if import_func:
                    logger.info(f"Importing {module} for integration ID {integration_id}")
                    import_func(importer)
        else:
            full_import = module_config.get('full_import')
            if full_import:
                logger.info(f"Starting full import for integration ID {integration_id}")
                full_import(importer)
            else:
                for import_func in module_config['import_methods'].values():
                    import_func(importer)
        
        logger.info(f"Data import for integration {integration_id} completed successfully.")
    except Exception as ex:
        logger.error(f"Data import failed for integration {integration_id}: {ex}", exc_info=True)
        raise ex
    finally:
        release_global_lock()

def get_high_priority_task():
    """
    Returns the earliest unprocessed high priority task.
    """
    try:
        task = HighPriorityTask.objects.filter(processed=False).order_by('created_at').first()
        return task
    except Exception as exc:
        logger.error("Error fetching high priority task: %s", exc, exc_info=True)
        return None

@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    This dispatcher polls for high-priority tasks in a continuous loop:
    - If a high-priority task exists, it dispatches process_data_import_task and marks the task as processed.
    - Otherwise, it dispatches run_data_sync.
    All logs are saved in TaskLog for admin review.
    """
    try:
        hp_task = get_high_priority_task()
        if hp_task:
            logger.info("High priority task found: %s", hp_task)
            process_data_import_task.delay(
                hp_task.integration.id,
                hp_task.integration_type,
                hp_task.since_date.strftime("%Y-%m-%d"),
                hp_task.selected_modules
            )
            hp_task.processed = True
            hp_task.save(update_fields=['processed'])
            log_task_event("process_data_import_task", "dispatched", f"High priority task {hp_task.id} dispatched at {timezone.now()}")
        else:
            logger.info("No high priority task. Running continuous data sync.")
            run_data_sync.delay()
            log_task_event("run_data_sync", "dispatched", f"Continuous sync task dispatched at {timezone.now()}")
    except Exception as exc:
        logger.error("Dispatcher encountered an error: %s", exc, exc_info=True)
        log_task_event("dispatcher", "failed", str(exc))
        try:
            raise self.retry(exc=exc, countdown=10)
        except self.MaxRetriesExceededError:
            log_task_event("dispatcher", "failed - max retries exceeded", f"Failed at {timezone.now()} with error: {exc}")
    else:
        logger.info("Dispatcher completed successfully.")
        log_task_event("dispatcher", "success", f"Task completed successfully at {timezone.now()}")
    finally:
        dispatcher.apply_async(countdown=5)
