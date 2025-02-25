import logging
from celery import shared_task
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime

logger = logging.getLogger(__name__)

GLOBAL_TASK_LOCK_KEY = "global_task_lock"

def acquire_global_lock(timeout=600):
    """
    Acquire a global lock to prevent overlapping task dispatches.
    Returns True if the lock is acquired.
    """
    return cache.add(GLOBAL_TASK_LOCK_KEY, "locked", timeout)

def release_global_lock():
    """
    Release the global lock.
    """
    cache.delete(GLOBAL_TASK_LOCK_KEY)

from core.models import TaskLog 

def log_task_event(task_name, status, detail):
    TaskLog.objects.create(
        task_name=task_name,
        status=status,
        detail=detail,
        timestamp=timezone.now()
    )

def get_high_priority_task():
    """
    Returns the earliest unprocessed high priority task.
    """
    try:
        from integrations.models.models import HighPriorityTask
        task = HighPriorityTask.objects.filter(processed=False).order_by('created_at').first()
        return task
    except Exception as exc:
        logger.error("Error fetching high priority task: %s", exc, exc_info=True)
        return None

@shared_task(bind=True, max_retries=3)
def run_data_sync(self):
    """
    Dispatches sync tasks for Netsuite and Xero integrations.
    """
    if not acquire_global_lock():
        logger.info("run_data_sync: Another top-level sync is in progress. Retrying in 10 seconds...")
        raise self.retry(countdown=10)
    try:
        from integrations.services.utils import get_integrations_by_integration_type
        eligible_netsuite_integrations = get_integrations_by_integration_type("netsuite")
        if eligible_netsuite_integrations.exists():
            for integration in eligible_netsuite_integrations:
                logger.info(f"Dispatching Netsuite sync for integration: {integration}")
                from .netsuite import sync_netsuite_data
                sync_netsuite_data.delay(integration.id)
        else:
            logger.warning("No eligible Netsuite integrations found.")
        
        logger.info("Dispatching Xero sync tasks.")
        from .xero import sync_xero_data
        sync_xero_data.delay()
        logger.info("All sync subtasks have been dispatched.")
    except Exception as e:
        logger.error("Error in run_data_sync: %s", e, exc_info=True)
        raise e
    finally:
        release_global_lock()

@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    Continuously polls for high priority tasks and normal sync tasks.
    If a high priority task is found, it processes that task inline;
    otherwise, it dispatches the normal sync tasks.
    This task re-enqueues itself every few seconds.
    """
    try:
        hp_task = get_high_priority_task()
        if hp_task:
            logger.info(
                "Dispatching high priority task: integration id: %s, integration type: %s, since_date: %s, modules: %s",
                hp_task.integration.id,
                hp_task.integration_type,
                hp_task.since_date,
                hp_task.selected_modules
            )

            # --- Begin inline business logic previously in process_data_import_task ---
            from integrations.models.models import Integration
            try:
                integration = Integration.objects.get(pk=hp_task.integration.id)
            except Integration.DoesNotExist:
                logger.error("Integration with ID %s does not exist.", hp_task.integration.id)
                hp_task.processed = True
                hp_task.save(update_fields=['processed'])
                log_task_event(
                    "process_data_import_task", "failed",
                    f"Integration with ID {hp_task.integration.id} does not exist at {timezone.now()}"
                )
            else:
                # Parse the since_date. (Assuming hp_task.since_date is a date/datetime object.)
                since_date = (
                    datetime.strptime(hp_task.since_date.strftime("%Y-%m-%d"), "%Y-%m-%d")
                    if hp_task.since_date else None
                )

                from integrations.modules import MODULES
                module_config = MODULES[hp_task.integration_type]
                ImporterClass = module_config['client']
                logger.info("ImporterClass: %s, integration: %s, since_date: %s", ImporterClass, integration, since_date)
                importer = ImporterClass(integration, since_date)

                if hp_task.selected_modules:
                    for module in hp_task.selected_modules:
                        import_func = module_config['import_methods'].get(module)
                        if import_func:
                            logger.info("Importing %s for integration ID %s", module, hp_task.integration.id)
                            import_func(importer)
                        else:
                            logger.warning("Unknown module %s for integration ID %s", module, hp_task.integration.id)
                else:
                    full_import = module_config.get('full_import')
                    if full_import:
                        logger.info("Starting full import for integration ID %s", hp_task.integration.id)
                        full_import(importer)
                    else:
                        for import_func in module_config['import_methods'].values():
                            import_func(importer)
                logger.info("Data import for integration %s completed successfully.", hp_task.integration.id)
                log_task_event(
                    "process_data_import_task", "dispatched",
                    f"High priority task for integration {hp_task.integration.id} processed at {timezone.now()}"
                )
            # --- End inline business logic ---

            # Mark the high priority task as processed regardless of outcome.
            hp_task.processed = True
            hp_task.save(update_fields=['processed'])
        else:
            logger.info("No high priority task. Running continuous data sync.")
            run_data_sync.delay()
            log_task_event(
                "run_data_sync", "dispatched",
                f"Continuous sync task dispatched at {timezone.now()}"
            )
    except Exception as exc:
        logger.error("Dispatcher encountered an error: %s", exc, exc_info=True)
        log_task_event("dispatcher", "failed", str(exc))
        raise self.retry(exc=exc, countdown=10)
    else:
        logger.info("Dispatcher completed successfully.")
        log_task_event("dispatcher", "success", f"Task completed successfully at {timezone.now()}")
    finally:
        dispatcher.apply_async(countdown=5)
