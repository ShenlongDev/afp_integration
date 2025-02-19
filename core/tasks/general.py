"""
General tasks and helpers for overall data syncing and import management.
"""

import logging
from celery import shared_task
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime

logger = logging.getLogger(__name__)

# Global lock functions
GLOBAL_TASK_LOCK_KEY = "global_task_lock"

def acquire_global_lock(timeout=600):
    """
    Acquire a global lock to prevent overlapping task dispatches.
    """
    return cache.add(GLOBAL_TASK_LOCK_KEY, "locked", timeout)

def release_global_lock():
    """
    Release the global lock.
    """
    cache.delete(GLOBAL_TASK_LOCK_KEY)

# Helper for logging task events (assumes TaskLog model exists)
from core.models import TaskLog 

def log_task_event(task_name, status, detail):
    TaskLog.objects.create(
        task_name=task_name,
        status=status,
        detail=detail,
        timestamp=timezone.now()
    )

# High priority task helper
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

@shared_task(bind=True, max_retries=5)
def process_data_import_task(self, integration_id, integration_type, since_date_str, selected_modules):
    """
    Processes data import for a specific integration.
    """
    if not acquire_global_lock():
        logger.info("process_data_import_task: Another task is in progress. Retrying in 10 seconds...")
        raise self.retry(countdown=10)
    
    try:
        from integrations.models.models import Integration
        integration = Integration.objects.get(pk=integration_id)
        since_date = datetime.strptime(since_date_str, "%Y-%m-%d")
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

@shared_task(bind=True, max_retries=3)
def run_data_sync(self):
    """
    Schedules Netsuite and Xero sync subtasks.
    Acquires a global lock to ensure only one top-level sync is scheduled at a time.
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
        
        logger.info("All Netsuite and Xero sync subtasks have been dispatched.")
    except Exception as e:
        logger.error("Error in run_data_sync: %s", e)
        raise e
    finally:
        release_global_lock()


@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    Polls for high priority tasks in a continuous loop.
    If a high priority task exists, dispatches process_data_import_task.
    Otherwise, dispatches run_data_sync.
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
        # Re-enqueue the dispatcher to run again after a delay.
        dispatcher.apply_async(countdown=5) 
        
        