import logging
from celery import shared_task, chain
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime
from config.celery import get_active_org_sync_tasks
from core.models import BusinessHours

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
    (Deprecated) Original integration‑level sync task.
    Kept for high priority tasks if needed.
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


@shared_task(bind=True, queue="org_sync")
def sync_organization(self, organization_id):
    """
    Syncs all integrations for a given organization.
    Uses a cache lock to prevent concurrent sync for the same organization.
    Each integration sync task is dispatched asynchronously so that a hanging task
    does not block the rest.
    """
    lock_key = f"org_sync_lock_{organization_id}"
    if not cache.add(lock_key, "in_progress", 600):
        logger.info("Organization %s is already being processed. Skipping.", organization_id)
        return

    try:
        from integrations.models.models import Integration
        logger.warning("Starting sync for organization %s", organization_id)
        org_integrations = Integration.objects.filter(org=organization_id).order_by('-id')
        for integration in org_integrations:
            logger.warning("Starting sync for integration %s", integration.id)
            # Determine integration type based on which credentials are populated.
            if integration.toast_client_id and integration.toast_client_secret and integration.toast_api_url:
                integration_type = "toast"
            elif integration.xero_client_id and integration.xero_client_secret:
                integration_type = "xero"
            elif integration.netsuite_client_id and integration.netsuite_client_secret:
                integration_type = "netsuite"
            else:
                integration_type = "unknown"

            if integration_type == "xero":
                logger.info("Dispatching Xero sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.xero import sync_single_xero_data
                sync_single_xero_data.apply_async(args=[integration.id])
            elif integration_type == "netsuite":
                logger.info("Dispatching NetSuite sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.netsuite import sync_single_netsuite_data
                sync_single_netsuite_data.apply_async(args=[integration.id])
            elif integration_type == "toast":
                logger.info("Dispatching Toast sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.toast import sync_toast_data
                # Explicitly route toast tasks to the same queue.
                sync_toast_data.apply_async(args=[integration.id], queue="org_sync")
            else:
                logger.warning("Unknown integration type for integration %s", integration.id)
        logger.info("Completed dispatching sync for organization %s", organization_id)
        log_task_event("sync_organization", "success", f"Organization {organization_id} sync dispatch completed at {timezone.now()}")
    except Exception as exc:
        logger.error("Error syncing organization %s: %s", organization_id, exc, exc_info=True)
        log_task_event("sync_organization", "failed", f"Organization {organization_id} sync failed: {exc}")
        raise exc
    finally:
        cache.delete(lock_key)


@shared_task(bind=True, queue="high_priority")
def process_high_priority(self, hp_task_id):
    from integrations.models.models import HighPriorityTask, Integration
    from integrations.modules import MODULES

    try:
        hp_task = HighPriorityTask.objects.get(pk=hp_task_id)
        logger.info("Processing High Priority task for integration: %s with since_date: %s",
                    hp_task.integration.id, hp_task.since_date)
    except HighPriorityTask.DoesNotExist:
        logger.error("HighPriorityTask with ID %s does not exist", hp_task_id)
        return
    
    try:
        integration = Integration.objects.get(pk=hp_task.integration.id)
        logger.info("Integration found: %s", integration.id)
    except Integration.DoesNotExist:
        logger.error("Integration with ID %s does not exist.", hp_task.integration.id)
        hp_task.processed = True
        hp_task.save(update_fields=["processed"])
        log_task_event("process_data_import_task", "failed",
                        f"Integration with ID {hp_task.integration.id} does not exist at {timezone.now()}")
        return

    # Prepare since_date and importer
    since_date = (hp_task.since_date.strftime("%Y-%m-%d") if hp_task.since_date else None)
    module_config = MODULES[hp_task.integration_type]
    logger.info("Module config: %s", {
        'client': module_config['client'].__name__,
        'import_methods': {k: v.__name__ for k, v in module_config.get('import_methods', {}).items()},
        'full_import': module_config.get('full_import').__name__ if module_config.get('full_import') else None
    })
    ImporterClass = module_config["client"]
    logger.info("Importer class: %s", ImporterClass.__name__)
    logger.info("Processing High Priority task for integration: %s with since_date: %s",
                integration, since_date)
    

    importer = ImporterClass(integration, since_date)
    
    if hp_task.selected_modules:
        for module in hp_task.selected_modules:
            import_func = module_config["import_methods"].get(module)
            logger.info("Import function: %s", import_func.__name__)
            if import_func:
                logger.info("Importing %s for integration ID %s", module, hp_task.integration.id)
                import_func(importer)
            else:
                logger.warning("Unknown module %s for integration ID %s", module, hp_task.integration.id)
    else:
        full_import = module_config.get("full_import")
        if full_import:
            logger.info("Starting full import for integration ID %s", hp_task.integration.id)
            full_import(importer)
        else:
            for import_func in module_config["import_methods"].values():
                import_func(importer)
    
    logger.info("Data import for integration %s completed successfully.", hp_task.integration.id)
    log_task_event("process_data_import_task", "dispatched",
                    f"High priority task for integration {hp_task.integration.id} processed at {timezone.now()}")
    hp_task.processed = True
    hp_task.save(update_fields=["processed"])
    
       
@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    Polls continuously for high priority tasks and organization sync tasks.
    During business hours (8am–6pm UTC), only organization‑level sync tasks are dispatched;
    outside those hours, only high priority tasks are processed.
    This task re-enqueues itself every 5 seconds.
    It will dispatch up to 3 organization sync tasks concurrently.
    """
    try:
        current_time = timezone.now()
        current_hour = current_time.hour


        from integrations.models.models import Integration
        # Get distinct organization IDs.
        org_ids = list(Integration.objects.values_list("org", flat=True).distinct().order_by("-org"))
        max_org_sync = 3
        active_count = get_active_org_sync_tasks()
        logger.info(f"Currently active organization sync tasks: {active_count}")
        # Loop through organizations and dispatch if we're under the limit.
        for org_id in org_ids:
            if get_active_org_sync_tasks() < max_org_sync:
                from core.tasks.general import sync_organization
                logger.info(f"Dispatching sync for organization {org_id}")
                sync_organization.apply_async(args=[org_id])
            else:
                logger.info("Maximum concurrent organization sync tasks reached; will try dispatching later.")
                break
        log_task_event("dispatcher", "dispatched", f"Organization sync tasks dispatched at {timezone.now()}")
        
        hp_task = get_high_priority_task()
        if hp_task:
            # Enqueue for the dedicated high priority worker
            process_high_priority.apply_async(args=[hp_task.id], queue="high_priority")
        else:
            logger.info("No high priority tasks found.")
    except Exception as exc:
        logger.error("Dispatcher encountered an error: %s", exc, exc_info=True)
        log_task_event("dispatcher", "failed", str(exc))
        raise self.retry(exc=exc, countdown=10)
    else:
        logger.info("Dispatcher completed successfully.")
        log_task_event("dispatcher", "success", f"Task completed successfully at {timezone.now()}")
    finally:
        # Re-enqueue dispatcher after a brief wait
        dispatcher.apply_async(countdown=5)


