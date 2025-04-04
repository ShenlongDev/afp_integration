import logging
from celery import shared_task, chain
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime, timedelta
from config.celery import get_active_org_sync_tasks

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
    Handles multiple integration types per integration record.
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
            
            # Check each integration type independently
            
            # Check Toast
            if integration.toast_client_id and integration.toast_client_secret and integration.toast_api_url:
                logger.info("Dispatching Toast sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.toast import sync_toast_data
                sync_toast_data.apply_async(args=[integration.id], queue="org_sync")
            
            # Check Xero
            if integration.xero_client_id and integration.xero_client_secret:
                logger.info("Dispatching Xero sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.xero import sync_single_xero_data
                sync_single_xero_data.apply_async(args=[integration.id])
            
            # Check NetSuite    
            if integration.netsuite_account_id and integration.netsuite_consumer_key:
                logger.info("Dispatching NetSuite sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.netsuite import sync_single_netsuite_data
                sync_single_netsuite_data.apply_async(args=[integration.id])
            
            # Check if no valid integration types were found
            if not (integration.toast_client_id or integration.xero_client_id or integration.netsuite_account_id):
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
    except HighPriorityTask.DoesNotExist:
        logger.error("HighPriorityTask with ID %s does not exist", hp_task_id)
        return
    
    try:
        integration = Integration.objects.get(pk=hp_task.integration.id)
    except Integration.DoesNotExist:
        logger.error("Integration with ID %s does not exist.", hp_task.integration.id)
        hp_task.processed = True
        hp_task.save(update_fields=["processed"])
        log_task_event("process_data_import_task", "failed",
                        f"Integration with ID {hp_task.integration.id} does not exist at {timezone.now()}")
        return

    try:
        # Check if integration_type exists in MODULES
        if hp_task.integration_type not in MODULES:
            logger.error(
                "Unknown integration type %s for task %s", 
                hp_task.integration_type, 
                hp_task_id
            )
            log_task_event(
                "process_data_import_task",
                "failed",
                f"Unknown integration type {hp_task.integration_type} for task {hp_task_id}"
            )
            hp_task.processed = True
            hp_task.save(update_fields=["processed"])
            return
            
        # Prepare since_date and importer
        since_date = hp_task.since_date
        until_date = hp_task.until_date
        
        module_config = MODULES[hp_task.integration_type]
        ImporterClass = module_config["client"]
        logger.info("Processing High Priority task for integration: %s with since_date: %s",
                    integration, since_date)
        
        # Create importer instance
        importer = ImporterClass(integration, since_date, until_date)
        
        if hp_task.selected_modules:
            processed_any_module = False
            for module in hp_task.selected_modules:
                import_func = module_config["import_methods"].get(module)
                if import_func:
                    try:
                        logger.info("Importing %s for integration ID %s", module, hp_task.integration.id)
                        import_func(importer)
                        processed_any_module = True
                    except Exception as module_exception:
                        logger.error(
                            "Error processing module %s for integration %s: %s",
                            module,
                            hp_task.integration.id,
                            module_exception,
                            exc_info=True,
                        )
                else:
                    logger.warning("Unknown module %s for integration ID %s", module, hp_task.integration.id)
                    
            if not processed_any_module:
                logger.warning("No valid modules were processed for task %s", hp_task_id)
        else:
            full_import = module_config.get("full_import")
            if full_import:
                try:
                    logger.info("Starting full import for integration ID %s", hp_task.integration.id)
                    full_import(importer)
                except Exception as full_exception:
                    logger.error(
                        "Error in full import for integration %s: %s",
                        hp_task.integration.id,
                        full_exception,
                        exc_info=True,
                    )
            else:
                processed_any_function = False
                for import_func in module_config["import_methods"].values():
                    try:
                        import_func(importer)
                        processed_any_function = True
                    except Exception as func_exception:
                        logger.error(
                            "Error processing import function for integration %s: %s",
                            hp_task.integration.id,
                            func_exception,
                            exc_info=True,
                        )
                
                if not processed_any_function:
                    logger.warning("No import functions were successfully processed for task %s", hp_task_id)
        
        logger.info("Data import for integration %s completed successfully.", hp_task.integration.id)
        log_task_event("process_data_import_task", "dispatched",
                        f"High priority task for integration {hp_task.integration.id} processed at {timezone.now()}")
    except Exception as e:
        logger.error(
            "General error processing high priority task %s: %s",
            hp_task_id,
            e,
            exc_info=True,
        )
        raise
    finally:
        # Always mark as processed when we're done, regardless of success/failure
        hp_task.processed = True
        hp_task.save(update_fields=["processed"])


@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    Polls continuously for high priority tasks and organization sync tasks.
    This task re-enqueues itself every 5 seconds.
    It will dispatch up to 3 organization sync tasks concurrently.
    """
    try:                
        hp_task = get_high_priority_task()
        if hp_task:
            # Enqueue for the dedicated high priority worker.
            process_high_priority.apply_async(args=[hp_task.id], queue="high_priority")
        else:
            logger.info("No high priority tasks found.")
        
        from integrations.models.models import Integration
        org_ids = list(Integration.objects.values_list("org", flat=True).distinct().order_by("-org"))
        max_org_sync = 3
        active_count = get_active_org_sync_tasks()
        logger.info(f"Currently active organization sync tasks: {active_count}")
        for org_id in org_ids:
            if get_active_org_sync_tasks() < max_org_sync:
                from core.tasks.general import sync_organization
                logger.info(f"Dispatching sync for organization {org_id}")
                sync_organization.apply_async(args=[org_id])
            else:
                logger.info("Maximum concurrent organization sync tasks reached; will try dispatching later.")
                break
        log_task_event("dispatcher", "dispatched", f"Organization sync tasks dispatched at {timezone.now()}")

    except Exception as exc:
        logger.error("Dispatcher encountered an error: %s", exc, exc_info=True)
        log_task_event("dispatcher", "failed", str(exc))
        raise self.retry(exc=exc, countdown=10)
    else:
        logger.info("Dispatcher completed successfully.")
        log_task_event("dispatcher", "success", f"Task completed successfully at {timezone.now()}")
    finally:
        dispatcher.apply_async(countdown=5)


@shared_task
def daily_previous_day_sync():
    """
    Scheduled task that runs daily to pull all data from the previous day
    for all integrations and ALL their modules. Handles multiple integration
    types per integration record.
    """
    yesterday = (timezone.now() - timedelta(days=1)).date()
    logger.info(f"Starting multi-type daily sync for previous day: {yesterday}")
    
    log_task_event(
        "daily_previous_day_sync", 
        "started", 
        f"Starting multi-type sync for previous day {yesterday}"
    )
    
    try:
        # Get all integrations
        from integrations.models.models import Integration, HighPriorityTask
        from integrations.modules import MODULES
        
        all_integrations = Integration.objects.all()
        
        if not all_integrations.exists():
            logger.info("No integrations found for daily sync")
            return
        
        total_sync_count = 0
        
        # Process each integration
        for integration in all_integrations:
            # Check for EACH integration type independently - no elif chain
            integration_types = []
            
            # Check Toast
            if integration.toast_client_id and integration.toast_client_secret and integration.toast_api_url:
                integration_types.append("toast")
                
            # Check Xero
            if integration.xero_client_id and integration.xero_client_secret:
                integration_types.append("xero")
                
            # Check NetSuite - use client_id and client_secret consistently
            if integration.netsuite_account_id and integration.netsuite_consumer_key:
                integration_types.append("netsuite")
            
            if not integration_types:
                logger.warning(f"No valid integration types found for integration {integration.id}, skipping")
                continue
                
            # Process each integration type for this integration
            for integration_type in integration_types:
                # Get all available modules for this integration type
                all_modules = list(MODULES.get(integration_type, {}).get("import_methods", {}).keys())
                
                if not all_modules:
                    logger.warning(f"No modules found for integration type {integration_type}, integration {integration.id}")
                    continue
                    
                # Create high priority task with ALL available modules explicitly listed
                hp_task = HighPriorityTask.objects.create(
                    integration=integration,
                    integration_type=integration_type,
                    since_date=yesterday,
                    until_date=yesterday,
                    processed=False,
                    selected_modules=all_modules  # Explicitly include ALL modules
                )
                
                module_list_str = ", ".join(all_modules)
                total_sync_count += 1
                logger.info(f"Created full sync task for integration {integration.id} (type: {integration_type}) with modules: {module_list_str}")
        
        log_task_event(
            "daily_previous_day_sync", 
            "success", 
            f"Scheduled {total_sync_count} complete sync tasks across multiple integration types for previous day {yesterday}"
        )
        
    except Exception as e:
        logger.error(f"Error in daily previous day sync: {str(e)}", exc_info=True)
        log_task_event("daily_previous_day_sync", "failed", f"Error: {str(e)}")
        raise


@shared_task
def trigger_previous_day_sync_test():
    """Test function to trigger the daily sync directly for debugging"""
    logger.info("Manually triggering daily sync for testing")
    try:
        # Run the sync directly, not as a separate task
        daily_previous_day_sync()
        return "Task executed successfully"
    except Exception as e:
        logger.error(f"Manual task execution failed: {str(e)}", exc_info=True)
        return f"Task execution failed: {str(e)}"
