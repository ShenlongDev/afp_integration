import logging
import time # Import time for sleep
from celery import shared_task, chain
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime, timedelta
from config.celery import get_active_org_sync_tasks
from core.models import TaskLog 

logger = logging.getLogger(__name__)

GLOBAL_TASK_LOCK_KEY = "global_task_lock"

DISPATCHER_LOCK_KEY = "dispatcher_task_lock"
DISPATCHER_LOCK_TIMEOUT = 60 
IN_FLIGHT_ORG_SYNC_COUNT_KEY = "in_flight_org_sync_count" 
COUNTER_TIMEOUT = 3600 
ORG_OFFSET_CACHE_KEY = "dispatcher_org_offset" 

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

def log_task_event(task_name, status, detail):
    TaskLog.objects.create(
        task_name=task_name,
        status=status,
        detail=detail,
        timestamp=timezone.now()
    )

def get_high_priority_task():
    """
    Returns the earliest unprocessed high priority task that is not already in progress.
    """
    try:
        from integrations.models.models import HighPriorityTask
        from django.db import transaction
        
        with transaction.atomic():
            task = HighPriorityTask.objects.select_for_update().filter(
                processed=False, 
                in_progress=False
            ).order_by('created_at').first()
            print(task)
            
            if task:
                task.in_progress = True
                task.in_progress_since = timezone.now()
                task.save(update_fields=["in_progress", "in_progress_since"])
                
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
    Decrements the in-flight counter upon completion or failure.
    """
    logger.info(f"SYNC_ORGANIZATION_TASK: Entered for Org ID: {organization_id}")
    lock_key = f"org_sync_lock_{organization_id}"

    # Check for duplicate processing lock first
    if not cache.add(lock_key, "in_progress", 600):
        logger.warning(f"Organization {organization_id} sync lock already held. Skipping this instance.")
        # Do NOT decrement the counter here. The instance that *holds* the lock
        # will be responsible for decrementing when it finishes.
        # If this instance was dispatched mistakenly due to a race condition
        # before the lock was fully set, the dispatcher's check *before* incr
        # should ideally prevent over-dispatching in the first place.
        return

    try:
        logger.info(f"Actually starting sync for organization {organization_id}")
        from integrations.models.models import Integration
        logger.warning("Starting sync for organization %s", organization_id)
        org_integrations = Integration.objects.filter(org=organization_id).order_by('-id')

        integration_dispatched = False # Flag to see if any sub-tasks were dispatched
        for integration in org_integrations:
            logger.warning("Starting sync for integration %s", integration.id)

            # Check Toast
            if integration.toast_client_id and integration.toast_client_secret and integration.toast_api_url:
                logger.info("Dispatching Toast sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.toast import sync_toast_data
                sync_toast_data.apply_async(args=[integration.id], queue="org_sync")
                integration_dispatched = True

            # Check Xero
            if integration.xero_client_id and integration.xero_client_secret:
                logger.info("Dispatching Xero sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.xero import sync_single_xero_data
                sync_single_xero_data.apply_async(args=[integration.id]) 
                integration_dispatched = True

            # Check NetSuite
            if integration.netsuite_account_id and integration.netsuite_consumer_key:
                logger.info("Dispatching NetSuite sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.netsuite import sync_single_netsuite_data
                sync_single_netsuite_data.apply_async(args=[integration.id])
                integration_dispatched = True

            if not integration_dispatched and not (integration.toast_client_id or integration.xero_client_id or integration.netsuite_account_id):
                 logger.warning("Unknown or incomplete integration type for integration %s", integration.id)


        if integration_dispatched:
            logger.info("Completed dispatching sub-tasks for organization %s", organization_id)
            log_task_event("sync_organization", "success", f"Organization {organization_id} sync dispatch completed at {timezone.now()}")
        else:
             logger.warning("No valid integration sub-tasks found or dispatched for organization %s", organization_id)
             log_task_event("sync_organization", "warning", f"No sub-tasks dispatched for Organization {organization_id} at {timezone.now()}")


    except Exception as exc:
        logger.error("Error during sync_organization %s: %s", organization_id, exc, exc_info=True)
        log_task_event("sync_organization", "failed", f"Organization {organization_id} sync failed: {exc}")
    finally:
        cache.delete(lock_key)
        try:
            new_val = cache.decr(IN_FLIGHT_ORG_SYNC_COUNT_KEY)
            cache.touch(IN_FLIGHT_ORG_SYNC_COUNT_KEY, COUNTER_TIMEOUT)
            logger.info(f"SYNC_ORGANIZATION_TASK: Decremented in-flight count for Org ID: {organization_id}. New count: {new_val}")
            if new_val < 0:
                 logger.warning(f"In-flight count went below zero ({new_val}) for Org {organization_id}. Resetting to 0.")
                 cache.set(IN_FLIGHT_ORG_SYNC_COUNT_KEY, 0, timeout=COUNTER_TIMEOUT)

        except Exception as e:
             logger.error(f"SYNC_ORGANIZATION_TASK: Failed to decrement or touch in-flight count for Org ID: {organization_id}. Error: {e}", exc_info=True)


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
            
        since_date = hp_task.since_date
        until_date = hp_task.until_date
        
        module_config = MODULES[hp_task.integration_type]
        ImporterClass = module_config["client"]
        logger.info("Processing High Priority task for integration: %s with since_date: %s",
                    integration, since_date)
        HighPriorityTask.objects.filter(pk=hp_task_id).update(in_progress=True, in_progress_since=timezone.now())

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
        current_time = timezone.now()
        hp_task.processed = True
        hp_task.in_progress = False
        hp_task.processed_at = current_time
        hp_task.save(update_fields=["processed", "in_progress", "processed_at"])


@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    Polls continuously for high priority tasks and organization sync tasks.
    This task re-enqueues itself every 5 seconds.
    Ensures only one instance runs at a time using a cache lock.
    It will dispatch up to MAX_CONCURRENT_ORG_SYNC_TASKS organization sync tasks concurrently.
    """
    lock_acquired = cache.add(DISPATCHER_LOCK_KEY, "running", DISPATCHER_LOCK_TIMEOUT)
    if not lock_acquired:
        logger.warning("Dispatcher lock ALREADY HELD. Skipping this execution.")
        return
    else:
        logger.info("Dispatcher lock ACQUIRED.")

    try:
        hp_task = get_high_priority_task()
        if hp_task:
            process_high_priority.apply_async(
                args=[hp_task.id],
                queue="high_priority",
                priority=9,
                countdown=0
            )
        else:
            logger.info("No high priority tasks found.")

        from integrations.models.models import Integration
        MAX_CONCURRENT_ORG_SYNC_TASKS = 3

        current_in_flight = cache.get(IN_FLIGHT_ORG_SYNC_COUNT_KEY)
        if current_in_flight is None:
            logger.info("In-flight counter key not found or expired. Initializing to 0.")
            cache.set(IN_FLIGHT_ORG_SYNC_COUNT_KEY, 0, timeout=COUNTER_TIMEOUT)
            current_in_flight = 0
        else:
            try:
                current_in_flight = int(current_in_flight)
                cache.touch(IN_FLIGHT_ORG_SYNC_COUNT_KEY, COUNTER_TIMEOUT)
            except (ValueError, TypeError):
                logger.warning(f"Could not parse in-flight count '{current_in_flight}'. Resetting to 0.")
                cache.set(IN_FLIGHT_ORG_SYNC_COUNT_KEY, 0, timeout=COUNTER_TIMEOUT)
                current_in_flight = 0

        if current_in_flight < 0:
            logger.warning(f"In-flight count was negative ({current_in_flight}). Resetting to 0.")
            cache.set(IN_FLIGHT_ORG_SYNC_COUNT_KEY, 0, timeout=COUNTER_TIMEOUT)
            current_in_flight = 0

        logger.info(f"Current in-flight organization sync tasks (from cache): {current_in_flight}")

        slots_to_fill = MAX_CONCURRENT_ORG_SYNC_TASKS - current_in_flight

        if slots_to_fill > 0:
            logger.info(f"Attempting to fill {slots_to_fill} slots.")
            all_orgs = list(Integration.objects.values_list("org", flat=True).distinct().order_by("org"))
            total_orgs = len(all_orgs)

            if total_orgs > 0:
                logger.info(f"Dispatcher: Attempting to read offset. Key: {ORG_OFFSET_CACHE_KEY}")
                org_offset = cache.get(ORG_OFFSET_CACHE_KEY, 0) 
                logger.info(f"Dispatcher: Read offset value: {org_offset}")
                try: 
                    org_offset = int(org_offset)
                except (ValueError, TypeError):
                    org_offset = 0

                orgs_dispatched_this_run = 0

                for i in range(slots_to_fill):
                    count_before_incr = cache.get(IN_FLIGHT_ORG_SYNC_COUNT_KEY, 0)
                    try:
                         count_before_incr = int(count_before_incr)
                         if count_before_incr < 0 : count_before_incr = 0
                    except(ValueError, TypeError):
                         count_before_incr = 0

                    if count_before_incr < MAX_CONCURRENT_ORG_SYNC_TASKS:
                        new_count = cache.incr(IN_FLIGHT_ORG_SYNC_COUNT_KEY)
                        cache.touch(IN_FLIGHT_ORG_SYNC_COUNT_KEY, COUNTER_TIMEOUT) 

                        current_index = (org_offset + orgs_dispatched_this_run) % total_orgs
                        org_to_dispatch = all_orgs[current_index]

                        logger.info(f"Dispatching sync for organization {org_to_dispatch} (In-flight count after incr: {new_count})")
                        sync_organization.apply_async(args=[org_to_dispatch], queue="org_sync")
                        orgs_dispatched_this_run += 1
                    else:
                        logger.info(f"Limit ({MAX_CONCURRENT_ORG_SYNC_TASKS}) reached (Current: {count_before_incr}). Breaking dispatch loop.")
                        break

                if orgs_dispatched_this_run > 0:
                    new_offset = (org_offset + orgs_dispatched_this_run) % total_orgs
                    logger.info(f"Dispatcher: Attempting to set offset. Key: {ORG_OFFSET_CACHE_KEY}, Value: {new_offset}")
                    cache.set(ORG_OFFSET_CACHE_KEY, new_offset, timeout=None) 
                    logger.info(f"Dispatcher: Set offset complete.")
                    log_task_event("dispatcher", "dispatched", f"Dispatched {orgs_dispatched_this_run} org tasks. Offset now {new_offset}.")
                else:
                     logger.info("No new organization tasks were dispatched in this run (limit reached or no orgs).")

            else: 
                 logger.info("No organizations found in database for dispatch.")
        else: 
             logger.info(f"In-flight count ({current_in_flight}) meets or exceeds limit ({MAX_CONCURRENT_ORG_SYNC_TASKS}). Waiting.")

    except Exception as exc:
        logger.error("Dispatcher encountered an error: %s", exc, exc_info=True)
        log_task_event("dispatcher", "failed", str(exc))
        raise 
    finally:
        if lock_acquired:
            cache.delete(DISPATCHER_LOCK_KEY)
            logger.info("Dispatcher lock RELEASED.")
            dispatcher.apply_async(countdown=5) 


@shared_task
def daily_previous_day_sync():
    """
    Scheduled task that runs daily to pull all data from the previous day
    for all integrations and ALL their modules. Handles multiple integration
    types per integration record.
    """
    yesterday = datetime.now().date() - timedelta(days=1)
    
    logger.info(f"Starting multi-type daily sync for previous day: {yesterday}")
    
    log_task_event(
        "daily_previous_day_sync", 
        "started", 
        f"Starting multi-type sync for previous day {yesterday}"
    )
    
    try:
        from integrations.models.models import Integration, HighPriorityTask
        from integrations.modules import MODULES
        
        all_integrations = Integration.objects.all()
        
        if not all_integrations.exists():
            logger.info("No integrations found for daily sync")
            return
        
        total_sync_count = 0
        
        for integration in all_integrations:
            integration_types = []
            
            if integration.toast_client_id and integration.toast_client_secret and integration.toast_api_url:
                integration_types.append("toast")
                
            if integration.xero_client_id and integration.xero_client_secret:
                integration_types.append("xero")
                
            if integration.netsuite_account_id and integration.netsuite_consumer_key:
                integration_types.append("netsuite")
            
            if not integration_types:
                logger.warning(f"No valid integration types found for integration {integration.id}, skipping")
                continue
                
            for integration_type in integration_types:
                all_modules = list(MODULES.get(integration_type, {}).get("import_methods", {}).keys())
                
                if not all_modules:
                    logger.warning(f"No modules found for integration type {integration_type}, integration {integration.id}")
                    continue
                    
                hp_task = HighPriorityTask.objects.create(
                    integration=integration,
                    integration_type=integration_type,
                    since_date=yesterday,
                    until_date=yesterday,
                    processed=False,
                    selected_modules=all_modules
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
        daily_previous_day_sync()
        return "Task executed successfully"
    except Exception as e:
        logger.error(f"Manual task execution failed: {str(e)}", exc_info=True)
        return f"Task execution failed: {str(e)}"
