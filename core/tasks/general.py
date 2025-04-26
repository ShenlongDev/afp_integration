import logging
import time
from celery import shared_task, chain
from django.core.cache import cache
from django.utils import timezone
from datetime import datetime, timedelta
from config.celery import get_active_org_sync_tasks
from core.models import TaskLog 
import signal
from django.db import close_old_connections
from celery.signals import worker_ready

logger = logging.getLogger(__name__)

GLOBAL_TASK_LOCK_KEY = "global_task_lock"

DISPATCHER_LOCK_KEY = "dispatcher_task_lock"
DISPATCHER_LOCK_TIMEOUT = 60 
IN_FLIGHT_ORG_SYNC_COUNT_KEY = "in_flight_org_sync_count" 
COUNTER_TIMEOUT = 3600 
ORG_OFFSET_CACHE_KEY = "dispatcher_org_offset" 

HIGH_PRIORITY_WORKER = False

# Create separate keys for different channels
DATA_TASK_ACTIVE_KEY = "active_high_priority_data_task"
SYSTEM_TASK_ACTIVE_KEY = "active_high_priority_system_task"

# Keys for our semaphore system
HIGH_PRIORITY_TASK_SEMAPHORE = "high_priority_data_task_semaphore"
HIGH_PRIORITY_TASK_ACTIVE_COUNT = "high_priority_data_task_active_count"
SYSTEM_TASK_ACTIVE_KEY = "high_priority_system_task_active"

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
        return

    try:
        logger.info(f"Actually starting sync for organization {organization_id}")
        from integrations.models.models import Integration
        logger.warning("Starting sync for organization %s", organization_id)
        org_integrations = Integration.objects.filter(organisation_id=organization_id).order_by('-id')

        integration_dispatched = False 
        for integration in org_integrations:
            logger.warning("Starting sync for integration %s", integration.id)
            integration_type = integration.integration_type.lower()
            settings = integration.settings
            print(settings)

            # Check Toast
            if integration_type == 'toast' and settings.get('client_id') and settings.get('client_secret') and settings.get('api_url'):
                logger.info("Dispatching Toast sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.toast import sync_toast_data
                sync_toast_data.apply_async(args=[integration.id], queue="org_sync")
                integration_dispatched = True

            # Check Xero
            if integration_type == 'xero' and settings.get('client_id') and settings.get('client_secret'):
                logger.info("Dispatching Xero sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.xero import sync_single_xero_data
                sync_single_xero_data.apply_async(args=[integration.id]) 
                integration_dispatched = True

            # Check NetSuite
            if integration_type == 'netsuite' and settings.get('account_id') and settings.get('consumer_key'):
                logger.info("Dispatching NetSuite sync for integration %s for organization %s", integration.id, organization_id)
                from core.tasks.netsuite import sync_single_netsuite_data
                sync_single_netsuite_data.apply_async(args=[integration.id])
                integration_dispatched = True

            if not integration_dispatched:
                logger.warning(f"No valid credentials found for integration {integration.id} of type {integration_type}")

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
def process_high_priority(self, hp_task_id, semaphore_id=None):
    """Process a high priority task, releasing the semaphore when done"""
    from integrations.models.models import HighPriorityTask, Integration
    from integrations.modules import MODULES

    # Close any stale connections
    close_old_connections()
    
    original_term_handler = signal.getsignal(signal.SIGTERM)
    
    def ignore_sigterm(*args, **kwargs):
        logger.warning(f"High priority task {hp_task_id} ignoring SIGTERM (warm shutdown)")
    
    signal.signal(signal.SIGTERM, ignore_sigterm)
    
    try:
        print(f"Processing high priority task {hp_task_id}")
        logger.warning(f"Processing high priority task")
        HighPriorityTask.objects.filter(pk=hp_task_id, in_progress=False).update(
            in_progress=True, 
            in_progress_since=timezone.now()
        )
        
        hp_task = HighPriorityTask.objects.get(pk=hp_task_id)
        
        if hp_task.in_progress and hp_task.in_progress_since and (timezone.now() - hp_task.in_progress_since).total_seconds() > 300:
            logger.warning(f"Task {hp_task_id} has been in progress for more than 5 minutes. Attempting to process anyway.")
            
        integration = Integration.objects.get(pk=hp_task.integration.id)

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
        # Make sure we restore the original signal handler
        signal.signal(signal.SIGTERM, original_term_handler)
        close_old_connections()
        
        try:
            current_time = timezone.now()
            HighPriorityTask.objects.filter(pk=hp_task_id).update(
                processed=True, 
                in_progress=False, 
                processed_at=current_time
            )
            logger.info(f"Successfully marked high priority task {hp_task_id} as processed")
            
            # Clear the active DATA task flag
            cache.delete(DATA_TASK_ACTIVE_KEY)
            
        except Exception as e:
            logger.error(f"Failed to update high priority task {hp_task_id} status: {e}")
            cache.delete(DATA_TASK_ACTIVE_KEY)
        
        if semaphore_id:
            release_data_task_semaphore(semaphore_id)
            logger.info(f"Released semaphore {semaphore_id} for task {hp_task_id}")


@shared_task(bind=True, max_retries=3)
def dispatcher(self):
    """
    Polls continuously for organization sync tasks only.
    This task re-enqueues itself every 5 seconds.
    """
    lock_acquired = cache.add(DISPATCHER_LOCK_KEY, "running", DISPATCHER_LOCK_TIMEOUT)
    if not lock_acquired:
        logger.warning("Dispatcher lock ALREADY HELD. Skipping this execution.")
        return
    else:
        logger.info("Dispatcher lock ACQUIRED.")

    try:
        
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
            all_orgs = list(Integration.objects.values_list("organisation_id", flat=True).distinct().order_by("organisation_id"))
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
        
        all_integrations = Integration.objects.filter(is_active=True)
        
        if not all_integrations.exists():
            logger.info("No active integrations found for daily sync")
            return
        
        total_sync_count = 0
        
        for integration in all_integrations:
            integration_type = integration.integration_type.lower()
            
            if integration_type not in MODULES:
                logger.warning(f"Unsupported integration type {integration_type} for integration {integration.id}, skipping")
                continue
                
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


@shared_task(queue="high_priority")
def monitor_stuck_high_priority_tasks():
    """
    Check for high priority tasks that were never processed and dispatch them
    to the high priority worker.
    """
    from integrations.models.models import HighPriorityTask
    from django.db import close_old_connections, transaction
    
    # Use a unique lock key for this monitor run to prevent duplicates
    monitor_lock_key = f"monitor_stuck_tasks_lock_{int(time.time())}"
    if not cache.add(monitor_lock_key, "locked", timeout=300):  # 5 minutes
        logger.info("Another monitor task is already running. Skipping.")
        return
    
    try:
        close_old_connections()
        
        active_system_task = cache.get(SYSTEM_TASK_ACTIVE_KEY)
        if active_system_task:
            logger.info(f"Another system task {active_system_task} is already running. Skipping monitoring.")
            return
        
        cache.set(SYSTEM_TASK_ACTIVE_KEY, "monitor_stuck_tasks", timeout=1800)  # 30 minutes
        
        try:
            current_count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
            try:
                current_count = int(current_count)
            except (ValueError, TypeError):
                current_count = 0
                
            slots_to_fill = 2 - current_count
            
            if slots_to_fill <= 0:
                logger.info(f"Already running {current_count} tasks. No slots to fill from monitor.")
                return
                
            logger.info(f"Monitor attempting to fill {slots_to_fill} slots for stuck tasks")
            
            created_threshold = timezone.now() - timedelta(minutes=1)
            missed_tasks = HighPriorityTask.objects.filter(
                in_progress=False,
                processed=False,
                created_at__lt=created_threshold
            ).order_by('created_at')[:slots_to_fill]
            
            if missed_tasks:
                logger.warning(f"Found {len(missed_tasks)} high priority tasks that were never processed")
                
                # Process each task
                for task in missed_tasks:
                    # Try to acquire a semaphore
                    semaphore_id = acquire_data_task_semaphore()
                    if not semaphore_id:
                        logger.info("Failed to acquire semaphore. No more slots available.")
                        break
                        
                    try:
                        with transaction.atomic():
                            locked_task = HighPriorityTask.objects.select_for_update(skip_locked=True).filter(
                                id=task.id, 
                                in_progress=False, 
                                processed=False
                            ).first()
                            
                            if locked_task:
                                locked_task.in_progress_since = timezone.now()
                                locked_task.save(update_fields=["in_progress_since"])
                                
                                process_high_priority.apply_async(
                                    args=[locked_task.id, semaphore_id],
                                    queue="high_priority",
                                    priority=9
                                )
                                logger.info(f"Monitor dispatched missed task {locked_task.id} to high_priority queue")
                            else:
                                release_data_task_semaphore(semaphore_id)
                                logger.info(f"Task {task.id} is no longer available for processing")
                                
                    except Exception as task_error:
                        logger.error(f"Error dispatching task {task.id}: {task_error}")
                        # Release the semaphore on error
                        release_data_task_semaphore(semaphore_id)
            else:
                logger.info("No missed high priority tasks found.")
                
        except Exception as inner_error:
            logger.error(f"Error processing missed tasks: {inner_error}", exc_info=True)
    
    except Exception as e:
        logger.error(f"Error monitoring for missed tasks: {e}", exc_info=True)
    finally:
        # Clear the system task flag
        cache.delete(SYSTEM_TASK_ACTIVE_KEY)
        cache.delete(monitor_lock_key)
        close_old_connections()


@shared_task(bind=True, max_retries=3, queue="high_priority")
def high_priority_dispatcher(self):
    """
    Dispatcher that only runs on high priority workers and handles high priority tasks.
    Tries to fill up to 2 data task slots at once.
    """
    lock_acquired = cache.add("high_priority_dispatcher_lock", "running", 60)
    if not lock_acquired:
        logger.warning("High priority dispatcher lock ALREADY HELD. Skipping this execution.")
        return
    
    try:
        current_count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
        try:
            current_count = int(current_count)
        except (ValueError, TypeError):
            current_count = 0
            
        slots_to_fill = 2 - current_count
        
        if slots_to_fill <= 0:
            logger.info(f"Already running {current_count} tasks. No slots to fill.")
        else:
            logger.info(f"Attempting to fill {slots_to_fill} slots for high priority tasks")
            
            from integrations.models.models import HighPriorityTask
            from django.db import transaction
            
            slots_filled = 0
            
            for _ in range(slots_to_fill):
                semaphore_id = acquire_data_task_semaphore()
                if not semaphore_id:
                    logger.info("Failed to acquire semaphore. No more slots available.")
                    break
                    
                try:
                    with transaction.atomic():
                        hp_task = HighPriorityTask.objects.select_for_update(skip_locked=True).filter(
                            processed=False, 
                            in_progress=False,
                            dispatched=False
                        ).order_by('created_at').first()
                        
                        if hp_task:
                            hp_task.dispatched = True
                            hp_task.dispatched_at = timezone.now()
                            hp_task.save(update_fields=["dispatched", "dispatched_at"])
                            
                            process_high_priority.apply_async(
                                args=[hp_task.id, semaphore_id],
                                queue="high_priority",
                                priority=9,
                                countdown=0
                            )
                            logger.info(f"High priority dispatcher sent task {hp_task.id} with semaphore {semaphore_id}")
                            slots_filled += 1
                        else:
                            release_data_task_semaphore(semaphore_id)
                            logger.info("No high priority tasks found for processing.")
                            break 
                except Exception as inner_exc:
                    logger.error(f"Error dispatching high priority task: {inner_exc}")
                    release_data_task_semaphore(semaphore_id)
            
            logger.info(f"Filled {slots_filled} out of {slots_to_fill} available slots")
                
    except Exception as exc:
        logger.error("High priority dispatcher encountered an error: %s", exc, exc_info=True)
        log_task_event("high_priority_dispatcher", "failed", str(exc))
        raise
    finally:
        if lock_acquired:
            cache.delete("high_priority_dispatcher_lock")
            logger.info("High priority dispatcher lock RELEASED.")
            high_priority_dispatcher.apply_async(countdown=5, queue="high_priority")


@worker_ready.connect
def at_start(sender, **kwargs):
    """
    When the worker is ready, send the appropriate kickstart task
    based on worker type.
    """
    global HIGH_PRIORITY_WORKER
    
    if HIGH_PRIORITY_WORKER:
        logger.info("Starting high priority dispatcher for high priority worker")
        from core.tasks.general import high_priority_dispatcher
        high_priority_dispatcher.apply_async(countdown=5, queue="high_priority")
    else:
        logger.info("Starting regular dispatcher for standard worker")
        from core.tasks.general import dispatcher
        dispatcher.apply_async(countdown=5)

def acquire_data_task_semaphore():
    """
    Attempt to acquire a slot for a data processing task.
    Returns a semaphore ID if acquired, False otherwise.
    
    This limits data tasks to max 2 concurrent executions.
    Works with any cache backend.
    """
    try:
        # Get current count
        count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
        try:
            count = int(count)
        except (ValueError, TypeError):
            count = 0
            
        if count >= 2:
            return False
            
        lock_key = f"semaphore_lock_{time.time()}"
        if not cache.add(lock_key, "locked", timeout=10):
            return False 
            
        try:
            count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
            try:
                count = int(count)
            except (ValueError, TypeError):
                count = 0
                
            if count >= 2:
                return False
                
            new_count = count + 1
            cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, new_count, timeout=259200)  # 3 days
            
            timestamp = int(time.time())
            task_id = f"task_{timestamp}_{new_count}"
            
            cache.set(f"{HIGH_PRIORITY_TASK_SEMAPHORE}:{task_id}", task_id, timeout=259200)
            
            return task_id
        finally:
            cache.delete(lock_key)
            
    except Exception as e:
        logger.error(f"Error acquiring data task semaphore: {e}")
        return False

def release_data_task_semaphore(task_id):
    """Release a previously acquired data task slot"""
    try:
        cache.delete(f"{HIGH_PRIORITY_TASK_SEMAPHORE}:{task_id}")
        
        lock_key = f"release_lock_{time.time()}"
        if not cache.add(lock_key, "locked", timeout=10):
            logger.warning("Failed to acquire lock for releasing semaphore")
            return False
            
        try:
            count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
            try:
                count = int(count)
            except (ValueError, TypeError):
                count = 0
                
            if count > 0:
                cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, count - 1)
            else:
                cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, 0)
                
            return True
        finally:
            cache.delete(lock_key)
            
    except Exception as e:
        logger.error(f"Error releasing data task semaphore: {e}")
        return False

@shared_task(queue="high_priority")
def refresh_netsuite_token_task():
    """Refresh the NetSuite token for all available integrations."""
    from django.db import close_old_connections
    from integrations.models.models import Integration
    from integrations.services.netsuite.auth import NetSuiteAuthService
    
    active_system_task = cache.get(SYSTEM_TASK_ACTIVE_KEY)
    if active_system_task:
        logger.info(f"Another system task {active_system_task} is already running. Skipping token refresh.")
        return
    
    cache.set(SYSTEM_TASK_ACTIVE_KEY, "netsuite_token_refresh", timeout=3600)  # 1 hour
    
    try:
        close_old_connections()
        
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

@shared_task
def monitor_stuck_semaphores():
    """Check for and fix stuck semaphores that might prevent task processing"""
    try:
        from integrations.models.models import HighPriorityTask
        from django.db import close_old_connections
        
        close_old_connections()
        logger.warning("Checking for stuck semaphores")
        
        threshold = timezone.now() - timedelta(minutes=30)
        stuck_tasks = HighPriorityTask.objects.filter(
            in_progress=True, 
            processed=False,
            in_progress_since__lt=threshold
        )
        
        if stuck_tasks.exists():
            logger.warning(f"Found {stuck_tasks.count()} stuck tasks in progress. Resetting.")
            stuck_tasks.update(in_progress=False, in_progress_since=None, dispatched=False, dispatched_at=None)
        
        actual_in_progress = HighPriorityTask.objects.filter(
            in_progress=True, 
            processed=False
        ).count()
        
        count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
        try:
            count = int(count)
        except (ValueError, TypeError):
            count = 0
            
        if count != actual_in_progress:
            logger.warning(f"Semaphore count mismatch: cached={count}, actual={actual_in_progress}. Fixing.")
            cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, min(actual_in_progress, 2))
        
        dispatcher_lock = cache.get("high_priority_dispatcher_lock")
        if not dispatcher_lock:
            logger.warning("Dispatcher lock not found. Restarting dispatcher.")
            from core.tasks.general import high_priority_dispatcher
            high_priority_dispatcher.apply_async(countdown=5, queue="high_priority")
            
    except Exception as e:
        logger.error(f"Error in semaphore monitor: {e}", exc_info=True)
        cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, 0)

@shared_task
def reset_high_priority_system():
    """Emergency reset of high priority task system"""
    try:
        from django.core.cache import cache
        
        cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, 0)
        
        cache.delete("high_priority_dispatcher_lock")
        
        from integrations.models.models import HighPriorityTask
        from django.db import transaction
        
        with transaction.atomic():
            HighPriorityTask.objects.filter(
                dispatched=True,
                in_progress=False,
                processed=False
            ).update(dispatched=False, dispatched_at=None)
            
            from django.utils import timezone
            from datetime import timedelta
            
            threshold = timezone.now() - timedelta(minutes=30)
            HighPriorityTask.objects.filter(
                in_progress=True,
                processed=False,
                in_progress_since__lt=threshold
            ).update(in_progress=False, in_progress_since=None, dispatched=False, dispatched_at=None)
            
        from core.tasks.general import high_priority_dispatcher
        high_priority_dispatcher.apply_async(countdown=5, queue="high_priority")
        
        return "High priority system reset complete"
    except Exception as e:
        return f"Error resetting high priority system: {e}"

@shared_task(queue="high_priority")
def monitor_in_progress_not_dispatched_tasks():
    """
    Monitor for anomalous tasks that are marked as in_progress but were never 
    properly dispatched. This can happen if there's a system error during processing.
    Immediately dispatches these tasks if slots are available.
    """
    from integrations.models.models import HighPriorityTask
    from django.db import close_old_connections, transaction
    from django.utils import timezone
    from datetime import timedelta
    
    monitor_lock_key = f"monitor_in_progress_not_dispatched_lock_{int(time.time())}"
    if not cache.add(monitor_lock_key, "locked", timeout=300):
        logger.info("Another in-progress monitor is already running. Skipping.")
        return
    
    try:
        close_old_connections()
        
        active_system_task = cache.get(SYSTEM_TASK_ACTIVE_KEY)
        if active_system_task:
            logger.info(f"Another system task {active_system_task} is running. Skipping in-progress monitor.")
            return
        
        cache.set(SYSTEM_TASK_ACTIVE_KEY, "monitor_in_progress_not_dispatched", timeout=1800)
        
        threshold = timezone.now() - timedelta(seconds=2)
        inconsistent_tasks = HighPriorityTask.objects.filter(
            in_progress=True,
            dispatched=False,
            in_progress_since__lt=threshold,
            processed=False
        ).order_by('created_at')
        
        if inconsistent_tasks.exists():
            count = inconsistent_tasks.count()
            logger.warning(f"Found {count} tasks marked as in_progress but never dispatched! Fixing and processing...")
            
            current_count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
            try:
                current_count = int(current_count)
            except (ValueError, TypeError):
                current_count = 0
                
            actual_in_progress = HighPriorityTask.objects.filter(
                in_progress=True, 
                dispatched=True,
                processed=False
            ).count()
            
            if current_count != actual_in_progress:
                logger.warning(f"Semaphore count mismatch: cached={current_count}, actual={actual_in_progress}. Fixing.")
                cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, min(actual_in_progress, 2))
                current_count = actual_in_progress
            
            # Reset all the inconsistent tasks so they can be picked up
            tasks_to_reset_ids = list(inconsistent_tasks.values_list('id', flat=True))
            HighPriorityTask.objects.filter(id__in=tasks_to_reset_ids).update(
                in_progress=False,
                in_progress_since=None
            )
            
            slots_to_fill = 2 - current_count
            tasks_dispatched = 0
            
            if slots_to_fill > 0:
                logger.info(f"Attempting to immediately dispatch {min(slots_to_fill, count)} previously stuck tasks")
                
                for task_id in tasks_to_reset_ids[:slots_to_fill]:
                    semaphore_id = acquire_data_task_semaphore()
                    if not semaphore_id:
                        logger.info("Failed to acquire semaphore. No more slots available.")
                        break
                    
                    try:
                        with transaction.atomic():
                            task = HighPriorityTask.objects.select_for_update(skip_locked=True).filter(
                                id=task_id,
                                processed=False,
                                in_progress=False
                            ).first()
                            
                            if task:
                                task.dispatched = True
                                task.dispatched_at = timezone.now()
                                task.save(update_fields=["dispatched", "dispatched_at"])
                                
                                process_high_priority.apply_async(
                                    args=[task.id, semaphore_id],
                                    queue="high_priority",
                                    priority=9,
                                    countdown=0
                                )
                                logger.info(f"Immediately dispatched previously stuck task {task.id}")
                                tasks_dispatched += 1
                            else:
                                release_data_task_semaphore(semaphore_id)
                                logger.info(f"Task {task_id} is no longer available for processing")
                    except Exception as e:
                        release_data_task_semaphore(semaphore_id)
                        logger.error(f"Error dispatching previously stuck task {task_id}: {e}")
            
            log_task_event(
                "monitor_in_progress_tasks", 
                "fixed", 
                f"Reset {count} inconsistent tasks, immediately dispatched {tasks_dispatched}"
            )
        else:
            logger.info("No inconsistent in_progress tasks found.")
            
    except Exception as e:
        logger.error(f"Error in in_progress task monitor: {e}", exc_info=True)
    finally:
        cache.delete(SYSTEM_TASK_ACTIVE_KEY)
        cache.delete(monitor_lock_key)
        close_old_connections()

@shared_task(queue="high_priority")
def comprehensive_task_state_monitor():
    """
    Comprehensive monitor that checks for all possible inconsistent task states and fixes them:
    1. Tasks marked as dispatched but not in_progress (stuck in dispatch)
    2. Tasks marked as in_progress but not dispatched (impossible state)
    3. Checks and fixes semaphore counters
    """
    from integrations.models.models import HighPriorityTask
    from django.db import close_old_connections, transaction
    from django.utils import timezone
    from datetime import timedelta
    
    monitor_lock_key = f"comprehensive_monitor_lock_{int(time.time())}"
    if not cache.add(monitor_lock_key, "locked", timeout=600):
        logger.info("Another comprehensive monitor is already running. Skipping.")
        return
    
    try:
        close_old_connections()
        
        active_system_task = cache.get(SYSTEM_TASK_ACTIVE_KEY)
        if active_system_task:
            logger.info(f"Another system task {active_system_task} is running. Skipping comprehensive monitor.")
            return
        
        cache.set(SYSTEM_TASK_ACTIVE_KEY, "comprehensive_task_monitor", timeout=3600)
        
        fixed_count = 0
        task_info = {}
        
        # 1. Find tasks that have been dispatched but not started processing
        dispatch_threshold = timezone.now() - timedelta(seconds=30)
        stuck_dispatched = HighPriorityTask.objects.filter(
            dispatched=True,
            dispatched_at__lt=dispatch_threshold,
            in_progress=False,
            processed=False
        )
        
        if stuck_dispatched.exists():
            count = stuck_dispatched.count()
            logger.warning(f"Found {count} dispatched tasks that never started processing")
            stuck_dispatched.update(dispatched=False, dispatched_at=None)
            fixed_count += count
            task_info['dispatched_not_started'] = count
        
        # 2. Find tasks that are in_progress but not dispatched (impossible state)
        in_progress_threshold = timezone.now() - timedelta(seconds=5)
        inconsistent_tasks = HighPriorityTask.objects.filter(
            in_progress=True,
            dispatched=False,
            in_progress_since__lt=in_progress_threshold,
            processed=False
        )
        
        if inconsistent_tasks.exists():
            count = inconsistent_tasks.count()
            logger.warning(f"Found {count} tasks marked as in_progress but never dispatched")
            inconsistent_tasks.update(in_progress=False, in_progress_since=None)
            fixed_count += count
            task_info['in_progress_not_dispatched'] = count
        
        # 3. Check and fix semaphore counter
        actual_in_progress = HighPriorityTask.objects.filter(
            in_progress=True, 
            processed=False
        ).count()
        
        count = cache.get(HIGH_PRIORITY_TASK_ACTIVE_COUNT) or 0
        try:
            count = int(count)
        except (ValueError, TypeError):
            count = 0
            
        if count != actual_in_progress:
            logger.warning(f"Semaphore count mismatch: cached={count}, actual={actual_in_progress}")
            cache.set(HIGH_PRIORITY_TASK_ACTIVE_COUNT, min(actual_in_progress, 2))
            task_info['semaphore_fixed'] = True
        
        # Check dispatcher lock
        dispatcher_lock = cache.get("high_priority_dispatcher_lock")
        if not dispatcher_lock:
            logger.warning("Dispatcher lock not found. Restarting dispatcher.")
            from core.tasks.general import high_priority_dispatcher
            high_priority_dispatcher.apply_async(countdown=5, queue="high_priority")
            task_info['dispatcher_restarted'] = True
        
        if fixed_count > 0 or 'semaphore_fixed' in task_info or 'dispatcher_restarted' in task_info:
            log_task_event(
                "comprehensive_task_monitor", 
                "fixed", 
                f"Fixed {fixed_count} inconsistent tasks. Details: {task_info}"
            )
        else:
            logger.info("Comprehensive check complete. No issues found.")
            
    except Exception as e:
        logger.error(f"Error in comprehensive task monitor: {e}", exc_info=True)
    finally:
        cache.delete(SYSTEM_TASK_ACTIVE_KEY)
        cache.delete(monitor_lock_key)
        close_old_connections()
