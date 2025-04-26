import os
import signal
from celery import Celery
from django.conf import settings
from celery.signals import worker_ready, worker_init, worker_shutting_down
from celery.schedules import crontab
import logging

logger = logging.getLogger(__name__)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('config')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

app.conf.broker_connection_retry_on_startup = True

app.conf.task_acks_late = True
app.conf.task_reject_on_worker_lost = True
app.conf.task_serializer = 'json'
app.conf.result_serializer = 'json'
app.conf.accept_content = ['json']
app.conf.broker_transport_options = {
    'visibility_timeout': 200000,
    'priority_steps': [1, 3, 5, 7, 9],
    'queue_order_strategy': 'priority',
}

app.conf.task_time_limit = 172000
app.conf.task_soft_time_limit = 170000

app.conf.beat_schedule = {
    'daily-previous-day-sync': {
        'task': 'core.tasks.general.daily_previous_day_sync',
        'schedule': crontab(hour=0, minute=5),
        'options': {
            'queue': 'high_priority',
            'expires': None,
            'task_track_started': True,
        }
    },
    'sales-report': {
        'task': 'integrations.tasks.send_weekly_sales_report',
        'schedule': crontab(minute=0, hour=6),
        'args': ([
            't.stanley@williamsstanley.co',  'm.nouman@williamsstanley.co'
        ],),
        'kwargs': {'site_id': '3'},
        'options': {
            'queue': 'celery',
            'expires': None,
            'task_track_started': True,
        }
    },
    'refresh-netsuite-tokens': {
        'task': 'core.tasks.netsuite.refresh_netsuite_token_task',
        'schedule': crontab(minute='*/20'),
        'options': {
            'queue': 'high_priority',
            'expires': None,
            'task_track_started': True,
        }
    },
    'monitor-stuck-tasks': {
        'task': 'core.tasks.general.monitor_stuck_high_priority_tasks',
        'schedule': crontab(minute='*/3'),
        'options': {
            'queue': 'high_priority',
            'expires': None,
        }
    },
    'monitor-semaphores': {
        'task': 'core.tasks.general.monitor_stuck_semaphores',
        'schedule': crontab(hour='*/15'),
        'options': {
            'queue': 'high_priority',
            'expires': None,
        }
    },
    'monitor-in-progress-not-dispatched': {
        'task': 'core.tasks.general.monitor_in_progress_not_dispatched_tasks',
        'schedule': crontab(minute='*/3'),
        'options': {
            'queue': 'high_priority',
            'expires': None,
        }
    },
    'comprehensive-task-monitor': {
        'task': 'core.tasks.general.comprehensive_task_state_monitor',
        'schedule': crontab(minute='*/10'),
        'options': {
            'queue': 'high_priority',
            'expires': None,
        }
    },
}

app.conf.worker_prefetch_multiplier = 1
app.conf.worker_concurrency = 1

app.conf.task_routes = {
    'core.tasks.general.process_high_priority': {'queue': 'high_priority'},
    'core.tasks.general.high_priority_dispatcher': {'queue': 'high_priority'},
    'core.tasks.netsuite.refresh_netsuite_token_task': {'queue': 'high_priority'},
    'core.tasks.general.monitor_stuck_high_priority_tasks': {'queue': 'high_priority'},
    'core.tasks.general.daily_previous_day_sync': {'queue': 'high_priority'},
    'core.tasks.general.monitor_stuck_semaphores': {'queue': 'high_priority'},
    'core.tasks.general.monitor_in_progress_not_dispatched_tasks': {'queue': 'high_priority'},
    'core.tasks.general.comprehensive_task_state_monitor': {'queue': 'high_priority'},
}

HIGH_PRIORITY_WORKER = False

@worker_init.connect
def mark_high_priority_worker(sender, instance, **kwargs):
    """
    Identify high priority workers and mark them globally
    """
    global HIGH_PRIORITY_WORKER
    
    queues = getattr(instance, 'queues', [])
    worker_name = getattr(sender, 'hostname', '')
    
    if queues and any('high_priority' in str(q) for q in queues):
        HIGH_PRIORITY_WORKER = True
        logger.warning(f"High priority worker detected: {worker_name}")
    elif 'high_priority' in worker_name:
        HIGH_PRIORITY_WORKER = True
        logger.warning(f"High priority worker detected by name: {worker_name}")
    
    if HIGH_PRIORITY_WORKER:
        os.environ['REMAP_SIGTERM'] = 'SIGQUIT'
        logger.warning(f"SIGTERM remapped to SIGQUIT for high priority worker: {worker_name}")
        
        instance.app.conf.worker_soft_shutdown_timeout = 9999999
        logger.warning(f"Soft shutdown timeout set to extremely high value for worker: {worker_name}")

@worker_shutting_down.connect
def handle_worker_shutdown(sender, sig, how, exitcode, **kwargs):
    """
    Handle shutdown signal for high priority workers
    """
    global HIGH_PRIORITY_WORKER
    
    if HIGH_PRIORITY_WORKER and how == 'warm':
        logger.warning(f"PREVENTING warm shutdown for high priority worker. Signal: {sig}")
        return False

def get_active_org_sync_tasks():
    """
    Returns the total number of active and reserved tasks of type 'core.tasks.general.sync_organization'.
    """
    i = app.control.inspect()
    active = i.active() or {}
    reserved = i.reserved() or {}
    
    count = 0
    for worker_tasks in active.values():
        for task in worker_tasks:
            if task.get("name") == "core.tasks.general.sync_organization":
                count += 1
    
    for worker_tasks in reserved.values():
        for task in worker_tasks:
            if task.get("name") == "core.tasks.general.sync_organization":
                count += 1
                
    return count

@worker_ready.connect
def at_start(sender, **kwargs):
    """
    When the worker is ready, send a kickstart task.
    In this case the 'dispatcher' task is enqueued, which is designed
    to check for high priority or normal tasks and re-enqueue itself when done.
    """
    from core.tasks.general import dispatcher
    dispatcher.apply_async(countdown=5)
