import os
from celery import Celery
from django.conf import settings
from celery.signals import worker_ready
from celery.schedules import crontab

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
}

app.conf.worker_prefetch_multiplier = 1
app.conf.worker_concurrency = 1

app.conf.task_routes = {
    'core.tasks.general.process_high_priority': {'queue': 'high_priority', 'routing_key': 'high_priority.urgent'},
}

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
