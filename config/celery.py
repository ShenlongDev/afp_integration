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

app.conf.beat_schedule = {
    'daily-previous-day-sync': {
        'task': 'core.tasks.general.daily_previous_day_sync',
        'schedule': crontab(hour=0, minute=0),
        'options': {
            'queue': 'high_priority',
            'expires': None,
            'task_track_started': True,
            'task_time_limit': 1800,
            'task_soft_time_limit': 1500,
        }
    },
}

def get_active_org_sync_tasks():
    """
    Returns the total number of active tasks of type 'core.tasks.general.sync_organization'.
    """
    i = app.control.inspect()
    active = i.active() or {}
    count = 0
    for worker_tasks in active.values():
        for task in worker_tasks:
            # Adjust the name if necessary to match your task's full path.
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
    # Optionally add a short countdown (e.g., 5 seconds) to give a clean startup.
    dispatcher.apply_async(countdown=5)
