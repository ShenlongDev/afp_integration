import logging
from celery import shared_task
from django.utils import timezone
from datetime import timedelta, datetime
from integrations.models.models import Integration  
from integrations.services.toast.client import ToastIntegrationService

logger = logging.getLogger(__name__)

@shared_task
def sync_toast_data(integration_id, start_date_str=None, end_date_str=None):
    """
    Celery task to sync Toast orders for a given integration.
    If start_date_str and end_date_str are provided (YYYY-MM-DD format), they will be used.
    Otherwise, defaults to orders from today's date (from midnight until the next midnight).
    """
    try:
        integration = Integration.objects.get(pk=integration_id)
    except Integration.DoesNotExist:
        logger.error("Integration with ID %s does not exist.", integration_id)
        return

    # Compute today's date as a string in YYYY-MM-DD format.
    today_str = timezone.now().strftime('%Y-%m-%d')
    try:
        # If provided, parse; otherwise set to today's date.
        if start_date_str:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        else:
            start_date = datetime.strptime(today_str, '%Y-%m-%d')
    except Exception as e:
        logger.error("Error parsing start_date_str: %s", e)
        start_date = datetime.strptime(today_str, '%Y-%m-%d')

    try:
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            # Default to next day, covering the full day of start_date.
            end_date = datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=1)
    except Exception as e:
        logger.error("Error parsing end_date_str: %s", e)
        end_date = datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=1)

    logger.info("Syncing Toast orders for integration %s from %s to %s",
                integration_id, start_date, end_date)
    importer = ToastIntegrationService(integration)
    orders = importer.import_orders(start_date, end_date)
    logger.info("Toast sync completed for integration %s, %d orders processed.",
                integration_id, len(orders)) 