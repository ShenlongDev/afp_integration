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
    Otherwise, defaults to orders from the last 24 hours.
    """
    try:
        integration = Integration.objects.get(pk=integration_id)
    except Integration.DoesNotExist:
        logger.error("Integration with ID %s does not exist.", integration_id)
        return

    # Determine start_date and end_date
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        except Exception as e:
            logger.error("Error parsing start_date_str: %s", e)
            start_date = timezone.now() - timedelta(days=1)
    else:
        start_date = timezone.now() - timedelta(days=1)

    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        except Exception as e:
            logger.error("Error parsing end_date_str: %s", e)
            end_date = timezone.now()
    else:
        end_date = timezone.now()

    logger.info("Syncing Toast orders for integration %s from %s to %s",
                integration_id, start_date, end_date)
    importer = ToastIntegrationService(integration)
    orders = importer.import_orders(start_date, end_date)
    logger.info("Toast sync completed for integration %s, %d orders processed.",
                integration_id, len(orders)) 