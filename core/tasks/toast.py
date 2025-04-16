import logging
from celery import shared_task
from django.utils import timezone
from datetime import timedelta, datetime
from integrations.models.models import Integration  
from integrations.services.toast.client import ToastIntegrationService

logger = logging.getLogger(__name__)

@shared_task
def sync_toast_data(integration_id, start_date_str=None, end_date_str=None, modules=None):
    """
    Celery task to sync Toast orders for a given integration.
    If start_date_str and end_date_str are provided (YYYY-MM-DD format), they will be used.
    Otherwise, defaults to orders from today's date (from midnight until the next midnight).
    
    modules: Optional list of modules to sync ['orders', 'restaurant_info', 'revenue_centers']
    If not provided, syncs all modules.
    """
    try:
        integration = Integration.objects.get(pk=integration_id)
    except Integration.DoesNotExist:
        logger.error("Integration with ID %s does not exist.", integration_id)
        return

    today_str = timezone.now().strftime('%Y-%m-%d')
    try:
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
            end_date = datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=1)
    except Exception as e:
        logger.error("Error parsing end_date_str: %s", e)
        end_date = datetime.strptime(today_str, '%Y-%m-%d') + timedelta(days=1)

    logger.info("Syncing Toast data for integration %s from %s to %s",
                integration_id, start_date, end_date)
    
    importer = ToastIntegrationService(integration, start_date, end_date)
    
    # If no modules specified, sync all
    if not modules:
        modules = ['orders', 'restaurant_info', 'revenue_centers']
    
    results = {}
    
    # Process each requested module
    if 'orders' in modules:
        logger.info("Syncing Toast orders for integration %s", integration_id)
        orders = importer.import_orders()
        results['orders'] = len(orders)
        
    if 'restaurant_info' in modules:
        logger.info("Syncing Toast restaurant info for integration %s", integration_id)
        restaurant_data = importer.import_restaurant_and_schedule_data()
        results['restaurant_info'] = len(restaurant_data)
        
    if 'revenue_centers' in modules:
        logger.info("Syncing Toast revenue centers for integration %s", integration_id)
        revenue_centers = importer.import_revenue_centers()
        results['revenue_centers'] = revenue_centers
    
    if 'service_areas' in modules:
        logger.info("Syncing Toast service areas for integration %s", integration_id)
        service_areas = importer.import_service_areas()
        results['service_areas'] = len(service_areas)
        
    if 'restaurant_services' in modules:
        logger.info("Syncing Toast restaurant services for integration %s", integration_id)
        restaurant_services = importer.import_restaurant_services()
        results['restaurant_services'] = restaurant_services

    if 'sales_categories' in modules:
        logger.info("Syncing Toast sales categories for integration %s", integration_id)
        sales_categories = importer.import_sales_categories()
        results['sales_categories'] = len(sales_categories)
        
    if 'dining_options' in modules:
        logger.info("Syncing Toast dining options for integration %s", integration_id)
        dining_options = importer.import_dining_options()
        results['dining_options'] = len(dining_options)    
        
    if 'payments' in modules:
        logger.info("Syncing Toast payments for integration %s", integration_id)
        payments = importer.import_payment_details()
        results['payments'] = len(payments)
    
    logger.info("Toast sync completed for integration %s: %s",
                integration_id, results)
    
    return results 