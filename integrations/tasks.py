from celery import shared_task
from .services.pos_sales_data_service import get_weekly_sales_and_weather
from .services.email_service import send_weekly_sales_report as send_report_email
from core.models import Site, Organisation

@shared_task
def send_weekly_sales_report(recipients, site_id=None):
    """
    Celery task to generate and send weekly sales report via email
    
    Args:
        recipients (list): List of email recipients
        site_id (str): The site ID to fetch data for
    """
    data = get_weekly_sales_and_weather(site_id=site_id) 
    
    site_name = "Insights"
    if site_id:
        try:
            site = Site.objects.get(id=site_id)
            site_name = site.name
            if site_name == "Default Site":
                organisation = Organisation.objects.filter(id=site.organisation_id).first()
                site_name = organisation.name if organisation else f"Organisation ID: {site.organisation_id}"
        except Site.DoesNotExist:
            site_name = "Insights"

    return send_report_email(recipients, data, site_name=site_name) 