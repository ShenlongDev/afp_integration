import os
from django.core.management.base import BaseCommand
from core.models import Site
from integrations.services.email_service import send_weekly_sales_report
from integrations.services.pos_sales_data_service import get_weekly_sales_and_weather

class Command(BaseCommand):
    help = 'Send weekly sales and weather report via email'

    def add_arguments(self, parser):
        parser.add_argument('--site_id', type=int, default=1,
                            help='Site ID to generate report for')
        parser.add_argument('--emails', nargs='+', type=str, 
                            help='Email addresses to send the report to (space-separated)')

    def handle(self, *args, **options):
        site_id = options.get('site_id')
        emails = options.get('emails', [])
        
        try:
            site = Site.objects.filter(id=site_id).first()
            site_name = site.name if site else f"Site ID: {site_id}"
            
            self.stdout.write(f"Sending weekly report for {site_name} to {', '.join(emails)}")
            
            data = get_weekly_sales_and_weather(site_id=site_id)
            self.stdout.write(f"Retrieved data with {len(data)} days for site {site_name}")
            
            send_weekly_sales_report(emails, data, site_name)
            self.stdout.write(self.style.SUCCESS(f"Successfully sent weekly report for {site_name}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error sending weekly report: {str(e)}"))
            raise 