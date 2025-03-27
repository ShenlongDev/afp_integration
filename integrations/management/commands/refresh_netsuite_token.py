from django.core.management.base import BaseCommand
from django.utils import timezone
from integrations.models.models import Integration, IntegrationAccessToken, INTEGRATION_TYPE_CHOICES
from integrations.services.netsuite.auth import NetSuiteAuthService, INTEGRATION_TYPE_NETSUITE
from core.models import TaskLog


class Command(BaseCommand):
    help = 'Refreshes all NetSuite OAuth tokens that are about to expire'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force refresh all tokens regardless of expiration status',
        )
        parser.add_argument(
            '--integration_id',
            type=int,
            help='Refresh token for a specific integration ID only',
        )

    def handle(self, *args, **options):
        force_refresh = options.get('force', False)
        integration_id = options.get('integration_id')
        
        # Get all NetSuite integrations, or a specific one if ID provided
        integrations_query = Integration.objects.filter(netsuite_account_id__isnull=False)
        if integration_id:
            integrations_query = integrations_query.filter(id=integration_id)
        
        integrations = integrations_query.all()
        
        if not integrations:
            self.stdout.write(self.style.WARNING('No NetSuite integrations found'))
            return
        
        self.stdout.write(f"Found {len(integrations)} NetSuite integration(s)")
        
        success_count = 0
        error_count = 0
        
        for integration in integrations:
            try:
                # Check if token exists and needs refresh
                try:
                    token_obj = IntegrationAccessToken.objects.get(
                        integration=integration,
                        integration_type=INTEGRATION_TYPE_NETSUITE
                    )
                    
                        
                except IntegrationAccessToken.DoesNotExist:
                    self.stdout.write(self.style.ERROR(
                        f"No token found for {integration}. Initial authorization required."
                    ))
                    error_count += 1
                    continue
                
                # Refresh the token
                auth_service = NetSuiteAuthService(integration)
                self.stdout.write(f"Refreshing token for {integration}")
                
                # Replace the call to non-existent _refresh_token method with obtain_access_token
                new_token = auth_service.obtain_access_token()
                
                success_count += 1
                self.stdout.write(self.style.SUCCESS(
                    f"Successfully refreshed token for {integration}"
                ))
                
                # Log the successful refresh
                TaskLog.objects.create(
                    task_name="netsuite_token_refresh",
                    status="SUCCESS",
                    detail=f"Refreshed NetSuite token for {integration}"
                )
                
            except Exception as e:
                error_count += 1
                error_msg = f"Error refreshing token for {integration}: {str(e)}"
                self.stdout.write(self.style.ERROR(error_msg))
                
                # Log the error
                TaskLog.objects.create(
                    task_name="netsuite_token_refresh",
                    status="ERROR",
                    detail=error_msg
                )
        
        summary = f"Token refresh completed: {success_count} successful, {error_count} failed"
        self.stdout.write(self.style.SUCCESS(summary))
