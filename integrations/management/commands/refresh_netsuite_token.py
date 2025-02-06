from django.core.management.base import BaseCommand
from integrations.models.models import Integration, IntegrationAccessToken
from integrations.services.netsuite.auth import NetSuiteAuthService, INTEGRATION_TYPE_NETSUITE
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Force refresh the NetSuite token for a given integration, regardless of its expiry."

    def add_arguments(self, parser):
        parser.add_argument(
            'integration_id',
            type=int,
            nargs='?', 
            default=2, 
            help="ID of the Integration for which to refresh the NetSuite token."
        )

    def handle(self, *args, **options):
        integration_id = options['integration_id']
        try:
            integration = Integration.objects.get(pk=integration_id)
        except Integration.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Integration with id {integration_id} does not exist."))
            return

        auth_service = NetSuiteAuthService(integration)
        try:
            token_obj = IntegrationAccessToken.objects.get(
                integration=integration,
                integration_type=INTEGRATION_TYPE_NETSUITE
            )
        except IntegrationAccessToken.DoesNotExist:
            self.stdout.write(self.style.ERROR("No existing token found for this integration. Please authorize first."))
            return

        try:
            new_token = auth_service._refresh_token(token_obj)
            self.stdout.write(self.style.SUCCESS(f"Successfully refreshed token: {new_token}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error refreshing token: {str(e)}"))
            logger.error(f"Error refreshing token for integration {integration_id}: {str(e)}", exc_info=True) 