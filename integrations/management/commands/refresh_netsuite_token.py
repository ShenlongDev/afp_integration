from django.core.management.base import BaseCommand
from integrations.models.models import Integration, IntegrationAccessToken
from integrations.services.netsuite.auth import NetSuiteAuthService, INTEGRATION_TYPE_NETSUITE
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Force refresh the NetSuite token for a given integration, regardless of its expiry. " \
           "If integration id is not provided, it loops through all available NetSuite integrations."

    def add_arguments(self, parser):
        parser.add_argument(
            'integration_id',
            type=int,
            nargs='?',
            help=("ID of the Integration for which to refresh the NetSuite token. "
                  "If not provided, refresh tokens for all available NetSuite integrations.")
        )

    def handle(self, *args, **options):
        integration_id = options.get('integration_id')
        if integration_id is not None:
            try:
                integration = Integration.objects.get(pk=integration_id)
            except Integration.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Integration with id {integration_id} does not exist."))
                return

            self.process_integration(integration)
        else:
            # Process all available NetSuite integrations.
            # Here we filter integrations that have at least one Access Token record with the NetSuite integration type.
            netsuite_integrations = Integration.objects.filter(
                netsuite_client_id__isnull=False,
            )

            if not netsuite_integrations.exists():
                self.stdout.write(self.style.WARNING("No NetSuite integrations found."))
                return

            for integration in netsuite_integrations:
                self.stdout.write(f"Refreshing token for Integration ID: {integration.id}")
                self.process_integration(integration)

    def process_integration(self, integration):
        auth_service = NetSuiteAuthService(integration)
        try:
            token_obj = IntegrationAccessToken.objects.get(
                integration=integration,
                integration_type=INTEGRATION_TYPE_NETSUITE
            )
        except IntegrationAccessToken.DoesNotExist:
            self.stdout.write(self.style.ERROR(
                f"No existing token found for Integration ID: {integration.id}. Please authorize first."
            ))
            return

        try:
            new_token = auth_service._refresh_token(token_obj)
            self.stdout.write(self.style.SUCCESS(
                f"Successfully refreshed token for Integration ID: {integration.id}: {new_token}"
            ))
        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f"Error refreshing token for Integration ID: {integration.id}: {str(e)}"
            ))
            logger.error(f"Error refreshing token for integration {integration.id}: {str(e)}", exc_info=True) 