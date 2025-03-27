"""
Tasks for handling Xero integrations including syncing and data import.
"""

import logging
from celery import shared_task, chain
from datetime import datetime
import time
from django.utils import timezone

logger = logging.getLogger(__name__)

from integrations.models.models import Integration
from integrations.services.xero.xero_client import XeroDataImporter

def get_xero_importer(integration_id, since_str=None, until_str=None):
    """
    Instantiate and return XeroDataImporter.
    The until_str is used to set the until_date for budget imports.
    """
    integration = Integration.objects.get(pk=integration_id)
    if since_str is None:
         since_str = timezone.now().strftime('%Y-%m-%d')
    since_date = datetime.strptime(since_str, '%Y-%m-%d')
    until_date = None
    if until_str:
         until_date = datetime.strptime(until_str, '%Y-%m-%d')
    return XeroDataImporter(integration, since_date, until_date)

@shared_task
def xero_sync_accounts_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.sync_xero_chart_of_accounts()
    logger.info(f"Xero accounts synced for integration id: {integration_id}")

@shared_task
def xero_import_journal_lines_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_journal_lines()
    logger.info(f"Xero journal lines imported for integration id: {integration_id}")

@shared_task
def xero_import_contacts_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_contacts()
    logger.info(f"Xero contacts imported for integration id: {integration_id}")

@shared_task
def xero_import_invoices_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_invoices()
    logger.info(f"Xero invoices imported for integration id: {integration_id}")

@shared_task
def xero_import_bank_transactions_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_bank_transactions()
    logger.info(f"Xero bank transactions imported for integration id: {integration_id}")

@shared_task
def xero_import_budgets_task(integration_id, since_str=None, until_str=None):
    importer = get_xero_importer(integration_id, since_str, until_str)
    importer.import_xero_budgets(until_date=until_str)
    logger.info(f"Xero budgets imported for integration id: {integration_id} from {since_str} to {until_str}")


@shared_task
def wait_60_seconds(integration_id):
    """
    Waits for 20 seconds before returning.
    """
    time.sleep(20)
    return integration_id

@shared_task
def sync_xero_data(since_str: str = None):
    """
    Finds all eligible Xero integrations and dispatches a chain of sync tasks
    for each. The since_str, if not provided, will be determined at the
    execution time of each individual task.
    """
    from integrations.services.utils import get_integrations_by_integration_type
    
    eligible_integrations = get_integrations_by_integration_type("xero")
    
    if not eligible_integrations.exists():
        logger.warning("No eligible integrations found with Xero credentials.")
        return

    for integration in eligible_integrations:
        task_chain = chain(
            xero_sync_accounts_task.si(integration.id, since_str),
            wait_60_seconds.si(integration.id),
            xero_import_journal_lines_task.si(integration.id, since_str),
            wait_60_seconds.si(integration.id),
            xero_import_contacts_task.si(integration.id, since_str),
            wait_60_seconds.si(integration.id),
            xero_import_invoices_task.si(integration.id, since_str),
            wait_60_seconds.si(integration.id),
            xero_import_bank_transactions_task.si(integration.id, since_str),
            wait_60_seconds.si(integration.id),
            xero_import_budgets_task.si(integration.id, since_str),
        )
        task_chain.apply_async()
        logger.info(f"Dispatched Xero sync tasks for integration: {integration}")

@shared_task
def sync_single_xero_data(integration_id, since_str: str = None):
    """
    Sync tasks for a single Xero integration.
    This sequential chain executes all required sub-tasks.
    The since_str, if not provided, will be determined by each task at runtime.
    """
    task_chain = chain( 
        xero_sync_accounts_task.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        xero_import_journal_lines_task.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        xero_import_contacts_task.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        xero_import_invoices_task.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        xero_import_bank_transactions_task.si(integration_id, since_str),
        wait_60_seconds.si(integration_id),
        xero_import_budgets_task.si(integration_id, since_str),
    )
    task_chain.apply_async() 