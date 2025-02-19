"""
Tasks for handling Xero integrations including syncing and data import.
"""

import logging
from celery import shared_task, chain
from datetime import datetime

logger = logging.getLogger(__name__)

from integrations.models.models import Integration
from integrations.services.xero.xero_client import XeroDataImporter

def get_xero_importer(integration_id, since_str=None):
    """
    Helper to instantiate XeroDataImporter for a given integration.
    Optionally accepts a since date string in YYYY-MM-DD format.
    """
    integration = Integration.objects.get(pk=integration_id)
    since_date = datetime.strptime(since_str, '%Y-%m-%d') if since_str else None
    return XeroDataImporter(integration, since_date)

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
def xero_import_budgets_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.import_xero_budgets()
    logger.info(f"Xero budgets imported for integration id: {integration_id}")

@shared_task
def xero_map_general_ledger_task(integration_id, since_str=None):
    importer = get_xero_importer(integration_id, since_str)
    importer.map_xero_general_ledger()
    logger.info(f"Xero general ledger mapped for integration id: {integration_id}")

@shared_task
def sync_xero_data(since_str: str = None):
    """
    Finds all eligible Xero integrations and begins the sync chain for each.
    """
    from integrations.services.utils import get_integrations_by_integration_type
    
    since_str = since_str or datetime.now().strftime('%Y-%m-%d')
    eligible_integrations = get_integrations_by_integration_type("xero")
    
    if not eligible_integrations.exists():
        logger.warning("No eligible integrations found with Xero credentials.")
        return

    for integration in eligible_integrations:
        logger.info(f"Dispatched Xero sync tasks for integration: {integration}") 