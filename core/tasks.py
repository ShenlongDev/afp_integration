from celery import shared_task
from integrations.services.netsuite_transformer import NetSuiteTransformer
from integrations.services.netsuite.importer import NetSuiteImporter
from integrations.services.netsuite_transformer import NetSuiteTransformer
import logging

logger = logging.getLogger(__name__)

@shared_task
def transform_netsuite_data():
    """Transform all NetSuite data"""
    transformer = NetSuiteTransformer()
    transformer.transform_all()


@shared_task
def sync_netsuite_data(consolidation_key: str):
    """Import and transform NetSuite data with budgets"""
    importer = NetSuiteImporter(consolidation_key)
   
    # Import reference data
    importer.import_accounting_periods()
    importer.import_entity()
    importer.import_vendors()
    importer.import_subsidiary()
    importer.import_departments()
    importer.import_budgets()
    importer.import_budget_lines()
   
    # Import transactions and related data
    importer.import_transactions()
    importer.import_transaction_lines()
    importer.import_transaction_accounting_lines()
   
    # Transform data
    transformer = NetSuiteTransformer()
   
    # Transform reference data first
    transformer.transform_subsidiaries()
    transformer.transform_departments()
    transformer.transform_accounting_periods()
    transformer.transform_accounts()
    transformer.transform_vendors()
    transformer.transform_entity()
   
    # Transform budgets
    transformer.transform_budget_period_balances()
   
    # Transform transactions
    transformer.transform_transactions()
    transformer.transform_transaction_lines()
    transformer.transform_transaction_accounting_lines()
   
    # Finally transform general ledger
    transformer.transform_general_ledger()


@shared_task
def sync_netsuite_data(consolidation_key: str):
    """Import and transform NetSuite data with budgets"""
    importer = NetSuiteImporter(consolidation_key)
   
    # Import reference data
    importer.import_accounts()  # Add this
    importer.import_accounting_periods()
    # ... rest of the imports ...


    # retry logic
@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=300,
    autoretry_for=(Exception,)
)
def sync_netsuite_data(self, consolidation_key: str):
    """Import and transform NetSuite data with budgets"""
    try:
        # ... existing code ...
        pass
    except Exception as e:
        logger.error(f"Error in sync_netsuite_data: {str(e)}")
        raise self.retry(exc=e)