from celery import shared_task, chain
from django.core.exceptions import ObjectDoesNotExist
from integrations.netsuite.importer import NetSuiteImporter
import logging

logger = logging.getLogger(__name__)


@shared_task
def netsuite_import_accounts(integration):
    importer = NetSuiteImporter(integration)
    importer.import_accounts()
    logger.info(f"NetSuite accounts imported for integration: {integration}")


@shared_task
def netsuite_import_accounting_periods(integration):
    importer = NetSuiteImporter(integration)
    importer.import_accounting_periods()
    logger.info(f"NetSuite accounting periods imported for integration: {integration}")


@shared_task
def netsuite_import_entity(integration):
    importer = NetSuiteImporter(integration)
    importer.import_entities()
    logger.info(f"NetSuite entity imported for integration: {integration}")


@shared_task
def netsuite_import_vendors(integration):
    importer = NetSuiteImporter(integration)
    importer.import_vendors()
    logger.info(f"NetSuite vendors imported for integration: {integration}")


@shared_task
def netsuite_import_subsidiary(integration):
    importer = NetSuiteImporter(integration)
    importer.import_subsidiaries()
    logger.info(f"NetSuite subsidiary imported for integration: {integration}")


@shared_task
def netsuite_import_departments(integration):
    importer = NetSuiteImporter(integration)
    importer.import_departments()
    logger.info(f"NetSuite departments imported for integration: {integration}")


@shared_task
def netsuite_import_transactions(integration):
    importer = NetSuiteImporter(integration)
    importer.import_transactions()
    logger.info(f"NetSuite transactions imported for integration: {integration}")


@shared_task
def netsuite_import_transaction_lines(integration):
    importer = NetSuiteImporter(integration)
    importer.import_transaction_lines()
    logger.info(f"NetSuite transaction lines imported for integration: {integration}")


@shared_task
def netsuite_import_transaction_accounting_lines(integration):
    importer = NetSuiteImporter(integration)
    importer.import_transaction_accounting_lines()
    logger.info(f"NetSuite transaction accounting lines imported for integration: {integration}")