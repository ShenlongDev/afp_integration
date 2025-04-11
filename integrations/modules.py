"""
This file provides mappings for integration modules and their import methods.

Each integration type (e.g., 'xero', 'netsuite', and now 'toast') is represented as a dictionary with:
- 'client': The client/importer class.
- 'import_methods': A dictionary mapping component names to functions. Each function takes an instantiated 
  importer object as a parameter and calls the corresponding method.
- 'full_import' (optional): A function to perform a full import using the importer.

Usage:
    from integrations.modules import MODULES

    # Retrieve an integration client and its methods mapping, for example Toast:
    toast_module = MODULES["toast"]
    ToastImporter = toast_module["client"]
    importer = ToastImporter(integration)
    import_func = toast_module["import_methods"].get("orders")
    if import_func:
        import_func(importer)
"""

from integrations.services.xero.xero_client import XeroDataImporter
from integrations.services.netsuite.importer import NetSuiteImporter
from integrations.services.toast.client import ToastIntegrationService
from django.utils import timezone
from datetime import timedelta

# Xero integration helper functions
def xero_sync_xero_chart_of_accounts(importer):
    return importer.sync_xero_chart_of_accounts()

def xero_import_xero_journal_lines(importer):
    return importer.import_xero_journal_lines()

def xero_import_xero_contacts(importer):
    return importer.import_xero_contacts()

def xero_import_xero_invoices(importer):
    return importer.import_xero_invoices()

def xero_import_xero_bank_transactions(importer):
    return importer.import_xero_bank_transactions()

def xero_import_xero_budgets(importer):
    return importer.import_xero_budgets()

def xero_import_xero_data(importer):
    return importer.import_xero_data()


# NetSuite integration helper functions
def netsuite_import_vendors(importer):
    return importer.import_vendors()

def netsuite_import_accounts(importer):
    return importer.import_accounts()

def netsuite_import_transactions(importer):
    return importer.import_transactions()

def netsuite_import_transaction_accounting_lines(importer):
    return importer.import_transaction_accounting_lines()

def netsuite_import_transaction_lines(importer):
    return importer.import_transaction_lines()

def netsuite_import_subsidiaries(importer):
    return importer.import_subsidiaries()

def netsuite_import_departments(importer):
    return importer.import_departments()

def netsuite_import_entities(importer):
    return importer.import_entities()

def netsuite_import_accounting_periods(importer):
    return importer.import_accounting_periods()

# Toast integration helper function
def toast_import_orders(importer):
    return importer.import_orders()

def toast_import_restaurant_data(importer):
    return importer.import_restaurant_and_schedule_data()

def toast_import_revenue_centers(importer):
    return importer.import_revenue_centers()

def netsuite_import_locations(importer):
    return importer.import_locations()

def netsuite_import_budgets(importer):
    return importer.import_budgets()

def toast_import_service_areas(importer):
    return importer.import_service_areas()

def toast_import_restaurant_services(importer):
    return importer.import_restaurant_services()

def toast_import_sales_categories(importer):
    return importer.import_sales_categories()

def toast_import_dining_options(importer):
    return importer.import_dining_options()





MODULES = {
    "xero": {
        "client": XeroDataImporter,
        "import_methods": {
            "accounts": xero_sync_xero_chart_of_accounts,
            "journal_lines": xero_import_xero_journal_lines,
            "contacts": xero_import_xero_contacts,
            "invoices": xero_import_xero_invoices,
            "bank_transactions": xero_import_xero_bank_transactions,
            "budgets": xero_import_xero_budgets,
        },
        "full_import": xero_import_xero_data,
    },
    "netsuite": {
        "client": NetSuiteImporter,
        "import_methods": {
            "vendors": netsuite_import_vendors,
            "accounts": netsuite_import_accounts,
            "transactions": netsuite_import_transactions,
            "transaction_accounting_lines": netsuite_import_transaction_accounting_lines,
            "transaction_lines": netsuite_import_transaction_lines,
            "subsidiaries": netsuite_import_subsidiaries,
            "departments": netsuite_import_departments,
            "entities": netsuite_import_entities,
            "accounting_periods": netsuite_import_accounting_periods,
            "locations": netsuite_import_locations,
            "budgets": netsuite_import_budgets,
        }
    },
    "toast": {
        "client": ToastIntegrationService,
        "import_methods": {
            "orders": toast_import_orders,
            "restaurant_info": toast_import_restaurant_data,
            "revenue_centers": toast_import_revenue_centers,
            "service_areas": toast_import_service_areas,
            "restaurant_services": toast_import_restaurant_services,
            "sales_categories": toast_import_sales_categories,
            "dining_options": toast_import_dining_options,
        },
    },
}
