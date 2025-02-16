"""
This file provides mappings for integration modules and their import methods.

Each integration type (e.g., 'xero' and 'netsuite') is represented as a dictionary with:
- 'client': The client/importer class.
- 'import_methods': A dictionary mapping component names to functions, which take an instantiated 
   importer object as parameter and call the corresponding method.
- 'full_import' (optional): A function to perform a full import using the importer.

Usage:
    from integrations.modules import MODULES

    # Retrieve Xero integration client and methods mapping
    xero_module = MODULES["xero"]
    XeroImporter = xero_module["client"]
    importer = XeroImporter(integration, since_date)
    # Dynamically call an import method, e.g., for accounts:
    import_func = xero_module["import_methods"].get("accounts")
    if import_func:
        import_func(importer)
"""

from integrations.services.xero.xero_client import XeroDataImporter
from integrations.services.netsuite.importer import NetSuiteImporter

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

def xero_map_xero_general_ledger(importer):
    return importer.map_xero_general_ledger()

def xero_import_xero_data(importer):
    return importer.import_xero_data()


# NetSuite integration helper functions
def netsuite_import_vendors(importer):
    # Note: vendors import requires a special load type argument
    return importer.import_vendors(load_type="drop_and_reload")

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

def netsuite_transform_general_ledger(importer):
    return importer.transform_general_ledger()

def netsuite_transform_transactions(importer):
    return importer.transform_transactions()


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
            "general_ledger": xero_map_xero_general_ledger,
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
            "general_ledger": netsuite_transform_general_ledger,
            "transformed_transactions": netsuite_transform_transactions,
        }
    },
}
