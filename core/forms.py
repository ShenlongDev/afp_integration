# myapp/forms.py
from django import forms
from django.core.exceptions import ValidationError
from integrations.models.models import Integration, Organisation
from integrations.modules import MODULES
from django.utils import timezone
from datetime import datetime
from integrations.services.utils import get_organisations_by_integration_type
from core.tasks.general import log_task_event
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from core.models import Organisation


def get_integration_type_choices():
    """
    Build a choices list for integration types based on available keys in MODULES.
    The key is used as the value, and the display label is a friendly version.
    """
    choices = []
    for key in MODULES.keys():
        # Customize labels if desired. Here, we simply capitalize.
        label = key.capitalize() if key != "netsuite" else "NetSuite"
        choices.append((key, label))
    return choices

def get_module_choices(integration_type):
    """
    For a given integration type (e.g., 'xero' or 'netsuite'),
    return a list of module/component choices based on available import methods.
    """
    if integration_type in MODULES:
        import_methods = MODULES[integration_type].get("import_methods", {})
        return [(key, key.replace('_', ' ').title()) for key in import_methods.keys()]
    return []

class DataImportForm(forms.Form):
    organisation = forms.ModelChoiceField(
        queryset=Organisation.objects.filter(status='active'),
        required=True,
        label="Organisation"
    )
    integration = forms.ModelChoiceField(
        queryset=Integration.objects.filter(is_active=True),
        required=False,  # Will be populated based on organisation selection
        label="Integration"
    )
    integration_type = forms.ChoiceField(
        choices=[('toast', 'Toast'), ('xero', 'Xero'), ('netsuite', 'NetSuite')],
        required=True, 
        label="Integration Type"
    )
    since_date = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date'}),
        required=True,
        label="Since Date"
    )
    modules = forms.MultipleChoiceField(
        choices=[],  # Will be populated dynamically
        required=False,
        widget=forms.CheckboxSelectMultiple,
        label="Modules to Import"
    )
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Setup dynamic module choices based on selected integration type
        if args and 'integration_type' in args[0]:
            integration_type = args[0]['integration_type'].lower()
            self.setup_module_choices(integration_type)
            
        if args and 'organisation' in args[0]:
            org_id = args[0]['organisation']
            self.fields['integration'].queryset = Integration.objects.filter(
                organisation_id=org_id, is_active=True
            )
    
    def setup_module_choices(self, integration_type):
        from integrations.modules import MODULES
        
        if integration_type in MODULES:
            module_choices = [(k, k.replace('_', ' ').title()) 
                           for k in MODULES[integration_type].get('import_methods', {}).keys()]
            self.fields['modules'].choices = module_choices
        else:
            self.fields['modules'].choices = []

    def clean_integration_type(self):
        integration_type = self.cleaned_data.get('integration_type', '')
        integration_type = integration_type.lower() if integration_type else ''
        
        if not integration_type:
            raise forms.ValidationError("Integration type is required.")
            
        if integration_type not in MODULES:
            valid_types = ", ".join(MODULES.keys())
            raise forms.ValidationError(
                f"'{integration_type}' is not a valid integration type. "
                f"Valid types are: {valid_types}"
            )
            
        return integration_type

    def clean_modules(self):
        modules = self.cleaned_data.get("modules")
        if not modules:
            raise forms.ValidationError("Please select at least one module/component to import.")
        return modules

    def clean(self):
        cleaned_data = super().clean()
        organisation = cleaned_data.get('organisation')
        integration_type = cleaned_data.get('integration_type')

        if organisation and integration_type:
            # Find integration based on the new settings structure 
            integration = Integration.objects.filter(
                organisation=organisation,
                integration_type=integration_type,
                is_active=True
            ).first()

            if not integration:
                raise ValidationError(
                    f"No {integration_type.capitalize()} integration found with valid credentials for "
                    f"this organisation."
                )

            cleaned_data['integration'] = integration

        return cleaned_data

    def process_import(self):
        """
        Process the import based on form data.
        Returns a tuple of (success_message, error_message)
        """
        integration = self.cleaned_data['integration']
        integration_type = self.cleaned_data['integration_type']
        since_date = datetime.combine(self.cleaned_data['since_date'], datetime.min.time())
        selected_modules = self.cleaned_data.get('modules', [])

        try:
            module_config = MODULES[integration_type]
            ImporterClass = module_config['client']
            importer = ImporterClass(integration, since_date)

            if selected_modules:
                for module in selected_modules:
                    import_func = module_config['import_methods'].get(module)
                    if import_func:
                        import_func(importer)
            else:
                full_import = module_config.get('full_import')
                if full_import:
                    full_import(importer)
                else:
                    for import_func in module_config['import_methods'].values():
                        import_func(importer)

            return (
                f"Successfully imported {integration_type} data for {integration.org.name}",
                None
            )

        except Exception as e:
            return (
                None,
                f"Error importing {integration_type} data: {str(e)}"
            )

class BudgetImportForm(forms.Form):
    organisation = forms.ModelChoiceField(
        queryset=Organisation.objects.filter(status='active'),
        required=True,
        label="Organisation"
    )
    integration = forms.ModelChoiceField(
        queryset=Integration.objects.filter(is_active=True, integration_type='xero'),
        required=False,  # Will be set based on organisation
        label="Xero Integration"
    )
    since_date = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date'}),
        required=True,
        label="Start Date"
    )
    until_date = forms.DateField(
        widget=forms.DateInput(attrs={'type': 'date'}),
        required=True,
        label="End Date"
    )
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        if args and 'organisation' in args[0]:
            org_id = args[0]['organisation']
            self.fields['integration'].queryset = Integration.objects.filter(
                organisation_id=org_id, is_active=True, integration_type='xero'
            )

    def clean(self):
        cleaned_data = super().clean()
        organisation = cleaned_data.get('organisation')
        since_date = cleaned_data.get('since_date')
        until_date = cleaned_data.get('until_date')
        
        if since_date and until_date and since_date > until_date:
            raise ValidationError("'From' date cannot be later than 'To' date.")

        if organisation:
            integration = Integration.objects.filter(
                organisation=organisation,
                integration_type='xero',
                is_active=True
            ).first()

            if not integration:
                raise ValidationError(
                    f"No Xero integration found with valid credentials for this organisation."
                )

            cleaned_data['integration'] = integration

        return cleaned_data

    def process_budget_import(self):
        """
        Process the budget import by triggering a Celery task.
        Returns a tuple of (success_message, error_message)
        """
        from core.tasks.xero import xero_import_budgets_task

        integration = self.cleaned_data['integration']
        since_date = self.cleaned_data['since_date']
        until_date = self.cleaned_data['until_date']

        try:
            
            xero_import_budgets_task.delay(
                integration.id,
                since_date.strftime('%Y-%m-%d'),
                until_date.strftime('%Y-%m-%d')
            )
            log_task_event(
                f"Xero budget import task initiated for {integration.org.name} from {since_date} to {until_date}",
                "success",
                None
            )
            return (
                f"Xero budget import task initiated for {integration.org.name} from {since_date} to {until_date}",
                None
            )
        except Exception as e:
            return (None, f"Error initiating Xero budget import task: {str(e)}")
