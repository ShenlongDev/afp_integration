# myapp/forms.py
from django import forms
from django.core.exceptions import ValidationError
from integrations.models.models import Integration, Organisation
from integrations.modules import MODULES
from django.utils import timezone
from datetime import datetime
from integrations.services.utils import get_organisations_by_integration_type
from core.tasks.general import log_task_event


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
    integration_type = forms.ChoiceField(
        choices=get_integration_type_choices(),
        label="Integration Type"
    )
    organisation = forms.ModelChoiceField(
        queryset=Organisation.objects.all(),
        label="Organisation"
    )
    since_date = forms.DateField(
        initial="2020-01-01",
        widget=forms.SelectDateWidget(
            attrs={
                'class': 'date-select',
                'style': 'width: 100%; display: inline-block; margin-right: 1%;'
            },
            years=range(2000, datetime.now().year + 1)
        ),
        label="Import Data Since"
    )
    modules = forms.MultipleChoiceField(
        choices=[],
        required=False,
        widget=forms.CheckboxSelectMultiple,
        label="Modules/Components to Import"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Always update integration_type choices from MODULES.
        integration_choices = get_integration_type_choices()
        self.fields['integration_type'].choices = integration_choices

        raw_integration_type = (
            self.data.get('integration_type') or self.initial.get('integration_type') or ''
        )
        integration_type = raw_integration_type.lower() if raw_integration_type else ''

        if not integration_type and integration_choices:
            integration_type = integration_choices[0][0].lower()
            self.initial['integration_type'] = integration_type

        self.fields['modules'].choices = get_module_choices(integration_type)
        self.fields['organisation'].queryset = get_organisations_by_integration_type(integration_type)

        if 'organisation' in self.data:
            try:
                org_id = int(self.data.get('organisation'))
                if 'integration' in self.fields:
                    self.fields['integration'].queryset = Integration.objects.filter(org_id=org_id)
            except (ValueError, TypeError):
                pass
        elif self.initial.get('organisation'):
            org = self.initial.get('organisation')
            if 'integration' in self.fields:
                self.fields['integration'].queryset = Integration.objects.filter(org=org)

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
            cred_fields = {
                'xero': ('xero_client_id', 'xero_client_secret'),
                'netsuite': ('netsuite_account_id', 'netsuite_consumer_key'),
                'toast': ('toast_client_id', 'toast_client_secret'),
            }
            id_field, secret_field = cred_fields.get(integration_type, (None, None))
            if id_field and secret_field:
                integration = Integration.objects.filter(
                    org=organisation,
                    **{
                        f'{id_field}__isnull': False,
                        f'{secret_field}__isnull': False
                    }
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
    """
    Form specifically for importing Xero budgets with date range control.
    """
    integration_type = forms.ChoiceField(
        choices=[("xero", "Xero")], 
        label="Integration Type",
        initial="xero",
        widget=forms.HiddenInput() 
    )
    organisation = forms.ModelChoiceField(
        queryset=Organisation.objects.all(),
        label="Organisation"
    )
    since_date = forms.DateField(
        initial="2020-01-01",
        widget=forms.SelectDateWidget(
            attrs={
                'class': 'date-select',
                'style': 'width: 100%; display: inline-block; margin-right: 1%;'
            },
            years=range(2000, datetime.now().year + 1)
        ),
        label="Budget Data From"
    )
    until_date = forms.DateField(
        initial=datetime.now().date(),
        widget=forms.SelectDateWidget(
            attrs={
                'class': 'date-select',
                'style': 'width: 100%; display: inline-block; margin-right: 1%;'
            },
            years=range(2000, datetime.now().year + 1)
        ),
        label="Budget Data To"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        integration_type = "xero" 
        self.fields['organisation'].queryset = get_organisations_by_integration_type(integration_type)

    def clean(self):
        cleaned_data = super().clean()
        organisation = cleaned_data.get('organisation')
        integration_type = cleaned_data.get('integration_type', 'xero')
        since_date = cleaned_data.get('since_date')
        until_date = cleaned_data.get('until_date')
        
        if since_date and until_date and since_date > until_date:
            raise ValidationError("'From' date cannot be later than 'To' date.")

        if organisation:
            integration = Integration.objects.filter(
                org=organisation,
                xero_client_id__isnull=False,
                xero_client_secret__isnull=False
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
