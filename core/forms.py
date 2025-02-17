# myapp/forms.py
from django import forms
from django.core.exceptions import ValidationError
from integrations.models.models import Organisation, Integration
from integrations.modules import MODULES
from django.utils import timezone
from datetime import datetime
from integrations.services.utils import get_organisations_by_integration_type

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
        # Create a tuple for each module key with a more user-friendly label.
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
        initial="2010-01-01",
        widget=forms.SelectDateWidget(
            attrs={
                'class': 'date-select',
                'style': 'width: 100%; display: inline-block; margin-right: 1%;'
            },
            years=range(2022, datetime.now().year + 1)
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
        
        # Always update the integration_type choices from MODULES
        integration_choices = get_integration_type_choices()
        self.fields['integration_type'].choices = integration_choices

        # Set a default integration type if none is provided
        integration_type = self.data.get('integration_type') or self.initial.get('integration_type')
        if not integration_type and integration_choices:
            integration_type = integration_choices[0][0]
            self.initial['integration_type'] = integration_type

        # Update the modules field based on the integration_type.
        if integration_type:
            self.fields['modules'].choices = get_module_choices(integration_type)
        else:
            self.fields['modules'].choices = []

        # NEW: Limit the organisations to only the ones eligible for the selected integration type.
        self.fields['organisation'].queryset = get_organisations_by_integration_type(integration_type)
        
        # Optionally, if you have a field to choose a specific integration,
        # update its queryset based on the selected organisation.
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

    def clean(self):
        cleaned_data = super().clean()
        organisation = cleaned_data.get('organisation')
        integration_type = cleaned_data.get('integration_type')

        if organisation and integration_type:
            # Get the appropriate credential fields based on integration type
            cred_fields = {
                'xero': ('xero_client_id', 'xero_client_secret'),
                'netsuite': ('netsuite_client_id', 'netsuite_client_secret'),
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
                        f"No {integration_type.capitalize()} integration found with valid credentials for this organisation."
                    )
                
                # Store the integration in cleaned_data for use in the view
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
            # Get the appropriate importer class and methods from MODULES
            module_config = MODULES[integration_type]
            ImporterClass = module_config['client']
            
            # Instantiate the importer with the integration and since_date
            importer = ImporterClass(integration, since_date)
            
            if selected_modules:
                # Process only selected modules
                for module in selected_modules:
                    import_func = module_config['import_methods'].get(module)
                    if import_func:
                        import_func(importer)
            else:
                # If no modules selected and full_import is available, use it
                full_import = module_config.get('full_import')
                if full_import:
                    full_import(importer)
                else:
                    # If no full_import available, process all modules
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
