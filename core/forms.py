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

        # Always update integration_type choices from MODULES.
        integration_choices = get_integration_type_choices()
        self.fields['integration_type'].choices = integration_choices

        # Get raw integration_type from POST data or initial data.
        raw_integration_type = (
            self.data.get('integration_type') or self.initial.get('integration_type') or ''
        )
        # Immediately ensure it is lower-case.
        integration_type = raw_integration_type.lower() if raw_integration_type else ''

        # If no integration type is provided, use the first available choice.
        if not integration_type and integration_choices:
            integration_type = integration_choices[0][0].lower()
            self.initial['integration_type'] = integration_type

        # Use the normalized integration_type consistently.
        self.fields['modules'].choices = get_module_choices(integration_type)
        self.fields['organisation'].queryset = get_organisations_by_integration_type(integration_type)

        # (Optional) If you have a dependent integration field, update its queryset.
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
        # Convert to lowercase for consistent comparison
        integration_type = integration_type.lower() if integration_type else ''
        
        # Check if integration_type exists in MODULES after normalization
        if not integration_type:
            raise forms.ValidationError("Integration type is required.")
            
        if integration_type not in MODULES:
            # Get list of valid types for error message
            valid_types = ", ".join(MODULES.keys())
            raise forms.ValidationError(
                f"'{integration_type}' is not a valid integration type. "
                f"Valid types are: {valid_types}"
            )
            
        return integration_type


    def clean(self):
        cleaned_data = super().clean()
        organisation = cleaned_data.get('organisation')
        integration_type = cleaned_data.get('integration_type')

        if organisation and integration_type:
            # Determine the required credential fields based on the integration type.
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
                        f"No {integration_type.capitalize()} integration found with valid credentials for "
                        f"this organisation."
                    )

                # Store the integration for later use.
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
            # Get importer class and methods.
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
