# myapp/forms.py
from django import forms
from integrations.models.models import Organisation, Integration

MODULE_CHOICES = (
    ('accounts', 'Accounts'),
    ('journal_lines', 'Journal Lines'),
    ('contacts', 'Contacts'),
    ('invoices', 'Invoices'),
    ('bank_transactions', 'Bank Transactions'),
)

INTEGRATION_TYPE_CHOICES = (
    ('XERO', 'Xero'),
    ('NETSUITE', 'NetSuite'),
)

class DataImportForm(forms.Form):
    integration_type = forms.ChoiceField(
        choices=INTEGRATION_TYPE_CHOICES,
        label="Integration Type"
    )
    organisation = forms.ModelChoiceField(
        queryset=Organisation.objects.all(),
        label="Organisation"
    )
    since_date = forms.DateField(
        initial="2010-01-01",
        widget=forms.SelectDateWidget,
        label="Import Data Since"
    )
    modules = forms.MultipleChoiceField(
        choices=MODULE_CHOICES,
        required=False,
        widget=forms.CheckboxSelectMultiple,
        label="Modules/Components to Import"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Optionally, you can filter the integrations based on the selected organisation.
        if 'organisation' in self.data:
            try:
                org_id = int(self.data.get('organisation'))
                self.fields['integration'].queryset = Integration.objects.filter(org_id=org_id)
            except (ValueError, TypeError):
                pass
        elif self.initial.get('organisation'):
            org = self.initial.get('organisation')
            self.fields['integration'].queryset = Integration.objects.filter(org=org)
