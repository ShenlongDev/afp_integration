from django.db import transaction, close_old_connections
import hashlib

class BatchUtils:
    @staticmethod
    def bulk_create_batches(model, objects, batch_size=10000):
        """
        Accepts a model and an iterable of objects.
        Creates the objects in batches (each in its own atomic block)
        and calls close_old_connections() after each batch.
        Returns the total number of objects created.
        """
        total_count = 0
        batch = []
        for obj in objects:
            batch.append(obj)
            if len(batch) >= batch_size:
                with transaction.atomic():
                    model.objects.bulk_create(batch, batch_size=batch_size)
                total_count += len(batch)
                batch.clear()
                close_old_connections()
        if batch:
            with transaction.atomic():
                model.objects.bulk_create(batch, batch_size=batch_size)
            total_count += len(batch)
            close_old_connections()
        return total_count

    @staticmethod
    def process_in_batches(items, process_func, batch_size=10000):
        """
        Accepts an iterable of items and a processing function.
        Processes each batch inside an atomic transaction.
        """
        batch = []
        for item in items:
            batch.append(item)
            if len(batch) >= batch_size:
                with transaction.atomic():
                    for i in batch:
                        process_func(i)
                batch.clear()
        if batch:
            with transaction.atomic():
                for i in batch:
                    process_func(i)



def compute_unique_key(row):
    """
    Generate a unique key for a transaction line by combining several fields.
    Adjust the fields as needed.
    """
    unique_str = f"{row.get('id')}-{row.get('linelastmodifieddate')}-{row.get('transactionid')}-{row.get('linesequencenumber')}"
    return hashlib.md5(unique_str.encode('utf-8')).hexdigest()


def get_organisations_by_integration_type(integration_type):
    """
    Returns a queryset of Organisation objects eligible for the given integration type.

    For 'xero', an Organisation is eligible if it has at least one related Integration with non-null 
    xero_client_id and xero_client_secret.
    
    For 'netsuite', an Organisation is eligible if it has at least one related Integration with non-null
    netsuite_client_id and netsuite_client_secret.
    
    Organisations can be eligible for both, and are included in the corresponding querysets.
    """
    from core.models import Organisation
    from integrations.models.models import Integration

    if integration_type.lower() == 'xero':
        return Organisation.objects.filter(
            integrations__xero_client_id__isnull=False,
            integrations__xero_client_secret__isnull=False
        ).distinct()
    elif integration_type.lower() == 'netsuite':
        return Organisation.objects.filter(
            integrations__netsuite_client_id__isnull=False,
            integrations__netsuite_client_secret__isnull=False
        ).distinct()

    return Organisation.objects.none()


def log_task_event(task_name, status, detail):
    from core.models import TaskLog
    from django.utils import timezone
    TaskLog.objects.create(
        task_name=task_name,
        status=status,
        detail=detail,
        timestamp=timezone.now()
    )
    

def get_integrations_by_integration_type(integration_type):
    """
    Returns a queryset of Integration objects eligible for the given integration type.
    """
    from integrations.models.models import Integration
    if integration_type.lower() == 'xero':
        return Integration.objects.filter(
            xero_client_id__isnull=False,
            xero_client_secret__isnull=False
        )
    elif integration_type.lower() == 'netsuite':
        return Integration.objects.filter(
            netsuite_client_id__isnull=False,
            netsuite_client_secret__isnull=False
        )
    return Integration.objects.none()