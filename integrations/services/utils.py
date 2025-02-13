from django.db import transaction, close_old_connections

class BatchUtils:
    @staticmethod
    def bulk_create_batches(model, objects, batch_size=1000):
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
    def process_in_batches(items, process_func, batch_size=1000):
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
