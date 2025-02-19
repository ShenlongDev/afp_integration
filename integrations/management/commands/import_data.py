import os
from django.core.management.base import BaseCommand
from django.conf import settings

class ImportDataCommand(BaseCommand):
    def handle(self, *args, **options):
        kill_switch_file = os.path.join(settings.BASE_DIR, 'KILL_SWITCH_ON')
        if os.path.exists(kill_switch_file):
            self.stdout.write("Hey!!!")
            return
        self.handle_command(*args, **options)
    
    def handle_command(self, *args, **options):
        raise NotImplementedError("Subclasses must implement handle_command.")
