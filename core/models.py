from django.db import models

    
    
class TaskLog(models.Model):
    task_name = models.CharField(max_length=255)
    status = models.CharField(max_length=50)
    detail = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.task_name} - {self.status}"
