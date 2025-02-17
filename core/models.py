from django.db import models


class Organisation(models.Model):
    name = models.CharField(max_length=255, unique=True)    

    def __str__(self):
        return self.name
    
    
class TaskLog(models.Model):
    task_name = models.CharField(max_length=255)
    status = models.CharField(max_length=50)
    detail = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.task_name} - {self.status}"
