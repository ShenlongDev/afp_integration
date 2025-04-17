from rest_framework import serializers

class EmailSerializer(serializers.Serializer):
    to = serializers.EmailField()
    subject = serializers.CharField(max_length=255)
    body = serializers.CharField() 