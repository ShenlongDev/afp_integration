from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.permissions import AllowAny
from django.conf import settings
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from .serializers import EmailSerializer
import logging
import json
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

class SendEmailView(APIView):
    permission_classes = [AllowAny]
    
    def post(self, request):
        """
        API endpoint to send emails using SendGrid.
        Expected JSON payload:
        {
            "to": "recipient@example.com",
            "subject": "Email Subject",
            "body": "Email body content"
        }
        """
        serializer = EmailSerializer(data=request.data)
        if not serializer.is_valid():
            logger.error(f"Invalid email data: {serializer.errors}")
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            data = serializer.validated_data
            
            sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
            sg.client.verify_ssl = False
            
            message = Mail(
                from_email=settings.SENDGRID_FROM_EMAIL,
                to_emails=data['to'],
                subject=data['subject'],
                html_content=data['body']
            )
            
            response = sg.send(message)
            
            logger.info(f"Email sent successfully to {data['to']}")
            return Response({
                'status': 'success',
                'message_id': response.headers['X-Message-Id']
            }, status=status.HTTP_200_OK)
            
        except urllib.error.HTTPError as e:
            try:
                error_body = e.read().decode()
                error_json = json.loads(error_body)
                error_message = error_json.get('errors', [{}])[0].get('message', 'Unknown SendGrid error')
                logger.error(f"SendGrid API error: {error_message}")
                return Response(
                    {'error': f'SendGrid API error: {error_message}'},
                    status=e.code
                )
            except (json.JSONDecodeError, IndexError, AttributeError) as parse_error:
                logger.error(f"HTTP error while sending email: {str(e)}")
                return Response(
                    {'error': f'HTTP error while sending email: {str(e)}'},
                    status=status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return Response(
                {'error': f'Unexpected error: {str(e)}'},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            ) 