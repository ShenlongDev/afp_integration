from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.html import strip_tags
import pandas as pd
import io
import base64
from datetime import datetime
import os
import requests
import logging

logger = logging.getLogger(__name__)

def send_email(recipients, subject, html_content, text_content=None):
    """
    Send email using SendGrid API
    
    Args:
        recipients (list): List of email recipients
        subject (str): Email subject
        html_content (str): HTML content of the email
        text_content (str, optional): Plain text content. If None, will be generated from HTML
        
    Returns:
        bool: True if email was sent successfully
        
    Raises:
        Exception: If sending fails
    """
    if text_content is None:
        text_content = strip_tags(html_content)
    
    sg_api_key = os.environ.get('SENDGRID_API_KEY')
    from_email = os.environ.get('SENDGRID_FROM_EMAIL')
    
    if not sg_api_key or not from_email:
        raise Exception("SendGrid API key or from email not configured")
    
    url = "https://api.sendgrid.com/v3/mail/send"
    headers = {
        "Authorization": f"Bearer {sg_api_key}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "personalizations": [
            {
                "to": [{"email": recipient} for recipient in recipients]
            }
        ],
        "from": {"email": from_email, "name": "Service Report"},
        "subject": subject,
        "content": [
            {
                "type": "text/plain",
                "value": text_content
            },
            {
                "type": "text/html",
                "value": html_content
            }
        ]
    }
    
    try:
        session = requests.Session()
        session.verify = False
        
        response = session.post(url, headers=headers, json=payload)
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Email sent successfully via SendGrid API: {response.status_code}")
            return True
        else:
            logger.error(f"SendGrid API failed with status code {response.status_code}: {response.text}")
            raise Exception(f"Failed to send email: {response.text}")
            
    except Exception as e:
        logger.error(f"SendGrid API request failed: {str(e)}")
        raise

def send_weekly_sales_report(recipients, data, site_name=""):
    """
    Generate and send weekly sales report email
    
    Args:
        recipients (list): List of email recipients
        data (list): Sales and weather data
        site_name (str): Name of the site for display in the report
        
    Returns:
        bool: True if email was sent successfully
    """
    for item in data['data']:
        if isinstance(item['DATE'], datetime):
            item['DATE'] = item['DATE'].strftime('%Y-%m-%d')
    
    df = pd.DataFrame(data['data'])
    
    total_sales = df['SALES'].sum()
    total_lw_sales = df['LW_SALES'].sum()
    total_covers = df['COVERS'].sum()
    total_lw_covers = df['LW_COVERS'].sum()
    
    sales_change = ((total_sales - total_lw_sales) / total_lw_sales * 100) if total_lw_sales else 0
    covers_change = ((total_covers - total_lw_covers) / total_lw_covers * 100) if total_lw_covers else 0
    
    context = {
        'site_name': site_name,
        'data': list(reversed(data['data'])),
        'total_sales': total_sales,
        'total_lw_sales': total_lw_sales,
        'sales_change': sales_change,
        'total_covers': total_covers,
        'total_lw_covers': total_lw_covers,
        'covers_change': covers_change,
        'week_ending': data['data'][-1]['DATE'] if data['data'] else 'N/A',
        'comments': list(reversed(data['comments']))
    }
    
    try:
        html_content = render_to_string('emails/weekly_report.html', context)
        text_content = strip_tags(html_content)
        
        subject = f"Daily Sales Report for {site_name} - Ending date {context['week_ending']}"
        
        output_path = os.path.join(os.getcwd(), 'weekly_report.html')
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        return send_email(recipients, subject, html_content, text_content)
        
    except Exception as e:
        logger.error(f"Error generating or sending report: {str(e)}")
        raise 