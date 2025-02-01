#!/bin/bash

echo "Pulling latest code..."
git pull origin main

echo "Activating virtual environment..."
source /var/www/WS-Insights/venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements/requirements.txt

echo "Running migrations..."
python manage.py makemigrations

echo "Applying migrations..."
python manage.py migrate

echo "Collecting static files..."
python manage.py collectstatic --noinput



echo "Restarting Gunicorn service..."
sudo systemctl restart gunicorn

echo "Starting Gunicorn..."
/var/www/WS-Insights/gunicorn config.wsgi:application --bind 127.0.0.1:8000 --timeout 300 --workers 2
echo "Deployment completed successfully!"
