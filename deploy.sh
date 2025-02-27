#!/bin/bash

echo "Pulling latest code..."
git pull origin main

echo "Activating virtual environment..."
source /var/www/WS-Insights/venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements/requirements.txt

echo "Running migrations..."
python manage.py makemigrations
python manage.py migrate

echo "Collecting static files..."
python manage.py collectstatic --noinput

# Gracefully stop existing Gunicorn
echo "Killing existing Gunicorn instances..."
pkill -f "gunicorn config.wsgi:application --bind 127.0.0.1:8000"

echo "Starting Gunicorn..."
gunicorn config.wsgi:application --bind 127.0.0.1:8000 --timeout 300 --workers 2 --daemon

# Gracefully stop and restart Celery Workers
echo "Gracefully stopping Celery workers..."
celery multi stopwait worker1 worker2 worker3 --timeout=30

echo "Starting Celery workers..."
celery multi start worker1 worker2 worker3 -A config -Q org_sync,celery -c 3 -l info

# Restart Celery Beat if not using a process manager:
echo "Stopping Celery Beat..."
pkill -f "celery beat -A config"
echo "Starting Celery Beat..."
celery -A config beat -l info --pidfile=/tmp/celerybeat.pid --detach

# Restart Flower
echo "Stopping Flower..."
pkill -f "flower -A config"
echo "Starting Flower..."
/var/www/WS-Insights/venv/bin/celery -A config flower --port=5555

echo "Deployment completed successfully!"
