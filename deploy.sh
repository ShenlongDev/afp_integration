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

# Gracefully stop Gunicorn
echo "Killing existing Gunicorn instances..."
pkill -f "gunicorn config.wsgi:application --bind 127.0.0.1:8000"

echo "Starting Gunicorn..."
gunicorn config.wsgi:application --bind 127.0.0.1:8000 --timeout 300 --workers 2 --daemon

# Gracefully stop Celery workers with a timeout, and if it fails, force kill.
echo "Gracefully stopping Celery workers..."
if ! timeout 30 celery multi stopwait worker1 worker2 worker3 --timeout=20; then
    echo "Graceful shutdown timed out; force killing workers..."
    celery multi kill worker1 worker2 worker3
fi

# Clean up stale pid files for Celery workers
echo "Cleaning up stale Celery worker pidfiles..."
rm -f /var/run/celery/*.pid

echo "Starting Celery workers..."
celery multi start worker1 worker2 worker3 -A config -Q org_sync,celery -c 3 -l info

# Restart Celery Beat
echo "Stopping Celery Beat..."
pkill -f "celery beat -A config"
echo "Cleaning up Celery Beat pidfile..."
rm -f /tmp/celerybeat.pid
echo "Starting Celery Beat..."
celery -A config beat -l info --pidfile=/tmp/celerybeat.pid --detach

# Restart Flower
echo "Stopping Flower..."
pkill -f "flower -A config"
sleep 3  # Allow some time for shutdown

# Optionally kill any process using port 5555
fuser -k 5555/tcp

# Remove Flower PID file if one exists (adjust the path as needed)
rm -f /tmp/flower.pid

echo "Starting Flower..."
/var/www/WS-Insights/venv/bin/celery -A config flower --daemon

echo "Deployment completed successfully!"
