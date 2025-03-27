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
pkill -f "gunicorn config.wsgi:application --bind 0.0.0.0:8000"

echo "Starting Gunicorn..."
gunicorn config.wsgi:application --bind 0.0.0.0:8000 --daemon

# Gracefully stop Celery workers with a timeout, and if it fails, force kill.
echo "Stopping Celery workers..."
pkill -f "celery -A config worker"
sleep 5  # Allow graceful shutdown

# Clean up stale pidfiles if required
rm -f /var/run/celery/*.pid

echo "Starting normal Celery worker..."
celery -A config worker -n normal_worker@%h -Q org_sync,celery -c 3 -l info \
  --logfile=/var/log/celery/worker.log --detach

echo "Starting high priority Celery worker..."
celery -A config worker -n high_priority_worker@%h -Q high_priority -c 1 -l info \
  --logfile=/var/log/celery/worker_high_priority.log --detach

# Restart Celery Beat
echo "Stopping Celery Beat..."
pkill -f "celery beat -A config"
echo "Cleaning up Celery Beat pidfile..."
rm -f /tmp/celerybeat.pid
echo "Starting Celery Beat..."
celery -A config beat -l info --pidfile=/tmp/celerybeat.pid --logfile=/var/log/celery/beat.log --detach

# Restart Flower
echo "Stopping Flower..."
pkill -f "flower -A config"
sleep 3  # Allow Flower to stop completely

# Optionally kill any process using port 5555
fuser -k 5555/tcp

# Remove Flower PID file if one exists (adjust the path as needed)
rm -f /tmp/flower.pid

echo "Starting Flower in background..."
# Use nohup and background (&) to run Flower detached.
nohup /var/www/WS-Insights/venv/bin/celery -A config flower --port=5555 > /var/log/celery/flower.log 2>&1 &
echo "Flower started."

echo "Deployment completed successfully!"
