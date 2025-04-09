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

# Gracefully stop Celery workers
echo "Stopping Celery workers..."
celery -A config control shutdown
sleep 30  # Allow time for graceful shutdown

# More aggressive worker termination as fallback
echo "Forcefully terminating any remaining Celery workers..."
pkill -f celery
# Add this to be absolutely sure workers are gone
pkill -9 -f "celery worker" || true

# Clean up stale pidfiles
echo "Cleaning up stale pidfiles..."
rm -f /var/run/celery/*.pid
rm -f /tmp/celerybeat.pid

# Ensure log directories exist with proper permissions
echo "Setting up log directories..."
mkdir -p /var/log/celery
chmod 777 /var/log/celery
touch /var/log/celery/worker.log /var/log/celery/worker_high_priority.log /var/log/celery/beat.log /var/log/celery/flower.log
chmod 666 /var/log/celery/*.log

# Check Redis health
echo "Checking Redis health..."
if ! redis-cli ping | grep -q "PONG"; then
    echo "Redis is not responding, attempting to restart..."
    systemctl restart redis
    sleep 5
    if ! redis-cli ping | grep -q "PONG"; then
        echo "Redis still not responding after restart. Deployment may fail!"
    else
        echo "Redis restarted successfully."
    fi
else
    echo "Redis is healthy."
fi

# Consider flushing Redis to clear any stale tasks (uncomment if needed)
echo "Flushing Redis to clear stale task state..."
redis-cli flushall
echo "Redis flushed successfully."

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

# Add diagnostics to verify worker status
echo "Waiting for workers to initialize..."
sleep 10 # Give workers time to start up

echo "Checking Celery worker status..."
celery -A config status

echo "Checking Celery worker queues..."
celery -A config inspect active_queues

echo "Deployment completed successfully!"
