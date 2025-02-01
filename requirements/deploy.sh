#!/bin/bash

echo "Pulling latest code..."
git pull origin main

echo "Activating virtual environment..."
source /var/www/WS-Insights/venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Applying migrations..."
python manage.py migrate

echo "Collecting static files..."
python manage.py collectstatic --noinput

echo "Restarting Gunicorn service..."
sudo systemctl restart gunicorn

echo "Deployment completed successfully!"
