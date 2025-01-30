#!/bin/bash

# Navigate to the project directory
cd /var/www/WS-Insights || exit

# Get the latest branch commit
git pull origin $(git rev-parse --abbrev-ref HEAD)

# # Navigate to the frontend folder and build
# cd frontend || exit
# npm run build
# cd ..

# Activate the virtual environment
source venv/bin/activate

# Run Django management commands
python manage.py makemigrations
python manage.py migrate
python manage.py collectstatic --noinput

# Run Gunicorn service
gunicorn config.wsgi:application --bind 0.0.0.0:8000 --daemon
