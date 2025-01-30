#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Navigate to the project directory
cd /var/www/WS-Insights || { echo "Failed to change directory"; exit 1; }

# Get the latest branch commit
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
git pull origin "$CURRENT_BRANCH" || { echo "Git pull failed"; exit 1; }

# # Navigate to the frontend folder and build
# cd frontend || { echo "Frontend directory not found"; exit 1; }
# npm install || { echo "npm install failed"; exit 1; }
# npm run build || { echo "npm run build failed"; exit 1; }
# cd ..

# Activate the virtual environment
source venv/bin/activate || { echo "Failed to activate virtual environment"; exit 1; }

# Install Python dependencies
pip install -r requirements/requirements.txt || { echo "pip install failed"; exit 1; }

# Run Django management commands
python manage.py makemigrations || { echo "makemigrations failed"; exit 1; }
python manage.py migrate || { echo "migrate failed"; exit 1; }
python manage.py collectstatic --noinput || { echo "collectstatic failed"; exit 1; }

# Restart Gunicorn service (Assuming you have a gunicorn.service)
sudo systemctl restart gunicorn.service || { echo "Failed to restart Gunicorn"; exit 1; }

echo "Deployment successful!"
