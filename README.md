# WS-Insights

WS-Insights is a tool designed to provide insights and analytics using various integrations. It helps in monitoring performance, tracking usage, and identifying potential issues.

---

## Overview

This README covers the complete setup of the Django project including installation, configuration, running the admin panel, using management commands for data import, customizing the data import form, and configuring Celery for background tasks.

---

## Setup and Installation

1. **Clone the Repository:**

   ```bash
   git clone <repository_url>
   cd WS-Insights
   ```

2. **Create and Configure the .env File:**

   The project expects a `.env` file to be present in the `requirements/` directory. This file contains environment-specific settings. Below is an example of the required variables:

   ```env
   SECRET_KEY=''
   DEBUG=
   ALLOWED_HOSTS=localhost,127.0.0.1, ip
   DB_NAME=
   DB_USER=
   DB_PASSWORD=
   DB_HOST=
   DB_PORT=
   ENVIRONMENT=
   DJANGO_LOG_FILE=<>/logs/django_debug.log

   CELERY_BROKER_URL=
   CELERY_RESULT_BACKEND=
   CELERY_SECRET_TOKEN=

   BACKEND_URL=http://localhost:8000
   ```

   **Note:** Make sure your `.env` file is in the same folder as your `requirements.txt` file (i.e. inside the `requirements/` directory).

3. **Install Python Dependencies:**

   Install all required dependencies by running:

   ```bash
   pip install -r requirements/requirements.txt
   ```

4. **Database Setup:**

   Run all migrations to set up your database:

   ```bash
   python manage.py migrate
   ```

5. **Create Django Admin User:**

   To access the Django admin panel, create a superuser. By default, we recommend using:
   - **Username:** `admin`
   - **Password:** `admin`

   Create the user with:

   ```bash
   python manage.py createsuperuser
   ```

   When prompted, enter `admin` for both the username and password. (You may change these credentials later if needed.)

---

## Running the Server

Start the Django development server by running:

```bash
python manage.py runserver
```

Then, navigate to:

```
http://localhost:8000/admin
```

Use the admin credentials (username: `admin`, password: `admin`) to log in.

---

## Django Management Commands for Data Import

WS-Insights includes several management commands to import data from various integrations. Below are the instructions and parameter explanations for each:

### 1. Xero Data Import

**Command Syntax:**

```bash
python manage.py import_xero_data [integration_id] [--since YYYY-MM-DD] [--components component1 component2 ...]
```

- **integration_id (optional):** The specific integration's ID. If omitted, data will be imported for all integrations that have Xero credentials.
- **--since:** An optional date to filter data from. The expected format is `YYYY-MM-DD`. Defaults to today's date if not provided.
- **--components:** A list of specific data components to import. Available options include:
  - `accounts`
  - `journal_lines`
  - `contacts`
  - `invoices`
  - `bank_transactions`
  - `budgets`

If no components are specified, the command will run a full import of Xero data.

### 2. Toast Data Import

**Command Syntax:**

```bash
python manage.py import_toast_data [integration_id] [--since YYYY-MM-DD] [--until YYYY-MM-DD]
```

- **integration_id (optional):** The Toast integration ID. If omitted, all available Toast integrations will be processed.
- **--since:** The start date for importing orders. Expected format is `YYYY-MM-DD`. Defaults to today's date.
- **--until:** The optional end date for orders to be imported. Expected format is `YYYY-MM-DD`.

### 3. NetSuite Data Import and Transformation

**Command Syntax:**

```bash
python manage.py import_netsuite_data [integration_id] [--since YYYY-MM-DD] [--until YYYY-MM-DD] [--components component1 component2 ...] [--transform-only]
```

- **integration_id (optional):** The specific NetSuite integration ID. If not specified, the command will target all integrations with valid NetSuite credentials.
- **--since:** The start date for the import. Format: `YYYY-MM-DD`. Defaults to today's date at midnight.
- **--until:** The end date for import (Format: `YYYY-MM-DD`).
- **--components:** A list of components to import. Options include:
  - `vendors`
  - `accounts`
  - `transactions`
  - `transaction_accounting_lines`
  - `transaction_lines`
  - `subsidiaries`
  - `departments`
  - `entities`
  - `accounting_periods`
- **--transform-only:** If this flag is provided, the command skips the data import and runs only the data transformations.

---

## Customizing the Data Import Form

The Data Import form used in the admin panel can be tweaked as per your needs. For these modifications, check the following file:

```
core/templates/admin/files
```

This template manages file uploads and data import interactions in the Django admin. Customize it to adjust the fields, validations, or processing logic.

---

## Celery Configuration

WS-Insights utilizes Celery for handling asynchronous background tasks. Two distinct workers are set up for different priorities:

- **Normal Tasks Worker:**  
  Processes "normal" priority tasks with a maximum concurrency of 3 tasks at a time.

- **High Priority Tasks Worker:**  
  Dedicated to handling high priority tasks.

**Starting the Celery Workers:**

1. **Normal Tasks Worker:**

   ```bash
   celery -A config worker -n normal_worker@%h -Q org_sync,celery -c 3 -l info \
  --logfile=/var/log/celery/worker.log --detach
   ```

2. **High Priority Tasks Worker:**

   ```bash
   celery -A config worker -n high_priority_worker@%h -Q high_priority -c 1 -l info \
  --logfile=/var/log/celery/worker_high_priority.log --detach
   ```

Make sure both workers are running so that all tasks are queued and processed appropriately.

---

## Summary

- **Installation:** Clone the repository, configure the `.env` file (in `requirements/`), install dependencies, and run migrations.
- **Django Setup:** Create an admin user (default: `admin`/`admin`), start the server, and access the admin panel.
- **Management Commands:** Three management commands are available to import data from Xero, Toast, and NetSuite. Each command offers several parameters such as `--since`, `--until`(only in toast),and  `--components`, where applicable.
- **Data Import Form:** The form template can be modified at `core/templates/admin/files` to suit custom needs.
- **Celery Workers:** Configure two Celery workers (one for normal tasks with 3-task concurrency and one for high priority tasks) to handle background processing.

Happy coding and analyzing your data with WS-Insights!