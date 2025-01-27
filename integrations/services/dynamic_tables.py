import re
import json
from django.db import connection

def sanitize_table_name(org_name: str, code: str, name: str) -> str:
    """
    Generate a unique table name using code, name, and organization name.
    Format: code_name_org_firstname (lowercase, underscores)
    """
    first_org_name = re.sub(r"\s+", "_", org_name.split()[0].lower())
    raw = f"{code}_{name}_org_{first_org_name}" if code else f"{name}_org_{first_org_name}"
    table_name = re.sub(r"[^\w]+", "_", raw.lower())
    table_name = table_name[:50].strip("_")
    return table_name or "unnamed_account"

def create_account_table(table_name: str):
    """
    Create a dynamic table for an account if it doesn't exist.
    """
    sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id BIGSERIAL PRIMARY KEY,
        insights_unique_id TEXT NOT NULL, 
        date DATE,
        reference VARCHAR(255),
        currency_code VARCHAR(10),
        currency_rate NUMERIC(10,5),
        status VARCHAR(50),
        description TEXT,
        quantity NUMERIC(10,2),
        unit_amount NUMERIC(14,2),
        account_code VARCHAR(50),
        item_code VARCHAR(50),
        line_item_id VARCHAR(50),
        tax_type VARCHAR(50),
        tax_amount NUMERIC(14,2),
        line_amount NUMERIC(14,2),
        tracking JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(sql)

def rename_account_table(old_name: str, new_name: str):
    """
    Rename an existing account table.
    """
    if old_name == new_name:
        return
    sql = f'ALTER TABLE IF EXISTS "{old_name}" RENAME TO "{new_name}";'
    with connection.cursor() as cursor:
        cursor.execute(sql)

def insert_transaction_row(table_name: str, data: dict):
    """
    Insert a transaction row into the specified dynamic table.
    """
    columns = []
    placeholders = []
    values = []

    for key, val in data.items():
        if isinstance(val, (dict, list)):
            val = json.dumps(val)
        columns.append(f'"{key}"')
        placeholders.append("%s")
        values.append(val)

    col_str = ", ".join(columns)
    ph_str = ", ".join(placeholders)
    sql = f'INSERT INTO "{table_name}" ({col_str}) VALUES ({ph_str});'
    
    with connection.cursor() as cursor:
        cursor.execute(sql, values)
