import os
import django
import csv
import pandas as pd
from django.db import connection
from decimal import Decimal

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

def compare_orders(csv_file_path):
    # Read CSV file
    df = pd.read_csv(csv_file_path)
    print("\nCSV Columns:")
    print(df.columns.tolist())
    
    # Query database
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT 
                display_number,
                business_date,
                order_net_sales,
                service_charges,
                tip,
                total_amount
            FROM integrations_toastorder
            WHERE business_date = 20250410
                AND restaurant_guid = '27cf31a1-fa69-471a-a1d2-e788b029cabc'
        """)
        db_records = cursor.fetchall()
    
    # Convert database records to DataFrame
    db_df = pd.DataFrame(db_records, columns=[
        'display_number',
        'business_date',
        'order_net_sales',
        'service_charges',
        'tip',
        'total_amount'
    ])
    
    # Print sample of order numbers from both sources
    print("\nSample of order numbers from CSV:")
    print(df['Order #'].head())
    print("\nSample of order numbers from database:")
    print(db_df['display_number'].head())
    
    # Compare data
    discrepancies = []
    for _, csv_row in df.iterrows():
        order_number = csv_row['Order #']
        # Convert order number to string for comparison
        order_number = str(order_number)
        db_row = db_df[db_df['display_number'].astype(str) == order_number]
        
        if db_row.empty:
            print(f"\nDebug: Order #{order_number} not found in database")
            print("Available order numbers in database:", db_df['display_number'].astype(str).tolist())
            discrepancies.append({
                'order_number': order_number,
                'issue': 'Order not found in database',
                'csv_data': csv_row.to_dict()
            })
            continue
            
        db_row = db_row.iloc[0]
        
        # Calculate total from CSV components
        csv_total = (
            Decimal(str(csv_row['Amount'])) + 
            Decimal(str(csv_row['Tax'])) + 
            Decimal(str(csv_row['Tip'])) + 
            Decimal(str(csv_row['Gratuity']))
        )
        
        # Compare values
        if abs(Decimal(str(csv_row['Amount'])) - Decimal(str(db_row['order_net_sales']))) > 0.01:
            discrepancies.append({
                'order_number': order_number,
                'issue': 'Net sales mismatch',
                'csv_value': csv_row['Amount'],
                'db_value': db_row['order_net_sales']
            })
            
        if abs(Decimal(str(csv_row['Gratuity'])) - Decimal(str(db_row['service_charges']))) > 0.01:
            discrepancies.append({
                'order_number': order_number,
                'issue': 'Service charges mismatch',
                'csv_value': csv_row['Gratuity'],
                'db_value': db_row['service_charges']
            })
            
        if abs(Decimal(str(csv_row['Tip'])) - Decimal(str(db_row['tip']))) > 0.01:
            discrepancies.append({
                'order_number': order_number,
                'issue': 'Tip mismatch',
                'csv_value': csv_row['Tip'],
                'db_value': db_row['tip']
            })
            
        if abs(csv_total - Decimal(str(db_row['total_amount']))) > 0.01:
            discrepancies.append({
                'order_number': order_number,
                'issue': 'Total amount mismatch',
                'csv_value': csv_total,
                'db_value': db_row['total_amount'],
                'csv_components': {
                    'Amount': csv_row['Amount'],
                    'Tax': csv_row['Tax'],
                    'Tip': csv_row['Tip'],
                    'Gratuity': csv_row['Gratuity']
                }
            })
    
    # Print discrepancies
    if discrepancies:
        print("\nFound discrepancies:")
        for d in discrepancies:
            print(f"\nOrder #: {d['order_number']}")
            print(f"Issue: {d['issue']}")
            if 'csv_value' in d:
                print(f"CSV Value: {d['csv_value']}")
                print(f"DB Value: {d['db_value']}")
                if 'csv_components' in d:
                    print("CSV Components:")
                    for component, value in d['csv_components'].items():
                        print(f"  {component}: {value}")
            else:
                print(f"CSV Data: {d['csv_data']}")
    else:
        print("\nNo discrepancies found!")

if __name__ == "__main__":
    csv_file_path = "./OrderDetails.csv"
    compare_orders(csv_file_path) 


# Use this command to run the script: python compare_toast_data.py