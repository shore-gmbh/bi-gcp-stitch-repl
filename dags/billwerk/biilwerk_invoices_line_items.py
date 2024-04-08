import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from airflow.models import Variable
from datetime import timedelta, datetime

# Constants
USERNAME = Variable.get("billwerk_username")
PASSWORD = Variable.get("billwerk_password")
CLIENT_ID = Variable.get("billwerk_client_id")
CLIENT_SECRET = Variable.get("billwerk_client_secret")
GCS_BUCKET_NAME = 'billwerk_1'
GCS_OBJECT_NAME = 'invoice_line_items.csv'
BIGQUERY_DATASET_NAME = 'billwerk'
BIGQUERY_TABLE_NAME = 'invoice_items'
PROJECT_ID = 'bi-data-replicaton-gcs'
GCP_CONN_ID = 'gcp_connection'



def get_access_token(username, password, client_id, client_secret):
    url = 'https://app.billwerk.com/oauth/token/'

    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }

    response = requests.post(url, data=data, auth=(client_id, client_secret))
    response = json.loads(response.text)

    return response['access_token']




df = None

def get_invoices(access_token):
    url = f'https://app.billwerk.com/api/v1/invoices?detailLevel=2&access_token={access_token}&take=500'
    response = requests.get(url)
    
    data, next_id = [], None
    while response.status_code == 200:
        data_batch = json.loads(response.text)
        if data_batch[-1]['Id'] == next_id:
            break
        data.extend(data_batch)
        next_id = data_batch[-1]['Id']
        response = requests.get(f'{url}&from={next_id}')
        
    return [invoice['Id'] for invoice in data]

def clean_text(text):
    if text:
        # Replacing newline characters with a space
        cleaned_text = text.replace('\n', ' ').replace('\r', ' ')
        
        # Escaping internal quotes
        cleaned_text = cleaned_text.replace('"', '""')
        
        # Enclosing the text within quotes
        cleaned_text = f'"{cleaned_text}"'
        
        return cleaned_text
    return '""'  # Return empty quoted string if the text is None

# def get_invoices(access_token):
#     url = f'https://app.billwerk.com/api/v1/invoices?detailLevel=2&access_token={access_token}&take=2'
#     response = requests.get(url)
    
#     if response.status_code == 200:
#         data = json.loads(response.text)
#         return [invoice['Id'] for invoice in data]  # returning only the invoice ids
#     else:
#         print(f"Failed to fetch invoices. Status code: {response.status_code}")
#         return []

def fetch_line_items_data_to_df(**kwargs):
    global df
    
    def get_invoice_details(access_token, invoice_id):
        url = f'https://app.billwerk.com/api/v1/invoices/{invoice_id}?detailLevel=2&access_token={access_token}'
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data for invoice_id {invoice_id}. Status code: {response.status_code}")
            return []
        return json.loads(response.text)
    
    # Get access token
    token = get_access_token(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET)
    
    # Fetch all invoice_ids
    invoice_ids = get_invoices(token)
    
    # Fetch all invoice details for each invoice_id concurrently
    all_data = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_invoice_details, token, invoice_id): invoice_id for invoice_id in invoice_ids}
        for future in as_completed(futures):
            invoice_data = future.result()
            item_list = invoice_data.get('ItemList', [])
            print(item_list)
            for item in item_list:
                record = {
                    'invoice_id': invoice_data.get('Id'),
                    'Quantity': item.get('Quantity'),
                    'PricePerUnit': item.get('PricePerUnit'),
                    'VatPercentage': item.get('VatPercentage'),
                    'PeriodStart': item.get('PeriodStart'),
                    'PeriodEnd': item.get('PeriodEnd'),
                    'PeriodMultiplier': item.get('PeriodMultiplier'),
                    'ScaleAmount': item.get('ScaleAmount'),
                    'ProductId': item.get('ProductId'),
                    'TotalNet': item.get('TotalNet'),
                    'TotalVat': item.get('TotalVat'),
                    'TotalGross': item.get('TotalGross'),
                    'TaxMarker': item.get('TaxMarker'),
                    'TaxPolicyId': item.get('TaxPolicyId'),
                    'ComponentSubscriptionId': item.get('ComponentSubscriptionId'),
                    'ProductDescription':  clean_text(item.get('ProductDescription', '')),
                    'Description': clean_text(item.get('Description', ''))
                }
                print(item.get('ProductDescription', ''))
                print(clean_text(item.get('ProductDescription', '')))
                all_data.append(record)
                
    # Load data into a DataFrame
    df = pd.DataFrame(all_data)
    
    # Save DataFrame to CSV temporarily
    df.to_csv(GCS_OBJECT_NAME, index=False)

default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'billwerk_invoice_line_items',
    default_args=default_args,
    description='Billwerk invoice line items',
    schedule_interval='15 0 * * *',
    tags=['Billwerk'],  
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data_to_df',
        python_callable=fetch_line_items_data_to_df,
    )

    save_to_gcs = LocalFilesystemToGCSOperator(
        task_id='save_df_to_gcs',
        src=GCS_OBJECT_NAME,
        dst=GCS_OBJECT_NAME,
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_data = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=GCS_BUCKET_NAME,
        source_objects=[GCS_OBJECT_NAME],  # example: ['path/to/your/file.csv']
        destination_project_dataset_table='billwerk.invoice_items',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
       
        gcp_conn_id=GCP_CONN_ID,
    )

    fetch_data >> save_to_gcs >> load_data