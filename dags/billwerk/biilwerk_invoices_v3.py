import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
from airflow.utils.dates import days_ago
import json
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from pandas import json_normalize
import csv
from datetime import timedelta, datetime
from airflow.models import Variable

# Constants
USERNAME = Variable.get("billwerk_username")
PASSWORD = Variable.get("billwerk_password")
CLIENT_ID = Variable.get("billwerk_client_id")
CLIENT_SECRET = Variable.get("billwerk_client_secret")
GCS_BUCKET_NAME = 'billwerk_1'
GCS_OBJECT_NAME = 'invoices.csv'
BIGQUERY_DATASET_NAME = 'billwerk'
BIGQUERY_TABLE_NAME = 'invoices_2'
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


# def fetch_data_and_save_to_gcs(**kwargs):
#     gcp_conn_id = kwargs.get('gcp_conn_id', 'google_cloud_default')
    
#     # Get access token
#     token = get_access_token(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET)
#     print('token received')
#     print(token)
    
#     # Fetch data from the API
#     API_URL = f'https://app.billwerk.com/api/v1/subscriptions?access_token={token}'
#     response = requests.get(API_URL)
#     response.raise_for_status()
#     data = response.json()

#     # Load data into a DataFrame
#     df = pd.DataFrame(data)
#     # Adding created_at and updated_at columns after the Id column
#     current_time = datetime.now().isoformat()
#     df.insert(loc=1, column='created_at', value=current_time)
#     df.insert(loc=2, column='updated_at', value=current_time)
#     # Save DataFrame to CSV
#     df.to_csv(GCS_OBJECT_NAME, index=False)
    
#     # Using GCSHook to upload data to GCS
#     gcs_hook = GCSHook(gcp_conn_id)
#     gcs_hook.upload(bucket_name=GCS_BUCKET_NAME,
#                     object_name=GCS_OBJECT_NAME,
#                     filename=GCS_OBJECT_NAME)

df = None

def fetch_data_to_df(**kwargs):
    global df
    
    def get_subscriptions(access_token):
        url = f'https://app.billwerk.com/api/v1/invoices?access_token={access_token}&take=500'
        response = requests.get(url)
        data, next_id = [], None
        while response.status_code == 200:
            data_batch = json.loads(response.text)
            if data_batch[-1]['Id'] == next_id:
                break
            data.extend(data_batch)
            next_id = data_batch[-1]['Id']
            response = requests.get(f'{url}&from={next_id}&take=500')
        return data
    
    # Get access token
    token = get_access_token(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET)
    
    # Fetch all subscriptions
    all_data = get_subscriptions(token)

    # Flatten JSON data
    flat_data = []
    for entry in all_data:
        if entry['Id'] == '59241f1d14a9ff09d89612d3':
            print('asjgdkjagsdagsdkgasdgsadgaskudf2323444')
            print(entry['Id'])
            print(entry.get('ExternalCustomerId'))
            break

        flat_entry = {
            'id': entry['Id'],
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),  # Assuming that the update time is now
            'invoice_number': entry.get('InvoiceNumber'),
            'customer_id': entry.get('CustomerId'),
            'contract_id': entry.get('ContractId'),
            'sent_at': entry.get('SentAt'),
            'due_date': entry.get('DueDate'),
            'recipient_name': entry.get('RecipientName'),
            'recipient_subname': entry.get('RecipientSubName'),
            'recipient_address': entry.get('RecipientAddress', {}).get('Street'),
            'recipient_zipcode': entry.get('RecipientAddress', {}).get('PostalCode'),
            'recipient_city': entry.get('RecipientAddress', {}).get('City'),
            'recipient_country': entry.get('RecipientAddress', {}).get('Country'),
            'currency': entry.get('Currency'),
            'total_net': entry.get('TotalNet'),
            'total_vat': entry.get('TotalVat'),
            'total_gross': entry.get('TotalGross'),
            'is_invoice': entry.get('IsInvoice'),
            'external_customer_id' : entry.get('ExternalCustomerId', ''),
            'document_date': entry.get('DocumentDate')
        }
        flat_data.append(flat_entry)

    # Convert the flattened data to a DataFrame
    df = pd.DataFrame(flat_data)

    # Save DataFrame to CSV temporarily
    df.to_csv(GCS_OBJECT_NAME, index=False, sep='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)


def load_data_into_bigquery(**kwargs):
    gcp_conn_id = kwargs.get('gcp_conn_id', 'google_cloud_default')
    
    # Initialize a BigQueryHook with the provided connection ID
    bq_hook = BigQueryHook(bigquery_conn_id=gcp_conn_id)
    
    # Get the BigQuery client from the hook
    bq_client = bq_hook.get_client()

    # Create a table reference
    dataset_ref = bq_client.dataset(BIGQUERY_DATASET_NAME)
    table_ref = dataset_ref.table(BIGQUERY_TABLE_NAME)
    
    # Load data from GCS to BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )
    uri = f"gs://{GCS_BUCKET_NAME}/{GCS_OBJECT_NAME}"
    load_job = bq_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )  
    load_job.result()  # Wait for the job to complete



default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'billwerk_invoices_v3',
    default_args=default_args,
    description='Billwerk Invoices',
    schedule_interval='45 23 * * *',
    tags=['Billwerk'],  
    catchup=False,
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data_to_df',
        python_callable=fetch_data_to_df,
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
        destination_project_dataset_table='billwerk.invoices_2',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    fetch_data >> save_to_gcs >> load_data