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
GCS_OBJECT_NAME = 'contracts.csv'
BIGQUERY_DATASET_NAME = 'billwerk'
BIGQUERY_TABLE_NAME = 'contracts_2'
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
        url = f'https://app.billwerk.com/api/v1/contracts?access_token={access_token}&take=500'

            # Make the API request
        #response = requests.get(url)
        
        # Check if the request was successful
        # if response.status_code == 200:
        #     data = response.json()
        #     return data
        # else:
        #     # If the response was not successful, raise an exception with the status code
        #     response.raise_for_status()
        
        response = requests.get(url)
        
        data, next_id = [], None
        while response.status_code == 200:
            data_batch = json.loads(response.text)
            
            if data_batch[-1]['Id'] == next_id:
                break
            
            data.extend(data_batch)
            next_id = data_batch[-1]['Id']
            
            response = requests.get(f'{url}&from={next_id}')
        
        return data
    
    # Get access token
    token = get_access_token(USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET)
    
    # Fetch all subscriptions
    all_data = get_subscriptions(token)

    # if isinstance(all_data, list):
        # Normalize the nested JSON data into a flat table
    print(all_data)
    flat_data = []
    for entry in all_data:
    # Ensure that 'PaymentBearer' and 'CurrentPhase' keys exist with default values
        if 'PaymentBearer' not in entry:
            entry['PaymentBearer'] = {}
        if 'CurrentPhase' not in entry:
            entry['CurrentPhase'] = {}

        # Flatten the main structure
        flat_entry = {
            'id': entry.get('Id', None),
            'created_at': pd.to_datetime('now').isoformat(),
            'updated_at': pd.to_datetime('now').isoformat(),
            'last_billing_date': entry.get('LastBillingDate', None),
            'next_billing_date': entry.get('NextBillingDate', None),
            'plan_id': entry.get('PlanId', None),
            'customer_id': entry.get('CustomerId', None),
            'is_deletable': entry.get('IsDeletable', None),
            'lifecycle_status': entry.get('LifecycleStatus', None),
            'customer_name': entry.get('CustomerName', None),
            'customer_is_locked': entry.get('CustomerIsLocked', None),
            'balance': entry.get('Balance', None),
            'reference_code': entry.get('ReferenceCode', None),
            'currency': entry.get('Currency', None),
            'plan_group_id': entry.get('PlanGroupId', None),
            'payment_bearer_email_address': entry.get('PaymentBearer', {}).get('EmailAddress', ''),
            'payment_bearer_type': entry.get('PaymentBearer', {}).get('Type', ''),
            'payment_provider': entry.get('PaymentProvider', None),
            'payment_provider_role': entry.get('PaymentProviderRole', None),
            'escalation_suspended': entry.get('EscalationSuspended', None),
            'recurring_payments_paused': entry.get('RecurringPaymentsPaused', None),
            'current_phase_type': entry.get('CurrentPhase', {}).get('Type', ''),
            'current_phase_start_date': entry.get('CurrentPhase', {}).get('StartDate', ''),
            'current_phase_plan_variant_id': entry.get('CurrentPhase', {}).get('PlanVariantId', ''),
            'current_phase_plan_id': entry.get('CurrentPhase', {}).get('PlanId', ''),
            'current_phase_quantity': entry.get('CurrentPhase', {}).get('Quantity', 0),
            'current_phase_inherit_start_date': entry.get('CurrentPhase', {}).get('InheritStartDate', False),
            'payment_provider_support_refunds': entry.get('PaymentProviderSupportRefunds', None),
            'billing_suspended': entry.get('BillingSuspended', None),
            'threshold_billing_disabled': entry.get('ThresholdBillingDisabled', None),
            'start_date': entry.get('StartDate', None),
            'billed_until': entry.get('BilledUntil', None),
            'plan_variant_id': entry.get('PlanVariantId', None),
            'custom_fields': json.dumps(entry.get('CustomFields', {})),  # assuming 'CustomFields' is a dictionary that needs to be converted to string
            'end_date': entry.get('EndDate', None)
        }   
        flat_data.append(flat_entry)
    # else:
    #     # If the response is not a list, we assume it is a single record
    #     df = pd.DataFrame([all_data])
    
    # Load data into a DataFrame
    #df = pd.DataFrame(all_data)
    #current_time = datetime.now().isoformat()
    #df.insert(loc=1, column='created_at', value=current_time)
    #df.insert(loc=2, column='updated_at', value=current_time)

    df = pd.DataFrame(flat_data)

    # Save DataFrame to CSV temporarily
    df.to_csv(GCS_OBJECT_NAME, index=False, sep='|')


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
    'billwerk_contracts',
    default_args=default_args,
    description='Billwerk Contracts',
    schedule_interval='0 22 * * *',
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
        destination_project_dataset_table='billwerk.contracts_2',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    fetch_data >> save_to_gcs >> load_data