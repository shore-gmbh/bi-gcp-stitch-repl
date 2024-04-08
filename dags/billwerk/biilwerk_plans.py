import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage, bigquery
from airflow.utils.dates import days_ago
import json
from datetime import datetime
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models import Variable
from datetime import timedelta, datetime

# Constants
USERNAME = Variable.get("billwerk_username")
PASSWORD = Variable.get("billwerk_password")
CLIENT_ID = Variable.get("billwerk_client_id")
CLIENT_SECRET = Variable.get("billwerk_client_secret")
GCS_BUCKET_NAME = 'billwerk_1'
GCS_OBJECT_NAME = 'plans.csv'
BIGQUERY_DATASET_NAME = 'billwerk'
BIGQUERY_TABLE_NAME = 'plans'
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

def fetch_data_to_df(**kwargs):
    global df
    
    def get_plans(access_token):
        url = f'https://app.billwerk.com/api/v1/plans?access_token={access_token}'
        
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
    all_data = get_plans(token)
    
    # Preprocess and flatten the data
    processed_data = []
    for record in all_data:
        flattened_record = {
            'Id': record['Id'],
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'PlanGroupId': record.get('PlanGroupId'),
            'name': record['Name'].get('_c') if record.get('Name') else None,
            'PlanDescription': record['PlanDescription'].get('_c') if record.get('PlanDescription') else None,
            'SetupDescription': record['SetupDescription'].get('_c') if record.get('SetupDescription') else None,
            'TrialEndPolicy': record.get('TrialEndPolicy'),
            'TaxPolicyId': record.get('TaxPolicyId'),
            'IsQuantityBased': record.get('IsQuantityBased'),
            'Hidden': record.get('Hidden'),
            'IsDeletable': record.get('IsDeletable'),
            'RequiresOrderApproval': record.get('RequiresOrderApproval')
        }
        processed_data.append(flattened_record)
    
    # Load data into a DataFrame
    df = pd.DataFrame(processed_data)
    
    # Save DataFrame to CSV temporarily
    df.to_csv(GCS_OBJECT_NAME, index=False)


default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'billwerk_plans',
    default_args=default_args,
    description='Billwerk plans',
    schedule_interval='15 23 * * *',
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
        destination_project_dataset_table='billwerk.plans',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    fetch_data >> save_to_gcs >> load_data