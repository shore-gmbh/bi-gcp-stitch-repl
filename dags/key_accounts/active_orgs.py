import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.oauth2.service_account import Credentials
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import gspread
from datetime import timedelta, datetime

# Your service account JSON file path
SERVICE_ACCOUNT_FILE = Variable.get("gsheet_key", deserialize_json=True)
# The ID of your Google sheet
SHEET_ID = '1D187sZk4jhuFfMSfuC_W52hrFr2mpvsAwTUac9yeZ3U'
SHEET_RANGE = 'Active Orgs'  # or whatever range you wish to fetch

GCS_BUCKET_NAME = 'key_accounts'
GCS_OBJECT_NAME = 'active_orgs.csv'
BIGQUERY_DATASET_NAME = 'suppl'
BIGQUERY_TABLE_NAME = 'active_orgs'
PROJECT_ID = 'bi-data-replicaton-gcs'
GCP_CONN_ID = 'gcp_connection'

def get_gsheet_data():
    # Authenticate with the Google Sheets API using the service account info
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets'])
    gc = gspread.authorize(creds)

    # Open the sheet and read data to DataFrame
    worksheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_RANGE)
    data = worksheet.get_all_values()
    
    # Convert to DataFrame
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)

    # Do something with the DataFrame (e.g., print, store, transform)
    df.to_csv(GCS_OBJECT_NAME, index=False)
    #return df

default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Initialize the DAG
with DAG(
    'key_accounts_active_orgs',
    default_args=default_args,
    description='Key accounts active org',
    schedule_interval='0 7 * * *',
    tags=['Key accounts'],  
    catchup=False,
) as dag:

    # Define the Python function as an Airflow task
    fetch_gsheet_task = PythonOperator(
        task_id='fetch_gsheet_data',
        python_callable=get_gsheet_data,
        dag=dag,
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
            destination_project_dataset_table='suppl.active_orgs',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID,
        )
    # Set the task in the DAG
    fetch_gsheet_task >> save_to_gcs >> load_data