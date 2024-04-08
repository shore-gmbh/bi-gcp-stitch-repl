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
SHEET_ID = '1TJ_AsiyAUwosrSe5XN_fyJBzqrrAWVS0D6mgilubZSc'
SHEET_RANGE = 'Billwerk Cease Date'  # or whatever range you wish to fetch

GCS_BUCKET_NAME = 'billwerk_1'
GCS_OBJECT_NAME = 'billwerk_cease_date.csv'
BIGQUERY_DATASET_NAME = 'billwerk'
BIGQUERY_TABLE_NAME = 'billwerk_cease_date'
PROJECT_ID = 'bi-data-replicaton-gcs'
GCP_CONN_ID = 'gcp_connection'

def dataset_transform(data):
    for i, date_str in enumerate(data['admin_cease_date']):
      if date_str:
        data.loc[i, 'admin_cease_date'] = pd.to_datetime(date_str, format='%Y-%m-%d').strftime('%Y-%m-%d')
    return data

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
    records_pd = pd.DataFrame(df)
    records_pd = records_pd[['No', 'customer_id', 'admin_cease_date', 'Note' ]]
    records_trans = dataset_transform(records_pd)

    # Do something with the DataFrame (e.g., print, store, transform)
    records_trans.to_csv(GCS_OBJECT_NAME, index=False)
    #return df

# Initialize the DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



with DAG(
    'billwerk_cease_date',
    default_args=default_args,
    description='Billwerk Cease date',
    schedule_interval='0 23 * * *',
    tags=['Billwerk'],  
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
            destination_project_dataset_table='billwerk.billwerk_cease_date',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID,
        )
    # Set the task in the DAG
    fetch_gsheet_task >> save_to_gcs >> load_data