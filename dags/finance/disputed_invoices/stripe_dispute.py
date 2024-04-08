import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.oauth2.service_account import Credentials
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import gspread
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery
from datetime import date, datetime
from google.auth import default
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import re
from datetime import timedelta, datetime

# Your service account JSON file path
SERVICE_ACCOUNT_FILE = Variable.get("gsheet_key", deserialize_json=True)
# The ID of your Google sheet
SHEET_ID = '1y6lSLwCvoFrC5wot6CWmJhTZbBPCJibNJ7F1g2AeqpM'
SHEET_RANGE = 'shore :: disputes stripe'  # or whatever range you wish to fetch

GCS_BUCKET_NAME = 'finance_scripts'
GCS_OBJECT_NAME = 'shore_disputes.csv'
BIGQUERY_DATASET_NAME = 'bi_finance'
BIGQUERY_TABLE_NAME = 'shore_disputes'
PROJECT_ID = 'bi-data-replicaton-gcs'
GCP_CONN_ID = 'gcp_connection'
TABLE_NAME = 'stripe_dispute_status_history'

sql = """
    INSERT INTO bi_finance.stripe_dispute_status_history
    SELECT temp.*
    FROM bi_finance.stripe_dispute_status_history_temp temp
    WHERE NOT EXISTS (
        SELECT 1
        FROM bi_finance.stripe_dispute_status_history main
        WHERE temp.stripe_id = main.stripe_id
        AND temp.stripe_account_slug = main.stripe_account_slug
        AND temp.date = main.date
        AND temp.status = main.status
    );

    DROP TABLE bi_finance.stripe_dispute_status_history_temp;
"""



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
    df['date'] = ''
    df['status'] = ''

    # Append the timestamp
    

    # Iterating over the DataFrame rows
    for index, row in df.iterrows():
        payment_date = row['payment date'].strip()
        if payment_date != '':
            date = row['payment date']  
            df.at[index, 'status'] = 'paid'
        elif row['cancelation date'].strip() != '':
            date = row['cancelation date']  
            df.at[index, 'status'] = 'canceled_dispute'

        formatted_date = re.sub(r'([0-9]{2}).([0-9]{2}).([0-9]{4})', r'\3-\2-\1', date)
        datetime_str = f"{formatted_date} 23:59:59"
        df.at[index, 'date'] = datetime_str

    current_time = datetime.now().isoformat()

    df['updated_at'] = current_time
    df['stripe_account_slug'] = df['entity']

    selected_columns = ['date', 'invoice id', 'stripe id', 'status', 'stripe_account_slug', 'updated_at']
    df_selected = df[selected_columns]

    
    df_selected.to_csv(GCS_OBJECT_NAME, index=False)
    #return df

default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Initialize the DAG
with DAG(
    'finance_shore_disputes',
   default_args=default_args,
    description='Excel shore disputes to dwh',
    schedule_interval='20 8 * * *',
    tags=['stripe'],  
    catchup=False,
) as dag:

    # Define the Python function as an Airflow task
    fetch_gsheet_task = PythonOperator(
        task_id='fetch_gsheet_data',
        python_callable=get_gsheet_data,
        dag=dag,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=GCS_OBJECT_NAME,  
        dst=GCS_OBJECT_NAME,  
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    create_temp_table_task = BigQueryExecuteQueryOperator(
        task_id='create_temp_table',
        sql="""
            CREATE TABLE IF NOT EXISTS bi_finance.stripe_dispute_status_history_temp
            LIKE bi_finance.stripe_dispute_status_history
        """,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        dag=dag
    )

    load_data = GCSToBigQueryOperator(
            task_id='gcs_to_bq',
            bucket=GCS_BUCKET_NAME,
            source_objects=[GCS_OBJECT_NAME],  # example: ['path/to/your/file.csv']
            destination_project_dataset_table='bi_finance.stripe_dispute_status_history_temp',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
            autodetect=True,
            gcp_conn_id=GCP_CONN_ID,
        )
    
    transfer_and_cleanup_task = BigQueryExecuteQueryOperator(
        task_id='transfer_data_and_cleanup',
        sql=sql,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        dag=dag
    )

    
    
        
    (
        fetch_gsheet_task 
        >> create_temp_table_task  
        >> upload_to_gcs_task 
        >> load_data
        >> transfer_and_cleanup_task
    )
    #>> load_data