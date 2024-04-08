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
import requests


GCS_BUCKET_NAME = 'bi_etl_storage'
GCS_OBJECT_NAME = 'charges.csv'
BIGQUERY_DATASET_NAME = 'bi_etl'
BIGQUERY_TABLE_NAME = 'stripe_charges'
PROJECT_ID = 'bi-data-replicaton-gcs'
GCP_CONN_ID = 'gcp_connection'
TABLE_NAME = 'stripe_charges'

sql = """
    delete from bi_etl.stripe_charges where id IN
        (select id
          from bi_etl.stripe_charges_temp);

        insert into bi_etl.stripe_charges
            select id, payment_intent_id, payment_method_id, type,
          card_brand, card_brand_product, card_fingerprint, card_network from bi_etl.stripe_charges_temp;

        drop table bi_etl.stripe_charges_temp;
"""



def get_all_charges():
    api_key = 'secret_key'
    batch_size = 100
    base_url = f'https://api.stripe.com/v1/charges?limit={batch_size}'
    headers = {'Authorization': f'Bearer {api_key}'}

    charges = []
    response = requests.get(base_url, headers=headers)

    while response.status_code == 200:
        charge_batch = response.json()
        charges.extend(charge_batch['data'])
        charge_batch['has_more'] = False
        if not charge_batch['has_more']:
            break

        next_id = charge_batch['data'][-1]['id']
        url = f'{base_url}&starting_after={next_id}'

        response = requests.get(url, headers=headers)

    # Convert the list of charges to a DataFrame
    df = pd.json_normalize(charges, max_level=3)
    
    rename_dict = {
        "payment_intent": "payment_intent_id",
        "payment_method": "payment_method_id",
        "payment_method_details.type": "type",
        "payment_method_details.card_present.brand": "card_brand",
        "payment_method_details.card_present.brand_product": "card_brand_product",
        "payment_method_details.card_present.fingerprint": "card_fingerprint",
        "payment_method_details.card_present.network": "card_network"
    }

    # # # Rename the columns
    df = df.rename(columns=rename_dict)

    # print(df.columns)
    # new_cols = [
    #     'id', 
    #    #  'payment_intent_id',
    #      #'payment_method_id', 
    #     # 'card_brand',
    #     # 'card_brand_product',
    #     # 'card_fingerprint',
    #     # 'card_network'
    #     ]
    # df_selectd = df[new_cols]
    # print(df_selectd)
    
    # # Assign new column names to the DataFrame
    # #df.columns = columns
    # print(df.columns)

    # Save the DataFrame to a CSV file
    df.to_csv('charges.csv', index=False)

# Initialize the DAG
with DAG(
    'stripe_charges',
   description='Stripe charges gsheet',
    schedule_interval='30 9 * * *', # This sets the DAG to run once a day
    start_date=datetime(2024, 1, 1),  # Specify the start date: Year, Month, Day
    catchup=False,  # This determines whether to catch up if the DAG was not run for past intervals
    tags=['stripe'],  # Add relevant tags here
) as dag:

    # Define the Python function as an Airflow task
    fetch_data_task = PythonOperator(
        task_id='fetch_gsheet_data',
        python_callable=get_all_charges,
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
            CREATE TABLE IF NOT EXISTS bi_etl.stripe_charges_temp
            LIKE bi_etl.stripe_charges
        """,
        use_legacy_sql=False,
        gcp_conn_id=GCP_CONN_ID,
        dag=dag
    )

    load_data = GCSToBigQueryOperator(
            task_id='gcs_to_bq',
            bucket=GCS_BUCKET_NAME,
            source_objects=[GCS_OBJECT_NAME],  # example: ['path/to/your/file.csv']
            destination_project_dataset_table='bi_etl.stripe_charges_temp',
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
        fetch_data_task 
        >> create_temp_table_task  
        >> upload_to_gcs_task 
        >> load_data
        >> transfer_and_cleanup_task
    )
    #>> load_data