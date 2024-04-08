from airflow import DAG
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import uuid
from datetime import date, datetime
import numpy as np
from google.cloud import bigquery
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime


MAX_WORKERS = 2 

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
)

DATASET_NAME = 'bexio_de'
TABLE_NAME = 'orders'
insert_rows_query = ""
SECRET_NAME = 'bexio_de_token'
GCP_CONN_ID = 'gcp_connection'
PROJECT_ID = 'bi-data-replication-gcs'
TABLE_PAYMENTS = 'order_intervals'


# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Bexio_de_order_intervals',
    default_args=default_args,
    description='Bexio de intervals',
    schedule_interval='40 20 * * *',
    tags=['Bexio'],  
    catchup=False,
)


#------------------ ALl the definations ----------------------------#
def get_secret_data(ti):  
    hook = SecretsManagerHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_conn()
    secret_payload = client.get_secret(secret_id=SECRET_NAME, project_id=PROJECT_ID)
    
    ti.xcom_push(key="secret_value", value=secret_payload)  

# Task 1: Fetch data from the API
def fetch_data(ti):
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 
    print(access_token)
    headers = {
        'Authorization': f'Bearer {access_token}',
        'accept': 'application/json',  # Adjust the content type as needed
    }

    url = 'https://api.bexio.com/2.0/kb_order?offset=0&limit=1000'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception('API request failed')
    



def store_data_in_dataframe(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_api_data')
    if data:
        df = pd.DataFrame(data)
        ti.xcom_push(key='invoice_data', value=df)

def extract_percentage(taxs):
    taxs_list = taxs.tolist()
    return [tax['percentage'] for tax in taxs_list]

def convert_unix_microseconds_to_datetime(timestamp_microseconds):
    timestamp_seconds = int(timestamp_microseconds) / 1000000
    date_wtmscds = datetime.utcfromtimestamp(timestamp_seconds)
    date_without_microseconds = date_wtmscds.replace(microsecond=0)
    return date_without_microseconds.strftime('%Y-%m-%d %H:%M:%S')


def current_datetime_to_custom_format():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

def convert_value(value):
    if value is None or pd.isna(value):
        return "NULL"
    elif isinstance(value, str):
        replacements = {
            '\n': '',
            '\r': '',
            "'": r"\u0027"
        }
        
        for old, new in replacements.items():
            value = value.replace(old, new)
        
        return f"'{value}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        # for future incase there's anyother situation
        return str(value)

def extract_percentage(row):
    if isinstance(row['taxs'], dict) and 'percentage' in row['taxs']:
        return row['taxs']['percentage']
    else:
        return None



# ------------ line items -----------------------#
# Define a function to fetch order items for a given order_id from Bexio API
# Define a function to fetch order items for a given order_id from Bexio API
def fetch_payments(id, headers):
    print(id)
    url = f"https://api.bexio.com/2.0/kb_order/{id}/repetition"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:  # Check for non-successful status code
        print(f"Error for id {id}: {response.text}")
        return id, None

    try:
        return id, response.json()
    except ValueError:  # Handle JSON decoding error
        print(f"Failed to decode JSON for id {id}")
        return id, None

def fetch_and_store_payments_task(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 
    print(access_token)
    headers = {
        'Authorization': f'Bearer {access_token}',
        'accept': 'application/json',
    }
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='invoice_data')
    
    # Fetch payments concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_payments, df['id'], [headers] * len(df['id'])))
    
    rows = []

    for id, data in results:
        if data:
            row = {
                'order_id': id,
                'start': data.get('start', None),
                'end': data.get('end', None),
                'type': data['repetition'].get('type', None) if 'repetition' in data else None,
                'interval': data['repetition'].get('interval', None) if 'repetition' in data else None,
                'schedule': data['repetition'].get('schedule', None) if 'repetition' in data else None
            }
            rows.append(row)

    # Create a DataFrame from the processed data
    order_items_df = pd.DataFrame(rows)

    print(order_items_df)
    ti.xcom_push(key='interval_data', value=order_items_df)


def transform_payments_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    order_df = ti.xcom_pull(task_ids='fetch_and_store_payments_task', key='interval_data') 

    if not order_df.empty:  # Check if the DataFrame isn't empty
        order_items = pd.DataFrame(order_df)
        

    results = ti.xcom_pull(task_ids='get_line_items_big_query')
    results_df = pd.DataFrame(results)

    if results_df is not None and not results_df.empty:
        #results_df = results_df.copy()
        results_df[1] = results_df[1].astype(int)
    else:
        results_df = pd.DataFrame()


    ids_to_delete = ','.join(map(str, order_df['order_id'].tolist()))
    sql_query = f"DELETE FROM `{DATASET_NAME}.{TABLE_PAYMENTS}` WHERE order_id IN ({ids_to_delete});"
    ti.xcom_push(key='delete_payment_record', value=sql_query)


    for index, row in order_items.iterrows():
        #matching_rows = results_df.loc[results_df[2] == row['id']]
        matching_rows = results_df.loc[results_df[1] == row['order_id']] if 1 in results_df.columns else pd.DataFrame()
        if not matching_rows.empty:
            order_df.at[index, 'created_at'] = convert_unix_microseconds_to_datetime(matching_rows[0].values[0])
        else:
            order_df.at[index, 'created_at'] = current_datetime_to_custom_format()
        
    
    order_df['updated_at'] = datetime.now()
    order_df['updated_at'] = order_df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    #order_items.to_excel("output.xlsx", index=False)
    # columns_to_convert_float = ['value']
    # order_items[columns_to_convert_float] = order_items[columns_to_convert_float].apply(pd.to_numeric, errors='coerce')
    # order_items['kb_credit_voucher_id'].fillna(0, inplace=True)
    # order_items['kb_credit_voucher_id'] = order_items['kb_credit_voucher_id'].astype(int)
    print(order_df.columns)
    print(order_items.columns)
    desired_order = [
        'created_at', 'updated_at', 'order_id', 'start', 'end', 'type', 'interval', 'schedule']  
    
    insert_rows = order_df[desired_order].apply(lambda row: tuple(convert_value(val) for val in row), axis=1).tolist()
    values_str = ', '.join(['(' + ', '.join(map(str, tup)) + ')' for tup in insert_rows])
    insert_rows_query = f"INSERT INTO {DATASET_NAME}.{TABLE_PAYMENTS} VALUES {values_str};"
    return insert_rows_query


#--------------------- ALl the tasks ------------------------------#
get_secret_key = PythonOperator(
        task_id='retrieve_secret',
        python_callable=get_secret_data,
        provide_context=True  # This provides the task instance (ti) as an argument
    )

fetch_task = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_data,
    dag=dag,
)

store_dataframe_task = PythonOperator(
    task_id='store_data_in_dataframe',
    python_callable=store_data_in_dataframe,
    dag=dag,
)

# Define your task using PythonOperator
fetch_and_store_payments = PythonOperator(
    task_id='fetch_and_store_payments_task',
    python_callable=fetch_and_store_payments_task,
    dag=dag,
)

#Task4 Get existing data from BQ 
get_line_items_BQ_task = BigQueryGetDataOperator(
    task_id='get_line_items_big_query',
    dataset_id=DATASET_NAME,
    table_id=TABLE_PAYMENTS,
    selected_fields=["created_at", "order_id"],  
    gcp_conn_id='gcp_connection',  
)

transform_payments = PythonOperator(
    task_id='transform_payments',
    python_callable=transform_payments_and_insert_to_bq,
    provide_context=True,
    dag=dag,
)


delete_query_order_items_job = BigQueryInsertJobOperator(
    task_id="delete_query_order_items_job",
    configuration={
        "query": {
            "query": "{{ti.xcom_pull(task_ids='transform_payments', key='delete_payment_record')}}",  # You will dynamically populate this query below
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id=GCP_CONN_ID,
)


# Insert the data to BQ Order items
insert_query_job_order_items = BigQueryInsertJobOperator(
    task_id="insert_query_job_order_items",
    configuration={
        "query": {
            "query": "{{ti.xcom_pull('transform_payments')}}",  
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id=GCP_CONN_ID,
)

# Define task dependencies
(
    get_secret_key 
    >> fetch_task 
    >> store_dataframe_task 
    >> fetch_and_store_payments
    >> get_line_items_BQ_task
    >> transform_payments
    >> delete_query_order_items_job
    >> insert_query_job_order_items
)