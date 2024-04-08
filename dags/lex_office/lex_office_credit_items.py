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
import time
from datetime import timedelta, datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Define a constant for max concurrent requests
MAX_WORKERS = 5  # Adjust as per your requirement


from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
)

DATASET_NAME = 'lexoffice'
TABLE_NAME = 'credits'
insert_rows_query = ""
SECRET_NAME = 'lex_office_token'
GCP_CONN_ID = 'gcp_connection'
PROJECT_ID = 'bi-data-replication-gcs'
TABLE_PAYMENTS = 'credit_items'
BATCH_SIZE = 500  # adjust as needed


# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Lexoffice_credit_items',
    default_args=default_args,
    description='Lex office credit items',
    schedule_interval='15 2 * * *',
    tags=['Lexoffice'],  
    catchup=False,
)


#------------------ ALl the definations ----------------------------#
def get_secret_data(ti):  
    hook = SecretsManagerHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_conn()
    secret_payload = client.get_secret(secret_id=SECRET_NAME, project_id=PROJECT_ID)
    
    ti.xcom_push(key="secret_value", value=secret_payload)  


# Task 1: Fetch data from the API
def fetch_data_from_lexoffice(ti):
    # Extract the access token
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value')
    print(access_token)
    # Define the URL, parameters, and headers for the request
    url = "https://api.lexoffice.io/v1/voucherlist"
    params = {
        "voucherType": "creditnote",
        "voucherStatus": "any",
        "size": 250,
        "page": 0
    }
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Initialize an empty dataframe to store all data
    df_all = pd.DataFrame()

    # Loop until all pages have been fetched
    while True:
        response = requests.get(url, params=params, headers=headers)
        
        # Check if the response is successful
        if response.status_code != 200:
            raise Exception(f"API request failed with status code: {response.status_code}")
        
        # Extract the data from the response and normalize it into a dataframe
        data = response.json()
        df = pd.json_normalize(data["content"])
        df_all = pd.concat([df_all, df], ignore_index=True)
        # Break out of the loop if it's the last page
        if data["last"] == True:
            break
            
        # Increment the page parameter for the next request
        params["page"] += 1

    # Return the final dataframe
    return df_all

# Task 2: Store data in a DataFrame
def store_data_in_dataframe(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_api_data')
    if data is not None and not data.empty:
        df = pd.DataFrame(data)
        ti.xcom_push(key='credit_data', value=df)



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
def fetch_payments(args):
    id, headers = args
    print(id)
    url = f"https://api.lexoffice.io/v1/credit-notes/{id}"
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=frozenset(['GET', 'POST']))
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.get(url, headers=headers)  # Adjust timeout as necessary
        response.raise_for_status()
        return id, response.json()
    except requests.exceptions.RequestException as e:  # Catches all request-related exceptions
        print(f"Error fetching data for id: {id}, error: {e}")
        return id, None
        

def fetch_and_store_payments_task(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 
    print(access_token)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='credit_data')
    
    # Fetch payments concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_payments, [(i, headers) for i in df['id']]))

    rows = []
    for  result in results:
        if data:
            id, data = result
            main_fields = {
                'credit_id': id,
                'organisation_id': data.get('organizationId', ''),
                'countrycode': data['address']['countryCode'],
                'currency': data['totalPrice']['currency'],
                'totalNetAmount': data['totalPrice']['totalNetAmount'],
                'totalGrossAmount': data['totalPrice']['totalGrossAmount'],
                'totalTaxAmount': data['totalPrice']['totalTaxAmount']
            }

            for line_item in data['lineItems']:
                # Extract fields from the line item
                line_item_fields = {
                    'productid': line_item.get('id', None),  # It seems 'id' was missing from your example. Adjust as needed.
                    'type': line_item['type'],
                    'name': line_item['name'],
                    'description': line_item.get('description', None), # Assuming 'description' might be optional.
                    'quantity': line_item['quantity'],
                    'unitName': line_item.get('unitName', None), # Assuming 'unitName' might be optional.
                    'netAmount': line_item['unitPrice']['netAmount'],
                    'grossAmount': line_item['unitPrice']['grossAmount'],
                    'taxRatePercentage': line_item['unitPrice']['taxRatePercentage'],
                    'lineItemAmount': line_item['lineItemAmount']
                }
                
                # Combine main fields with line item fields for this row
                combined = {**main_fields, **line_item_fields}
                rows.append(combined)

    # Create a DataFrame from the combined data
    order_items_df = pd.DataFrame(rows)
    
    print(order_items_df)
    ti.xcom_push(key='credit_items_data', value=order_items_df)

def transform_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_and_store_payments_task', key='credit_items_data')
    
    sql_query = f"DELETE FROM `{DATASET_NAME}.{TABLE_PAYMENTS}` WHERE TRUE;"
    ti.xcom_push(key='delete_record', value=sql_query) 

    columns_to_convert_float = ['openAmount']
    df['updated_at'] = datetime.now()
    df['updated_at'] = df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

    print(df.columns)
    columns_to_convert = ['totalNetAmount', 'totalGrossAmount', 'totalTaxAmount', 'quantity', 'netAmount',
       'grossAmount', 'taxRatePercentage', 'lineItemAmount' ]
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
   
    desired_order =  ['credit_id', 'organisation_id', 'countrycode', 'productid',
       'type','name', 'description', 'quantity','unitName', 'netAmount', 
       'grossAmount', 'taxRatePercentage',  'lineItemAmount',  'currency', 
       'totalNetAmount', 'totalGrossAmount', 'totalTaxAmount', 
         'updated_at']
    insert_rows = df[desired_order].apply(lambda row: tuple(convert_value(val) for val in row), axis=1).tolist()
    
    # Splitting the rows into batches
    insert_batches = [insert_rows[i:i + BATCH_SIZE] for i in range(0, len(insert_rows), BATCH_SIZE)]
    
    # Generating a query for each batch and storing it
    for index, batch in enumerate(insert_batches):
        values_str = ', '.join(['(' + ', '.join(map(str, tup)) + ')' for tup in batch])
        insert_rows_query = f"INSERT INTO {DATASET_NAME}.{TABLE_PAYMENTS} VALUES {values_str};"
        ti.xcom_push(key=f'insert_query_batch_{index}', value=insert_rows_query)


#--------------------- ALl the tasks ------------------------------#
get_secret_key = PythonOperator(
        task_id='retrieve_secret',
        python_callable=get_secret_data,
        provide_context=True  # This provides the task instance (ti) as an argument
    )

fetch_task = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_data_from_lexoffice,
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



transform_payments = PythonOperator(
    task_id='transform_payments',
    python_callable=transform_and_insert_to_bq,
    provide_context=True,
    dag=dag,
)


delete_query_order_items_job = BigQueryInsertJobOperator(
    task_id="delete_query_order_items_job",
    configuration={
        "query": {
            "query": "{{ti.xcom_pull(task_ids='transform_payments', key='delete_record')}}",  # You will dynamically populate this query below
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id=GCP_CONN_ID,
)


# Use a dummy maximum batch estimate for now
MAX_BATCHES_ESTIMATE = 2  # Adjust this value based on your expectation

insert_jobs = []

for index in range(MAX_BATCHES_ESTIMATE):
    task = BigQueryInsertJobOperator(
        task_id=f"insert_query_job_batch_{index}",
        configuration={
            "query": {
                "query": f"{{{{ti.xcom_pull(key='insert_query_batch_{index}')}}}}",  # You will dynamically pull the query for each batch
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        gcp_conn_id='gcp_connection',
        dag=dag,
    )
    insert_jobs.append(task)


# Define task dependencies
(
    get_secret_key 
    >> fetch_task
    >> store_dataframe_task
    >> fetch_and_store_payments
    >> transform_payments
    >> delete_query_order_items_job
    >> insert_jobs
)