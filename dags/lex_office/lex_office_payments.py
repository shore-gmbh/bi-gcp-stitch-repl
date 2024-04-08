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
    BigQueryGetDataOperator,
    BigQueryExecuteQueryOperator
)


DATASET_NAME = 'lexoffice'
TABLE_NAME = 'invoices'
insert_rows_query = ""
SECRET_NAME = 'lex_office_token'
GCP_CONN_ID = 'gcp_connection'
PROJECT_ID = 'bi-data-replication-gcs'
TABLE_PAYMENTS = 'payments'
BATCH_SIZE = 500  # adjust as needed


# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Lexoffice_payments',
    default_args=default_args,
    description='Lex payments',
    schedule_interval='50 1 * * *',
    tags=['Lexoffice'],  
    catchup=False,
)

sql_query = """
        SELECT invoice_id as id FROM lexoffice.invoices
        order by invoice_id
        limit 500 offset 3000
        
    """


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
        "voucherType": "purchaseinvoice,invoice",
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
    data = ti.xcom_pull(task_ids='get_invoice_data', key='return_value')
    if data:
        # Convert the list of tuples to a DataFrame
        df = pd.DataFrame(data, columns=['id'])
        ti.xcom_push(key='invoice_data', value=df)

def store_data_in_bq(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_data_from_bigquery')
    if data:
        results_new = pd.DataFrame(data)
        ti.xcom_push(key='results_data', value=results_new)



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
    time.sleep(2)
    id, headers = args
    url = f"https://api.lexoffice.io/v1/payments/{id}"
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
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='invoice_data')
    
    # Fetch payments concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_payments, [(i, headers) for i in df['id']]))
        

    dfs = []
    for id, res in results:
        if res:
            if isinstance(res, dict):
                res = [res]
            temp_df = pd.DataFrame(res)
            temp_df['invoice_id'] = id  # add the id column
            dfs.append(temp_df)

    # Concatenate all the dataframes
    order_items_df = pd.concat(dfs, ignore_index=True)

    print(order_items_df)
    ti.xcom_push(key='payments_data', value=order_items_df)

def transform_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_and_store_payments_task', key='payments_data')
    
    #sql_query = f"DELETE FROM `{DATASET_NAME}.{TABLE_PAYMENTS}` WHERE TRUE;"
    #ti.xcom_push(key='delete_record', value=sql_query) 

    results = ti.xcom_pull(task_ids='store_data_from_bq', key='results_data')

    if results is not None and not results.empty:
        results_df = results.copy()
        #results_df[2] = results_df[2]
    else:
        results_df = pd.DataFrame()

    print(results_df)
    if 'invoice_id' in df.columns:
        ids_to_delete = ','.join([f"'{id}'" for id in df['invoice_id'].tolist()])
    else:
        ids_to_delete = ''

# Continue only if ids_to_delete is not empty
    if ids_to_delete:
        sql_query_new = f"DELETE FROM `{DATASET_NAME}.{TABLE_PAYMENTS}` WHERE invoice_id IN ({ids_to_delete});"
    else:
        sql_query_new = f"select * FROM `{DATASET_NAME}.{TABLE_PAYMENTS}`;"

    ti.xcom_push(key='delete_record', value=sql_query_new)

    for index, row in df.iterrows():
        #print(row['invoice_id'])
        matching_rows = results_df.loc[results_df[0] == row['invoice_id']] if 2 in results_df.columns else pd.DataFrame()
        if not matching_rows.empty:
            df.at[index, 'updated_at'] = convert_unix_microseconds_to_datetime(matching_rows[0].values[0])
        else:
            df.at[index, 'updated_at'] = current_datetime_to_custom_format()

    columns_to_convert_float = ['openAmount']
    #df['updated_at'] = datetime.now()
    #df['updated_at'] = df['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df[columns_to_convert_float] = df[columns_to_convert_float].apply(pd.to_numeric, errors='coerce')
    desired_order =  ['invoice_id', 'openAmount', 'paymentStatus', 'currency', 'voucherType',
       'voucherStatus', 'paidDate', 'updated_at']  
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


# Task to execute the query and store results in BigQuery
execute_query = BigQueryExecuteQueryOperator(
    task_id='execute_query',
    sql=sql_query,
    destination_dataset_table='lexoffice.temp_payments',  # Specify your dataset and a temporary table to store the results
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id='gcp_connection',  # Replace with your Google Cloud connection ID
    dag=dag,
)

# Task to get data from the temporary table and push it to XCom
get_invoice_data = BigQueryGetDataOperator(
    task_id='get_invoice_data',
    dataset_id='lexoffice',
    table_id='temp_payments',
    gcp_conn_id='gcp_connection',
    max_results=1000,
    dag=dag,
)



store_dataframe_task = PythonOperator(
    task_id='store_data_in_dataframe',
    python_callable=store_data_in_dataframe,
    dag=dag,
)

#Task4 Get existing data from BQ 
get_data_task = BigQueryGetDataOperator(
    task_id='get_data_from_bigquery',
    dataset_id=DATASET_NAME,
    table_id=TABLE_PAYMENTS,
    selected_fields=["invoice_id",  "updated_at"],  
    max_results=20000,
    gcp_conn_id='gcp_connection',  
)


store_dataframe_bq = PythonOperator(
    task_id='store_data_from_bq',
    python_callable=store_data_in_bq,
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
MAX_BATCHES_ESTIMATE = 6  # Adjust this value based on your expectation

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
    #>> fetch_task
    >> execute_query
   >> get_invoice_data
   >> store_dataframe_task
     >> get_data_task
     >> store_dataframe_bq
    >> fetch_and_store_payments
    >> transform_payments
    >> delete_query_order_items_job
    >> insert_jobs
)