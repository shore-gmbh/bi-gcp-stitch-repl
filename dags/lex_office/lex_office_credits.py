from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import uuid
from datetime import date, datetime
import numpy as np
from google.cloud import bigquery
import json
import math
from datetime import timedelta, datetime


from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
)

DATASET_NAME = 'lexoffice'
TABLE_NAME = 'credits'
insert_rows_query = ""

PROJECT_ID = 'bi-data-replication-gcs'
SECRET_NAME = 'lex_office_token'
GCP_CONN_ID = 'gcp_connection'
BATCH_SIZE = 500  # adjust as needed


# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'Lexoffice_credits',
    default_args=default_args,
    description='Lex office credit ',
    schedule_interval='10 1 * * *',
    tags=['Lexoffice'],  
    catchup=False,
)

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
        ti.xcom_push(key='invoice_data', value=df)




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

# Task 3: Transform the data to sql statement
def transform_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='invoice_data')
    
    sql_query = f"DELETE FROM `{DATASET_NAME}.{TABLE_NAME}` WHERE TRUE;"
    ti.xcom_push(key='delete_record', value=sql_query) 

    columns_to_convert_float = ['totalAmount', 'openAmount']
    df[columns_to_convert_float] = df[columns_to_convert_float].apply(pd.to_numeric, errors='coerce')
    desired_order =  ['id', 'voucherType', 'voucherStatus', 'voucherNumber', 'voucherDate',
       'createdDate', 'updatedDate', 'contactId', 'contactName', 'totalAmount','openAmount',
       'currency', 'archived' ]  
    insert_rows = df[desired_order].apply(lambda row: tuple(convert_value(val) for val in row), axis=1).tolist()
    
    # Splitting the rows into batches
    insert_batches = [insert_rows[i:i + BATCH_SIZE] for i in range(0, len(insert_rows), BATCH_SIZE)]
    
    # Generating a query for each batch and storing it
    for index, batch in enumerate(insert_batches):
        values_str = ', '.join(['(' + ', '.join(map(str, tup)) + ')' for tup in batch])
        insert_rows_query = f"INSERT INTO {DATASET_NAME}.{TABLE_NAME} VALUES {values_str};"
        ti.xcom_push(key=f'insert_query_batch_{index}', value=insert_rows_query)

# Execute the BigQuery insert query here




    # Update the query in the configuration
#---------- all the definations ------------------------------#
def get_secret_data(ti):  # Passing task instance (ti) as an argument
    hook = SecretsManagerHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_conn()
    secret_payload = client.get_secret(secret_id=SECRET_NAME, project_id=PROJECT_ID)
    
    ti.xcom_push(key="secret_value", value=secret_payload)  # Pushing the secret value to XCom
   


#------------------------ all the tasks ------------------------------#

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


transform_and_insert_task = PythonOperator(
    task_id='transform_and_insert_to_bq',
    python_callable=transform_and_insert_to_bq,
    dag=dag,
)

# insert_query_job = BigQueryInsertJobOperator(
#     task_id="insert_query_job",
#     configuration={
#         "query": {
#             "query": "{{ti.xcom_pull('transform_and_insert_to_bq')}}",  # You will dynamically populate this query below
#             "useLegacySql": False,
#             "priority": "BATCH",
#         }
#     },
#     gcp_conn_id='gcp_connection',
# )



delete_query_job = BigQueryInsertJobOperator(
    task_id="delete_query_job",
    configuration={
        "query": {
            #"query": "delete FROM bi-data-replication-gcs.bexio_ch.invoices  where bexio_id IN (22,22);",
            "query": "{{ti.xcom_pull(task_ids='transform_and_insert_to_bq', key='delete_record')}}",  # You will dynamically populate this query below
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id='gcp_connection',
)

# Use a dummy maximum batch estimate for now
MAX_BATCHES_ESTIMATE = 1  # Adjust this value based on your expectation

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
>> transform_and_insert_task 
>>  delete_query_job 
>> insert_jobs
)
