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

DATASET_NAME = 'bexio_de'
TABLE_NAME = 'contacts'
insert_rows_query = ""

PROJECT_ID = 'bi-data-replication-gcs'
SECRET_NAME = 'bexio_de_token'
GCP_CONN_ID = 'gcp_connection'



# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21',  
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'shore_bexio_de_contacts',
    default_args=default_args,
    schedule_interval='0 21 * * *',
    description='Bexio DE contacts',  
    tags=['Bexio'],
    catchup=False,
)

# Task 1: Fetch data from the API
def fetch_data(ti):
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 

    headers = {
        'Authorization': f'Bearer {access_token}',
        'accept': 'application/json',  # Adjust the content type as needed
    }

    url = 'https://api.bexio.com/2.0/contact'
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception('API request failed')



# Task 2: Store data in a DataFrame
def store_data_in_dataframe(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_api_data')
    if data:
        df = pd.DataFrame(data)
        ti.xcom_push(key='contacts_data', value=df)


# Task 2: Store data in a DataFrame
def store_data_in_bq(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_data_from_bigquery')
    if data:
        results_new = pd.DataFrame(data)
        ti.xcom_push(key='results_data', value=results_new)

store_dataframe_bq = PythonOperator(
    task_id='store_data_from_bq',
    python_callable=store_data_in_bq,
    dag=dag,
)

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

# Task 3: Transform the data to sql statement
def transform_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='contacts_data')
    results = ti.xcom_pull(task_ids='store_data_from_bq', key='results_data')
    
    if results is not None and not results.empty:
        results_df = results.copy()
        results_df[2] = results_df[2].astype(int)
    else:
        results_df = pd.DataFrame()

    ids_to_delete = ','.join(map(str, df['id'].tolist()))
    sql_query = f"DELETE FROM `{DATASET_NAME}.{TABLE_NAME}` WHERE bexio_id IN ({ids_to_delete});"
    ti.xcom_push(key='delete_record', value=sql_query) 

    for index, row in df.iterrows():
        matching_rows = results_df.loc[results_df[2] == row['id']] if 2 in results_df.columns else pd.DataFrame()
        if not matching_rows.empty:
            df.at[index, 'u_id'] = matching_rows[1].values[0]
            df.at[index, 'created_at'] = convert_unix_microseconds_to_datetime(matching_rows[0].values[0])
        else:
            df.at[index, 'u_id'] = str(uuid.uuid4())
            df.at[index, 'created_at'] = current_datetime_to_custom_format()
        
    df['language_id'].fillna(0, inplace=True)
    df['language_id'] = df['language_id'].astype(int)

    desired_order = ['updated_at', 'updated_at', 'u_id', 'id', 'nr', 'contact_type_id', 'name_1',
                      'name_2', 'address', 'postcode', 'city', 'country_id', 'mail',
                      "mail_second", "phone_fixed", "phone_fixed_second","phone_mobile",
                      "fax", "url", "skype_name", "remarks", "language_id",
                     "is_lead",
                      ]           
    
    insert_rows = df[desired_order].apply(lambda row: tuple(convert_value(val) for val in row), axis=1).tolist()
    values_str = ', '.join(['(' + ', '.join(map(str, tup)) + ')' for tup in insert_rows])
    insert_rows_query = f"INSERT INTO {DATASET_NAME}.{TABLE_NAME} VALUES {values_str};"

    return insert_rows_query

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
    python_callable=fetch_data,
    dag=dag,
)


store_dataframe_task = PythonOperator(
    task_id='store_data_in_dataframe',
    python_callable=store_data_in_dataframe,
    dag=dag,
)


# Create a task to retrieve data from BigQuery for all rows
get_data_task = BigQueryGetDataOperator(
    task_id='get_data_from_bigquery',
    dataset_id=DATASET_NAME,
    table_id=TABLE_NAME,
    selected_fields=["id", "created_at", "bexio_id"],  
    gcp_conn_id='gcp_connection',  
)

transform_and_insert_task = PythonOperator(
    task_id='transform_and_insert_to_bq',
    python_callable=transform_and_insert_to_bq,
    dag=dag,
)

insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": "{{ti.xcom_pull('transform_and_insert_to_bq')}}",  # You will dynamically populate this query below
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    gcp_conn_id='gcp_connection',
)

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


# Define task dependencies

(
    
get_secret_key
>> fetch_task 
>> store_dataframe_task 
>> get_data_task 
>> store_dataframe_bq 
>> transform_and_insert_task 
>>  delete_query_job 
>> insert_query_job
)
