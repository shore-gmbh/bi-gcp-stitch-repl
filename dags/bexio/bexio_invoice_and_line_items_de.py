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


from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryGetDataOperator
)

DATASET_NAME = 'bexio_de'
TABLE_NAME = 'invoices'
insert_rows_query = ""
SECRET_NAME = 'bexio_de_token'
GCP_CONN_ID = 'gcp_connection'
PROJECT_ID = 'bi-data-replication-gcs'
TABLE_LINE_ITEMS = 'invoice_items'



MAX_WORKERS = 10  # Adjust as per your requirement


# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    'Bexio_de_invoice_and_line_items',
    default_args=default_args,
    description='Bexio DE invoice and invoice line items',
    schedule_interval='35 21 * * *',
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

    url = 'https://api.bexio.com/2.0/kb_invoice?offset=1000&limit=500' 
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception('API request failed')
    

def store_data_in_bq(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_data_from_bigquery')
    if data:
        results_new = pd.DataFrame(data)
        ti.xcom_push(key='results_data', value=results_new)

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

# Task 3: Transform the data to sql statement
def transform_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='invoice_data')
    
    # Retrieve the results from the task
    results = ti.xcom_pull(task_ids='store_data_from_bq', key='results_data')

    if results is not None and not results.empty:
        results_df = results.copy()
        results_df[2] = results_df[2].astype(int)
    else:
        results_df = pd.DataFrame()


    bexio_del_ids = df['id'].tolist()
    ids_to_delete = ','.join(map(str, bexio_del_ids))
    sql_query = f"delete FROM `bi-data-replication-gcs.bexio_de.invoices`  where bexio_id IN ({ids_to_delete});"
    ti.xcom_push(key='delete_record', value=sql_query) 
   
    if  results is not None:
        results_df[2] = results_df[2].astype(int)
    
    for index, row in df.iterrows():
        matching_rows = results_df.loc[results_df[2] == row['id']] if 2 in results_df.columns else pd.DataFrame()
        if not matching_rows.empty:
            df.at[index, 'u_id'] = matching_rows[1].values[0]
            df.at[index, 'created_at'] = convert_unix_microseconds_to_datetime(matching_rows[0].values[0])
        else:
            df.at[index, 'u_id'] = str(uuid.uuid4())
            df.at[index, 'created_at'] = current_datetime_to_custom_format()
      
    df['percentage'] = df['taxs'].apply(lambda tax_list: tax_list[0]['percentage'] if len(tax_list) > 0 else None)
    print(df['percentage'])
   
    columns_to_convert = [ 'total_gross', 'total_net', 'total_taxes', 'total_received_payments',
                     'total_credit_vouchers', 'total_remaining_payments', 'total',  'mwst_type', 'percentage']

    # Use pd.to_numeric to convert the specified columns to float
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    #df['viewed_by_client_at'] = df['viewed_by_client_at'].astype(str)
    #print(df['viewed_by_client_at'])
    
    desired_order = ['created_at', 'updated_at', 'u_id', 'id', 'contact_id', 'kb_item_status_id',
                      'language_id', 'currency_id', 'payment_type_id', 'esr_id', 'document_nr', 'is_valid_from', 
                    'is_valid_to', 'title', 'total_gross', 'total_net', 'total_taxes', 'total_received_payments',
                     'total_credit_vouchers', 'total_remaining_payments', 'total',  'mwst_type',  
                     'mwst_is_net', 'contact_address', 'viewed_by_client_at', 'percentage']
    #selected_df = df[desired_order]

    insert_rows = df[desired_order].apply(lambda row: tuple(convert_value(val) for val in row), axis=1).tolist()
    values_str = ', '.join(['(' + ', '.join(map(str, tup)) + ')' for tup in insert_rows])
    insert_rows_query = f"INSERT INTO {DATASET_NAME}.{TABLE_NAME} VALUES {values_str};"

    return insert_rows_query

# ------------ line items -----------------------#
# Define a function to fetch order items for a given order_id from Bexio API
def fetch_line_items(args):
    id, headers = args
    url = f"https://api.bexio.com/2.0/kb_invoice/{id}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises HTTPError if not a 2xx response
    return response.json()

def fetch_and_store_line_items(**kwargs):
    ti = kwargs['ti']
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 
    print(access_token)
    headers = {
        'Authorization': f'Bearer {access_token}',
        'accept': 'application/json',  # Adjust the content type as needed
    }
    df = ti.xcom_pull(task_ids='store_data_in_dataframe', key='invoice_data')
    
    # Fetch line items concurrently using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(fetch_line_items, [(i, headers) for i in df['id']]))

    # Filter out None values, ensure they're dictionaries, and concatenate dataframes
    order_items_dfs = [pd.DataFrame([res]) for res in results if res and isinstance(res, dict)]
    order_items_df = pd.concat(order_items_dfs, ignore_index=True)

    print(order_items_df)
    ti.xcom_push(key='line_items_data', value=order_items_df)




def extract_positions_from_order(order_df):
    data_list = []

    # Iterate through each row in the DataFrame
    for index, order in order_df.iterrows():
        positions = order.get('positions', [])
        
        for pos in positions:
            pos_data = pos.copy()  # Clone the position data
            pos_data['invoice_id'] = order.get('id', None)
            pos_data['updated_at'] = order.get('updated_at', None)
            data_list.append(pos_data)

    return pd.DataFrame(data_list)



def transform_line_items_and_insert_to_bq(**kwargs):
    ti = kwargs['ti']
    order_df = ti.xcom_pull(task_ids='fetch_and_store_line_items', key='line_items_data') 
    access_token = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 

    if not order_df.empty:  # Check if the DataFrame isn't empty
        order_items = extract_positions_from_order(order_df)
        

    results = ti.xcom_pull(task_ids='get_line_items_big_query')
    results_df = pd.DataFrame(results)

    if results_df is not None and not results_df.empty:
        #results_df = results_df.copy()
        results_df[2] = results_df[2].astype(int)
    else:
        results_df = pd.DataFrame()

    #print(order_items['invoice_id'])
    ids_to_delete = ','.join(map(str, order_items['invoice_id'].tolist()))
    sql_query = f"DELETE FROM `{DATASET_NAME}.{TABLE_LINE_ITEMS}` WHERE invoice_id IN ({ids_to_delete});"
    ti.xcom_push(key='delete_line_items_record', value=sql_query)


    for index, row in order_items.iterrows():
        #matching_rows = results_df.loc[results_df[2] == row['id']]
        matching_rows = results_df.loc[results_df[2] == row['id']] if 2 in results_df.columns else pd.DataFrame()
        if not matching_rows.empty:
            order_items.at[index, 'u_id'] = matching_rows[1].values[0]
            order_items.at[index, 'created_at'] = convert_unix_microseconds_to_datetime(matching_rows[0].values[0])
        else:
            order_items.at[index, 'u_id'] = str(uuid.uuid4())
            order_items.at[index, 'created_at'] = current_datetime_to_custom_format()
        
        if row['type'] == 'KbPositionDiscount':
                 #check this again
            order_items.at[index, 'discount_amount'] = get_discount_total(row['invoice_id'], access_token)
        else:
            order_items.at[index, 'discount_amount'] = 0

    #order_items.to_excel("output.xlsx", index=False)
    columns_to_convert_float = ['amount', 'tax_value', 'unit_price', 'position_total', 'discount_in_percent', 'discount_amount']
    order_items[columns_to_convert_float] = order_items[columns_to_convert_float].apply(pd.to_numeric, errors='coerce')
    order_items['account_id'].fillna(0, inplace=True)
    order_items['tax_id'].fillna(0, inplace=True)
    order_items['unit_id'].fillna(0, inplace=True)
    order_items['account_id'] = order_items['account_id'].astype(int)
    order_items['tax_id'] = order_items['tax_id'].astype(int)
    order_items['unit_id'] = order_items['unit_id'].astype(int)
    order_items['article_id'] =None
    desired_order = [
        'created_at', 'updated_at', 'u_id', 'id', 'invoice_id', 'account_id', 'type',
          'unit_name', 'text', 'amount', 'unit_price', 'position_total', 
          'tax_id', 'tax_value',  'discount_in_percent', 'is_optional', 'article_id',
            'discount_amount', 'unit_id', 
        ]
    
    insert_rows = order_items[desired_order].apply(lambda row: tuple(convert_value(val) for val in row), axis=1).tolist()
    values_str = ', '.join(['(' + ', '.join(map(str, tup)) + ')' for tup in insert_rows])
    insert_rows_query = f"INSERT INTO {DATASET_NAME}.{TABLE_LINE_ITEMS} VALUES {values_str};"
    return insert_rows_query


def get_discount_total(id, access_token):
    print('id')
    print(id)
    url = f"https://api.bexio.com/2.0/kb_invoice/{id}/kb_position_discount"
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an exception if the HTTP request returned an error

    discounts = response.json()

    if discounts and 'discount_total' in discounts[0]:
        return discounts[0]['discount_total']
    else:
        return None


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

get_data_task = BigQueryGetDataOperator(
    task_id='get_data_from_bigquery',
    dataset_id=DATASET_NAME,
    table_id=TABLE_NAME,
    selected_fields=["id", "created_at", "bexio_id"],  
    gcp_conn_id=GCP_CONN_ID,  
)


store_dataframe_bq = PythonOperator(
    task_id='store_data_from_bq',
    python_callable=store_data_in_bq,
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

# Execute the BigQuery insert query here
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

# Define your task using PythonOperator
fetch_and_store_line_items_task = PythonOperator(
    task_id='fetch_and_store_line_items',
    python_callable=fetch_and_store_line_items,
    dag=dag,
)

#Task4 Get existing data from BQ 
get_line_items_BQ_task = BigQueryGetDataOperator(
    task_id='get_line_items_big_query',
    dataset_id=DATASET_NAME,
    table_id=TABLE_LINE_ITEMS,
    selected_fields=["id", "created_at", "bexio_id"],  
    gcp_conn_id='gcp_connection',  
)

transform_line_items = PythonOperator(
    task_id='transform_line_items',
    python_callable=transform_line_items_and_insert_to_bq,
    provide_context=True,
    dag=dag,
)


delete_query_order_items_job = BigQueryInsertJobOperator(
    task_id="delete_query_order_items_job",
    configuration={
        "query": {
            "query": "{{ti.xcom_pull(task_ids='transform_line_items', key='delete_line_items_record')}}",  # You will dynamically populate this query below
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
            "query": "{{ti.xcom_pull('transform_line_items')}}",  
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
    >> get_data_task 
    >> store_dataframe_bq 
    >> transform_and_insert_task  
    >> delete_query_job 
    >> insert_query_job
    >> fetch_and_store_line_items_task
    # >> get_line_items_BQ_task
    # >> transform_line_items
    # >> delete_query_order_items_job
    # >> insert_query_job_order_items
)