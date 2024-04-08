from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException
import requests
import logging 
import pandas as pd
import numpy as np

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hubspot_tickets_v2',
    default_args=default_args,
    description='DAG to import tickets data from hubspot v2',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 30),
    tags=['hubspot'],
    catchup = False
)

gcp_service_account_content = Variable.get("gcp_service_account", deserialize_json=True)

project_id = 'bi-data-replication-gcs'
dataset_id = 'hubspot'
table_name = 'tickets_v2'

destination_table_id = f"{project_id}.{dataset_id}.{table_name}"

def fetch_and_process_tickets():

    api_key = Variable.get('hubspot_private_app_token')
    header= {f'Authorization': f'Bearer {api_key}'}

    batch_limit = 100

    url = f"https://api.hubapi.com/crm/v3/objects/tickets?associations=companies&properties=createdate&limit={batch_limit}&archived=false"

    response = requests.get(url, headers=header)
    tickets, next_url = [], None
    while response.status_code == 200:
        data = response.json()
        tickets_batch = data['results']
        tickets.extend(tickets_batch)

        if 'paging' in data:
            next_url = data['paging']['next']['link']
            logging.info(f"Found paging link: {next_url}")
        else:
            break
        
        response = requests.get(next_url, headers=header)

    logging.info(tickets)
    return process_tickets(tickets)

def process_tickets(tickets):
    tickets = pd.json_normalize(tickets, max_level=5)

    tickets['associations.companies.results'] = tickets['associations.companies.results'].apply(lambda r: r[0]['id'] if r is not np.nan else r)

    tickets.rename(columns = {'associations.companies.results': 'company_id'}, inplace=True)

    return tickets

def save_tickets_data(**kwargs):
    """Return the DataFrame as JSON."""
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_and_process_tickets')
    logging.info(df)
    #pandas_gbq.to_gbq(df, destination_table_id, project_id=project_id)   
    return df
   
def load_data_to_bigquery(**kwargs):
    """Upload JSON data to BigQuery."""
        
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='save_tickets_data')
    logging.info(f"Received data: {df}")
    
    destination_table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    logging.info(f"Starting to upload JSON data to BigQuery")
    client = bigquery.Client.from_service_account_info(gcp_service_account_content)
    job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    
    job = client.load_table_from_dataframe(df, destination_table_id, job_config=job_config)
    
    try:
        job.result()  # Wait for the job to complete.
        logging.info(f"Loaded {job.output_rows} rows into {destination_table_id}.")
    except Exception as e:
        logging.error(f"BigQuery job failed: {e}")
        raise AirflowFailException("Failed to upload data to BigQuery")

# Define tasks at the DAG level
fetch_and_process_churned_tickets_task = PythonOperator(
    task_id='fetch_and_process_tickets',
    python_callable=fetch_and_process_tickets,
    dag=dag,
)

save_churned_tickets_data_task = PythonOperator(
    task_id='save_tickets_data',
    python_callable=save_tickets_data,
    provide_context=True,
    dag=dag,
)

insert_churned_tickets_into_dwh_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_and_process_churned_tickets_task >> save_churned_tickets_data_task >> insert_churned_tickets_into_dwh_task