from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException
import requests
import logging 
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hubspot_churn_reasons',
    default_args=default_args,
    description='DAG to import churn tickets data from hubspot',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 23),
    tags=['hubspot'],
    catchup = False
)

gcp_service_account_content = Variable.get("gcp_service_account", deserialize_json=True)

project_id = 'bi-data-replication-gcs'
dataset_id = 'hubspot'
table_name = 'churn_reasons'

destination_table_id = f"{project_id}.{dataset_id}.{table_name}"

def fetch_and_process_tickets():

    api_key = Variable.get('hubspot_private_app_token')
    header= {f'Authorization': f'Bearer {api_key}'}

    batch_limit = 100
    properties = '&properties='.join([
        '', 'createdate', 'closed_date', 'organization_id__b_m_', 'djangolink__pos_', 'subject', 'content',  
        'hs_pipeline', 'churn_reason_2', 'subcategory_churn_reason', 'churn_status', 'churn_outcome', 
        'reason_for_dissatisfaction', 'source_type', 'comments', 'churned_product2', 'hs_pipeline_stage', 
        'first_close_date_of_ticket', 'hs_lastcontacted', 'last_reply_date', 'hs_ticket_category'
    ])
    url = f"https://api.hubapi.com/crm/v3/objects/tickets?limit={batch_limit}&archived=false{properties}"

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
    return process_churn_tickets(tickets)

def process_churn_tickets(tickets):
    tickets = pd.json_normalize(tickets, max_level=5)

    # filter tickets that belong to churn pipeline
    tickets = tickets[tickets['properties.hs_pipeline'].isin(['13404239', '10054635'])]

    tickets.rename(columns = {'properties.subject': 'ticket_name'
                            , 'properties.content': 'ticket_description'
                            , 'createdAt': 'createdate'
                            , 'updatedAt': 'updated_at'
                            , 'properties.churn_outcome': 'churn_outcome'
                            , 'properties.churn_status': 'churn_status'
                            , 'properties.churn_reason_2': 'churn_reason'
                            , 'properties.churned_product2': 'churned_product2'
                            , 'properties.closed_date': 'closed_at'
                            , 'properties.djangolink__pos_': 'djangolink'
                            , 'properties.first_close_date_of_ticket': 'first_close_date_of_ticket'
                            , 'properties.hs_lastcontacted': 'hs_lastcontacted'
                            , 'properties.hs_lastmodifieddate': 'hs_lastmodifieddate'
                            , 'properties.hs_object_id': 'object_id'
                            , 'properties.hs_pipeline': 'hs_pipeline'
                            , 'properties.hs_pipeline_stage': 'hs_pipeline_stage'
                            , 'properties.hs_ticket_category': 'hs_ticket_category'
                            , 'properties.hubspot_team_id': 'hubspot_team_id'
                            , 'properties.organization_id__b_m_': 'organization_id'
                            , 'properties.last_reply_date': 'last_reply_date'
                            , 'properties.reason_for_dissatisfaction': 'properties.reason_for_dissatisfaction'
                            , 'properties.source_type': 'source'
                            , 'properties.subcategory_churn_reason': 'churn_reason_subcategory'
                            , 'properties.reason_for_dissatisfaction': 'reason_for_dissatisfaction'
                            , 'properties.comments': 'comments'
                            , 'createdat': 'created_at'}, inplace=True)

    selected_columns = ['id', 'churn_status', 'closed_at', 'djangolink', 'first_close_date_of_ticket'
                       , 'churned_product2', 'ticket_name', 'hs_pipeline', 'createdate', 'source', 'churn_outcome'
                       , 'hs_lastcontacted', 'hs_pipeline_stage', 'ticket_description', 'hs_ticket_category'
                       , 'churn_reason_subcategory', 'last_reply_date', 'churn_reason', 'organization_id'
                       , 'comments']

    tickets = tickets[selected_columns]
    return tickets

def save_tickets_data(**kwargs):
    """Return the DataFrame as JSON."""
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='fetch_and_process_tickets')
    #logging.info(df)
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