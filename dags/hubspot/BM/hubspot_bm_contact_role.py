from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.secret_manager import SecretsManagerHook
from airflow.models import XCom
import logging
import math
import requests
import json

SECRET_NAME = 'hubspot_api_key'
GCP_CONN_ID = 'gcp_connection'
PROJECT_ID = 'bi-data-replication-gcs'

def get_secret_data(ti):  
    hook = SecretsManagerHook(gcp_conn_id=GCP_CONN_ID)
    client = hook.get_conn()
    secret_payload = client.get_secret(secret_id=SECRET_NAME, project_id=PROJECT_ID)
    
    ti.xcom_push(key="secret_value", value=secret_payload)  


def process_batches(ti):
    # Retrieve batches from XCom
    batches = ti.xcom_pull(task_ids='prepare_organizations', key='batches')
    print(batches)
    
    batches_failed = 0
    #api_key = ti.xcom_pull(task_ids='retrieve_secret', key='secret_value') 
    api_key = 'api_key'
    # Example process for each batch
    for batch in batches:
        try:
            # Placeholder for your batch processing logic
            for batch in batches:
                response = update_contacts_per_batch(api_key, batch)
                if response.status_code != 200:
                    logging.debug(f'batch failed. Reason: {response.text}')
                    logging.debug(f'failed batch details: {batch}')
                    batches_failed += 1
                    # exit()
    
            if batches_failed > 0:
                logging.debug(f'HubSpot parameter company updates: {batches_failed} batch(es) out of {len(batches)} have failed due to invalid data')
                    # If your processing logic involves API calls or other operations,
                    # implement the logic here and catch exceptions as failures.
        except Exception as e:
            # Log the exception or error
            logging.error(f"Failed to process batch: {e}")
            batches_failed += 1
    
    # Log the number of failed batches
    logging.info(f"Total batches failed: {batches_failed}")


def update_contacts_per_batch(api_key, batch):
    url = f'https://api.hubapi.com/crm/v3/objects/contacts/batch/update'

    headers = {f'Authorization': f'Bearer {api_key}', 'Content-type': 'application/json'}
    data=json.dumps(batch)
    r = requests.post(url=url, headers=headers, json=batch)

    return r

def process_organizations(ti):
    # Fetch the query results from XCom
    query_results = ti.xcom_pull(key='query_results', task_ids='query_bigquery')

    organizations = {}
    for row in query_results:
        company_id = row.get('vid')
        role = row.get('role_in_organizaiton')  

        if company_id is not None:
            company_id = math.trunc(company_id)
            # Update the dictionary to include new fields
            properties = {
                'role_in_organizaiton': role,  
            }
            organizations = create_or_update_property(organizations, company_id, properties)

    ti.xcom_push(key='organizations', value=organizations)
    logging.info(organizations)

def generate_contact_update_new(item, value):
    # Placeholder for your logic to format an organization's data
    # You need to replace this with your actual implementation
    return {"company_id": item, "data": value}

def generate_batches_new(data, batch_size=100):
    batches, batch = [], []
    for item, value in data.items():
        batch.append(generate_contact_update_new(item, value))
        if len(batch) % batch_size == 0:
            batches.append({"inputs": batch})
            batch = []
    if len(batch) != 0:
        batches.append({"inputs": batch})
    return batches

def prepare_batches_new(ti):
    # Assume 'organizations' data is fetched from XCom or generated in this task
    organizations = ti.xcom_pull(task_ids='process_organizations', key='organizations')
    print('organizations')
    print(organizations)
    
    # Assuming organizations is a dictionary with company_id as keys and other data as values
    c_batch_size = 10
    batches = generate_batches_new(organizations, c_batch_size)
    
    # Here you can push the batches to XCom, process them directly, or save them as needed
    # For example, pushing to XCom
    ti.xcom_push(key='batches', value=batches)

    # For logging or debugging
    logging.info(f"Generated {len(batches)} batches.")

def create_or_update_property(d, key, properties):
    if d.get(key, None) is None:
        d[key] = properties
    else:
        for k, v in properties.items():
            if k in d[key]:
                # Optional: Handle duplicate key scenario, e.g., append, replace, etc.
                pass
            else:
                d[key][k] = v
    return d

def query_bigquery(ti):
    hook = BigQueryHook(gcp_conn_id='gcp_connection', use_legacy_sql=False)
    client = hook.get_client()

    query = """
                   with hcf as (
                            select vid, company_id , organization_id, email, djangolink
                            from dbt_integrations .hubspot_contacts__filtered hcf 
                            where email is not null 
                            group by 1,2,3,4, 5
                        )
                        , employee_emails as (
                            select distinct ee.organization_id 
                                , ee.merchant_id 
                                , ee.email 
                                , ee.first_name, last_name
                                , ee.full_name, permission_level 
                            from dbt_marketing.employee_emails ee
                        )
                        , bm_final_output as (
                            select distinct ee.email , hcf.vid
                                    , ee.organization_id 
                                    , hcf.company_id
                                    , ee.merchant_id , ee.first_name, last_name
                                    , ee.full_name, permission_level, hcf.djangolink
                            from hcf
                            join employee_emails ee
                                    on hcf.email = ee.email )
                                    select vid, permission_level as role_in_organizaiton from bm_final_output
    """
    
    query_job = client.query(query)
    results = query_job.result()  # Waits for the query to finish
    
    # Convert each row to a dictionary
    rows = [dict(row) for row in results]
    
    ti.xcom_push(key='query_results', value=rows)

def process_query_results(ti):
    query_results = ti.xcom_pull(key='query_results', task_ids='query_bigquery')
    
    # Process your results here
    for row in query_results:
        logging.info(row)
        # Implement your processing logic here

default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hubpost_bm_dwh_contact_role',
    default_args=default_args,
    description='Hubspot BM DWH contact role sync',
    schedule_interval='20 3 * * *',
    tags=['Hubspot'],  
    catchup=False,  
) as dag:
    
    get_secret_key = PythonOperator(
        task_id='retrieve_secret',
        python_callable=get_secret_data,
        provide_context=True  # This provides the task instance (ti) as an argument
    )

    query_bigquery_task = PythonOperator(
        task_id='query_bigquery',
        python_callable=query_bigquery,
    )

    process_query_results_task = PythonOperator(
        task_id='process_query_results',
        python_callable=process_query_results,
    )

    # Add the new task to the DAG
    process_organizations_task = PythonOperator(
        task_id='process_organizations',
        python_callable=process_organizations,
        dag=dag,
    )

    
    prepare_organizations_task = PythonOperator(
        task_id='prepare_organizations',
        python_callable=prepare_batches_new,
        dag=dag,
    )
    # Add the new task to the DAG
    process_batches_task = PythonOperator(
        task_id='process_batches',
        python_callable=process_batches,
        dag=dag,
    )


    (
        get_secret_key >>
        query_bigquery_task >>
          process_query_results_task 
          >> process_organizations_task 
          >> prepare_organizations_task 
          >> process_batches_task
    )