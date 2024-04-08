from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging 
import requests 
import json 
from airflow.models import Variable
import io 

gcp_service_account_content = Variable.get("gcp_service_account", deserialize_json=True)

last_id = Variable.get("billwerk_inv_last_processed_id", default_var=0)
MAX_LOOP_COUNT = 10

project_id = 'bi-data-replication-gcs'
dataset_id = 'billwerk'
table_name = 'invoices'

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2021, 10, 10),
    'row_limit' : 10
}

dag = DAG('billwerk_invoices', default_args=default_args, schedule_interval=None)

def fetch_billwerk_token():
    billwerk_username = Variable.get("billwerk_username")
    billwerk_password = Variable.get("billwerk_password")
    billwerk_client_id = Variable.get("billwerk_client_id")
    billwerk_client_secret = Variable.get("billwerk_client_secret")

    # Fetching token logic here
    url = 'https://app.billwerk.com/oauth/token/'

    data = {
        'grant_type': 'password',
        'username': billwerk_username,
        'password': billwerk_password
    }

    response = requests.post(url, data=data, auth=(billwerk_client_id, billwerk_client_secret))
    response = json.loads(response.text)

    return response['access_token']

def fetch_invoices(last_id=0, row_limit=None):
    access_token = fetch_billwerk_token()
    url = f'https://app.billwerk.com/api/v1/invoices?detailLevel=2&access_token={access_token}'

    if last_id and last_id != '0':
        url = f'{url}&from={last_id}'

    fetched_rows = 0
    data, next_id = [], None
    
    while True:
        response = requests.get(url if next_id is None else f'{url}&from={next_id}')
        if response.status_code != 200:
            logging.error(f"API request failed with status code {response.status_code}. Response: {response.text}")
            break
        
        data_batch = json.loads(response.text)
        
        # Check termination condition
        if not data_batch or (data_batch[-1]['Id'] == next_id):
            break
        
        data.extend(data_batch)
        fetched_rows += len(data_batch)
        
        # Check if we've fetched the desired number of rows
        if row_limit and fetched_rows >= row_limit:
            data = data[:row_limit]  # In case we fetched more than needed in the last batch
            break
        
        next_id = data_batch[-1]['Id']

    logging.info(f'Total rows fetched: {len(data)}')
    return data

def fetch_data(**kwargs):

    # Get the last processed ID from Airflow Variable
    last_id = Variable.get("last_processed_id", default_var="0")

    # Fetch the next batch of data
    data = fetch_invoices(last_id=(last_id)) #no row limit

    # Ensure to return the max ID to update "last_processed_id" at the end.
    max_id = max([item['Id'] for item in data]) if data else last_id

    # Log the number of invoices fetched
    logging.info(f'Number of invoices fetched: {len(data)}')
    logging.info(f'Max ID: {max_id}')
    
    return data, max_id

def connect_and_insert_bigquery(data, chunk_size=1000):
    # Assuming you've already set up `gcp_service_account_content`, `project_id`, `dataset_id`, and `table_name`
    client = bigquery.Client.from_service_account_info(gcp_service_account_content)

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        # Use the JSON source format since our data is in JSON-like structure
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        # Auto-detect schema and any transformations that should be performed on the data
        autodetect=True,
    )

    # Break data into chunks
    for i in range(0, len(data), chunk_size):
        chunked_data = data[i:i + chunk_size]

        try:
            # Convert list of dictionaries to newline-delimited JSON strings
            json_rows = [json.dumps(item) for item in chunked_data]
            json_data = "\n".join(json_rows).encode("utf-8")

            # Upload JSON data to BigQuery
            logging.info(f"Inserting data to BigQuery...")
            job = client.load_table_from_file(
                io.BytesIO(json_data), table_id, job_config=job_config
            )

            job.result()  # Wait for the job to complete
            logging.info("Data successfully inserted to BigQuery.")
            return True  # indicate success

        except Exception as e:
            logging.error(f"Failed to upload data to BigQuery. Error: {e}")
            return False  # indicate failure
    
def upload_to_bigquery(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='fetch_data')
    print(f'Result: {result}')
    print(f"Type of result: {type(result)}")


    if not result:
        logging.error("No data fetched from fetch_data task.")
        return
    elif not isinstance(result, tuple) or len(result) != 2:
        logging.error(f"Unexpected data format from fetch_data task: {result}")
        return

    fetched_data, max_id = result
    print(f'Fetched data: {fetched_data}')
    print(f'Max ID: {max_id}')

    # Log the number of rows to be inserted
    logging.info(f'Number of rows to be inserted to BigQuery: {len(fetched_data)}')

    upload_success = connect_and_insert_bigquery(fetched_data)

    if upload_success:
        # Update the last_processed_id Airflow Variable after successful upload
        Variable.set("last_processed_id", str(max_id))
        logging.info(f"Successfully inserted {len(fetched_data)} rows to BigQuery.")
        return len(fetched_data)  # return the number of rows inserted
    else:
        logging.error(f"Failed to upload data to BigQuery.")
        return "Upload failure"  # return error message

    
fetch_token_task = PythonOperator(
    task_id='fetch_billwerk_token',
    python_callable=fetch_billwerk_token,
    dag=dag
)

fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag,
)

upload_to_bigquery_task = PythonOperator(
    task_id='upload_to_bigquery',
    python_callable=upload_to_bigquery,
    provide_context=True,
    dag=dag,
)

end_workflow_task = DummyOperator(task_id='end_workflow', dag=dag)

# Define Task Dependencies
fetch_token_task >> fetch_data_task >> upload_to_bigquery_task >> end_workflow_task


