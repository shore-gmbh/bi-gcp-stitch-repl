from datetime import timedelta, datetime
import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from airflow.exceptions import AirflowFailException

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'google_sheets_to_bigquery_gross_margin',
    default_args=default_args,
    description='DAG to import data from google sheets - BI Dev Gross Margin',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 24),
    tags=['price-increase'],
    catchup = False
)

gcp_service_account_content = Variable.get("gcp_service_account", deserialize_json=True)

project_id = 'bi-data-replication-gcs'
dataset_id = 'price_increase'
table_name = 'bi_dev_gross_margin'

def get_records_from_gsheet(scope, appkey, key, worksheet_name):
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(appkey, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(key).worksheet(worksheet_name)
        records = [e[0:7] for e in sheet.get_all_values()]
        records_df = pd.DataFrame(records[157:191], columns=records[156], dtype='str')
        records_df = records_df.drop([''], axis=1)
        return records_df
    except Exception as e:
        logging.error(f"Error: {e}")
        return None

def transform_data(records_data):

    # Transformations
    records_data['year-month'] = records_data['year-month'].replace('', pd.NA)
    records_data['year_month'] = pd.to_datetime(records_data['year-month'], format='%b-%y').dt.to_period('M').dt.to_timestamp()
    records_data['year_month'] = records_data['year_month'] + pd.offsets.MonthEnd(0)
    records_data['year_month'] = records_data['year_month'].dt.strftime('%Y-%m-%d')
    records_data = records_data.dropna(subset=['year_month'])
    records_data['gross_margin'] = records_data['gross margin'].replace('n.a.', float('nan'))
    records_data['gross_margin'] = pd.to_numeric(records_data['gross_margin'].str.rstrip('%')) / 100
    records_data['gross_margin'] = records_data['gross_margin'].round(3)

    records_data.fillna(0, inplace=True)

    #logging.info(records_data)
    return records_data


def transform_and_save_to_csv(**kwargs):
    filename = "price_increase_bi_dev_gross_margin.csv"
    ti = kwargs['ti']
    records_data = ti.xcom_pull(task_ids='get_data_from_google_sheets')
    #logging.info(records_data)

    # call Transformations here
    records_data = transform_data(records_data)

    # Select required columns
    records_data = records_data[['year_month', 'gross_margin', 'asp_trx_rev']]

    records_data.to_csv(filename, index=False, sep='|')

    return filename

def upload_csv_to_bigquery(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='transform_and_save_to_csv')
    logging.info(f'Filename: {filename}')
    
    if not filename or not os.path.exists(filename):
        logging.error("No CSV file found.")
        raise AirflowFailException("No CSV file found")
    
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        logging.info("Starting to upload CSV file to BigQuery")
        client = bigquery.Client.from_service_account_info(gcp_service_account_content)
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, autodetect=True)
    
        with open(filename, "rb") as source_file:
            source_file.seek(0)
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
    
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into {table_id}.")

    except Exception as e:
        logging.error(f"BigQuery job failed: {e}")
        raise AirflowFailException("Failed to upload data to BigQuery")

get_data_task = PythonOperator(
    task_id='get_data_from_google_sheets',
    python_callable=get_records_from_gsheet,
    op_args=['https://spreadsheets.google.com/feeds', Variable.get('google_app_key', deserialize_json=True), Variable.get('key_BI_Dev_LTV'), 'BI Sheet'],
    dag=dag,
)

transform_and_save_to_csv_task = PythonOperator(
    task_id='transform_and_save_to_csv',
    python_callable=transform_and_save_to_csv,
    provide_context=True,
    dag=dag,
)

insert_data_into_dwh_task = PythonOperator(
    task_id='upload_csv_to_bigquery',
    python_callable=upload_csv_to_bigquery,
    provide_context=True,
    dag=dag,
)

get_data_task >> transform_and_save_to_csv_task >> insert_data_into_dwh_task
