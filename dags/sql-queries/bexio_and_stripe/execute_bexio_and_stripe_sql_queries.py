from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.operators.empty import EmptyOperator
from datetime import date, datetime
import numpy as np
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud import bigquery
import json
from datetime import timedelta, datetime


# Define default_args and DAG
default_args = {
    'owner': 'BI',
    'start_date': '2024-01-21', 
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'execute_bexio_and_stripe_sql_queries',
    default_args=default_args,
    description='execute bexio and stripe queries',
    schedule_interval='20 5 * * *',
    tags=['sql'],  
    catchup=False,
)

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)





insert_task = BigQueryInsertJobOperator(
    task_id='join_and_insert_data',
    configuration={
        'query': {
            'query': """
           insert into bexio_all.invoice_status_history
            select
                i.updated_at as date,
                i.id as invoice_id,
                i.bexio_id as invoice_bexio_id,
                i.source,
                i.status
            from bexio_all.invoices i
            left join bexio_all.invoice_status_history ish
                on ish.invoice_id = i.id and ish.status = i.status
            where ish.invoice_id is null;
            """,
            'useLegacySql': False,
        }
    },
    dag=dag,
    gcp_conn_id='gcp_connection' 
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

start_task >> insert_task >> end_task