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
    'bexio_data_invoices_all_sql',
    default_args=default_args,
    description='Bexio invoices insert to sql',
    schedule_interval='0 5 * * *',
    tags=['sql'],  
    catchup=False,
)


start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)



join_and_insert_query = """
INSERT INTO `bexio_all.invoices`
SELECT * FROM `bexio_de.invoices`
UNION ALL
SELECT * FROM `bexio_ch.invoices`;
"""

join_and_insert_task = BigQueryInsertJobOperator(
    task_id='join_and_insert_data',
    configuration={
        'query': {
            'query': """
                INSERT INTO `bexio_all.invoices`
                SELECT *, 'bexio_de' as source, 'blank' as status FROM `bexio_de.invoices`
                UNION ALL
                SELECT *, 'bexio_ch' as source, 'blank' as status FROM `bexio_ch.invoices`;
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

start_task >> join_and_insert_task >> end_task