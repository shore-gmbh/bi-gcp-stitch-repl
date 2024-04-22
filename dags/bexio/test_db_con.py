from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    'test_db_connection',
    default_args=default_args,
    description='A simple test DAG to check DB connection',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    test_query = PostgresOperator(
        task_id='test_postgres_connection',
        postgres_conn_id='postgresql+psycopg2://adl:adfjaljalcai2313a@34.159.240.197:5432/bi-dwh',
        sql="SELECT 1;",
    )

test_query