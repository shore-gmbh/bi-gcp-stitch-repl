from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    return 'Hello!'

def print_goodbye():
    return 'Goodbye!'

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 12), # Ensure this is in the past when you run it
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'test_dag', 
    default_args=default_args, 
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
)

# Define tasks
start_task = EmptyOperator(task_id='start_task', dag=dag)

hello_task = PythonOperator(
    task_id='print_hello', 
    python_callable=print_hello, 
    dag=dag,
)

goodbye_task = PythonOperator(
    task_id='print_goodbye', 
    python_callable=print_goodbye, 
    dag=dag,
)

end_task = EmptyOperator(task_id='end_task', dag=dag)

# Set task dependencies
start_task >> hello_task >> goodbye_task >> end_task