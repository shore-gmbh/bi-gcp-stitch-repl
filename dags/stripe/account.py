import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.oauth2.service_account import Credentials
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import gspread
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery
from datetime import date, datetime
from google.auth import default
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import requests


GCS_BUCKET_NAME = 'bi_etl_storage'
GCS_OBJECT_NAME = 'stripe_accounts.csv'
BIGQUERY_DATASET_NAME = 'bi_etl'
BIGQUERY_TABLE_NAME = 'stripe_charges'
PROJECT_ID = 'bi-data-replicaton-gcs'
GCP_CONN_ID = 'gcp_connection'
TABLE_NAME = 'stripe_charges'
API_KEY = 'secret_key'

sql = """
    delete from bi_etl.stripe_charges where id IN
        (select id
          from bi_etl.stripe_charges_temp);

        insert into bi_etl.stripe_charges
            select id, payment_intent_id, payment_method_id, type,
          card_brand, card_brand_product, card_fingerprint, card_network from bi_etl.stripe_charges_temp;

        drop table bi_etl.stripe_charges_temp;
"""

def add_balance_volume_to_account_(account, api_key):
    id = account['id']
    header= {f'Authorization': f'Bearer {api_key}', 'Stripe-Account': f'{id}'}
    balance_url = f'https://api.stripe.com/v1/balance'
    transactions_url = f'https://api.stripe.com/v1/balance_transactions?limit=100'

    response = requests.get(balance_url, headers=header)
    balance_info = response.json()
    balance = 0
    for key in balance_info:
        if key in ['available', 'connect_reserved', 'pending']:
            for amount_info in balance_info[key]:
                balance += amount_info['amount']
    
    response = requests.get(transactions_url, headers=header)
    payout = 0
    volume = 0
    while response.status_code == 200:
        trn_info_batch = response.json()
        for trn_info in trn_info_batch['data']:
            if trn_info['type'] == "payout":
                payout += trn_info['amount']
            elif trn_info['type'] == 'payment':
                volume += trn_info['amount']

        if trn_info_batch['has_more'] == False:
            print("No more data from stripe")
            break

        next_id = trn_info_batch['data'][-1]['id']
        url = f'{transactions_url}&starting_after={next_id}'
        response = requests.get(url, headers=header)

    account['balance'] = balance
    account['volume'] = volume
    account['payout'] = payout

    return account
    


def get_all_stripe_accounts(api_key):
    batch_size = 10
    base_url = f'https://api.stripe.com/v1/accounts?limit={batch_size}'
    header= {f'Authorization': f'Bearer {api_key}'}
    response = requests.get(base_url, headers=header)

    accounts = []
    while response.status_code == 200:
        account_batch = response.json()
        print(f"bacth size: {len(account_batch['data'])}")
        accounts.extend(account_batch['data'])
        account_batch['has_more'] = False
        if account_batch['has_more'] == False:
            print("No More data from stripe 2")
            break

        next_id = account_batch['data'][-1]['id']
        url = f'{base_url}&starting_after={next_id}'
        print(url)
        response = requests.get(url, headers=header)

    print(accounts)
    return accounts

def save_accounts_to_csv(**kwargs):
    # Retrieve the processed accounts data
    accounts = kwargs['ti'].xcom_pull(task_ids='process_accounts_task', key='processed_accounts')
    print('accounts')
    print(accounts)
    # Normalize JSON data into a flat table
    df = pd.json_normalize(accounts, max_level=1)

    # Select and rename columns
    columns = ['id', 'email', 'metadata.pos_account_id', 'created', 'type', 
               'metadata.shore_application', 'default_currency', 'balance', 
               'volume', 'payout', 'object', 'charges_enabled', 
               'details_submitted', 'payouts_enabled']
    df = df[columns]

    # Convert the 'created' column to datetime
    df['created'] = pd.to_datetime(df['created'], unit="s")

    print(df.columns)

    # Save to CSV
    csv_file_name = GCS_OBJECT_NAME
    df.to_csv(csv_file_name, index=False)
    print(f"Saved DataFrame to {csv_file_name}")




# Initialize the DAG
with DAG(
    'stripe_accounts',
    description='Stripe accounts daily',
    schedule_interval='10 9 * * *', # This sets the DAG to run once a day
    start_date=datetime(2024, 1, 1),  # Specify the start date: Year, Month, Day
    catchup=False,  # This determines whether to catch up if the DAG was not run for past intervals
    tags=['stripe'],  # Add relevant tags here
) as dag:

    
    def get_accounts(**kwargs):
        api_key = kwargs.get('api_key', API_KEY)
        accounts = get_all_stripe_accounts(api_key)
        kwargs['ti'].xcom_push(key='accounts', value=accounts)

    def process_accounts(**kwargs):
        api_key = kwargs.get('api_key', API_KEY)
        accounts = kwargs['ti'].xcom_pull(task_ids='get_accounts_task', key='accounts')
        processed_accounts = []
        for account in accounts:
            #add_balance_volume_to_account_(account, api_key)
            processed_account = add_balance_volume_to_account_(account, api_key)
            processed_accounts.append(processed_account)

        kwargs['ti'].xcom_push(key='processed_accounts', value=processed_accounts)

    get_accounts_task = PythonOperator(
        task_id='get_accounts_task',
        python_callable=get_accounts,
        op_kwargs={'api_key': API_KEY},
        dag=dag,
    )

    process_accounts_task = PythonOperator(
        task_id='process_accounts_task',
        python_callable=process_accounts,
        op_kwargs={'api_key': API_KEY},
        dag=dag,
    )

    save_accounts_to_csv_task = PythonOperator(
        task_id='save_accounts_to_csv_task',
        python_callable=save_accounts_to_csv,
        dag=dag,
    )

    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src=GCS_OBJECT_NAME,  
        dst=GCS_OBJECT_NAME,  
        bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
        dag=dag,
    )

    (
        get_accounts_task 
        >> process_accounts_task 
        >> save_accounts_to_csv_task 
        >> upload_to_gcs_task 
    )
        
