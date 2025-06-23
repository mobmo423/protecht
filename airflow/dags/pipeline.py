from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))

EXTRACT_PATH = os.path.join(BASE_DIR, 'extract.py')
TRANSFORM_PATH = os.path.join(BASE_DIR, 'transform.py')
UPLOAD_PATH = os.path.join(BASE_DIR, 'upload_to_s3.py')

with DAG(
    dag_id='daily_currency_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for currency metrics',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 7 * * *',  # Cron format: At 07:00 AM daily
    catchup=False,
    tags=['etl', 'currency'],
) as dag:

    extract_task = BashOperator(
        task_id='extract_raw_data',
        bash_command=f'python {EXTRACT_PATH}',
    )

    transform_task = BashOperator(
        task_id='transform_currency_models',
        bash_command=f'python {TRANSFORM_PATH}',
    )

    upload_task = BashOperator(
        task_id='upload_to_s3',
        bash_command=f'python {UPLOAD_PATH}',
    )

    extract_task >> transform_task >> upload_task
