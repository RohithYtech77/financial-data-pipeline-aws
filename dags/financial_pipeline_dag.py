from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'rohith',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract():
    print("Extracting financial data from S3...")

def transform():
    print("Transforming data with PySpark...")

def load():
    print("Loading into Snowflake...")

with DAG(
    dag_id='financial_data_pipeline',
    default_args=default_args,
    description='Daily financial data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    transform_task = PythonOperator(task_id='transform', python_callable=transform)
    load_task = PythonOperator(task_id='load', python_callable=load)

    extract_task >> transform_task >> load_task
