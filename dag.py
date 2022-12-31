from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['shitsfuckedup@frigoff.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    "twitter-dag",
    default_args=default_args,
    description="demo etl code"
)

run_etl = PythonOperator(
    task_id="twitter_etl",
    python_callable=run_twitter_etl,
    dag=dag
)