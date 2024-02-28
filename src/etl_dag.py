from datetime import timedelta, datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
import subprocess


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['leeningyu@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_python_script():
    script_path = Variable.get('path')
    subprocess.run(["python3", script_path], check=True)


dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='An ETL job for my project.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 29),
    catchup=False,
)


run_etl_script = PythonOperator(
    task_id='run_etl_script',
    python_callable=run_python_script,
    dag=dag,
)

run_etl_script
