from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "personne3",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_init():
    subprocess.run(["python", "/opt/airflow/dags/scripts/init_postgres.py"], check=True)

def run_load():
    subprocess.run(["python", "/opt/airflow/dags/scripts/load_to_postgres.py"], check=True)

def run_gold():
    subprocess.run(["python", "/opt/airflow/dags/scripts/compute_gold.py"], check=True)

def run_quality():
    subprocess.run(["python", "/opt/airflow/dags/scripts/quality_checks.py"], check=True)

with DAG(
    dag_id="dag_dw_refresh",
    default_args=default_args,
    description="Chargement et refresh du Data Warehouse",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="init_schema",    python_callable=run_init)
    t2 = PythonOperator(task_id="load_bronze",    python_callable=run_load)
    t3 = PythonOperator(task_id="compute_gold",   python_callable=run_gold)
    t4 = PythonOperator(task_id="quality_checks", python_callable=run_quality)

    t1 >> t2 >> t3 >> t4