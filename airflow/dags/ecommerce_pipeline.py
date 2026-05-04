from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess

PROJECT_PATH = "/mnt/c/Users/motir/Desktop/E-commerce Project"

# ---------- TASK FUNCTIONS ----------

def run_bronze():
    subprocess.run(
        ["python3", f"{PROJECT_PATH}/src/bronze/main.py"],
        check=True
    )

def run_silver():
    subprocess.run(
        ["python3", f"{PROJECT_PATH}/src/silver/main.py"],
        check=True
    )

def run_quality():
    subprocess.run(
        ["python3", f"{PROJECT_PATH}/src/quality_layer/main.py"],
        check=True
    )

def run_gold():
    subprocess.run(
        ["python3", f"{PROJECT_PATH}/src/gold/main.py"],
        check=True
    )

# ---------- DAG CONFIG ----------

default_args = {
    "owner": "Tushar",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    start = EmptyOperator(task_id="start")

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze
    )

    silver = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver
    )

    quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_quality
    )

    gold = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold
    )

    end = EmptyOperator(task_id="end")

    # ---------- FLOW ----------
    start >> bronze >> silver >> quality >> gold >> end