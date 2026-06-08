from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Note: Keeping PROJECT_PATH for reference, but using the explicit 
# Windows path syntax inside the bash commands to prevent directory errors.
PROJECT_PATH = "/mnt/c/Users/motir/Desktop/E-commerce Project"

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

    # ---------- STABLE BASH COMMANDS USING .venv311 ----------

    bronze = BashOperator(
        task_id="bronze_ingestion",
        # Navigates to project folder, then forces execution via your .venv311 environment
        bash_command='/mnt/c/Windows/System32/cmd.exe /c "cd /d C:\\Users\\motir\\Desktop\\E-commerce Project && .venv311\\Scripts\\python.exe src\\bronze\\main.py"'
    )
 
    silver = BashOperator(
        task_id="silver_transformation",
        # Solves both the memory/connection issue and the 'yaml' module missing error
        bash_command='/mnt/c/Windows/System32/cmd.exe /c "cd /d C:\\Users\\motir\\Desktop\\E-commerce Project && .venv311\\Scripts\\python.exe src\\silver\\main.py"'
    )

    gold = BashOperator(
        task_id="gold_aggregation",
        bash_command='/mnt/c/Windows/System32/cmd.exe /c "cd /d C:\\Users\\motir\\Desktop\\E-commerce Project && .venv311\\Scripts\\python.exe src\\gold\\main.py"'
    )

    end = EmptyOperator(task_id="end")

    # ---------- FLOW ----------
    start >> bronze >> silver >> gold >> end