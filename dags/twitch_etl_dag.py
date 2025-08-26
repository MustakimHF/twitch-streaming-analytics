from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Our scripts live inside the container at /opt/airflow/dags/scripts
SCRIPTS = "/opt/airflow/dags/scripts"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="twitch_streaming_etl_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",   # run once per day; change to "*/30 * * * *" for every 30 mins
    catchup=False,
    default_args=default_args,
    tags=["twitch","etl"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"python {SCRIPTS}/extract_twitch.py"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"python {SCRIPTS}/transform.py"
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"python {SCRIPTS}/load_db.py"
    )

    extract >> transform >> load
