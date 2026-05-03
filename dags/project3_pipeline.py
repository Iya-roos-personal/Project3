from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta


PYTHON = "/home/airflow/.local/bin/python"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="project3_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    description="Project 3 CDC and taxi lakehouse pipeline",
) as dag:

    register_connector = BashOperator(
        task_id="register_connector",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/register_connector.py",
    )

    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="connect",
        endpoint="/connectors/postgres-connector/status",
        response_check=lambda r: (
            r.status_code == 200
            and r.json().get("connector", {}).get("state") == "RUNNING"
            and all(
                task.get("state") == "RUNNING"
                for task in r.json().get("tasks", [])
            )
        ),
        poke_interval=10,
        timeout=90,
        mode="poke",
    )

    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/bronze_cdc.py",
    )

    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/silver_cdc.py",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/validate.py",
    )

    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/bronze_taxi.py",
    )

    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/silver_taxi.py",
    )

    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=f"{PYTHON} /home/jovyan/project/jobs/gold_taxi.py",
    )

    register_connector >> connector_health >> [bronze_cdc, bronze_taxi]

    bronze_cdc >> silver_cdc >> validate
    bronze_taxi >> silver_taxi >> gold_taxi