from airflow.operators.bash import BashOperator
from airflow import DAG

import os
from datetime import datetime

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")



# Define the DAG
with DAG(
    dag_id="generate_dbt_docs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Only run manually
    catchup=False,
    tags=["example"],
) as dag:

    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt docs generate "
            f"--project-dir {os.path.join(AIRFLOW_HOME, 'dags', 'dbt')} "
            f"--profiles-dir /.dbt "
            f"--target dev"
        )
    )

    generate_dbt_docs