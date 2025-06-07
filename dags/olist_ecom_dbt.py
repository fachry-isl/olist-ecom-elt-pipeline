import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator


from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup

import shutil

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

DBT_PROFILE_PATH = os.environ.get("DBT_PROFILE_PATH")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'olist_ecom_all')

FILES = [
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
]

profile_config = ProfileConfig(
    profile_name="dbt_olist_ecom_profile",
    target_name="dev",

    # Use file path to the profiles.yml file
    profiles_yml_filepath="/.dbt/profiles.yml",
)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ecom_dbt_dag",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    dbt_task = DbtTaskGroup(
        group_id="transform_tasks_dbt",
        project_config=ProjectConfig(
            f"{os.environ['AIRFLOW_HOME']}/dags/dbt",
        ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        )
    )
    
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt docs generate "
            f"--project-dir {os.path.join(AIRFLOW_HOME, 'dags', 'dbt')} "
            f"--profiles-dir /.dbt "
            f"--target dev"
        )
    )

    
    dbt_task >> generate_dbt_docs

