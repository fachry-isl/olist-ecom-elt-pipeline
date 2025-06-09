import os
from pathlib import Path
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from datetime import timedelta

from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup, RenderConfig

# ─── Environment Variables ─────────────────────────────────────────────
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
DBT_PROFILE_PATH = os.getenv("DBT_PROFILE_PATH", "/.dbt")
DBT_EXECUTABLE_PATH = f"{AIRFLOW_HOME}/dbt_venv/bin/dbt"
PROJECT_DIR = f"{AIRFLOW_HOME}/dbt"

# Optional: log these if debugging
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "olist_ecom_all")

# ─── Profile Config ─────────────────────────────────────────────────────
profile_config = ProfileConfig(
    profile_name="dbt_olist_ecom_profile",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROFILE_PATH}/profiles.yml",
)

# ─── Default DAG Args ───────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# ─── Helper: Create DBT TaskGroup ───────────────────────────────────────
def create_dbt_task_group(group_id, select_path):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(PROJECT_DIR),
        render_config=RenderConfig(select=[select_path]),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)
    )

# ─── DAG Definition ─────────────────────────────────────────────────────
with DAG(
    dag_id="ecom_dbt_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
    description="Daily DBT transformations for Olist E-commerce data",
) as dag:

    # ── DBT Task Groups ───────────────────────────────────────────────
    dbt_staging = create_dbt_task_group("staging", "path:models/staging")
    dim_modeling = create_dbt_task_group("dim_modeling", "path:models/marts/dim")
    fact_modeling = create_dbt_task_group("fact_modeling", "path:models/marts/fact")

    # ── Generate DBT Docs ─────────────────────────────────────────────
    generate_dbt_docs = BashOperator(
        task_id="generate_dbt_docs",
        bash_command=(
            f"{DBT_EXECUTABLE_PATH} docs generate "
            f"--project-dir {PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILE_PATH} "
            f"--target dev"
        ),
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # ── DAG Dependencies ──────────────────────────────────────────────
    dbt_staging >> dim_modeling >> fact_modeling >> generate_dbt_docs
