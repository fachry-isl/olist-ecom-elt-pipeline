import os
import logging
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup, RenderConfig


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

FILES = [
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
]

# ─── Helper: Create DBT TaskGroup ───────────────────────────────────────
def create_dbt_task_group(group_id, select_path):
    return DbtTaskGroup(
        group_id=group_id,
        project_config=ProjectConfig(PROJECT_DIR),
        render_config=RenderConfig(select=[select_path]),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)
    )


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    # Ensure 'datasets/parquet/' directory exists
    parquet_dir = os.path.join(os.path.dirname(src_file), "parquet")
    os.makedirs(parquet_dir, exist_ok=True)

    # Generate target Parquet path
    file_name = os.path.basename(src_file).replace(".csv", ".parquet")
    parquet_file = os.path.join(parquet_dir, file_name)

    table = pv.read_csv(src_file)
    pq.write_table(table, parquet_file)

    print(f"Converted to Parquet: {parquet_file}")



def check_file_exists(file_path):
    if os.path.isfile(file_path):
        print(f"File exists: {file_path}")
        return True
    else:
        print(f"File does NOT exist: {file_path}")
        return False

    

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="ecom_main_dag",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    with TaskGroup(group_id="extract_load_tasks") as extract_load_group:
        
        for file in FILES:
            
            check_file_task = PythonOperator(
                task_id=f"check_local_{file}",
                python_callable=check_file_exists,
                op_kwargs={"file_path": f"{AIRFLOW_HOME}/datasets/{file}.csv"}
            )

            convert_to_parquet_task = PythonOperator(
                task_id=f"convert_to_parquet_{file}",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{AIRFLOW_HOME}/datasets/{file}.csv",
                }
            )
            
            local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_{file}",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{file}/{file}.parquet",
                "local_file": f"{AIRFLOW_HOME}/datasets/parquet/{file}.parquet",
                }
            )         
            
            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                task_id=f"bigquery_external_table_{file}_task",
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": BIGQUERY_DATASET,
                        "tableId": file.replace("olist_", "").replace("_dataset", ""),
                    },
                    "externalDataConfiguration": {
                        "sourceFormat": "PARQUET",
                        "sourceUris": [f"gs://{BUCKET}/raw/{file}/{file}.parquet"],
                    },
                },
            )
            
            # Set task dependencies
            check_file_task >> convert_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task



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
    extract_load_group >> dbt_staging >> dim_modeling >> fact_modeling >> generate_dbt_docs
    

