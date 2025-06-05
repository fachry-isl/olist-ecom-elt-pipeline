import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

DBT_PROFILE_PATH = os.environ.get("DBT_PROFILE_PATH")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# FILES = [
#     "olist_customers_dataset",
#     "olist_geolocation_dataset",
#     "olist_order_items_dataset",
#     "olist_order_payments_dataset",
#     "olist_order_reviews_dataset",
#     "olist_orders_dataset",
#     "olist_products_dataset",
#     "olist_sellers_dataset",
#     "product_category_name_translation"
# ]

FILES = [
    "olist_customers_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
]

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

profile_config = ProfileConfig(
    profile_name="dbt_taxi_profile",
    target_name="dev",

    # Use file path to the profiles.yml file
    profiles_yml_filepath="/.dbt/profiles.yml",
)

# NOTE: DAG declaration - using a Context Manager (an implicit way)
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
                op_kwargs={"file_path": f"{path_to_local_home}/datasets/{file}.csv"}
            )

            convert_to_parquet_task = PythonOperator(
                task_id=f"convert_to_parquet_{file}",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": f"{path_to_local_home}/datasets/{file}.csv",
                }
            )
            
            local_to_gcs_task = PythonOperator(
            task_id=f"local_to_gcs_{file}",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{file}/{file}.parquet",
                "local_file": f"{path_to_local_home}/datasets/parquet/{file}.parquet",
                }
            )

            check_file_task >> convert_to_parquet_task >> local_to_gcs_task
