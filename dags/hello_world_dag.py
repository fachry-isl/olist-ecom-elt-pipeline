from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function
def hello_world():
    print("Hello, World!")

# Define the DAG
with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Only run manually
    catchup=False,
    tags=["example"],
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=hello_world,
    )

    hello_task
