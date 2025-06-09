from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Simple function to print hello
def say_hello(branch_name, fail=False):
    if fail:
        raise Exception(f"Simulated failure in {branch_name}")
    print(f"Hello from {branch_name}!")


with DAG(
    dag_id="hello_branch_graph_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    tags=["test", "visual"],
) as dag:

    start = PythonOperator(task_id="start", python_callable=say_hello, op_args=["start"])

    branch_1 = PythonOperator(
        task_id="hello_branch_1",
        python_callable=say_hello,
        op_args=["branch_1"]
    )

    branch_2 = PythonOperator(
        task_id="hello_branch_2",
        python_callable=say_hello,
        op_args=["branch_2"],
        op_kwargs={"fail": True},  # this will simulate a failure
    )

    branch_3 = PythonOperator(
        task_id="hello_branch_3",
        python_callable=say_hello,
        op_args=["branch_3"]
    )

    end = PythonOperator(task_id="end", python_callable=say_hello, op_args=["end"], trigger_rule="all_success")

    start >> [branch_1, branch_2, branch_3] >> end
