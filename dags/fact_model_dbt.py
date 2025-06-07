from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from datetime import datetime

import os


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")

# Configure your dbt project
profile_config = ProfileConfig(
    profile_name="dbt_olist_ecom_profile",
    target_name="dev",

    # Use file path to the profiles.yml file
    profiles_yml_filepath="/.dbt/profiles.yml",
)


#Create DAG with specific model selection
dbt_build_dag = DbtDag(
    dag_id="fact_table_only",
    project_config=ProjectConfig(
            f"{os.environ['AIRFLOW_HOME']}/dags/dbt",
        ),
    profile_config=profile_config,
    # Run only the fact table model
    render_config=RenderConfig(
        select=["fact_order_sales"],
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    )
)

