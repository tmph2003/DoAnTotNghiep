import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import ProfileConfig
from cosmos.operators import DbtDocsS3Operator

from plugins.config import config

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_EXECUTABLE_PATH = f"/home/airflow/.local/bin/dbt"
local_tz = pendulum.timezone(config.DWH_TIMEZONE)

profile_name = "chatwoot"
project_dir = f"{DBT_ROOT_PATH}/{profile_name}"
profile_config = ProfileConfig(
    profile_name=profile_name,
    target_name="lakehouse",
    profiles_yml_filepath=f"{DBT_ROOT_PATH}/profiles.yml"
)

default_args = {
    "email": ["tmph2003@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retry_delay": timedelta(minutes=5),
    "schedule_interval": None
}

with DAG(
        dag_id="gen_dbt_docs",
        default_args=default_args,
        catchup=False,
) as dag:
    start_gen = EmptyOperator(task_id="start_gen_docs")

    generate_dbt_docs_aws = DbtDocsS3Operator(
        task_id="generate_dbt_docs_minio",
        project_dir=project_dir,
        profile_config=profile_config,
        install_deps=True,
        connection_id="minio",
        bucket_name="dbt-docs"
    )

    end_gen = EmptyOperator(task_id="end_gen_docs")

    start_gen >> generate_dbt_docs_aws >> end_gen
