import os
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig

from plugins.config import config

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
DBT_EXECUTABLE_PATH = f"/home/airflow/.local/bin/dbt"

project_config = ProjectConfig(
    DBT_ROOT_PATH
)
profile_config = ProfileConfig(
    profile_name="chatwoot",
    target_name="lakehouse",
    profiles_yml_filepath="/app/dags/dbt/chatwoot/profiles.yml"
)
execution_config = ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH)

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

default_args = {
    'owner': 'tmph2003',
    'depends_on_past': False,
    'trigger_rule': 'all_done',  # https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['tmph2003@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='dags-dbt',
        default_args=default_args,
        description="Run ddl for Lakehouse",
        start_date=datetime(2024, 1, 1, tzinfo=local_tz),
        max_active_runs=1,
        schedule_interval="*/10 * * * *",
        catchup=False,
        tags=["chatwoot", "lakehouse", "dbt"]
) as dag:
    start_dbt = EmptyOperator(task_id="start_dbt")
    build_cube = DbtTaskGroup(
        group_id="build_cube",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=[
                "cube_account", "cube_active_users_cumulated", "cube_active_users_today",
                "cube_ws_template_status", "stg_hc_silver__dim_account", "stg_hc_silver__dim_user"],
        ),
        operator_args={
            "install_deps": True
        },
        execution_config=execution_config,
        default_args={"retries": 2},
    )
    end_dbt = EmptyOperator(task_id="end_dbt")

    start_dbt >> build_cube >> end_dbt
