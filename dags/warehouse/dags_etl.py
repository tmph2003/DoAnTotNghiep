import logging
import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.config import config
from plugins.warehouse.common.db_helper import DatabaseConnection
from plugins.warehouse.common.trino_helper import TrinoHelper
from plugins.warehouse.etl.ddl.run_ddl import run
from plugins.warehouse.etl.chatwoot import ETL

local_tz = pendulum.timezone(config.DWH_TIMEZONE)

logger = logging.getLogger('airflow.task')
trino_db = DatabaseConnection(
    db_type='trino',
    catalog=config.LH_CHATWOOT_CATALOG,
    schema=config.LH_CHATWOOT_SCHEMA
)
trino_cli = TrinoHelper(logger=logger, client=trino_db)


def _unsure_ddl():
    run(trino=trino_cli)


def _transform_data():
    hc_etl = ETL(logger=logger, trino=trino_cli)
    hc_etl.transform()


default_args = {
    'owner': 'tmph2003',
    'depends_on_past': False,
    'trigger_rule': 'all_success',  # https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['tmph2003@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='dags-ETL',
        default_args=default_args,
        description="Run ETL for Chatwoot",
        start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
        max_active_runs=1,
        schedule="10/20 * * * *" ,
        catchup=False,
        tags=["chatwoot", "silver"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup('Chatwoot', tooltip=f"Build dim/fact for Chatwoot") as etl_group:
        ensure_ddl = PythonOperator(
            task_id=f"pre-check_ddl",
            python_callable=_unsure_ddl,
            provide_context=True
        )

        transform = PythonOperator(
            task_id=f"transform",
            python_callable=_transform_data
        )

        ensure_ddl >> transform

    start >> etl_group >> end
