import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from plugins.config import config
from plugins.warehouse.common.db_helper import DatabaseConnection
from plugins.warehouse.common.trino_helper import TrinoHelper
from plugins.warehouse.etl.maintainance import Maintenance

logger = logging.getLogger('airflow.task')
trino_db = DatabaseConnection(
    db_type='trino',
    catalog=config.LH_CHATWOOT_CATALOG,
    schema=config.LH_CHATWOOT_SCHEMA
)
trino_cli = TrinoHelper(logger=logger, client=trino_db)
local_tz = pendulum.timezone(config.DWH_TIMEZONE)


def maintain_table():
    maintain = Maintenance(logger=logger, trino=trino_cli, schema="chatwoot")
    maintain.run()


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
        dag_id='dags-Maintainance',
        default_args=default_args,
        description="Maintainance for Chatwoot tables",
        start_date=datetime(2024, 1, 1),
        max_active_runs=1,
        schedule_interval="0 2 * * *" ,
        catchup=False,
        tags=["chatwoot", "maintainance"]
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    maintain = PythonOperator(
        task_id=f"maintainance-hc",
        python_callable=maintain_table,
        provide_context=True
    )

    start >> maintain >> end
