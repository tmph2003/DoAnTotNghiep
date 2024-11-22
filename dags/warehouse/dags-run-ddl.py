from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from plugins.config import config
from plugins.utils.notifier import TelegramNotifier
from plugins.warehouse.common.db_helper import DatabaseConnection
from plugins.warehouse.common.logging_helper import LoggingHelper
from plugins.warehouse.common.trino_helper import TrinoHelper
from plugins.warehouse.etl.ddl.run_ddl import run

logger = LoggingHelper.get_configured_logger("hebela_chat.extraction")
trino_db = DatabaseConnection(
    db_type='trino',
    catalog=config.LH_CHATWOOT_CATALOG,
    schema=config.LH_CHATWOOT_SCHEMA
)
trino_cli = TrinoHelper(logger=logger, client=trino_db)
local_tz = pendulum.timezone(config.DWH_TIMEZONE)
notifier = TelegramNotifier(
    chat_id=config.TG_CHATWOOT_CHAT_ID,
    bot_token=config.TG_CHATWOOT_BOT_TOKEN
)

default_args = {
    'owner': 'lam.nguyen3',
    'depends_on_past': False,
    'trigger_rule': 'all_done',
    'email': ['tmph2003@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': notifier.notify,
    'retries': 1 if config.ENV == "prod" else 0,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='run_ddl',
        default_args=default_args,
        description="Run ddl for Lakehouse",
        start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
        max_active_runs=1,
        schedule_interval="@once" if config.ENV == "prod" else None,
        catchup=False,
        tags=["lakehouse", "ddl"]
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    run_ddl = PythonOperator(
        task_id=f"run_ddl",
        python_callable=run,
        provide_context=True,
        op_kwargs={'trino': trino_cli}
    )

    start >> run_ddl >> end
