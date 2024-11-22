import logging
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from plugins.config import config
from plugins.warehouse.common.data_helper import DEFAULT_DATETIME_FORMAT
from plugins.warehouse.common.db_helper import DatabaseConnection
from plugins.warehouse.common.trino_helper import TrinoHelper
from plugins.warehouse.etl.bronze.extractor import SourceDataset, TrinoExtractor
from plugins.utils.notifier import TelegramNotifier

local_tz = pendulum.timezone(config.DWH_TIMEZONE)
notifier = TelegramNotifier(
    chat_id=config.TG_CHATWOOT_CHAT_ID,
    bot_token=config.TG_CHATWOOT_BOT_TOKEN
)

CHATWOOT_SOURCES = Variable.get('CHATWOOT_SOURCES', default_var=[], deserialize_json=True)
logger = logging.getLogger('airflow.task')
trino_db = DatabaseConnection(
    db_type='trino',
    catalog=config.LH_CHATWOOT_CATALOG,
    schema=config.LH_CHATWOOT_SCHEMA
)
trino_cli = TrinoHelper(logger=logger, client=trino_db)


def _unsure_ddl():
    trino_cli.execute('CREATE SCHEMA IF NOT EXISTS iceberg.hebela_chat')


def _extract_data(ts, **kwargs):
    current_ts = ts.replace('T', ' ')[:18]  # timezone: utc+0, source_db: utc+0
    partition_value = (datetime.strptime(current_ts, DEFAULT_DATETIME_FORMAT) + timedelta(
        hours=7)).date()  # usually from orchestrator -%S

    source_datasets = []
    for table_name, ingestion_info in CHATWOOT_SOURCES.items():
        audit_latest_value = ingestion_info.get('audit_prev_latest_value', None)
        if not audit_latest_value:
            audit_latest_value = ingestion_info.get('audit_latest_value', None)

        source_datasets.append(
            SourceDataset(
                catalog=config.S_CHATWOOT_CATALOG,
                schema=config.S_CHATWOOT_SCHEMA,
                table_name=table_name,
                ingest_type=ingestion_info.get('ingest_type', 'snapshot'),
                only_latest=bool(ingestion_info.get('only_latest', 0)),
                audit_column=ingestion_info.get('audit_column', 'updated_at'),
                # Lấy của partition trước theo ngày (TODO: làm một cái parse cái crontab scheduler của từng bảng).
                audit_latest_value=audit_latest_value,
                partition_value=partition_value,
                skip_publish=False,
                replace_partition=True
            )
        )

    trino_extractor = TrinoExtractor(
        logger=logger,
        trino=trino_cli,
        source_datasets=source_datasets,
        execution_datetime=current_ts
    )
    trino_extractor.run()


default_args = {
    'owner': 'lam.nguyen3',
    'depends_on_past': False,
    'trigger_rule': 'all_done',  # https://marclamberti.com/blog/airflow-trigger-rules-all-you-need-to-know/
    'email': ['tmph2003@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': notifier.notify,
    'retries': 0 if config.ENV == "prod" else 0,
    'retry_delay': timedelta(minutes=2)
}
with DAG(
        dag_id='dags-HC-to-Lakehouse',
        default_args=default_args,
        description="Extract data from Chatwoot to Lakehouse's bronze",
        start_date=datetime(2024, 1, 1, 0, tzinfo=local_tz),
        max_active_runs=1,
        schedule="2/20 * * * *" if config.ENV == "prod" else None,
        catchup=False,
        tags=["hebela_chat", "bronze"]
) as dag:
    start = EmptyOperator(task_id="start_ingestion")
    end = EmptyOperator(task_id="end_ingestion")

    ensure_ddl = PythonOperator(
        task_id=f"pre-check_ddl",
        python_callable=_unsure_ddl,
        provide_context=True
    )

    with TaskGroup('Hebela_Chat', tooltip=f"Ingest all tables for reporting") as ingest_group:
        extract = PythonOperator(
            task_id=f"extract",
            python_callable=_extract_data,
            provide_context=True,
            op_kwargs={'ts': '{{ ts }}'}
        )

        extract

    start >> ensure_ddl >> ingest_group >> end
