import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging import Logger
from threading import Lock
from typing import Optional, List, Dict

from airflow.models import Variable

from plugins.config import config
from plugins.warehouse.common.data_helper import DEFAULT_DATETIME_FORMAT
from plugins.warehouse.common.schema_helper import convert_trino_to_trino_col_type, TRINO_DATA_TYPES
from plugins.warehouse.common.trino_helper import TrinoHelper


@dataclass
class SourceDataset:
    catalog: str
    schema: str
    table_name: str
    ingest_type: str
    only_latest: bool
    audit_column: str
    audit_latest_value: str
    partition_value: str
    skip_publish: bool = False
    replace_partition: bool = False


class TrinoExtractor:
    def __init__(
            self,
            logger: Logger,
            trino: TrinoHelper,
            source_datasets: List[SourceDataset],
            execution_datetime: str
    ):
        self.logger = logger
        self.trino = trino
        self.source_datasets = source_datasets
        self.execution_datetime = execution_datetime

    def pre_check(self, source_ds: SourceDataset):
        # Source tables
        self.logger.debug(f'Pre check for {source_ds.catalog}.{source_ds.schema}.{source_ds.table_name}...')
        source_table_id = f'{source_ds.catalog}.{source_ds.schema}.{source_ds.table_name}'
        if self.trino.check_table_is_exist(source_table_id):
            source_table_schema = self.trino.execute(f"DESCRIBE {source_table_id}")
        else:
            self.logger.debug(f'[IGNORE] Source table: {source_table_id} not exists...')
            return False

        # Destination tables
        dest_table_id = f'{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.{source_ds.table_name}'
        if not self.trino.check_table_is_exist(dest_table_id):
            self.logger.debug(f'Destination table: {source_table_id} not exists...')
            dest_columns = [{'name': column[0], 'type': convert_trino_to_trino_col_type(column[1])} for column
                            in source_table_schema]
            dest_columns.extend(
                [{'name': 'partition_value', 'type': 'DATE'}, {'name': 'etl_inserted', 'type': 'TIMESTAMP(6)'}])
            self.trino.create_table(
                table_id=dest_table_id,
                columns=dest_columns,
                ingest_type=source_ds.ingest_type,
                audit_column=source_ds.audit_column
            )
        else:
            dest_table_schema = self.trino.execute(f"DESCRIBE {dest_table_id}")
            self.trino.resolve_schema_evolution(source_table_schema, dest_table_schema, dest_table_id)

        return True

    def extract_table(self, source_ds: SourceDataset, excepted_columns: Optional[List[str]]):
        source_table_id = f'{source_ds.catalog}.{source_ds.schema}.{source_ds.table_name}'
        dest_table_id = f'{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.{source_ds.table_name}'
        self.logger.debug(f'Extract {source_table_id} from Trino...')

        source_table_schema = self.trino.execute(f"DESCRIBE {source_table_id}")
        dest_table_schema = self.trino.execute(f"DESCRIBE {dest_table_id}")

        accepted_column_names = [item[0] for item in source_table_schema if item[0] not in excepted_columns]
        dest_columns = [(item[0], item[1]) for item in dest_table_schema]
        dest_column_names = [item[0] for item in dest_table_schema]

        temp = []
        for col_name, col_type in dest_columns:
            value = None
            if col_name == 'partition_value':
                value = f"CAST('{source_ds.partition_value}' AS DATE) AS {col_name}"
            elif col_name == 'etl_inserted':
                value = f"(current_timestamp at time zone 'Asia/Bangkok') AS {col_name}"
            else:
                if col_name in accepted_column_names:
                    value = col_name
                    if col_type in ['timestamp(6)']:
                        value = f"({col_name}) AS {col_name}"
                else:
                    value = f"NULL AS {col_name}"
            temp.append(value)

        # Map data type cho cột trong INSERT
        source_columns = [{'name': column[0], 'type': column[1].upper()} for column in source_table_schema]
        for column in source_columns:
            for index in range(0, len(temp)):
                if column['name'] == temp[index] and column['type'] not in TRINO_DATA_TYPES:
                    if column['type'] == 'JSON':
                        temp[index] = f'JSON_FORMAT({temp[index]}) AS {temp[index]}'
                    else:
                        temp[index] = f'CAST({temp[index]} AS VARCHAR) AS {temp[index]}'

        select_sql = f"SELECT {', '.join(temp)} FROM {source_table_id}"
        condition_sql = "t.id = s.id"
        matched_condition_sql = 'TRUE'

        if source_ds.ingest_type == 'snapshot':
            self.trino.execute(f"TRUNCATE TABLE {dest_table_id}")
        elif source_ds.ingest_type == 'incremental':
            matched_condition_sql = f"t.{source_ds.audit_column} <> s.{source_ds.audit_column}"
            if not source_ds.only_latest:
                condition_sql = f'{condition_sql} AND t.partition_value = s.partition_value'

            select_sql = f"{select_sql} WHERE {source_ds.audit_column} <= CAST('{self.execution_datetime}' AS timestamp)"
            select_sql += f" AND {source_ds.audit_column} > CAST('{source_ds.audit_latest_value}' AS timestamp)" if source_ds.audit_latest_value else ''

        return self.trino.upsert_table(
            dest_table_id=dest_table_id,
            dest_columns=dest_column_names,
            source_sql=select_sql,
            matched_condition_sql=matched_condition_sql,
            condition_sql=condition_sql,
        )

    def extract_task(self, source_ds: SourceDataset, PROJ_SOURCES: Dict, lock: Lock):
        self.logger.info("#####################################")
        self.logger.info(f"Extract {source_ds.catalog}.{source_ds.schema}.{source_ds.table_name} ...")
        valid = self.pre_check(source_ds=source_ds)

        if valid:
            result = self.extract_table(
                source_ds=source_ds,
                excepted_columns=[]
            )

            if result is None and source_ds.ingest_type == 'incremental':
                self.logger.info(
                    f'Update ingestion infor for {source_ds.table_name} with ingest_type: {source_ds.ingest_type}')
                # Save audit_latest_value from execution_datetime
                ingestion_info = PROJ_SOURCES.get(source_ds.table_name, {})
                audit_latest_value = ingestion_info.get('audit_latest_value', None)

                latest_partition = None
                if audit_latest_value:
                    audit_latest_datetime = datetime.strptime(audit_latest_value, DEFAULT_DATETIME_FORMAT) + timedelta(
                        hours=7)
                    latest_partition = audit_latest_datetime.date()

                if latest_partition != source_ds.partition_value:
                    ingestion_info['audit_prev_latest_value'] = audit_latest_value
                ingestion_info['audit_latest_value'] = self.execution_datetime

                with lock:
                    PROJ_SOURCES[source_ds.table_name] = ingestion_info

    def run(self):
        CHATWOOT_SOURCES = Variable.get('CHATWOOT_SOURCES', default_var=[], deserialize_json=True)
        lock = threading.Lock()

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.extract_task, source_ds, CHATWOOT_SOURCES, lock) for source_ds in
                       self.source_datasets]
            for future in as_completed(futures):
                future.result()

        Variable.set("CHATWOOT_SOURCES", CHATWOOT_SOURCES, serialize_json=True)
