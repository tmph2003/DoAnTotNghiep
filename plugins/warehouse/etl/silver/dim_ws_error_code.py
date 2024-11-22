from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper
import pandas as pd


def build_dim_ws_error_code(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_ws_error_code...')
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_ws_error_code"

    data = pd.read_csv("include//csv//error_code.csv", header=1)
    rows = [tuple(row) for row in data.itertuples(index=False, name=None)]

    return trino.executemany(f'''
        INSERT INTO {dest_table_id} (code, error_type, description)
        VALUES (?, ?, ?)
    ''', rows=rows)
