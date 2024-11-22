import pandas as pd

from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_ws_template_price(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_ws_template_price...')
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_ws_template_price"

    data = pd.read_csv("include//csv//dim_ws_template_price.csv", header=1)
    rows = [tuple(row) for row in data.itertuples(index=False, name=None)]

    return trino.executemany(f'''
        INSERT INTO {dest_table_id} (market, currency, marketing, utility, authentication, authentication_international, service, phone_code)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', rows=rows)
