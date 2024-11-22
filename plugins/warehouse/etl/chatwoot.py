from logging import Logger

from plugins.warehouse.common.trino_helper import TrinoHelper
from plugins.warehouse.etl.silver import *


class HebelaChatETL:
    def __init__(self, logger: Logger, trino: TrinoHelper):
        self.logger = logger
        self.trino = trino

    def transform(self):
        # dim
        build_dim_inbox(logger=self.logger, trino=self.trino)
        build_dim_contact(logger=self.logger, trino=self.trino)
        build_dim_user(logger=self.logger, trino=self.trino)
        build_dim_account(logger=self.logger, trino=self.trino)
        build_dim_account_user(logger=self.logger, trino=self.trino)
        build_dim_ws_template(logger=self.logger, trino=self.trino)
        build_dim_campaign(logger=self.logger, trino=self.trino)
        build_dim_channel_whatsapp(logger=self.logger, trino=self.trino)

        # fact
        build_fct_message(logger=self.logger, trino=self.trino)
        build_fct_conversation(logger=self.logger, trino=self.trino)
        # build_fct_ws_template_status_daily(logger=self.logger, trino=self.trino)
        build_fct_campaign_transaction(logger=self.logger, trino=self.trino)
        build_fct_account_agg(logger=self.logger, trino=self.trino)
        build_fct_campaign_agg(logger=self.logger, trino=self.trino)

        # DWH  # build_fct_ws_template_status_daily_dwh(logger=self.logger, trino=self.trino)

    def once_time(self):
        build_dim_ws_template_price(logger=self.logger, trino=self.trino)
        build_dim_ws_error_code(logger=self.logger, trino=self.trino)
        build_dim_date(logger=self.logger, trino=self.trino)
        build_fct_active_users_daily(logger=self.logger, trino=self.trino)
