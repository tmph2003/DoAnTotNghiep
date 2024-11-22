from plugins.warehouse.common.trino_helper import TrinoHelper
from plugins.warehouse.etl.ddl import *


def run(trino: TrinoHelper):
    # Run DDL for dim tables
    trino.execute(DIM_USER)
    trino.execute(DIM_ACCOUNT)
    trino.execute(DIM_ACCOUNT_USER)
    trino.execute(DIM_CONTACT)
    trino.execute(DIM_INBOX)
    trino.execute(DIM_CAMPAIGN)
    trino.execute(DIM_CHANNEL_WHATSAPP)
    trino.execute(DIM_WS_TEMPLATE)
    trino.execute(DIM_WS_ERROR_CODE)
    trino.execute(DIM_DATE)
    trino.execute(DIM_WS_TEMPLATE_PRICE)

    # Run DDL for fact tables
    trino.execute(FCT_MESSAGE)
    trino.execute(FCT_CONVERSATION)
    trino.execute(FCT_ACTIVE_USERS_DAILY)
    trino.execute(FCT_ACTIVE_USERS_DAILY_DWH)
    trino.execute(FCT_WS_TEMPLATE_STATUS_DAILY)
    trino.execute(FCT_WS_TEMPLATE_STATUS_DAILY_DWH)
    trino.execute(FCT_CAMPAIGN_TRANSACTION)
    trino.execute(FCT_CAMPAIGN_AGG)
    trino.execute(FCT_ACCOUNT_AGG)

    # Run DDL for cube tables
    trino.execute(CUBE_ACCOUNT)
    trino.execute(CUBE_ACCOUNT_BI)
    trino.execute(CUBE_CAMPAIGN_TRANSACTION)
    trino.execute(CUBE_CAMPAIGN_TRANSACTION_BI)
