from plugins.warehouse.etl.ddl.dim_account import DIM_ACCOUNT
from plugins.warehouse.etl.ddl.dim_account_user import DIM_ACCOUNT_USER
from plugins.warehouse.etl.ddl.dim_campaign import DIM_CAMPAIGN
from plugins.warehouse.etl.ddl.dim_channel_whatsapp import DIM_CHANNEL_WHATSAPP
from plugins.warehouse.etl.ddl.dim_contact import DIM_CONTACT
from plugins.warehouse.etl.ddl.dim_date import DIM_DATE
from plugins.warehouse.etl.ddl.dim_inbox import DIM_INBOX
from plugins.warehouse.etl.ddl.dim_user import DIM_USER
from plugins.warehouse.etl.ddl.dim_ws_error_code import DIM_WS_ERROR_CODE
from plugins.warehouse.etl.ddl.dim_ws_template import DIM_WS_TEMPLATE
from plugins.warehouse.etl.ddl.dim_ws_template_price import DIM_WS_TEMPLATE_PRICE
from plugins.warehouse.etl.ddl.fct_account_agg import FCT_ACCOUNT_AGG
from plugins.warehouse.etl.ddl.fct_active_users_cumulated_dwh import FCT_ACTIVE_USERS_CUMULATED_DWH
from plugins.warehouse.etl.ddl.fct_active_users_daily import FCT_ACTIVE_USERS_DAILY
from plugins.warehouse.etl.ddl.fct_active_users_daily_dwh import FCT_ACTIVE_USERS_DAILY_DWH
from plugins.warehouse.etl.ddl.fct_campaign_agg import FCT_CAMPAIGN_AGG
from plugins.warehouse.etl.ddl.fct_campaign_transaction import FCT_CAMPAIGN_TRANSACTION
from plugins.warehouse.etl.ddl.fct_conversation import FCT_CONVERSATION
from plugins.warehouse.etl.ddl.fct_message import FCT_MESSAGE
from plugins.warehouse.etl.ddl.fct_ws_template_status_daily import FCT_WS_TEMPLATE_STATUS_DAILY
from plugins.warehouse.etl.ddl.fct_ws_template_status_daily_dwh import FCT_WS_TEMPLATE_STATUS_DAILY_DWH
from plugins.warehouse.etl.ddl.cube_account import CUBE_ACCOUNT, CUBE_ACCOUNT_BI
from plugins.warehouse.etl.ddl.cube_campaign_transaction import CUBE_CAMPAIGN_TRANSACTION, CUBE_CAMPAIGN_TRANSACTION_BI
