from plugins.warehouse.etl.silver.dim_account import build_dim_account
from plugins.warehouse.etl.silver.dim_account_user import build_dim_account_user
from plugins.warehouse.etl.silver.dim_campaign import build_dim_campaign
from plugins.warehouse.etl.silver.dim_channel_whatsapp import build_dim_channel_whatsapp
from plugins.warehouse.etl.silver.dim_contact import build_dim_contact
from plugins.warehouse.etl.silver.dim_date import build_dim_date
from plugins.warehouse.etl.silver.dim_inbox import build_dim_inbox
from plugins.warehouse.etl.silver.dim_user import build_dim_user
from plugins.warehouse.etl.silver.dim_ws_error_code import build_dim_ws_error_code
from plugins.warehouse.etl.silver.dim_ws_template import build_dim_ws_template
from plugins.warehouse.etl.silver.dim_ws_template_price import build_dim_ws_template_price
from plugins.warehouse.etl.silver.fct_account_agg import build_fct_account_agg
from plugins.warehouse.etl.silver.fct_active_users_daily_dwh import build_fct_active_users_daily_dwh
from plugins.warehouse.etl.silver.fct_active_users_cumulated_dwh import build_fct_active_users_cumulated_dwh
from plugins.warehouse.etl.silver.fct_active_users_daily import build_fct_active_users_daily
from plugins.warehouse.etl.silver.fct_conversation import build_fct_conversation
from plugins.warehouse.etl.silver.fct_message import build_fct_message
