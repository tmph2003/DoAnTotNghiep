FCT_WS_TEMPLATE_STATUS_DAILY = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.fct_ws_template_status_daily (
    ws_template_id   VARCHAR(20),
    num_sent        INTEGER,
    num_delivered    INTEGER,
    num_read         INTEGER,
    num_failed_due_sys  INTEGER,
    num_failed_due_ws   INTEGER,
    total_cost       DOUBLE,
    date_id          INTEGER,
    etl_inserted     TIMESTAMP
)
"""
