FCT_WS_TEMPLATE_STATUS_DAILY_DWH = """
    CREATE TABLE IF NOT EXISTS clickhouse.iceberg.chatwoot..fct_ws_template_status_daily (
        ws_template_id   VARCHAR(20) NOT NULL,
        date_id          INTEGER NOT NULL,
        month_id         INTEGER NOT NULL,
        num_sent        INTEGER,
        num_delivered    INTEGER,
        num_read         INTEGER,
        num_failed_due_sys  INTEGER,
        num_failed_due_ws   INTEGER,
        total_cost       DOUBLE,
        etl_inserted     TIMESTAMP(0)
    )
    WITH (
        engine = 'MergeTree',
        order_by = ARRAY['ws_template_id', 'date_id'],
        partition_by = ARRAY['month_id'],
        primary_key = ARRAY['ws_template_id', 'date_id']
    )
"""
