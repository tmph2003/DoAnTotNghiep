DIM_WS_ERROR_CODE = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_ws_error_code
(
    code           INTEGER,
    error_type     VARCHAR,
    description    VARCHAR
)
"""
