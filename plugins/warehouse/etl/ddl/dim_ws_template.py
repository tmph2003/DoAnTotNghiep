DIM_WS_TEMPLATE = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_ws_template (
    id VARCHAR(20),
    name VARCHAR(20),
    status VARCHAR(20),
    category VARCHAR(20),
    language VARCHAR(20),
    header_type VARCHAR(10)
)
"""
