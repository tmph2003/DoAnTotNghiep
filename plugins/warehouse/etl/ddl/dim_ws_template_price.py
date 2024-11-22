DIM_WS_TEMPLATE_PRICE = """
CREATE TABLE IF NOT EXISTS iceberg.chatwoot.dim_ws_template_price (           
    market VARCHAR,                                                 
    currency VARCHAR,                                               
    marketing DOUBLE,                                               
    utility DOUBLE,                                                 
    authentication DOUBLE,                                          
    authentication_international DOUBLE,                            
    service DOUBLE,                                                 
    phone_code VARCHAR                                              
)
"""
