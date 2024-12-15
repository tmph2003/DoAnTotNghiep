import os


class AppConfig(object):
    """
    Access environment variables here.
    """

    def __init__(self):
        pass

    ENV = os.getenv("ENV", "dev")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "debug")

    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = os.getenv("REDIS_PORT", 6379)

    TRINO_HOST = os.getenv("TRINO_HOST", "192.168.1.1")
    TRINO_PORT = os.getenv("TRINO_PORT", 8080)
    TRINO_USER = os.getenv("TRINO_USER", "trino")
    TRINO_PASS = os.getenv("TRINO_PASS", "")

    DWH_TIMEZONE = 'Asia/Ho_Chi_Minh'

    LH_CHATWOOT_CATALOG = os.getenv("LH_CHATWOOT_CATALOG", "iceberg")
    LH_CHATWOOT_SCHEMA = os.getenv("LH_CHATWOOT_SCHEMA", "chatwoot")
    S_CHATWOOT_CATALOG = os.getenv("S_CHATWOOT_CATALOG", "chatwoot")
    S_CHATWOOT_SCHEMA = os.getenv("S_CHATWOOT_SCHEMA", "public")
    WH_CHATWOOT_CATALOG = os.getenv("WH_CHATWOOT_CATALOG", "clickhouse")
    WH_CHATWOOT_SCHEMA = os.getenv("WH_CHATWOOT_SCHEMA", "chatwoot")
    TG_CHATWOOT_CHAT_ID = os.getenv("TG_CHATWOOT_CHAT_ID", "unknown")
    TG_CHATWOOT_BOT_TOKEN = os.getenv("TG_CHATWOOT_BOT_TOKEN", "unknown")

    BI_CATALOG = os.getenv("BI_CATALOG", "clickhouse")

config = AppConfig()
