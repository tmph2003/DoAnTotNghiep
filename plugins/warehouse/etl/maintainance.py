from logging import Logger
from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper

class Maintenance:
    def __init__(self, logger: Logger, trino: TrinoHelper, schema: str):
        self.logger = logger
        self.trino = trino
        self.schema = schema

    def run(self):
        table_list = self.trino.execute(f"SHOW TABLES FROM iceberg.{self.schema}")
        for table in table_list:
            # The files is smaller than 128MB, will be merged into one file.
            self.trino.execute(f"ALTER TABLE iceberg.{self.schema}.{table[0]} EXECUTE optimize(file_size_threshold => '128MB')")
            # The snapshots expired after 3 days, will be deleted.
            self.trino.execute(f"ALTER TABLE iceberg.{self.schema}.{table[0]} EXECUTE expire_snapshots(retention_threshold => '3d')")
            # The orphan files (not referenced by any metadata ) will be removed after 3 days.
            self.trino.execute(f"ALTER TABLE iceberg.{self.schema}.{table[0]} EXECUTE remove_orphan_files(retention_threshold => '3d')")

        rows = self.trino.execute(f"""
            SELECT a.hl_agent_info
            FROM {config.HIVE_METASTORE_CATALOG}.{config.HIVE_METASTORE_SCHEMA}.hive_locks a
            WHERE a.hl_acquired_at IS NOT NULL
        """)[0]
        # Lặp qua các bản ghi để kiểm tra query state
        for hl_agent_info in rows:
            state = self.trino.execute(f"""
                SELECT 
                    CASE 
                        WHEN EXISTS (
                            SELECT *
                            FROM system.runtime.queries a
                            WHERE a.query_id = '{hl_agent_info}'
                        ) THEN (
                            SELECT state
                            FROM system.runtime.queries a
                            WHERE a.query_id = '{hl_agent_info}'
                        )
                        ELSE 'NOT_EXISTS'
                    END
            """)[0][0]

            # Nếu trạng thái là "FINISHED" hoặc query không tồn tại
            if state == "FINISHED" or state == "NOT_EXISTS":
                self.trino.execute(f"""
                    DELETE FROM {config.HIVE_METASTORE_CATALOG}.{config.HIVE_METASTORE_SCHEMA}.hive_locks
                    WHERE hl_agent_info = '{hl_agent_info}'
                """)