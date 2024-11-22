from logging import Logger

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
