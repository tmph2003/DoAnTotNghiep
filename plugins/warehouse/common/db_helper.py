import psycopg2
import trino
from trino.auth import BasicAuthentication
from contextlib import contextmanager

from plugins.config import config
from plugins.warehouse.common.logging_helper import LoggingHelper

logger = LoggingHelper('DatabaseHelper')


class DatabaseConnection:
    def __init__(self, db_type: str = 'postgres', **kwargs) -> None:
        """Class to connect to a database.

        Args:
            db_type (str, optional): Database type.
                Defaults to 'postgres'.
        """
        self._db_type = db_type
        self._kwargs = kwargs

    @contextmanager
    def managed_cursor(self):
        """Function to create a managed database cursor.

        Yields:
            Cursor: A cursor.
        """
        if self._db_type == 'postgres':
            _conn = psycopg2.connect(**self._kwargs)
        elif self._db_type == 'trino':
            _conn = trino.dbapi.connect(
                host=config.TRINO_HOST,
                port=config.TRINO_PORT,
                user=config.TRINO_USER,
                http_scheme='http'
            )
        cur = _conn.cursor()

        try:
            yield cur
        except psycopg2.Error as e:
            if self._db_type != 'trino':
                _conn.rollback()
        finally:
            if self._db_type != 'trino':
                _conn.commit()
            cur.close()
            _conn.close()


class DatabaseHelper:
    def __init__(self, logger, client=None):
        self.logger = logger
        self.client = client

    def execute(self, query):
        try:
            with self.client.managed_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
        except Exception as e:
            self.logger.debug(f'Error execute query in Database: {query}')
            self.logger.error(e)
            raise e

        return -1

    @staticmethod
    def upsert_table(target_table: str,
                     source_table: str,
                     conflict_columns: list[str],
                     select_columns: list[str],
                     insert_columns: list[str],
                     conflict_action: str = 'do_update',  # 'do_update' or 'do_nothing'
                     non_default_exclude_columns: dict = None):

        if conflict_action == 'do_update' and not conflict_columns:
            raise ValueError("conflict_columns is required when conflict_action is 'do_update'")

        if len(insert_columns) != len(select_columns):
            raise ValueError("The number of insert_columns must match the number of select_columns")

        conflict_columns_str = ", ".join(conflict_columns)
        exclude_columns = non_default_exclude_columns or {}
        default_exclude_columns = {col: col for col in insert_columns}
        exclude_columns.update({k: v for k, v in default_exclude_columns.items() if k not in exclude_columns})
        update_set = ', '.join([f"{col} = EXCLUDED.{col}" for col in exclude_columns])

        if conflict_action == 'do_update':
            conflict_action_sql = f"ON CONFLICT ({conflict_columns_str}) DO UPDATE SET {update_set}"
        elif conflict_action == 'do_nothing':
            conflict_action_sql = f"ON CONFLICT ({conflict_columns_str}) DO NOTHING"
        else:
            raise ValueError(f"Unsupported conflict action: {conflict_action}")

        upsert_query = f"""
            INSERT INTO {target_table} ({", ".join(conflict_columns + insert_columns)})
            SELECT {", ".join(conflict_columns + select_columns)} FROM {source_table}
            {conflict_action_sql};
        """
        # self.execute(upsert_query)

        return upsert_query
