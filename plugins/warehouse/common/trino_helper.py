import time

from plugins.warehouse.common.schema_helper import convert_trino_to_trino_col_type


class TrinoHelper:
    def __init__(self, logger, client=None):
        self.logger = logger
        self.client = client

    def execute(self, query):
        self.logger.info('##########################')
        self.logger.debug(query)

        start_time = time.time()
        try:
            with self.client.managed_cursor() as cursor:
                if isinstance(query, list):
                    for item in query:
                        cursor.execute(item)
                        results = cursor.fetchall()
                else:
                    cursor.execute(query)
                    results = cursor.fetchall()

                end_time = time.time()
                execution_time = end_time - start_time
                self.logger.info(f'Query executed in {execution_time:.2f} seconds...')
                self.logger.info('##########################')

                return results
        except Exception as e:
            self.logger.error(f'Error execute query in Trino: {query}')
            self.logger.error(e)

            end_time = time.time()
            execution_time = end_time - start_time
            self.logger.info(f'Query failed after {execution_time:.2f} seconds')
            self.logger.info('##########################')

            raise e

        return -1

    def executemany(self, query, rows=[]):
        self.logger.info('##########################')
        self.logger.debug(f'Execute many query in Trino: {query}')

        start_time = time.time()
        try:
            with self.client.managed_cursor() as cursor:
                cursor.executemany(query, rows)
                results = cursor.fetchall()

                end_time = time.time()
                execution_time = end_time - start_time
                self.logger.info(f'Query executed in {execution_time:.2f} seconds...')
                self.logger.info('##########################')

                return results
        except Exception as e:
            self.logger.error(f'Error execute query in Trino: {query}')
            self.logger.error(e)

            end_time = time.time()
            execution_time = end_time - start_time
            self.logger.info(f'Query executed in {execution_time:.2f} seconds...')
            self.logger.info('##########################')

            raise e

        return -1

    def check_table_is_exist(self, table_source_id):
        table_catalog, table_schema, table_name = table_source_id.split('.')
        is_exist = self.execute(f"""
        SELECT EXISTS(
            SELECT * 
            FROM {table_catalog}.information_schema.tables 
            WHERE 
              table_schema = '{table_schema}' AND 
              table_name = '{table_name}'
        )
        """)[0][0]
        return is_exist

    def create_table(self, table_id, columns, ingest_type=None, audit_column=None):
        self.logger.info(f'Create table: {table_id} ...')

        sorted_by = []
        if 'id' in [column['name'] for column in columns]:
            sorted_by.append("'id'")
        if audit_column and audit_column in [column['name'] for column in columns]:
            sorted_by.append(f"'{audit_column}'")

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_id} (
        {', '.join(['"' + column['name'] + '"' + ' ' + convert_trino_to_trino_col_type(column['type']) for column in columns])}
        )
        with (
            format = 'PARQUET'
            {", partitioning = ARRAY['month(partition_value)']" if ingest_type != 'snapshot' else ""}
            {", sorted_by = ARRAY[" + ", ".join(sorted_by) + "]" if len(sorted_by) > 0 else ""}
        )
        """
        self.execute(create_sql)

    def upsert_table(self, dest_table_id, dest_columns, source_sql, matched_condition_sql='TRUE',
                     condition_sql="t.id = s.id"):
        upsert_sql = f"""
        MERGE INTO {dest_table_id} AS t
        USING (
            {source_sql}
        ) AS s
        ON ({condition_sql})
        WHEN MATCHED AND {matched_condition_sql}
        THEN UPDATE
            SET {', '.join([col + '= s.' + col for col in dest_columns])} 
        WHEN NOT MATCHED
            THEN INSERT ({', '.join(dest_columns)})
                  VALUES({', '.join(['s.' + col for col in dest_columns])})
        """
        self.execute(upsert_sql)

    def resolve_schema_evolution(self, source_table_schema, dest_table_schema, dest_table_id):
        dest_column_names = [column[0] for column in dest_table_schema]
        missing_cols = [{'name': column[0], 'type': column[1].upper()} for column in source_table_schema if
                        column[0] not in dest_column_names]

        for column in missing_cols:
            self.execute(
                f"ALTER TABLE {dest_table_id} ADD COLUMN {column['name']} {convert_trino_to_trino_col_type(column['type'])}")
