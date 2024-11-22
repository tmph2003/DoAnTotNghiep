import re
import pandas as pd

TRINO_DATA_TYPES = ["BOOLEAN", "INTEGER", "SMALLINT", "BIGINT", "REAL", "DOUBLE", "DECIMAL(p,s)", "DATE", "TIME(6)", "TIMESTAMP(6)",
                    "TIMESTAMP(6) WITH TIME ZONE", "VARCHAR", "UUID", "VARBINARY", "ROW(...)", "ARRAY(e)", "MAP(k,v)"]

def drop_null_columns(df):
    """
    This function drops columns containing all null values.
    :param df: A PySpark DataFrame
    """
    null_counts = df.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v >= df.count()]
    df = df.drop(*to_drop)

    return df


def get_schema_from_trino(table_id, trino_conn=None):
    df_schema = pd.read_sql(f"DESCRIBE {table_id}", trino_conn)
    col_to_type_dict = dict(zip(df_schema['Column'], df_schema['Type']))

    return col_to_type_dict


def convert_trino_to_trino_col_type(trino_type=None):
    trino_type = trino_type.upper()
    
    decimal_pattern = r"DECIMAL\(\d+,\d+\)"
    if re.match(decimal_pattern, trino_type):
        return trino_type
    if trino_type not in TRINO_DATA_TYPES:
        return "VARCHAR"
    else:
        return trino_type
