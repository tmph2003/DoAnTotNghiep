from plugins.config import config
from plugins.warehouse.common.trino_helper import TrinoHelper


def build_dim_date(logger, trino: TrinoHelper):
    logger.info('[SCD1] Building dim_date...')
    dest_table_id = f"{config.LH_CHATWOOT_CATALOG}.{config.LH_CHATWOOT_SCHEMA}.dim_date"

    return trino.execute(f'''
    MERGE INTO {dest_table_id} AS t
    USING (WITH date_range AS (SELECT date_add('day', sequence_index, DATE '2010-01-01') AS date_value
                               FROM UNNEST(sequence(0, date_diff('day', DATE '2010-01-01', DATE '2030-12-31'))) AS t(sequence_index))
           SELECT CAST(date_format(date_value, '%Y%m%d') AS INTEGER)                     AS id,
                  CAST(day_of_week(date_value) AS INTEGER)                               AS day_of_week,
                  CAST(day_of_month(date_value) AS INTEGER)                              AS day_of_month,
                  CAST(day_of_year(date_value) AS INTEGER)                               AS day_of_year,
                  CASE
                      WHEN day_of_month(date_value) = day_of_month(last_day_of_month(date_value))
                          THEN 1
                      ELSE 0
                      END                                                                AS is_last_day_of_month,
                  CASE WHEN day_of_week(date_value) IN (6, 7) THEN 1 ELSE 0 END          AS is_weekend,
                  CAST(date_format(date_trunc('week', date_value), '%Y%m%d') AS INTEGER) AS week_start_id,
                  CAST(date_format(date_add('day', 6, date_trunc('week', date_value)),
                                   '%Y%m%d') AS INTEGER)                                 AS week_end_id,
                  CAST(week_of_year(date_value) AS INTEGER)                              AS week_of_year,
                  CAST(date_format(date_value, '%Y%m') AS INTEGER) AS month,
            CONCAT(date_format(date_value, '%Y'), 'Q', CAST(quarter(date_value) AS VARCHAR)) AS quarter,
            CAST(year(date_value) AS INTEGER) AS year,
            0 AS is_holiday,  -- Chỉnh sửa để thêm ngày nghỉ lễ nếu cần thiết
            CAST(to_unixtime(date_value) AS INTEGER) AS unix_timestamp,
            date_value AS "date"
           FROM date_range) AS s
    ON (t.id = s.id)
    WHEN NOT MATCHED THEN
        INSERT (
                id,
                day_of_week,
                day_of_month,
                day_of_year,
                is_last_day_of_month,
                is_weekend,
                week_start_id,
                week_end_id,
                week_of_year,
                month,
                quarter,
                year,
                is_holiday,
                unix_timestamp,
                "date"
            )
            VALUES (s.id,
                    s.day_of_week,
                    s.day_of_month,
                    s.day_of_year,
                    s.is_last_day_of_month,
                    s.is_weekend,
                    s.week_start_id,
                    s.week_end_id,
                    s.week_of_year,
                    s.month,
                    s.quarter,
                    s.year,
                    s.is_holiday,
                    s.unix_timestamp,
                    s."date")
    ''')
