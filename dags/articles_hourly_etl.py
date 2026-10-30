from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from etl_common import (COLUMNS, assert_no_duplicates, extract_to_gcs,
                        load_staging, merge_sql, run_query)

LOOKBACK = timedelta(hours=1)


@dag(
    dag_id="articles_hourly_etl",
    schedule="0 * * * *",               # tiap jam, setiap hari
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                  # staging table dipakai bersama
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["kumparan"],
)
def articles_hourly_etl():
    @task
    def extract(data_interval_start=None, data_interval_end=None, ts_nodash=None):
        sql = (f"SELECT {', '.join(COLUMNS)} FROM articles "
               "WHERE updated_at >= %s AND updated_at < %s ORDER BY id")
        return extract_to_gcs(
            sql, (data_interval_start - LOOKBACK, data_interval_end),
            prefix=f"articles/hourly/{ts_nodash}/")

    @task.short_circuit
    def has_rows(n):
        return n > 0                    # tidak ada perubahan -> skip sisa task

    @task
    def load(ts_nodash=None):
        load_staging(f"articles/hourly/{ts_nodash}/", "articles_hourly")

    @task
    def merge():
        run_query(merge_sql("articles_hourly"))

    @task
    def check():
        assert_no_duplicates()

    has_rows(extract()) >> load() >> merge() >> check()


articles_hourly_etl()