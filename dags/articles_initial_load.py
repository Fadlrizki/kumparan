import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl_common import (COLUMNS, PG_CONN, PROJECT_ID, assert_no_duplicates,
                        extract_to_gcs, load_staging, merge_sql, run_query)


@dag(
    dag_id="articles_initial_load",
    schedule=None,                      # manual trigger
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["kumparan", "backfill"],
)
def articles_initial_load():
    @task
    def extract(ts_nodash=None):
        sql = f"SELECT {', '.join(COLUMNS)} FROM articles ORDER BY id"
        return extract_to_gcs(sql, None, prefix=f"articles/initial/{ts_nodash}/")

    @task
    def load(ts_nodash=None):
        load_staging(f"articles/initial/{ts_nodash}/", "articles_initial")

    @task
    def merge():
        run_query(merge_sql("articles_initial"))

    @task
    def check():
        assert_no_duplicates()
        src = PostgresHook(postgres_conn_id=PG_CONN).get_first(
            "SELECT count(*) FROM articles")[0]
        dwh = run_query(
            f"SELECT COUNT(*) AS c FROM `{PROJECT_ID}.dwh.articles`")[0]["c"]
        if src != dwh:
            raise ValueError(f"Row count tidak sama: source={src}, dwh={dwh}")

    extract() >> load() >> merge() >> check()


articles_initial_load()
