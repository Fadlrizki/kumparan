import pandas as pd
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

from etl_common import (BQ_LOCATION, DWH, PG_CONN, STAGING_CANDIDATES, STAGING_IDS,
                        assert_no_duplicates, bq_client, init_dwh, run_query)


@dag(
    dag_id="articles_reconcile_deletes",
    schedule="30 2 * * *", 
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=30)},
    params={"max_delete_ratio": Param(0.05, type="number", minimum=0, maximum=1)},
    tags=["kumparan", "reconcile"],
)
def articles_reconcile_deletes():
    @task
    def snapshot():
        init_dwh()
        run_query(f"CREATE OR REPLACE TABLE `{STAGING_CANDIDATES}` AS "
                  f"SELECT id FROM `{DWH}` WHERE deleted_at IS NULL")
        rows = PostgresHook(postgres_conn_id=PG_CONN).get_records("SELECT id FROM articles")
        if not rows:
            raise ValueError("Source mengembalikan 0 id: kemungkinan masalah koneksi, dibatalkan")
        df = pd.DataFrame(rows, columns=["id"]).astype("int64")
        bq_client().load_table_from_dataframe(
            df, STAGING_IDS,
            job_config=bigquery.LoadJobConfig(schema=[bigquery.SchemaField("id", "INT64")],
                                              write_disposition="WRITE_TRUNCATE"),
            location=BQ_LOCATION).result()

    @task(pool="dwh_write")
    def mark_deleted(params=None):
        s = list(run_query(f"""
            SELECT COUNTIF(S.id IS NULL) AS missing, COUNT(*) AS candidates
            FROM `{STAGING_CANDIDATES}` C LEFT JOIN `{STAGING_IDS}` S ON S.id = C.id""").result())[0]
        ratio = s["missing"] / max(s["candidates"], 1)
        print(f"missing={s['missing']} candidates={s['candidates']} ratio={ratio:.4f}")
        if ratio > params["max_delete_ratio"]:
            raise ValueError(f"{ratio:.1%} baris akan ditandai terhapus (batas "
                             f"{params['max_delete_ratio']:.0%}). Dibatalkan, cek source manual.")

        job = run_query(f"""
            UPDATE `{DWH}` T
            SET deleted_at = CURRENT_TIMESTAMP()
            WHERE T.deleted_at IS NULL
              AND EXISTS     (SELECT 1 FROM `{STAGING_CANDIDATES}` C WHERE C.id = T.id)
              AND NOT EXISTS (SELECT 1 FROM `{STAGING_IDS}` S WHERE S.id = T.id)""")
        print(f"ditandai terhapus: {job.num_dml_affected_rows}")

    @task
    def check():
        assert_no_duplicates()                

    snapshot() >> mark_deleted() >> check()


articles_reconcile_deletes()
