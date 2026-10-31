from datetime import datetime, timezone

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

from etl_common import BQ_LOCATION, GCP_CONN, PG_CONN, PROJECT_ID, run_query

DWH = f"{PROJECT_ID}.dwh.articles"
IDS = f"{PROJECT_ID}.staging.articles_ids"


def _eligible(ts: str) -> str:
    # Hanya baris yang sudah cukup lama di DWH. Menghindari salah tandai baris
    # yang baru dibuat setelah snapshot id diambil (race dengan ETL hourly).
    return (f"T.deleted_at IS NULL AND "
            f"T.created_at < TIMESTAMP_SUB(TIMESTAMP('{ts}'), INTERVAL 1 HOUR)")


@dag(
    dag_id="articles_reconcile_deletes",
    schedule="0 2 * * *",               # harian, jam 02:00 UTC
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"max_delete_ratio": Param(0.05, type="number", minimum=0, maximum=1)},
    tags=["kumparan", "reconcile"],
)
def articles_reconcile_deletes():
    @task
    def snapshot_ids():
        snapshot_ts = datetime.now(timezone.utc)      # diambil SEBELUM baca id
        rows = PostgresHook(postgres_conn_id=PG_CONN).get_records(
            "SELECT id FROM articles")
        if not rows:
            raise ValueError("Source mengembalikan 0 id, dibatalkan "
                             "(kemungkinan masalah koneksi, bukan semua data terhapus)")
        df = pd.DataFrame(rows, columns=["id"]).astype("int64")
        client = BigQueryHook(gcp_conn_id=GCP_CONN, location=BQ_LOCATION).get_client(
            project_id=PROJECT_ID, location=BQ_LOCATION)
        client.load_table_from_dataframe(
            df, IDS,
            job_config=bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField("id", "INT64")],
                write_disposition="WRITE_TRUNCATE"),
            location=BQ_LOCATION,
        ).result()
        return {"snapshot_ts": snapshot_ts.isoformat(), "source_ids": len(df)}

    @task
    def check_guard(snap, params=None):
        r = run_query(f"""
            SELECT COUNTIF(S.id IS NULL) AS missing, COUNT(*) AS active
            FROM `{DWH}` T LEFT JOIN `{IDS}` S ON S.id = T.id
            WHERE {_eligible(snap['snapshot_ts'])}""")[0]
        ratio = r["missing"] / max(r["active"], 1)
        print(f"missing={r['missing']} active={r['active']} ratio={ratio:.4f}")
        if ratio > params["max_delete_ratio"]:
            raise ValueError(
                f"{ratio:.1%} baris akan ditandai terhapus (batas "
                f"{params['max_delete_ratio']:.0%}). Dibatalkan, cek source secara manual.")
        return {**snap, "missing": r["missing"]}

    @task
    def mark_deleted(info):
        run_query(f"""
            UPDATE `{DWH}` T
            SET deleted_at = CURRENT_TIMESTAMP(), _loaded_at = CURRENT_TIMESTAMP()
            WHERE {_eligible(info['snapshot_ts'])}
              AND NOT EXISTS (SELECT 1 FROM `{IDS}` S WHERE S.id = T.id)""")
        print(f"ditandai terhapus: {info['missing']}")

    mark_deleted(check_guard(snapshot_ids()))


articles_reconcile_deletes()
