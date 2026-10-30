import os
import tempfile

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
BUCKET = f"{PROJECT_ID}-kumparan-staging"
BQ_LOCATION = "asia-southeast2"
GCP_CONN = "google_cloud_default"
PG_CONN = "source_postgres"

COLUMNS = ["id", "title", "content", "published_at", "author_id",
           "created_at", "updated_at", "deleted_at"]
TS_COLS = ["published_at", "created_at", "updated_at", "deleted_at"]

SCHEMA = [
    bigquery.SchemaField("id", "INT64"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("content", "STRING"),
    bigquery.SchemaField("published_at", "TIMESTAMP"),
    bigquery.SchemaField("author_id", "INT64"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("deleted_at", "TIMESTAMP"),
]


def extract_to_gcs(sql, params, prefix, chunk_rows=10_000) -> int:
    """Baca dari Postgres per chunk (server-side cursor), tulis parquet ke GCS."""
    gcs = GCSHook(gcp_conn_id=GCP_CONN)

    # idempotent: bersihkan sisa run/retry sebelumnya di prefix yang sama
    for obj in gcs.list(BUCKET, prefix=prefix) or []:
        gcs.delete(BUCKET, obj)

    conn = PostgresHook(postgres_conn_id=PG_CONN).get_conn()
    total, part = 0, 0
    try:
        with conn.cursor(name="articles_extract") as cur:   # named = server-side
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(chunk_rows)
                if not rows:
                    break
                df = pd.DataFrame(rows, columns=COLUMNS)
                df["id"] = df["id"].astype("int64")
                df["author_id"] = df["author_id"].astype("Int64")
                for c in TS_COLS:
                    df[c] = pd.to_datetime(df[c], utc=True)

                with tempfile.TemporaryDirectory() as d:
                    local = f"{d}/part.parquet"
                    df.to_parquet(local, index=False,
                                  coerce_timestamps="us",
                                  allow_truncated_timestamps=True)
                    gcs.upload(BUCKET, f"{prefix}part-{part:05d}.parquet", local)
                total += len(df)
                part += 1
    finally:
        conn.close()
    return total


def _bq_client():
    return BigQueryHook(gcp_conn_id=GCP_CONN, location=BQ_LOCATION).get_client(
        project_id=PROJECT_ID, location=BQ_LOCATION)


def load_staging(prefix, table):
    """Load parquet di GCS ke tabel staging (ditimpa tiap run)."""
    job = _bq_client().load_table_from_uri(
        f"gs://{BUCKET}/{prefix}*.parquet",
        f"{PROJECT_ID}.staging.{table}",
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        ),
        location=BQ_LOCATION,
    )
    job.result()


def run_query(sql):
    return list(_bq_client().query(sql, location=BQ_LOCATION).result())


def merge_sql(staging_table: str) -> str:
    return f"""
MERGE `{PROJECT_ID}.dwh.articles` T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
    FROM `{PROJECT_ID}.staging.{staging_table}`
  ) WHERE rn = 1
) S
ON T.id = S.id
WHEN MATCHED AND S.updated_at >= T.updated_at THEN UPDATE SET
  title = S.title, content = S.content, published_at = S.published_at,
  author_id = S.author_id, created_at = S.created_at,
  updated_at = S.updated_at, deleted_at = S.deleted_at,
  _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (id, title, content, published_at, author_id, created_at, updated_at, deleted_at, _loaded_at)
VALUES
  (S.id, S.title, S.content, S.published_at, S.author_id, S.created_at,
   S.updated_at, S.deleted_at, CURRENT_TIMESTAMP())
"""


def assert_no_duplicates():
    r = run_query(
        f"SELECT COUNT(*) - COUNT(DISTINCT id) AS dup FROM `{PROJECT_ID}.dwh.articles`")[0]
    if r["dup"] > 0:
        raise ValueError(f"Ditemukan {r['dup']} id duplikat di dwh.articles")
