# helper
import os
import tempfile

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-kumparan-staging")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "asia-southeast2")
MAX_BYTES_BILLED = int(os.environ.get("BQ_MAX_BYTES_BILLED", 50 * 1024**3)) 
GCP_CONN, PG_CONN = "google_cloud_default", "source_postgres"

DWH = f"{PROJECT_ID}.dwh.articles"
STAGING_INCR = f"{PROJECT_ID}.staging.articles_incr"
STAGING_IDS = f"{PROJECT_ID}.staging.articles_ids"
STAGING_CANDIDATES = f"{PROJECT_ID}.staging.articles_candidates"
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

_TYPES = {"id": "INT64", "title": "STRING", "content": "STRING",
          "published_at": "TIMESTAMP", "author_id": "INT64",
          "created_at": "TIMESTAMP", "updated_at": "TIMESTAMP", "deleted_at": "TIMESTAMP"}
COLUMNS = list(_TYPES)
TS_COLS = [c for c, t in _TYPES.items() if t == "TIMESTAMP"]
SCHEMA = [bigquery.SchemaField(c, t) for c, t in _TYPES.items()]
REQUIRED = ["id", "created_at", "updated_at"]


# ---------- BigQuery ----------
def bq_client():
    return BigQueryHook(gcp_conn_id=GCP_CONN, location=BQ_LOCATION).get_client(
        project_id=PROJECT_ID, location=BQ_LOCATION)


def run_query(sql, params=None):
    cfg = bigquery.QueryJobConfig(query_parameters=params or [],
                                  maximum_bytes_billed=MAX_BYTES_BILLED)
    job = bq_client().query(sql, job_config=cfg, location=BQ_LOCATION)
    job.result()
    return job

# ---------- dataset ----------
def init_dwh():
    with open(os.path.join(SQL_DIR, "ddl_dwh.sql")) as f:
        run_query(f.read())


def get_watermark():
    rows = list(run_query(
        f"SELECT MAX(updated_at) AS wm FROM `{DWH}` WHERE updated_at <= CURRENT_TIMESTAMP()").result())
    return rows[0]["wm"]

# ---------- Parquet ----------
def load_staging(prefix, table):
    bq_client().load_table_from_uri(
        f"gs://{BUCKET}/{prefix}*.parquet", table,
        job_config=bigquery.LoadJobConfig(
            schema=SCHEMA, source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE"),
        location=BQ_LOCATION,
    ).result()

# ---------- Upsert ----------
def merge_sql(staging: str) -> str:
    sets = ", ".join(f"{c} = S.{c}" for c in COLUMNS if c != "id")
    cols = ", ".join(COLUMNS)
    vals = ", ".join(f"S.{c}" for c in COLUMNS)
    return f"""
MERGE `{DWH}` T
USING `{staging}` S
ON T.id = S.id
WHEN MATCHED AND S.updated_at >= T.updated_at THEN
  UPDATE SET {sets}
WHEN NOT MATCHED THEN
  INSERT ({cols}) VALUES ({vals})
"""


def assert_no_duplicates():
    dup = list(run_query(
        f"SELECT COUNT(*) - COUNT(DISTINCT id) AS dup FROM `{DWH}`").result())[0]["dup"]
    if dup:
        raise ValueError(f"{dup} id duplikat di {DWH}")


# ---------- Postgres -> parquet -> GCS ----------
def extract_to_gcs(where_sql, params, prefix, chunk_rows=50_000) -> int:
    gcs = GCSHook(gcp_conn_id=GCP_CONN)
    for name in gcs.list(BUCKET, prefix=prefix) or []:      
        gcs.delete(BUCKET, name)

    conn = PostgresHook(postgres_conn_id=PG_CONN).get_conn()
    total, part = 0, 0
    try:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")               
        with conn.cursor(name="articles_extract") as cur:    
            cur.execute(f"SELECT {', '.join(COLUMNS)} FROM articles {where_sql}", params)
            while rows := cur.fetchmany(chunk_rows):
                df = pd.DataFrame(rows, columns=COLUMNS)
                df["id"] = df["id"].astype("int64")
                df["author_id"] = df["author_id"].astype("Int64")
                for c in TS_COLS:
                    df[c] = pd.to_datetime(df[c], utc=True)  
                if df[REQUIRED].isna().any().any():
                    raise ValueError("id/created_at/updated_at NULL di source: kontrak data dilanggar")
                with tempfile.TemporaryDirectory() as d:
                    local = f"{d}/part.parquet"
                    df.to_parquet(local, index=False, coerce_timestamps="us",
                                  allow_truncated_timestamps=True)  
                    gcs.upload(BUCKET, f"{prefix}part-{part:05d}.parquet", local)
                total += len(df)
                part += 1
    finally:
        conn.close()
    return total
