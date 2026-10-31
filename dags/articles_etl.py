"""Satu DAG untuk full load, incremental, dan backfill.

Batas bawah extract = watermark di DWH (MAX(updated_at)) - OVERLAP, bukan jendela dari jadwal.
Akibatnya:
  * DWH kosong            -> full load otomatis (tidak perlu DAG initial_load terpisah)
  * scheduler mati / run gagal -> run berikutnya otomatis mengejar (self-healing, catchup tidak perlu)
  * replay/backfill       -> trigger manual dengan param since / until
"""
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param

from etl_common import (STAGING_INCR, extract_to_gcs, get_watermark, init_dwh,
                        load_staging, merge_sql, run_query)

OVERLAP = timedelta(hours=1)


@dag(
    dag_id="articles_etl",
    schedule="0 * * * *",                   
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,                         
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),   
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5),
                  "execution_timeout": timedelta(minutes=20)},
    params={
        "since": Param(None, type=["null", "string"],
                       description="ISO timestamp. Kosong = pakai watermark DWH (atau full load jika DWH kosong)."),
        "until": Param(None, type=["null", "string"],
                       description="ISO timestamp batas atas (eksklusif). Untuk backfill per potongan."),
    },
    tags=["kumparan"],
)
def articles_etl():
    @task
    def init():
        init_dwh()

    @task
    def extract(ts_nodash=None, params=None):
        if params.get("since"):
            lo = pendulum.parse(params["since"])
        else:
            wm = get_watermark()
            lo = wm - OVERLAP if wm else None
        conds, args = [], []
        if lo:
            conds.append("updated_at >= %s")
            args.append(lo)
        if params.get("until"):
            conds.append("updated_at < %s")
            args.append(pendulum.parse(params["until"]))
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        n = extract_to_gcs(where, tuple(args) or None, prefix=f"articles/{ts_nodash}/")
        print(f"lo={lo} until={params.get('until')} rows={n}")
        return n

    @task(pool="dwh_write")
    def load_and_merge(n, ts_nodash=None):
        if not n:
            raise AirflowSkipException("tidak ada perubahan")
        load_staging(f"articles/{ts_nodash}/", STAGING_INCR)
        job = run_query(merge_sql(STAGING_INCR))     
        print(f"merge selesai, baris terpengaruh={job.num_dml_affected_rows}")

    ready = init()
    n = extract()
    ready >> n
    load_and_merge(n)


articles_etl()
