import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

PG_CONN = "source_postgres"


def _exec(sql, args):
    conn = PostgresHook(postgres_conn_id=PG_CONN).get_conn()
    try:
        with conn, conn.cursor() as cur:      # commit otomatis di akhir blok
            cur.execute(sql, args)
            return cur.rowcount
    finally:
        conn.close()


@dag(
    dag_id="source_simulator",
    schedule="30 * * * *",                    # menit ke-30, di antara dua run ETL
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "inserts": Param(0, type="integer", minimum=0),
        "updates": Param(0, type="integer", minimum=0),
        "soft_deletes": Param(0, type="integer", minimum=0),
        "hard_deletes": Param(10, type="integer", minimum=0),
    },
    tags=["kumparan", "simulator"],
)
def source_simulator():
    @task
    def insert_new(params=None):
        n = _exec(
            """
            INSERT INTO articles
                (id, title, content, published_at, author_id, created_at, updated_at)
            SELECT (SELECT COALESCE(MAX(id), 0) FROM articles) + ROW_NUMBER() OVER (),
                   '[SIM] ' || title, content, now(), author_id, now(), now()
            FROM (SELECT title, content, author_id
                  FROM articles ORDER BY random() LIMIT %s) s
            """,
            (int(params["inserts"]),),
        )
        print(f"inserted: {n}")

    @task
    def update_existing(params=None):
        n = _exec(
            r"""
            UPDATE articles
            SET title = regexp_replace(title, ' \[edited\]$', '') || ' [edited]'
            WHERE id IN (SELECT id FROM articles
                         WHERE deleted_at IS NULL ORDER BY random() LIMIT %s)
            """,
            (int(params["updates"]),),
        )
        print(f"updated: {n}")

    @task
    def soft_delete(params=None):
        n = _exec(
            """
            UPDATE articles SET deleted_at = now()
            WHERE id IN (SELECT id FROM articles
                         WHERE deleted_at IS NULL ORDER BY random() LIMIT %s)
            """,
            (int(params["soft_deletes"]),),
        )
        print(f"soft deleted: {n}")

    @task
    def hard_delete(params=None):
        n = _exec(
            "DELETE FROM articles WHERE id IN "
            "(SELECT id FROM articles ORDER BY random() LIMIT %s)",
            (int(params["hard_deletes"]),),
        )
        print(f"hard deleted: {n}")

    insert_new() >> update_existing() >> soft_delete() >> hard_delete()


source_simulator()
