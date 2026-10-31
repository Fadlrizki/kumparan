"""Seed Postgres source dari CSV HuggingFace. Jalankan: set -a; source .env; set +a; python scripts/seed_source.py"""
import argparse
import io
import os
import zlib

import pandas as pd
import psycopg2

CSV_PATH = "data/raw/data.csv"
USECOLS = ["id", "url", "title", "content", "date", "created_at", "updated_at"]
COLS = ["id", "title", "content", "published_at", "author_id", "created_at", "updated_at", "deleted_at"]


def connect():
    return psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "localhost"), port=int(os.getenv("SOURCE_DB_PORT", "5433")),
        user=os.getenv("SOURCE_DB_USER", "source"), password=os.environ["SOURCE_DB_PASSWORD"],
        dbname=os.getenv("SOURCE_DB_NAME", "kumparan_source"))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    published = pd.to_datetime(df["date"], utc=True, errors="coerce")
    created = pd.to_datetime(df["created_at"], utc=True, errors="coerce").fillna(published)
    updated = pd.to_datetime(df["updated_at"], utc=True, errors="coerce").fillna(created)
    out = pd.DataFrame({
        "id": df["id"], "title": df["title"], "content": df["content"],
        "published_at": published,
        "author_id": df["url"].fillna("").map(lambda u: zlib.crc32(u.encode()) % 200 + 1),  # sintetis
        "created_at": created, "updated_at": updated, "deleted_at": pd.NaT,
    })
    before = len(out)
    out = out.dropna(subset=["created_at"]).drop_duplicates("id")[COLS]   # created_at wajib
    if len(out) < before:
        print(f"  dibuang {before - len(out)} baris (tanpa tanggal / id ganda)")
    return out


def seed_dataset(conn, limit):
    read = inserted = 0
    for chunk in pd.read_csv(CSV_PATH, usecols=USECOLS, chunksize=20_000):   # usecols membuang embedding
        df = transform(chunk)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        with conn, conn.cursor() as cur:                                      # satu transaksi per chunk
            cur.execute("CREATE TEMP TABLE tmp_articles (LIKE articles INCLUDING DEFAULTS) ON COMMIT DROP")
            cur.copy_expert(f"COPY tmp_articles ({','.join(COLS)}) FROM STDIN WITH (FORMAT csv)", buf)
            cur.execute(f"INSERT INTO articles ({','.join(COLS)}) SELECT {','.join(COLS)} "
                        "FROM tmp_articles ON CONFLICT (id) DO NOTHING")
            inserted += cur.rowcount                                          # yang benar-benar masuk
        read += len(df)
        print(f"read={read} inserted={inserted}")
        if limit and read >= limit:
            break


def add_history_2016(conn, n):
    """Data historis sintetis: salinan acak artikel yang ada dengan tanggal dipindah ke 2016."""
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM articles "
                    "WHERE created_at >= '2016-01-01' AND created_at < '2017-01-01'")
        if cur.fetchone()[0]:
            print("data 2016 sudah ada, dilewati")
            return
        cur.execute(r"""
            INSERT INTO articles (id, title, content, published_at, author_id, created_at, updated_at)
            SELECT (SELECT COALESCE(MAX(id), 0) FROM articles) + ROW_NUMBER() OVER (),
                   title, content, pub, author_id, ts, pub
            FROM (
                SELECT title, content, author_id, ts,
                       ts + random() * interval '3 hours' AS pub
                FROM (
                    SELECT regexp_replace(title, '( \[(edited|outage2?)\])+$', '') AS title,
                           content, author_id,
                           timestamptz '2016-01-01 00:00:00+00' + random() * interval '365 days' AS ts
                    FROM articles
                    WHERE created_at < '2024-01-01'
                    ORDER BY random() LIMIT %s
                ) a
            ) b
        """, (n,))
        print(f"data 2016 ditambahkan={cur.rowcount}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=200_000)
    p.add_argument("--history-2016", type=int, default=0, help="jumlah baris historis sintetis tahun 2016")
    a = p.parse_args()
    c = connect()
    seed_dataset(c, a.limit)
    if a.history_2016:
        add_history_2016(c, a.history_2016)
