import argparse
import io
import zlib

import pandas as pd
import psycopg2

CSV_PATH = "data/raw/data.csv"
USECOLS = ["id", "url", "title", "content", "date", "created_at", "updated_at"]
COLS = ["id", "title", "content", "published_at", "author_id",
        "created_at", "updated_at", "deleted_at"]


def transform(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "id": df["id"],
        "title": df["title"],
        "content": df["content"],
        "published_at": pd.to_datetime(df["date"], utc=True, errors="coerce"),
        "author_id": df["url"].fillna("").map(lambda u: zlib.crc32(u.encode()) % 200 + 1),
        "created_at": pd.to_datetime(df["created_at"], utc=True, errors="coerce"),
        "updated_at": pd.to_datetime(df["updated_at"], utc=True, errors="coerce"),
        "deleted_at": pd.NaT,
    })
    return out.drop_duplicates("id")[COLS]


def main(limit: int | None):
    conn = psycopg2.connect(host="localhost", port=5433, user="source",
                            password="source", dbname="kumparan_source")
    loaded = 0
    # usecols membuang kolom embedding yang besar
    for chunk in pd.read_csv(CSV_PATH, usecols=USECOLS, chunksize=20_000):
        df = transform(chunk)
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)

        with conn, conn.cursor() as cur:          # satu transaksi per chunk
            cur.execute("CREATE TEMP TABLE tmp_articles "
                        "(LIKE articles INCLUDING DEFAULTS) ON COMMIT DROP")
            cur.copy_expert(
                f"COPY tmp_articles ({','.join(COLS)}) FROM STDIN WITH (FORMAT csv)", buf)
            cur.execute(
                f"INSERT INTO articles ({','.join(COLS)}) "
                f"SELECT {','.join(COLS)} FROM tmp_articles "
                "ON CONFLICT (id) DO NOTHING")

        loaded += len(df)
        print(f"loaded {loaded} rows")
        if limit and loaded >= limit:
            break


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=200_000)
    main(p.parse_args().limit)