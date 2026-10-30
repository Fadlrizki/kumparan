CREATE TABLE IF NOT EXISTS dwh.articles (
  id           INT64 NOT NULL,
  title        STRING,
  content      STRING,
  published_at TIMESTAMP,
  author_id    INT64,
  created_at   TIMESTAMP,
  updated_at   TIMESTAMP,
  deleted_at   TIMESTAMP,
  _loaded_at   TIMESTAMP   -- metadata: kapan baris ini terakhir ditulis pipeline
)
PARTITION BY DATE(published_at)
CLUSTER BY id;