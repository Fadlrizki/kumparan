-- Dijalankan otomatis oleh task init_dwh (idempotent).
-- CATATAN: CREATE TABLE IF NOT EXISTS tidak mengubah tabel yang sudah ada. Kalau tabel lama
-- dipartisi by published_at, DROP dulu lalu jalankan ulang pipeline (full load otomatis).
CREATE SCHEMA IF NOT EXISTS staging OPTIONS (location = "asia-southeast2");
CREATE SCHEMA IF NOT EXISTS dwh     OPTIONS (location = "asia-southeast2");

-- Partisi pada created_at: tidak berubah seumur baris, NOT NULL, tidak ada partisi NULL untuk draft.
CREATE TABLE IF NOT EXISTS dwh.articles (
  id           INT64     NOT NULL,
  title        STRING,
  content      STRING,
  published_at TIMESTAMP,
  author_id    INT64,
  created_at   TIMESTAMP NOT NULL,
  updated_at   TIMESTAMP NOT NULL,
  deleted_at   TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY id;

-- Konsumen BI sebaiknya query view ini, bukan tabel mentah.
CREATE OR REPLACE VIEW dwh.articles_active AS
SELECT * FROM dwh.articles WHERE deleted_at IS NULL;
