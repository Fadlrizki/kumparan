-- USULAN: ddl_source.sql asli sudah dihapus dari repo. Ganti dengan versi aslimu kalau berbeda,
-- tapi 3 hal ini wajib: PK pada id, kolom timestamptz, dan updated_at ikut berubah di SETIAP UPDATE
-- (termasuk soft delete) + index pada updated_at. Pipeline bergantung pada kontrak ini.
CREATE TABLE IF NOT EXISTS articles (
  id           BIGINT PRIMARY KEY,
  title        TEXT,
  content      TEXT,
  published_at TIMESTAMPTZ,
  author_id    BIGINT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_articles_updated_at ON articles (updated_at);

CREATE OR REPLACE FUNCTION articles_touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END $$ LANGUAGE plpgsql;

-- hanya BEFORE UPDATE: INSERT (seed data historis) tetap membawa updated_at aslinya
DROP TRIGGER IF EXISTS trg_articles_touch ON articles;
CREATE TRIGGER trg_articles_touch BEFORE UPDATE ON articles
  FOR EACH ROW EXECUTE FUNCTION articles_touch_updated_at();
