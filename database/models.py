"""
database/models.py — SQLite Tablo Tanımları ve Oluşturma

Tablolar:
  clubs             — Kulüp–Şehir–Bölge mapping (Excel'den sync edilir)
  athletes          — Kayıtlı sporcular (isim + doğum yılı bazlı)
  athlete_clubs     — Sporcu kulüp geçmişi (kulüp değişikliğini takip eder)
  athlete_conflicts — Belirsiz eşleşmeler (kullanıcı onayı bekler)
  races             — Analiz edilen yarışlar
  results           — Yarış sonuçları (her satır: 1 sporcu × 1 yarış × 1 mesafe)

Sporcu Kimliği Tasarımı:
  Birincil kimlik: (name_normalized, birth_year) çifti
  Aynı yarışta aynı isim+YB ama farklı kulüp → 2 farklı sporcu (farklı çocuk)
  Farklı yarışlarda aynı isim+YB ama farklı kulüp → conflict kaydı (kullanıcıya sor)
  Kulüp değişikliği onaylanırsa → athlete_clubs'a eklenir, aynı athlete_id kullanılır
"""

# Tüm SQL CREATE TABLE ifadeleri burada.
# database/db.py bu modülü import eder ve tabloları oluşturur.

SCHEMA_SQL = """

-- ─────────────────────────────────────────────────────────────────────────────
-- Kulüp–Şehir–Bölge Mapping (Excel'den sync edilir)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS clubs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name_alt         TEXT NOT NULL,      -- Alternatif isim (Excel Sütun A)
    name_canonical   TEXT,               -- Kanonik isim   (Excel Sütun C, NULL olabilir)
    name_normalized  TEXT NOT NULL,      -- normalize_for_lookup() sonucu — arama anahtarı
    city             TEXT NOT NULL,      -- Şehir (Excel Sütun E)
    region           INTEGER NOT NULL,   -- Bölge 1-6 (Excel Sütun G)
    synced_at        TEXT DEFAULT (datetime('now')),
    UNIQUE(name_normalized)              -- Aynı normalize değer bir kez kayıtlı olur
);
CREATE INDEX IF NOT EXISTS idx_clubs_normalized ON clubs(name_normalized);


-- ─────────────────────────────────────────────────────────────────────────────
-- Sporcular
-- Birincil kimlik: (name_normalized, birth_year)
-- Aynı isim+YB ama farklı kulüp → farklı sporcu (athlete_conflicts üzerinden yönetilir)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS athletes (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,      -- Görüntüleme adı (normalize_display sonucu)
    name_normalized  TEXT NOT NULL,      -- normalize_for_lookup() — karşılaştırma için
    birth_year       INTEGER NOT NULL,   -- 4 haneli: 2013, 1999 vs.
    gender           TEXT,               -- 'M' veya 'F' (NULL: bilinmiyor)
    school           TEXT,               -- Okul adı (okul yarışları için, NULL: kulüp sporcusu)
    created_at       TEXT DEFAULT (datetime('now')),
    notes            TEXT,               -- Kulüp değişikliği notu vb.
    UNIQUE(name_normalized, birth_year)  -- Aynı (isim, YB) bir kez kaydedilir
);
CREATE INDEX IF NOT EXISTS idx_athletes_name ON athletes(name_normalized);
CREATE INDEX IF NOT EXISTS idx_athletes_yb   ON athletes(birth_year);


-- ─────────────────────────────────────────────────────────────────────────────
-- Sporcu Kulüp Geçmişi
-- Bir sporcu kulüp değiştirdiğinde yeni satır eklenir.
-- Aynı anda birden fazla aktif kayıt olmaz (last_seen_race_url güncellenir).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS athlete_clubs (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    athlete_id           INTEGER NOT NULL REFERENCES athletes(id),
    club_name            TEXT NOT NULL,   -- Yarışta görünen kulüp adı
    club_normalized      TEXT NOT NULL,   -- normalize_for_lookup() sonucu
    city                 TEXT,
    region               INTEGER,
    is_ferdi             INTEGER DEFAULT 0,  -- 1: Ferdi/bağımsız sporcu
    first_seen_race_url  TEXT,
    last_seen_race_url   TEXT,
    first_seen_date      TEXT,
    last_seen_date       TEXT,
    added_at             TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_athlete_clubs_athlete ON athlete_clubs(athlete_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- Belirsiz Sporcu Eşleşmeleri (Kullanıcı Onayı Bekleyen)
-- Farklı yarışlarda aynı (isim, YB) ama farklı kulüp çıkarsa bu tabloya düşer.
-- Kullanıcı 'same_person' veya 'different_person' kararı verir.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS athlete_conflicts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,        -- Sporcu adı (görüntüleme)
    birth_year       INTEGER NOT NULL,
    existing_club    TEXT NOT NULL,        -- DB'deki mevcut kulüp
    new_club         TEXT NOT NULL,        -- Yeni yarıştan gelen kulüp
    race_url         TEXT,                 -- Yeni kulübün geldiği yarış
    status           TEXT DEFAULT 'pending',
                                           -- 'pending' | 'same_person' | 'different_person'
    athlete_id_a     INTEGER REFERENCES athletes(id),  -- Mevcut sporcu
    athlete_id_b     INTEGER REFERENCES athletes(id),  -- Yeni sporcu (different_person ise)
    created_at       TEXT DEFAULT (datetime('now')),
    resolved_at      TEXT,
    resolved_by      TEXT,                 -- 'user' veya 'auto'
    notes            TEXT
);
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON athlete_conflicts(status);
CREATE INDEX IF NOT EXISTS idx_conflicts_name   ON athlete_conflicts(name, birth_year);


-- ─────────────────────────────────────────────────────────────────────────────
-- Yarışlar
-- Her analiz edilen yarışın kaydı
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS races (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    url          TEXT UNIQUE NOT NULL,   -- Yarış URL'si (benzersiz)
    title        TEXT,                   -- Yarış adı (sayfadan çekilir)
    race_date    TEXT,                   -- Yarış tarihi (YYYY-MM-DD)
    location     TEXT,                   -- Şehir/tesis
    source_type  TEXT,                   -- 'lenex' | 'pdf' | 'html'
    scraped_at   TEXT DEFAULT (datetime('now')),
    result_count INTEGER DEFAULT 0       -- Kaç sonuç çıkarıldı
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Yarış Sonuçları
-- Her satır: 1 sporcu × 1 yarış × 1 (stil + mesafe) kombinasyonu
-- Aynı sporcu aynı yarışta elim+final koşarsa min süre alınır (en iyisi kaydedilir).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id         INTEGER NOT NULL REFERENCES races(id),
    athlete_id      INTEGER REFERENCES athletes(id),
                    -- NULL: conflict çözülene kadar bağlanmamış
    conflict_id     INTEGER REFERENCES athlete_conflicts(id),
                    -- NULL: çakışma yok

    -- Yarıştan gelen ham değerler (değiştirilmez)
    name_raw        TEXT NOT NULL,    -- Kaynaktan gelen orijinal isim
    yb_raw          TEXT,             -- 2 haneli YB, kaynaktan
    club_raw        TEXT NOT NULL,    -- Kaynaktan gelen orijinal kulüp adı

    -- Hesaplanan / zenginleştirilmiş değerler
    birth_year      INTEGER,          -- 4 haneli
    age             INTEGER,          -- COMPETITION_YEAR - birth_year
    city            TEXT,
    region          INTEGER,
    gender          TEXT,             -- 'M' | 'F'
    stroke          TEXT,             -- 'Serbest' | 'Sirtüstü' | 'Kurbağalama' | 'Kelebek' | 'Karışık'
    distance        INTEGER,          -- 50 | 100 | 200 | 400 | 800 | 1500
    time_text       TEXT,             -- "00:30.45" formatı
    time_seconds    REAL,             -- Sıralama/karşılaştırma için saniye cinsinden

    added_at        TEXT DEFAULT (datetime('now')),

    -- Aynı (race, athlete, stroke, distance) için tekrar engelidir.
    -- Farklı yarışlar aynı (stroke, distance) kayıt için iki farklı satır tutar.
    UNIQUE(race_id, athlete_id, stroke, distance)
);
CREATE INDEX IF NOT EXISTS idx_results_race    ON results(race_id);
CREATE INDEX IF NOT EXISTS idx_results_athlete ON results(athlete_id);
CREATE INDEX IF NOT EXISTS idx_results_event   ON results(stroke, distance);


-- ─────────────────────────────────────────────────────────────────────────────
-- Mapping Sync Geçmişi
-- Excel'den ne zaman sync yapıldığını tutar
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    synced_at    TEXT DEFAULT (datetime('now')),
    source_file  TEXT,
    rows_loaded  INTEGER,
    rows_skipped INTEGER,
    notes        TEXT
);


-- ─────────────────────────────────────────────────────────────────────────────
-- Sporcu Düzey Geçersizlemeler (Athlete Overrides)
-- Belirli bir sporcu için kulüp/şehir/bölge ve görüntüleme adı düzeltmesi.
-- Örnek kullanım:
--   "Ferdi" yazılan ulusal yarıştaki sporcu için gerçek şehir ataması.
--   ı/i encoding hatasından kaynaklanan isim düzeltmesi (Deniz Anil → Deniz Anıl).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS athlete_overrides (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name_normalized  TEXT NOT NULL,   -- normalize_for_lookup() sonucu
    birth_year       INTEGER,         -- NULL = tüm yıllar eşleşsin
    club_override    TEXT,            -- Yeni kulüp ismi (NULL = değiştirme)
    city             TEXT,            -- Yeni şehir (NULL = değiştirme)
    region           INTEGER,         -- Yeni bölge (NULL = değiştirme)
    display_name     TEXT,            -- Görüntüleme adı düzeltmesi (NULL = değiştirme)
    notes            TEXT,
    created_at       TEXT DEFAULT (datetime('now')),
    UNIQUE(name_normalized, birth_year)
);
CREATE INDEX IF NOT EXISTS idx_overrides_name ON athlete_overrides(name_normalized);

"""
