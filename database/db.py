"""
database/db.py — SQLite Bağlantı ve CRUD İşlemleri

Singleton bağlantı yönetimi: get_connection() her zaman aynı bağlantıyı döner.
Tablolar yoksa otomatik oluşturulur (init_db).

Kullanım:
  from database.db import get_connection, init_db

  init_db()                          # İlk çalıştırmada tabloları oluşturur
  conn = get_connection()            # Bağlantı al
  cursor = conn.cursor()
  cursor.execute("SELECT ...")
"""

import sqlite3
import logging
import os
from pathlib import Path

from config import DB_PATH
from database.models import SCHEMA_SQL

logger = logging.getLogger(__name__)

_connection: sqlite3.Connection | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Bağlantı yönetimi
# ─────────────────────────────────────────────────────────────────────────────

def get_connection() -> sqlite3.Connection:
    """
    SQLite bağlantısını döner. İlk çağrıda oluşturur (singleton).
    Bağlantı thread içinde paylaşılır — dashboard için yeterli (tek kullanıcı).

    Row factory: sqlite3.Row → sütun adıyla erişim (row['city'] gibi)
    """
    global _connection
    if _connection is None:
        # data/ klasörü yoksa oluştur
        Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

        _connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        _connection.row_factory = sqlite3.Row

        # Performans ayarları
        _connection.execute("PRAGMA journal_mode=WAL")   # Okuma-yazma eşzamanlı
        _connection.execute("PRAGMA foreign_keys=ON")    # FK kısıtları aktif
        _connection.execute("PRAGMA synchronous=NORMAL") # Daha hızlı write

        logger.info("DB bağlantısı açıldı: %s", DB_PATH)

    return _connection


def close_connection() -> None:
    """Bağlantıyı kapat (uygulama kapanışında çağrılır)."""
    global _connection
    if _connection:
        _connection.close()
        _connection = None
        logger.info("DB bağlantısı kapatıldı.")


def init_db() -> None:
    """
    Tabloları oluşturur (IF NOT EXISTS — mevcut veriyi silmez).
    Uygulama başlangıcında bir kez çağrılır.
    """
    conn = get_connection()
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    logger.info("DB tabloları hazır: %s", DB_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# Kulüp CRUD
# ─────────────────────────────────────────────────────────────────────────────

def upsert_club(name_alt: str, name_canonical: str | None,
                name_normalized: str, city: str, region: int) -> None:
    """
    Kulüp kaydını ekler veya günceller (UPSERT).
    sync_mapping.py tarafından çağrılır.
    """
    conn = get_connection()
    conn.execute("""
        INSERT INTO clubs (name_alt, name_canonical, name_normalized, city, region)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(name_normalized)
        DO UPDATE SET
            name_alt       = excluded.name_alt,
            name_canonical = excluded.name_canonical,
            city           = excluded.city,
            region         = excluded.region,
            synced_at      = datetime('now')
    """, (name_alt, name_canonical, name_normalized, city, region))


def lookup_club_db(name_normalized: str) -> sqlite3.Row | None:
    """
    Normalize edilmiş kulüp adıyla DB'de arar.
    Döndürür: sqlite3.Row (name_canonical, city, region) veya None
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT name_canonical, city, region FROM clubs WHERE name_normalized = ?",
        (name_normalized,)
    ).fetchone()
    return row


def lookup_club_db_nospace(name_normalized_nospace: str) -> sqlite3.Row | None:
    """
    Boşluk kaldırılmış normalize anahtarıyla DB'de arar (OCR fallback).
    OCR birleşik kulüp adları için: "fenerbahcesporkulubu" → "Fenerbahçe Spor Kulübü"
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT name_canonical, city, region FROM clubs"
        " WHERE REPLACE(name_normalized, ' ', '') = ? LIMIT 1",
        (name_normalized_nospace,)
    ).fetchone()
    return row


def get_all_clubs_count() -> int:
    conn = get_connection()
    return conn.execute("SELECT COUNT(*) FROM clubs").fetchone()[0]


# ─────────────────────────────────────────────────────────────────────────────
# Sporcu CRUD
# ─────────────────────────────────────────────────────────────────────────────

def find_athlete(name_normalized: str, birth_year: int) -> sqlite3.Row | None:
    """
    (isim_normalized, doğum_yılı) çiftiyle sporcu arar.
    Döndürür: athletes satırı veya None
    """
    conn = get_connection()
    return conn.execute(
        "SELECT * FROM athletes WHERE name_normalized = ? AND birth_year = ?",
        (name_normalized, birth_year)
    ).fetchone()


def find_athlete_by_id(athlete_id: int) -> sqlite3.Row | None:
    conn = get_connection()
    return conn.execute(
        "SELECT * FROM athletes WHERE id = ?", (athlete_id,)
    ).fetchone()


def create_athlete(name: str, name_normalized: str, birth_year: int,
                   gender: str | None = None, notes: str | None = None) -> int:
    """
    Yeni sporcu oluşturur. Döndürür: yeni athlete_id
    """
    conn = get_connection()
    cur = conn.execute(
        "INSERT INTO athletes (name, name_normalized, birth_year, gender, notes) VALUES (?,?,?,?,?)",
        (name, name_normalized, birth_year, gender, notes)
    )
    conn.commit()
    return cur.lastrowid


def update_athlete_notes(athlete_id: int, notes: str) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE athletes SET notes = ? WHERE id = ?", (notes, athlete_id)
    )
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Sporcu Kulüp Geçmişi
# ─────────────────────────────────────────────────────────────────────────────

def get_athlete_current_club(athlete_id: int) -> sqlite3.Row | None:
    """
    Sporcunun en son görülen kulüp kaydını döner.
    """
    conn = get_connection()
    return conn.execute("""
        SELECT * FROM athlete_clubs
        WHERE athlete_id = ?
        ORDER BY last_seen_date DESC, added_at DESC
        LIMIT 1
    """, (athlete_id,)).fetchone()


def upsert_athlete_club(athlete_id: int, club_name: str, club_normalized: str,
                         city: str | None, region: int | None, is_ferdi: bool,
                         race_url: str, race_date: str | None) -> None:
    """
    Sporcunun kulüp kaydını ekler veya günceller.

    Mantık:
      - Aynı kulüp → last_seen güncellenir
      - Farklı kulüp → yeni satır eklenir (geçmiş korunur)
    """
    conn = get_connection()

    # Aynı kulüpte kayıt var mı?
    existing = conn.execute("""
        SELECT id FROM athlete_clubs
        WHERE athlete_id = ? AND club_normalized = ?
    """, (athlete_id, club_normalized)).fetchone()

    if existing:
        conn.execute("""
            UPDATE athlete_clubs
            SET last_seen_race_url = ?, last_seen_date = ?
            WHERE id = ?
        """, (race_url, race_date, existing["id"]))
    else:
        conn.execute("""
            INSERT INTO athlete_clubs
                (athlete_id, club_name, club_normalized, city, region,
                 is_ferdi, first_seen_race_url, last_seen_race_url,
                 first_seen_date, last_seen_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (athlete_id, club_name, club_normalized, city, region,
              int(is_ferdi), race_url, race_url, race_date, race_date))

    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Çakışma (Conflict) Yönetimi
# ─────────────────────────────────────────────────────────────────────────────

def create_conflict(name: str, birth_year: int, existing_club: str,
                    new_club: str, race_url: str,
                    athlete_id_a: int) -> int:
    """
    Belirsiz sporcu eşleşmesi kaydeder. Döndürür: conflict_id
    """
    conn = get_connection()
    cur = conn.execute("""
        INSERT INTO athlete_conflicts
            (name, birth_year, existing_club, new_club, race_url, athlete_id_a)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (name, birth_year, existing_club, new_club, race_url, athlete_id_a))
    conn.commit()
    logger.warning(
        "Conflict oluşturuldu: '%s' (%d) — '%s' vs '%s'",
        name, birth_year, existing_club, new_club
    )
    return cur.lastrowid


def get_pending_conflicts() -> list[sqlite3.Row]:
    """Kullanıcı onayı bekleyen çakışmaları listeler."""
    conn = get_connection()
    return conn.execute("""
        SELECT * FROM athlete_conflicts WHERE status = 'pending'
        ORDER BY created_at
    """).fetchall()


def resolve_conflict(conflict_id: int, decision: str,
                     athlete_id_b: int | None = None,
                     notes: str | None = None) -> None:
    """
    Çakışmayı çözer.

    Parametreler:
      decision: 'same_person' veya 'different_person'
      athlete_id_b: 'different_person' ise yeni sporcu ID'si
      notes: Kullanıcı notu (opsiyonel)
    """
    assert decision in ("same_person", "different_person"), \
        "decision 'same_person' veya 'different_person' olmalı"

    conn = get_connection()
    conn.execute("""
        UPDATE athlete_conflicts
        SET status       = ?,
            athlete_id_b = ?,
            resolved_at  = datetime('now'),
            resolved_by  = 'user',
            notes        = ?
        WHERE id = ?
    """, (decision, athlete_id_b, notes, conflict_id))
    conn.commit()
    logger.info("Conflict %d çözüldü: %s", conflict_id, decision)


# ─────────────────────────────────────────────────────────────────────────────
# Yarış CRUD
# ─────────────────────────────────────────────────────────────────────────────

def get_or_create_race(url: str, title: str | None = None,
                       race_date: str | None = None, location: str | None = None,
                       source_type: str | None = None) -> int:
    """
    Yarışı döner veya oluşturur. Döndürür: race_id
    """
    conn = get_connection()
    existing = conn.execute(
        "SELECT id FROM races WHERE url = ?", (url,)
    ).fetchone()

    if existing:
        return existing["id"]

    cur = conn.execute("""
        INSERT INTO races (url, title, race_date, location, source_type)
        VALUES (?, ?, ?, ?, ?)
    """, (url, title, race_date, location, source_type))
    conn.commit()
    return cur.lastrowid


def update_race_result_count(race_id: int) -> None:
    conn = get_connection()
    conn.execute("""
        UPDATE races SET result_count = (
            SELECT COUNT(*) FROM results WHERE race_id = ?
        ) WHERE id = ?
    """, (race_id, race_id))
    conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Sonuç CRUD
# ─────────────────────────────────────────────────────────────────────────────

def upsert_result(race_id: int, athlete_id: int | None,
                  conflict_id: int | None, name_raw: str, yb_raw: str | None,
                  club_raw: str, birth_year: int | None, age: int | None,
                  city: str | None, region: int | None, gender: str | None,
                  stroke: str, distance: int,
                  time_text: str, time_seconds: float) -> None:
    """
    Sonucu ekler veya en iyi süreyle günceller (UPSERT).

    Aynı (race_id, athlete_id, stroke, distance) çifti için:
      - Yeni süre daha iyiyse (daha küçük) → günceller
      - Değilse → dokunmaz (zaten en iyi kayıtlı)
    """
    conn = get_connection()

    existing = conn.execute("""
        SELECT id, time_seconds FROM results
        WHERE race_id = ? AND athlete_id = ? AND stroke = ? AND distance = ?
    """, (race_id, athlete_id, stroke, distance)).fetchone()

    if existing:
        if time_seconds < existing["time_seconds"]:
            conn.execute("""
                UPDATE results SET time_text = ?, time_seconds = ? WHERE id = ?
            """, (time_text, time_seconds, existing["id"]))
            conn.commit()
    else:
        conn.execute("""
            INSERT INTO results
                (race_id, athlete_id, conflict_id, name_raw, yb_raw, club_raw,
                 birth_year, age, city, region, gender, stroke, distance,
                 time_text, time_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (race_id, athlete_id, conflict_id, name_raw, yb_raw, club_raw,
              birth_year, age, city, region, gender, stroke, distance,
              time_text, time_seconds))
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Genel sorgu yardımcıları
# ─────────────────────────────────────────────────────────────────────────────

def get_db_stats() -> dict:
    """DB içeriği hakkında özet bilgi."""
    conn = get_connection()
    return {
        "clubs":           conn.execute("SELECT COUNT(*) FROM clubs").fetchone()[0],
        "athletes":        conn.execute("SELECT COUNT(*) FROM athletes").fetchone()[0],
        "races":           conn.execute("SELECT COUNT(*) FROM races").fetchone()[0],
        "results":         conn.execute("SELECT COUNT(*) FROM results").fetchone()[0],
        "pending_conflicts": conn.execute(
            "SELECT COUNT(*) FROM athlete_conflicts WHERE status='pending'"
        ).fetchone()[0],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Sporcu Düzey Geçersizlemeler (Athlete Overrides)
# ─────────────────────────────────────────────────────────────────────────────

def upsert_athlete_override(
    name_normalized: str,
    birth_year: int | None = None,
    club_override: str | None = None,
    city: str | None = None,
    region: int | None = None,
    display_name: str | None = None,
    notes: str | None = None,
) -> None:
    """
    Sporcu düzey geçersizleme ekler veya günceller.

    Örnekler:
      # Ferdi yazılan ulusal yarışta gerçek şehir bilgisi:
      upsert_athlete_override("tuna kocayigit", 2014, "Sakarya Ferdi", "Sakarya", 2)

      # ı/i encoding hatası düzeltmesi:
      upsert_athlete_override("deniz anil", 2012, display_name="Deniz Anıl")
    """
    conn = get_connection()
    conn.execute("""
        INSERT INTO athlete_overrides
            (name_normalized, birth_year, club_override, city, region, display_name, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name_normalized, birth_year) DO UPDATE SET
            club_override = excluded.club_override,
            city          = excluded.city,
            region        = excluded.region,
            display_name  = excluded.display_name,
            notes         = excluded.notes
    """, (name_normalized, birth_year, club_override, city, region, display_name, notes))
    conn.commit()


def lookup_athlete_override(name_normalized: str,
                             birth_year: int | None) -> sqlite3.Row | None:
    """
    Sporcu için override kaydını döner.
    Önce (name, birth_year) eşleşmesine bakar,
    sonra (name, NULL) genel eşleşmeye bakar.
    """
    conn = get_connection()
    # Tam eşleşme — ORDER BY id DESC: en son eklenen/güncellenen kazanır
    if birth_year is not None:
        row = conn.execute(
            "SELECT * FROM athlete_overrides WHERE name_normalized=? AND birth_year=?"
            " ORDER BY id DESC LIMIT 1",
            (name_normalized, birth_year)
        ).fetchone()
        if row:
            return row
    # Genel eşleşme (birth_year=NULL) — ORDER BY id DESC: en son eklenen kazanır
    return conn.execute(
        "SELECT * FROM athlete_overrides WHERE name_normalized=? AND birth_year IS NULL"
        " ORDER BY id DESC LIMIT 1",
        (name_normalized,)
    ).fetchone()


def get_duplicate_display_names() -> list[sqlite3.Row]:
    """
    athletes tablosunda aynı görüntüleme adına sahip (farklı ID'li) kayıtları listeler.
    """
    conn = get_connection()
    return conn.execute("""
        SELECT name, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM athletes
        GROUP BY name
        HAVING cnt > 1
        ORDER BY name
    """).fetchall()
