"""
federasyon/db_fed.py
--------------------
Federasyon karma verisi için SQLite tablolar ve CRUD.

Mevcut bolge_karmalari.db'e ek tablolar ekler:
  fed_results      — ham yarış sonuçları (sporcu × branş × şehir)
  fed_athlete_best — her sporcu için her branştan en iyi puan (materialized)
"""

import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "bolge_karmalari.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS fed_start_list (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    race_leg     TEXT    NOT NULL,
    athlete_name TEXT    NOT NULL,
    birth_year   INTEGER NOT NULL,
    gender       TEXT    NOT NULL,
    stroke       TEXT    NOT NULL,
    distance     INTEGER NOT NULL,
    entry_time   TEXT,
    pdf_seq      INTEGER,
    added_at     TEXT    DEFAULT (datetime('now')),
    UNIQUE(race_leg, athlete_name, birth_year, stroke, distance)
);

CREATE TABLE IF NOT EXISTS fed_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    race_leg        TEXT    NOT NULL,   -- 'antalya' | 'edirne'
    race_date       TEXT,
    athlete_name    TEXT    NOT NULL,
    birth_year      INTEGER NOT NULL,
    gender          TEXT    NOT NULL,   -- 'F' | 'M'
    region          INTEGER,
    city            TEXT,
    club            TEXT,
    stroke          TEXT    NOT NULL,
    distance        INTEGER NOT NULL,
    time_text       TEXT,
    time_seconds    REAL,
    points          INTEGER DEFAULT 0,
    source_pdf_seq  INTEGER,           -- ResultList_N.pdf sıra numarası
    added_at        TEXT    DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_fed_results
  ON fed_results(race_leg, athlete_name, birth_year, stroke, distance);

CREATE TABLE IF NOT EXISTS fed_athlete_best (
    athlete_name  TEXT    NOT NULL,
    birth_year    INTEGER NOT NULL,
    gender        TEXT    NOT NULL,
    region        INTEGER,
    city          TEXT,
    club          TEXT,
    stroke        TEXT    NOT NULL,
    distance      INTEGER NOT NULL,
    best_points   INTEGER DEFAULT 0,
    best_time_sec REAL,
    best_time_txt TEXT,
    best_leg      TEXT,
    PRIMARY KEY (athlete_name, birth_year, stroke, distance)
);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, col_type: str):
    """Tablo zaten varsa eksik kolonu ekler (migrasyon yardımcısı)."""
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def init_fed_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Eski tablolara geriye dönük uyumlu kolon eklemeleri
        _add_column_if_missing(conn, "fed_results",    "source_pdf_seq", "INTEGER")
        _add_column_if_missing(conn, "fed_start_list", "pdf_seq",        "INTEGER")
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# fed_results yazma
# ─────────────────────────────────────────────────────────────────────────────
def _canonical_name(conn: sqlite3.Connection, athlete_name: str, birth_year: int) -> str:
    """Mevcut kayıtlarda normalize edilmiş eşleşen adı döner (i/ı toleransı)."""
    from modules.m1_normalize import normalize_for_lookup
    name_norm = normalize_for_lookup(athlete_name)
    rows = conn.execute(
        "SELECT DISTINCT athlete_name FROM fed_results WHERE birth_year=?",
        (birth_year,)
    ).fetchall()
    for row in rows:
        if normalize_for_lookup(row["athlete_name"]) == name_norm:
            return row["athlete_name"]
    return athlete_name
def upsert_result(race_leg: str, race_date: str, athlete_name: str,
                  birth_year: int, gender: str, region: int, city: str, club: str,
                  stroke: str, distance: int, time_text: str,
                  time_seconds: float, points: int, pdf_seq: int = None):
    """
    Yeni sonuç ekle veya güncelle.
    Aynı yarış bacağı (leg) + sporcu + branşta daha iyi süre gelirse günceller.
    pdf_seq: ResultList_N.pdf'deki N — bekleyen yarış tespiti için kullanılır.
    """
    with get_conn() as conn:
        athlete_name = _canonical_name(conn, athlete_name, birth_year)
        existing = conn.execute(
            "SELECT id, time_seconds FROM fed_results "
            "WHERE race_leg=? AND athlete_name=? AND birth_year=? "
            "AND stroke=? AND distance=?",
            (race_leg, athlete_name, birth_year, stroke, distance)
        ).fetchone()

        if existing:
            if time_seconds is not None and (
                existing["time_seconds"] is None or
                time_seconds < existing["time_seconds"]
            ):
                conn.execute(
                    "UPDATE fed_results SET time_text=?, time_seconds=?, points=?, "
                    "race_date=?, gender=?, region=?, city=?, club=?, source_pdf_seq=? "
                    "WHERE id=?",
                    (time_text, time_seconds, points, race_date,
                     gender, region, city, club, pdf_seq, existing["id"])
                )
        else:
            conn.execute(
                "INSERT INTO fed_results "
                "(race_leg, race_date, athlete_name, birth_year, gender, region, "
                " city, club, stroke, distance, time_text, time_seconds, points, source_pdf_seq) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (race_leg, race_date, athlete_name, birth_year, gender, region,
                 city, club, stroke, distance, time_text, time_seconds, points, pdf_seq)
            )
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# fed_athlete_best yeniden hesapla
# ─────────────────────────────────────────────────────────────────────────────

def rebuild_athlete_best():
    """
    fed_results'tan her sporcu × branş için en iyi puanı yeniden hesaplar.
    """
    with get_conn() as conn:
        conn.execute("DELETE FROM fed_athlete_best")

        # Her sporcu × branş grubunun max puanını bul
        max_rows = conn.execute("""
            SELECT athlete_name, birth_year, stroke, distance, MAX(points) as best_points
            FROM fed_results
            GROUP BY athlete_name, birth_year, stroke, distance
        """).fetchall()

        for mr in max_rows:
            # O max puana ait ilk satırı getir (zaman + meta)
            detail = conn.execute("""
                SELECT gender, region, city, club, time_seconds, time_text, race_leg
                FROM fed_results
                WHERE athlete_name=? AND birth_year=? AND stroke=? AND distance=?
                  AND points=?
                ORDER BY time_seconds ASC
                LIMIT 1
            """, (mr["athlete_name"], mr["birth_year"],
                  mr["stroke"], mr["distance"], mr["best_points"])).fetchone()

            if not detail:
                continue

            conn.execute("""
                INSERT OR REPLACE INTO fed_athlete_best
                (athlete_name, birth_year, gender, region, city, club,
                 stroke, distance, best_points, best_time_sec, best_time_txt, best_leg)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (mr["athlete_name"], mr["birth_year"],
                  detail["gender"], detail["region"], detail["city"], detail["club"],
                  mr["stroke"], mr["distance"], mr["best_points"],
                  detail["time_seconds"], detail["time_text"], detail["race_leg"]))

        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Sorgu: sıralama için veri çek
# ─────────────────────────────────────────────────────────────────────────────

def load_athletes_for_ranking(birth_years: list[int] = None) -> list[dict]:
    """
    fed_athlete_best'ten sporcu bazlı event_scores dict'i oluşturur.
    Her sporcu için bir dict döner:
      {name, birth_year, gender, region, city, club, event_scores: {(s,d): pts}}
    """
    with get_conn() as conn:
        if birth_years:
            placeholders = ",".join("?" * len(birth_years))
            rows = conn.execute(
                f"SELECT * FROM fed_athlete_best WHERE birth_year IN ({placeholders})",
                birth_years
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM fed_athlete_best").fetchall()

    athletes: dict[tuple, dict] = {}
    for r in rows:
        key = (r["athlete_name"], r["birth_year"])
        if key not in athletes:
            athletes[key] = {
                "name":         r["athlete_name"],
                "birth_year":   r["birth_year"],
                "gender":       r["gender"],
                "region":       r["region"],
                "city":         r["city"],
                "club":         r["club"],
                "event_scores": {},
            }
        if r["best_points"] and r["best_points"] > 0:
            athletes[key]["event_scores"][(r["stroke"], r["distance"])] = r["best_points"]

    return list(athletes.values())


def load_athletes_for_ranking_by_leg(birth_years: list[int] = None) -> dict:
    """
    Her sporcu için leg bazlı event_scores döndürür:
    {(name, by): {
        'antalya': {(stroke,dist): pts},
        'edirne':  {(stroke,dist): pts},
        'combined':{(stroke,dist): pts},   # her branşta max
        'meta': {name, birth_year, gender, region, city, club}
    }}
    """
    with get_conn() as conn:
        if birth_years:
            placeholders = ",".join("?" * len(birth_years))
            rows = conn.execute(
                f"SELECT * FROM fed_results WHERE birth_year IN ({placeholders})",
                birth_years
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM fed_results").fetchall()

    athletes: dict = {}
    for r in rows:
        key = (r["athlete_name"], r["birth_year"])
        if key not in athletes:
            athletes[key] = {
                "name":       r["athlete_name"],
                "birth_year": r["birth_year"],
                "gender":     r["gender"],
                "region":     r["region"],
                "city":       r["city"],
                "club":       r["club"],
                "antalya":    {},
                "edirne":     {},
                "combined":   {},
            }
        ev = (r["stroke"], r["distance"])
        pts = r["points"] or 0
        if pts <= 0:
            continue
        leg = r["race_leg"]
        if leg in ("antalya", "edirne"):
            prev = athletes[key][leg].get(ev, 0)
            if pts > prev:
                athletes[key][leg][ev] = pts
        prev_c = athletes[key]["combined"].get(ev, 0)
        if pts > prev_c:
            athletes[key]["combined"][ev] = pts
        # güncelle meta (daha güncel satırdan)
        if r["region"]:
            athletes[key]["region"] = r["region"]
        if r["city"]:
            athletes[key]["city"] = r["city"]

    return athletes


def get_event_winners(birth_years: list = None) -> dict:
    """
    Her (race_leg, gender, stroke, distance) kombinasyonunda en iyi zamana sahip
    sporcuyu döner. Yaş grupları beraber değerlendirilir (13+14+15 aynı havuz).
    Kız/Erkek ayrı.

    Dönen yapı:
      {(race_leg, gender, stroke, distance): "sporcu adı"}
    """
    bys = birth_years or [2011, 2012, 2013]
    placeholders = ",".join("?" * len(bys))
    with get_conn() as conn:
        rows = conn.execute(f"""
            SELECT race_leg, gender, stroke, distance, athlete_name, time_seconds
            FROM fed_results
            WHERE birth_year IN ({placeholders})
              AND time_seconds IS NOT NULL
              AND time_seconds > 0
            ORDER BY race_leg, gender, stroke, distance, time_seconds ASC
        """, bys).fetchall()

    winners = {}
    for row in rows:
        key = (row["race_leg"], row["gender"], row["stroke"], row["distance"])
        if key not in winners:  # İlk kayıt = en iyi zaman (ORDER BY asc)
            winners[key] = row["athlete_name"]
    return winners


def get_stats() -> dict:
    with get_conn() as conn:
        r = conn.execute("""
            SELECT
              COUNT(DISTINCT athlete_name || birth_year) as athletes,
              COUNT(*) as result_rows,
              GROUP_CONCAT(DISTINCT race_leg) as legs
            FROM fed_results
        """).fetchone()
        return dict(r) if r else {}


# ─────────────────────────────────────────────────────────────────────────────
# fed_start_list — start list (henüz yüzülmemiş kayıtlar)
# ─────────────────────────────────────────────────────────────────────────────

def save_start_list(race_leg: str, entries: list[dict]):
    """
    Start list entry'lerini DB'ye yazar.
    entries: [{"name_raw"|"name": str, "birth_year": int, "gender": str,
               "stroke": str, "distance": int, "entry_time_txt": str|None}]

    İsim normalizasyonu: fed_results'taki canonical isimle eşleştirilir (i/ı toleransı).
    """
    if not entries:
        return 0
    from modules.m1_normalize import normalize_for_lookup

    written = 0
    with get_conn() as conn:
        # fed_results'taki mevcut isimleri önbelleğe al (normalize → canonical)
        existing = conn.execute(
            "SELECT DISTINCT athlete_name, birth_year FROM fed_results"
        ).fetchall()
        canonical_map: dict[tuple, str] = {}
        for row in existing:
            key = (normalize_for_lookup(row["athlete_name"]), row["birth_year"])
            canonical_map[key] = row["athlete_name"]

        # Önce bu leg'e ait eski start list'i temizle
        conn.execute("DELETE FROM fed_start_list WHERE race_leg=?", (race_leg,))

        for e in entries:
            name = e.get("name") or e.get("name_raw", "")
            by   = e.get("birth_year")
            if not name or not by:
                continue
            # fed_results'ta eşleşen canonical isim varsa onu kullan
            norm_key = (normalize_for_lookup(name), by)
            name = canonical_map.get(norm_key, name)
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO fed_start_list
                    (race_leg, athlete_name, birth_year, gender, stroke, distance, entry_time, pdf_seq)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (race_leg, name, by, e.get("gender",""),
                      e.get("stroke",""), e.get("distance",0),
                      e.get("entry_time_txt"), e.get("pdf_seq")))
                written += 1
            except Exception:
                pass
        conn.commit()
    return written


def get_pending_events(race_leg: str, birth_years: list[int] = None) -> dict:
    """
    start list'te olup o leg'de henüz yüzülmemiş branşları döner.
    Döner: {(athlete_name, birth_year): [{"stroke": s, "dist": d, "entry_time": t}, ...]}

    Üç katmanlı kontrol:

    1. source_pdf_seq varsa (yeni veri):
       - pdf_seq < max_seq → sonraki yarış başladı → bu yarış kesinlikle bitti
       - pdf_seq == max_seq → son aktif yarış → sporcu bazlı kontrol:
           sporcunun sonucu varsa → yüzdü, bekleyende değil
           yoksa → henüz yüzmedi, bekleyende
       - pdf_seq is NULL → eski veri fallback

    2. source_pdf_seq yoksa (NULL/eski veri):
       - (birth_year, stroke, distance) kombinasyonunda herhangi bir sonuç varsa → bitti
       - gender ile de karşılaştır; gender boş/yanlışsa sadece by+stroke+dist yeterli

    3. Hiç sonuç yoksa → tüm start list sporcuları bekleyende
    """
    with get_conn() as conn:
        by_clause = ""
        params_sl: list = [race_leg]
        params_r:  list = [race_leg]
        if birth_years:
            ph = ",".join("?" * len(birth_years))
            by_clause = f" AND birth_year IN ({ph})"
            params_sl += birth_years
            params_r  += birth_years

        sl_rows = conn.execute(
            f"SELECT athlete_name, birth_year, gender, stroke, distance, entry_time "
            f"FROM fed_start_list WHERE race_leg=?{by_clause}",
            params_sl
        ).fetchall()

        # Her etkinlik için sonuç bilgileri (pdf_seq ile birlikte)
        event_rows = conn.execute(
            f"SELECT DISTINCT birth_year, gender, stroke, distance, "
            f"MIN(source_pdf_seq) as pdf_seq "
            f"FROM fed_results WHERE race_leg=?{by_clause} "
            f"GROUP BY birth_year, gender, stroke, distance",
            params_r
        ).fetchall()

        # Bu leg'deki maksimum pdf_seq (son aktif PDF)
        max_seq_row = conn.execute(
            f"SELECT MAX(source_pdf_seq) as ms FROM fed_results WHERE race_leg=?{by_clause}",
            params_r
        ).fetchone()
        max_seq = max_seq_row["ms"] if max_seq_row else None

        # Son aktif pdf_seq'deki sporcuların isimleri (athlete-level check için)
        athletes_done_in_last_pdf = set()
        if max_seq is not None:
            last_pdf_rows = conn.execute(
                f"SELECT DISTINCT athlete_name, birth_year, stroke, distance "
                f"FROM fed_results WHERE race_leg=? AND source_pdf_seq=?{by_clause}",
                [race_leg, max_seq] + (list(birth_years) if birth_years else [])
            ).fetchall()
            athletes_done_in_last_pdf = {
                (r["athlete_name"], r["birth_year"], r["stroke"], r["distance"])
                for r in last_pdf_rows
            }

    # Tamamlanan etkinlik setleri
    # gender'lı ve gender'sız — start list'te gender boş olabilir
    completed_with_gender: set = set()   # (by, gender, stroke, dist) — kesin eşleşme
    completed_no_gender:   set = set()   # (by, stroke, dist) — gender toleranslı
    last_active_events:    set = set()   # pdf_seq == max_seq olan etkinlikler

    for r in event_rows:
        by, g, s, d = r["birth_year"], r["gender"], r["stroke"], r["distance"]
        seq = r["pdf_seq"]

        if seq is not None and max_seq is not None:
            if seq < max_seq:
                # Sonraki PDF zaten başlamış → bu etkinlik kesinlikle bitti
                completed_with_gender.add((by, g, s, d))
                completed_no_gender.add((by, s, d))
            else:
                # seq == max_seq: son aktif etkinlik → sporcu bazlı kontrol
                last_active_events.add((by, g, s, d))
                # gender toleranslı versiyon da ekle
                last_active_events.add((by, "", s, d))
        else:
            # pdf_seq yok (eski veri) → herhangi bir sonuç varsa bitti say
            completed_with_gender.add((by, g, s, d))
            completed_no_gender.add((by, s, d))

    result: dict = {}
    for r in sl_rows:
        by, g, s, d = r["birth_year"], r["gender"] or "", r["stroke"], r["distance"]
        name = r["athlete_name"]

        # 1. Kesin tamamlanmış mı?
        if g:
            if (by, g, s, d) in completed_with_gender:
                continue
        else:
            if (by, s, d) in completed_no_gender:
                continue

        # 2. Son aktif etkinlikte (partial results) → sporcu bazlı kontrol
        in_last = (by, g, s, d) in last_active_events or (by, "", s, d) in last_active_events
        if in_last:
            # Bu sporcunun sonucu var mı?
            from modules.m1_normalize import normalize_for_lookup
            norm = normalize_for_lookup(name)
            found = any(
                normalize_for_lookup(k[0]) == norm and k[1] == by and k[2] == s and k[3] == d
                for k in athletes_done_in_last_pdf
            )
            if found:
                continue  # Yüzdü → bekleyende değil

        # 3. Bekleyende
        key = (name, by)
        result.setdefault(key, []).append({
            "stroke":     s,
            "dist":       d,
            "entry_time": r["entry_time"],
        })
    return result
