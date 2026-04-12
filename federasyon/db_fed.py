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
CREATE TABLE IF NOT EXISTS fed_results (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    race_leg      TEXT    NOT NULL,   -- 'antalya' | 'edirne'
    race_date     TEXT,
    athlete_name  TEXT    NOT NULL,
    birth_year    INTEGER NOT NULL,
    gender        TEXT    NOT NULL,   -- 'F' | 'M'
    region        INTEGER,
    city          TEXT,
    club          TEXT,
    stroke        TEXT    NOT NULL,
    distance      INTEGER NOT NULL,
    time_text     TEXT,
    time_seconds  REAL,
    points        INTEGER DEFAULT 0,
    added_at      TEXT    DEFAULT (datetime('now'))
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


def init_fed_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# fed_results yazma
# ─────────────────────────────────────────────────────────────────────────────

def upsert_result(race_leg: str, race_date: str, athlete_name: str,
                  birth_year: int, gender: str, region: int, city: str, club: str,
                  stroke: str, distance: int, time_text: str,
                  time_seconds: float, points: int):
    """
    Yeni sonuç ekle veya güncelle.
    Aynı yarış bacağı (leg) + sporcu + branşta daha iyi süre gelirse günceller.
    """
    with get_conn() as conn:
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
                    "race_date=?, gender=?, region=?, city=?, club=? "
                    "WHERE id=?",
                    (time_text, time_seconds, points, race_date,
                     gender, region, city, club, existing["id"])
                )
        else:
            conn.execute(
                "INSERT INTO fed_results "
                "(race_leg, race_date, athlete_name, birth_year, gender, region, "
                " city, club, stroke, distance, time_text, time_seconds, points) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (race_leg, race_date, athlete_name, birth_year, gender, region,
                 city, club, stroke, distance, time_text, time_seconds, points)
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
