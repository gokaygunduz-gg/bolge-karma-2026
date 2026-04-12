"""
process_antalya_karma.py
------------------------
Antalya (cs-371) Excel verisini okur, puanları hesaplar ve
fed_results + fed_athlete_best tablolarına yazar.

Çalıştırma:
  cd "Bölge Karmaları 2026"
  python process_antalya_karma.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import openpyxl
from federasyon.scoring_tables import EXCEL_COL_TO_EVENT
from federasyon.scorer import score_athlete_row, parse_time, score_event
from federasyon.db_fed import init_fed_db, upsert_result, rebuild_athlete_best, get_stats

ANTALYA_EXCEL = os.path.join(
    os.path.dirname(__file__),
    "Çıktılar", "Yarış Sonuçları Çıktı",
    "2025.12.20_cs371_Antalya_0_Lenex.xlsx"
)
RACE_LEG  = "antalya"
RACE_DATE = "2025.12.20"

# Sadece federasyon karma için ilgili doğum yılları
TARGET_BYS = {2013, 2012, 2011}

# Cinsiyet eşlemesi (Excel'deki Türkçe → 'F'|'M')
GENDER_MAP = {"Kadın": "F", "Kız": "F", "Erkek": "M", "K": "F", "E": "M", "F": "F", "M": "M"}


def main():
    print("=== Antalya Federasyon Karma Veri İşleme ===\n")

    init_fed_db()
    print("DB hazır.")

    # ── Excel oku ─────────────────────────────────────────────────────────────
    wb = openpyxl.load_workbook(ANTALYA_EXCEL, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    headers = [str(h) if h else "" for h in rows[0]]
    col_idx = {h: i for i, h in enumerate(headers)}

    print(f"Excel yüklendi: {len(rows)-1} sporcu")
    print(f"Sütunlar: {headers}\n")

    # ── Her sporcu işle ────────────────────────────────────────────────────────
    processed = 0
    skipped   = 0
    event_written = 0

    for row_vals in rows[1:]:
        row = {h: row_vals[i] for i, h in enumerate(headers)}

        # Doğum yılı
        yb_raw = str(row.get("YB", "") or "").strip()
        if len(yb_raw) == 2:
            yb_int = int(yb_raw)
            birth_year = (2000 + yb_int) if yb_int <= 26 else (1900 + yb_int)
        elif len(yb_raw) == 4:
            birth_year = int(yb_raw)
        else:
            skipped += 1
            continue

        if birth_year not in TARGET_BYS:
            skipped += 1
            continue

        # Cinsiyet
        gender_raw = str(row.get("Cinsiyet", "") or "").strip()
        gender = GENDER_MAP.get(gender_raw, gender_raw[:1].upper() if gender_raw else None)
        if gender not in ("F", "M"):
            skipped += 1
            continue

        name      = str(row.get("Ad Soyad", "") or "").strip()
        club      = str(row.get("Kulüp",    "") or "").strip()
        city      = str(row.get("Şehir",    "") or "").strip()
        region_v  = row.get("Bölge")
        region    = int(region_v) if region_v and str(region_v).isdigit() else None

        if not name:
            skipped += 1
            continue

        # Her branş için puan hesapla ve yaz
        event_scores = score_athlete_row(
            {col: row.get(col) for col in EXCEL_COL_TO_EVENT},
            birth_year, gender
        )

        for (stroke, distance), points in event_scores.items():
            col_name = next(
                (c for c, e in EXCEL_COL_TO_EVENT.items() if e == (stroke, distance)),
                None
            )
            time_str = row.get(col_name) if col_name else None
            time_sec = parse_time(str(time_str)) if time_str else None

            upsert_result(
                race_leg=RACE_LEG, race_date=RACE_DATE,
                athlete_name=name, birth_year=birth_year, gender=gender,
                region=region, city=city, club=club,
                stroke=stroke, distance=distance,
                time_text=str(time_str) if time_str else None,
                time_seconds=time_sec,
                points=points
            )
            event_written += 1

        processed += 1

    print(f"İşlenen: {processed} sporcu")
    print(f"Atlanan: {skipped} (farklı yaş grubu / eksik veri)")
    print(f"Yazılan branş sonucu: {event_written}\n")

    # ── Özet hesapla ──────────────────────────────────────────────────────────
    print("fed_athlete_best yeniden hesaplanıyor...")
    rebuild_athlete_best()

    stats = get_stats()
    print(f"DB istatistik: {dict(stats)}\n")

    # ── Doğrulama: birkaç sporcu örneği ──────────────────────────────────────
    print("=== Örnek Puanlama (ilk 5 sporcu, her YB+cinsiyet grubundan) ===")
    from federasyon.db_fed import load_athletes_for_ranking
    from federasyon.scorer import best_scores_sequence
    from federasyon.ranker import format_seq_display

    athletes = load_athletes_for_ranking([2013, 2012, 2011])

    shown = {(2013,"F"): 0, (2013,"M"): 0, (2012,"F"): 0, (2012,"M"): 0,
             (2011,"F"): 0, (2011,"M"): 0}

    for a in sorted(athletes, key=lambda x: (x["birth_year"], x["gender"], x["name"])):
        key = (a["birth_year"], a["gender"])
        if shown.get(key, 5) >= 5:
            continue
        shown[key] = shown.get(key, 0) + 1
        seq = best_scores_sequence(a["event_scores"])
        total = sum(seq[:3])
        ev_str = ", ".join(
            f"{s[0][:3]}{d}={p}" for (s,d),p in sorted(a["event_scores"].items(),
                                                          key=lambda x: -x[1])
        )
        print(f"  [{a['birth_year']} {'K' if a['gender']=='F' else 'E'}] "
              f"{a['name']:30s} | top3={total:2d} | {format_seq_display(seq)} | {ev_str[:60]}")

    print("\n✓ Antalya verisi işlendi. Sıralama için: python show_rankings.py")


if __name__ == "__main__":
    main()
