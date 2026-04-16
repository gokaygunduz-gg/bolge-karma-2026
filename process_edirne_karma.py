"""
process_edirne_karma.py
-----------------------
Edirne (17-19 Nisan 2026) yarışını çeker, puanlar ve Antalya ile birleştirir.
Panel JSON'unu günceller.

Çalıştırma (tek seferlik):
  python process_edirne_karma.py --url https://canli.tyf.gov.tr/tyf/cs-XXX/

Edirne yarışı sırasında canlı döngü:
  python process_edirne_karma.py --url https://canli.tyf.gov.tr/tyf/cs-XXX/ --loop 60

--loop N: N saniyede bir yeniden çeker ve JSON'u günceller.
"""

import sys, os, time, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from database.db import init_db
from modules.m2_scraper import scrape_race
from modules.m4_mapping import reload_mapping, clear_missing_clubs, apply_overrides_to_mapping
from federasyon.scoring_tables import EXCEL_COL_TO_EVENT
from federasyon.scorer import score_athlete_row, parse_time
from federasyon.db_fed import init_fed_db, upsert_result, rebuild_athlete_best

RACE_LEG  = "edirne"
RACE_DATE = "2026.04.17"
TARGET_BYS = {2013, 2012, 2011}
GENDER_MAP = {"Kadın": "F", "Kız": "F", "Erkek": "M", "K": "F", "E": "M", "F": "F", "M": "M"}


def _load_name_overrides() -> dict:
    """manual_overrides.json'dan sporcu override'larını yükle."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manual_overrides.json")
    if not os.path.exists(path):
        return {}
    try:
        import json
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        overrides = {}
        for item in data.get("name_overrides", []):
            if item.get("resolution") == "same":
                key = (item["name"].lower().strip(), item["birth_year"], item["gender"])
                overrides[key] = item
        return overrides
    except Exception as e:
        print(f"  ⚠ name_overrides yüklenemedi: {e}")
        return {}


def process_edirne(url: str, verbose: bool = True):
    """
    Verilen URL'den Edirne sonuçlarını çeker, puanlar ve DB'ye yazar.
    Sonra panel JSON'unu günceller.
    """
    print(f"\n[{time.strftime('%H:%M:%S')}] Edirne verisi çekiliyor: {url}")

    init_db()
    init_fed_db()
    reload_mapping()
    overrides_added = apply_overrides_to_mapping()
    if overrides_added:
        print(f"  ℹ {overrides_added} kulüp override mapping'e eklendi.")
    clear_missing_clubs()

    name_overrides = _load_name_overrides()
    if name_overrides:
        print(f"  ℹ {len(name_overrides)} sporcu override yüklendi.")

    results = scrape_race(url, verbose=verbose)
    if not results:
        print("  ⚠ Sonuç bulunamadı (henüz yayınlanmadı?)")
        return 0

    processed  = 0
    ev_written = 0

    for r in results:
        birth_year = r.get("birth_year")
        if birth_year not in TARGET_BYS:
            continue

        gender_raw = r.get("gender", "") or ""
        gender     = GENDER_MAP.get(gender_raw, gender_raw[:1].upper() if gender_raw else None)
        if gender not in ("F", "M"):
            continue

        name   = r.get("name", "") or ""
        club   = r.get("club", "") or ""
        city   = r.get("city", "") or ""
        region = r.get("region")

        # Sporcu override uygula (aynı isimli ama farklı kulüp durumu)
        ovr_key = (name.lower().strip(), birth_year, gender)
        if ovr_key in name_overrides:
            ovr = name_overrides[ovr_key]
            club   = ovr.get("canonical_club", club) or club
            city   = ovr.get("canonical_city", city) or city
            region = ovr.get("canonical_region", region) or region
        stroke = r.get("stroke") or ""
        dist   = r.get("distance") or 0
        t_txt  = r.get("time_text") or ""
        t_sec  = r.get("time_seconds")

        # Puanla
        from federasyon.scorer import score_event
        pts = score_event(t_sec, birth_year, gender, stroke, dist) if t_sec else 0

        if pts == 0:
            continue   # Barajı geçemeyen sonuçları kaydetme

        upsert_result(
            race_leg=RACE_LEG, race_date=RACE_DATE,
            athlete_name=name, birth_year=birth_year, gender=gender,
            region=region, city=city, club=club,
            stroke=stroke, distance=dist,
            time_text=t_txt, time_seconds=t_sec, points=pts
        )
        ev_written += 1
        processed += 1

    print(f"  ✓ {processed} sonuç işlendi, {ev_written} branş DB'ye yazıldı")

    # Özet güncelle
    rebuild_athlete_best()

    # Panel JSON üret
    import subprocess
    subprocess.run([sys.executable, "generate_rankings_json.py"], check=False)
    print(f"  ✓ Panel güncellendi")

    return processed


def main():
    parser = argparse.ArgumentParser(description="Edirne karma işleyici")
    parser.add_argument("--url",  required=True,  help="canli.tyf.gov.tr yarış URL'i")
    parser.add_argument("--loop", type=int, default=0, help="Canlı mod: N saniyede bir tekrar (0=tek seferlik)")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    if args.loop > 0:
        print(f"Canlı mod: her {args.loop} saniyede bir güncelleniyor. Durdurmak: Ctrl+C")
        while True:
            try:
                process_edirne(args.url, verbose=not args.quiet)
                print(f"  Sonraki güncelleme: {args.loop}s sonra...\n")
                time.sleep(args.loop)
            except KeyboardInterrupt:
                print("\nDurduruldu.")
                break
    else:
        process_edirne(args.url, verbose=not args.quiet)


if __name__ == "__main__":
    main()
