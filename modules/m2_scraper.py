"""
modules/m2_scraper.py — Yarış Veri Çekme Orkestratörü

Üç kaynaktan veri çeker, öncelik sırası:
  1. Lenex (.lxf) — En güvenilir. Varsa PDF'lere bakılmaz.
  2. ResultList PDF'leri — Lenex yoksa.
  3. Direkt PDF URL — Kullanıcı PDF linki verirse.

Sonuçları m4_mapping ile kulüp → şehir/bölge eşleştirir,
m3_age ile yaş hesaplar, m1_normalize ile isimleri düzeltir.

Kullanım:
  from modules.m2_scraper import scrape_race, scrape_direct_pdf

  # Yarış URL'sinden
  results = scrape_race("https://canli.tyf.gov.tr/tyf/cs-370/")

  # Direkt PDF URL'sinden
  results = scrape_direct_pdf("https://dosya.tyf.gov.tr/...1773597265.pdf")

  # Her sonuç bir dict:
  {
    "name_raw":     "Eymen ÖZKAN",
    "name":         "Eymen Özkan",       ← normalize_display sonucu
    "yb":           "14",
    "birth_year":   2014,
    "age":          12,                  ← 2026 - 2014
    "club_raw":     "Pamukkale Olimpik Sporlar Spor Kulübü",
    "club":         "Pamukkale Olimpik Sporlar Spor Kulübü",
    "city":         "Denizli",
    "region":       3,
    "gender":       "M",
    "stroke":       "Serbest",
    "distance":     50,
    "time_text":    "28.51",
    "time_seconds": 28.51,
    "source":       "lenex",
    "club_found":   True,               ← mapping'de bulundu mu?
  }
"""

import re
import logging
from dataclasses import dataclass, asdict

from parsers.lenex_parser      import download_lenex, parse_lenex, parse_lenex_date, RawResult
from parsers.html_parser       import parse_race_page, RacePageInfo
from parsers.pdf_parser        import (parse_pdf_from_url, parse_pdf, download_pdf,
                                        parse_pdf_auto, parse_pdf_from_url_auto)
from parsers.progression_parser import parse_progression_from_url
from modules.m1_normalize  import normalize_display, normalize_for_lookup, restore_turkish_display
from modules.m3_age        import calc_age
from modules.m4_mapping    import lookup_club, get_missing_clubs, clear_missing_clubs
from database.db           import lookup_athlete_override

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Sonuç zenginleştirme
# ─────────────────────────────────────────────────────────────────────────────

def _enrich(raw: RawResult) -> dict:
    """
    Ham sonucu (RawResult) kulüp/şehir/bölge/yaş bilgileriyle zenginleştirir.

    Öncelik sırası:
      1. athlete_overrides tablosu (bireysel düzeltme)
      2. clubs tablosu (kulüp mapping)
      3. Kaynak verinin normalize_display'i (fallback)

    Döndürür: tam sonuç dict'i.
    """
    # ── 1. Sporcu düzey override kontrolü ────────────────────────────────────
    name_norm = normalize_for_lookup(raw.name_raw)
    override  = lookup_athlete_override(name_norm, raw.birth_year)

    # Override'dan kulüp/şehir/bölge al (varsa)
    if override and (override["club_override"] or override["city"] or override["region"]):
        club_raw_for_lookup = override["club_override"] or raw.club_raw
        city    = override["city"]   if override["city"]   else None
        region  = override["region"] if override["region"] else None
        club_display = override["club_override"] or normalize_display(raw.club_raw)
        club_found   = True
    else:
        club_raw_for_lookup = raw.club_raw
        # ── 2. Kulüp mapping lookup ───────────────────────────────────────────
        club_info  = lookup_club(club_raw_for_lookup)
        city       = club_info["city"]   if club_info else None
        region     = club_info["region"] if club_info else None
        club_canon = club_info["club_canonical"] if club_info else raw.club_raw
        club_display = (
            club_canon
            if (club_info and club_canon != club_canon.lower())
            else normalize_display(raw.club_raw)
        )
        club_found = club_info is not None

    # ── Override'dan görüntüleme adı (ı/i düzeltmesi vb.) ────────────────────
    if override and override["display_name"]:
        # Override display_name'e de sözlük restorasyonu uygula:
        # Eski override'lar ASCII formda kaydedilmiş olabilir (örn. "Altuğ" yerine "Altug").
        # restore_turkish_display sayesinde dict genişledikçe eski override'lar da otomatik düzelir.
        display_name = restore_turkish_display(override["display_name"])
    else:
        # Önce TitleCase, ardından Türkçe isim sözlüğüyle karakter restorasyonu
        display_name = restore_turkish_display(normalize_display(raw.name_raw))

    # Yaş hesapla
    age = calc_age(raw.birth_year)

    return {
        "name_raw":         raw.name_raw,
        "name":             display_name,
        "yb":               raw.yb_raw or "",
        "birth_year":       raw.birth_year,
        "age":              age,
        "club_raw":         raw.club_raw,
        "club":             club_display,
        "city":             city,
        "region":           region,
        "gender":           raw.gender,
        "stroke":           raw.stroke,
        "distance":         raw.distance,
        "time_text":        raw.time_text,
        "time_seconds":     raw.time_seconds,
        "source":           raw.source,
        "club_found":       club_found,
        "participant_type": raw.participant_type,  # "TK"/"FD"/"TD" veya None
    }


def _enrich_all(raw_results: list[RawResult]) -> list[dict]:
    """RawResult listesini zenginleştirilmiş dict listesine çevirir."""
    return [_enrich(r) for r in raw_results]


def _merge_abbreviated_names(results: list[dict]) -> list[dict]:
    """
    Kısaltılmış isim çiftlerini birleştirir: "R. Karahanoğullari" → "Rafet Çınar Karahanoğullari"

    Tüm sonuçlarda aynı yüzüncü (R.) ile başlayan ve aynı soyadı ile biten bir isim,
    aynı kulüp ve YB'ye sahip tam isimle eşleşirse:
      - Kısaltılmış ismin tüm sonuçları, tam ismin name_raw/name ile yeniden etiketlenir.
      - Ardından _dedup_best_time() doğal olarak ikisini birleştirir.
    """
    # Kısaltılmış isim tespiti: baş harf + nokta ("R.", "H." gibi)
    _ABBREV_RE = re.compile(r"^([A-ZÇĞİÖŞÜ])\.\s+(.+)$", re.UNICODE)

    # İsim → doğum yılı → tam isim arama dict'i
    # key: (normalize(soyad_son_kelime), normalize(club_raw), birth_year)
    full_names: dict[tuple, dict] = {}  # key → result (tam isimli)
    abbrev_map: dict[str, dict] = {}    # normalize(abbrev_name_raw) → matched full result

    for r in results:
        name_raw = r.get("name_raw", "")
        if not _ABBREV_RE.match(name_raw):
            # Tam isim → kayıt et
            words = name_raw.strip().split()
            if len(words) >= 2:
                last_word = normalize_for_lookup(words[-1])
                key = (last_word, normalize_for_lookup(r["club_raw"]), r.get("birth_year"))
                full_names[key] = r

    # Kısaltılmış isimleri eşleştir
    abbrev_results = []
    for r in results:
        name_raw = r.get("name_raw", "")
        m = _ABBREV_RE.match(name_raw)
        if m:
            initial   = m.group(1)  # "R"
            remainder = m.group(2)  # "Karahanoğullari"
            last_word = normalize_for_lookup(remainder.split()[-1])
            key       = (last_word, normalize_for_lookup(r["club_raw"]), r.get("birth_year"))
            full = full_names.get(key)
            if full and full["name_raw"][0].upper() == initial:
                # Eşleşme bulundu → tam isimle yeniden etiketle
                r = dict(r)
                r["name_raw"] = full["name_raw"]
                r["name"]     = full["name"]
            abbrev_results.append(r)
        else:
            abbrev_results.append(r)

    return abbrev_results


def _dedup_best_time(results: list[dict]) -> list[dict]:
    """
    Aynı (sporcu, stroke, distance) için en iyi (en düşük) süreyi tutar.

    Name key: boşluk kaldırılmış normalize isim + doğum yılı.
    Bu sayede OCR-birleşik varyantlar da birleşir:
      "Kerem Eymen Tunc" ↔ "Keremeymentunc" → aynı key → tek kayıt.

    İki aşamalı:
      1. Her event için en iyi süre seçilir.
      2. Sporcunun HERHANGİ bir eventindeki boşluklu isim, tüm eventlere uygulanır.
         Böylece "Keremeymentunc" tüm eventlerde düzelir.
    """
    from collections import defaultdict

    def _name_key(r: dict) -> str:
        name = r.get("name") or r["name_raw"]
        return normalize_for_lookup(name).replace(" ", "")

    def _has_good_name(r: dict) -> bool:
        """Görüntüleme adı OCR-birleşik değil mi? (boşluklu veya kısa)"""
        name = r.get("name") or r["name_raw"]
        return " " in name or len(name) < 8

    # Aşama 1: Her event için en iyi süreyi bul
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in results:
        key = (_name_key(r), r.get("birth_year"), r["stroke"], r["distance"])
        groups[key].append(r)

    per_event_best: dict[tuple, dict]   = {}
    per_event_pool: dict[tuple, list]   = {}
    for key, group in groups.items():
        best = min(group, key=lambda r: r["time_seconds"])
        per_event_best[key] = best
        per_event_pool[key] = group

    # Aşama 2: Sporcu bazında en iyi görüntüleme adını bul (tüm eventler arasında)
    # athlete_key: (name_no_space, birth_year)
    athlete_all_entries: dict[tuple, list[dict]] = defaultdict(list)
    for (name_no_space, birth_year, stroke, distance), group in per_event_pool.items():
        athlete_all_entries[(name_no_space, birth_year)].extend(group)

    best_donor: dict[tuple, dict] = {}
    for athlete_key, all_entries in athlete_all_entries.items():
        good = [r for r in all_entries if _has_good_name(r)]
        if good:
            # Daha fazla kelime = OCR'nin daha iyi ayırdığı isim → onu donor seç.
            # "Kuzey Dora Coşkun" (3 kelime) > "Kuzeydora Coşkun" (2 kelime)
            best_donor[athlete_key] = max(good, key=lambda r: len((r.get("name") or r["name_raw"]).split()))

    # Aşama 3: Her event sonucunu oluştur — gerekirse cross-event donor uygula
    merged = []
    for (name_no_space, birth_year, stroke, distance), best in per_event_best.items():
        group = per_event_pool[(name_no_space, birth_year, stroke, distance)]

        # Aynı event içinde iyi isimli donor var mı?
        if not _has_good_name(best):
            good_in_event = [r for r in group if _has_good_name(r)]
            if good_in_event:
                donor = good_in_event[0]
            else:
                # Event içinde yok → tüm eventlerden bak (cross-event)
                donor = best_donor.get((name_no_space, birth_year))

            if donor:
                best = dict(best)
                best["name"]       = donor.get("name")       or best["name"]
                best["name_raw"]   = donor.get("name_raw")   or best["name_raw"]
                best["club"]       = donor.get("club")       or best.get("club")
                best["club_raw"]   = donor.get("club_raw")   or best.get("club_raw")
                best["city"]       = donor.get("city")       or best.get("city")
                best["region"]     = donor.get("region")     or best.get("region")
                best["club_found"] = donor.get("club_found") or best.get("club_found")

        merged.append(best)

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Ana scraping fonksiyonları
# ─────────────────────────────────────────────────────────────────────────────

def scrape_race(url: str, verbose: bool = True,
                include_progression: bool = False) -> list[dict]:
    """
    Yarış URL'sinden tüm bireysel sonuçları çeker.

    Önce Lenex dener, yoksa PDF'lere geçer.
    Sonuçları kulüp/şehir/bölge/yaş ile zenginleştirir.
    Eşleşmeyen kulüpler için get_missing_clubs() listesine ekler.

    include_progression=True: ProgressionDetails.pdf'i de çeker (ek doğrulama).
      Not: Bu 94+ sayfa OCR eklediğinden çekimi önemli ölçüde yavaşlatır.
      Varsayılan: False (hızlı mod).

    Döndürür: sonuç dict listesi
    """
    clear_missing_clubs()
    url = url.rstrip("/") + "/"

    if verbose:
        print(f"\nYarış analiz ediliyor: {url}")

    race_date = ""  # YYYY.MM.DD formatında, bulunamazsa ""

    # ── 1. Lenex dene ─────────────────────────────────────────────────────────
    lenex_content = download_lenex(url)
    if lenex_content:
        if verbose:
            print("  ✓ Lenex bulundu — XML parse ediliyor...")
        race_date   = parse_lenex_date(lenex_content)
        raw_results = parse_lenex(lenex_content)
        if raw_results:
            results = _enrich_all(raw_results)
            results = _merge_abbreviated_names(results)
            results = _dedup_best_time(results)
            _add_race_date(results, race_date)
            if verbose:
                _print_summary(results, source="lenex")
            return results
        else:
            if verbose:
                print("  ⚠ Lenex parse edildi ama sonuç yok, PDF'lere geçiliyor...")

    # ── 2. HTML sayfasını al, PDF event map'ini çıkar ─────────────────────────
    if verbose:
        print("  ℹ Lenex yok — HTML sayfa analiz ediliyor...")

    page_info = parse_race_page(url)
    if page_info is None:
        logger.error("Yarış sayfası alınamadı: %s", url)
        if verbose:
            print("  ✗ Yarış sayfası alınamadı!")
        return []

    race_date = _parse_race_date_str(page_info.date)

    if verbose:
        print(f"  Yarış: {page_info.title}")
        print(f"  {len(page_info.pdf_links)} PDF, {len(page_info.event_map)} event eşleşmesi")

    # ── 3. PDF'leri parse et ──────────────────────────────────────────────────
    raw_results: list[RawResult] = []
    for pdf_url in page_info.pdf_links:
        pdf_filename = pdf_url.split("/")[-1]
        hint_event   = page_info.event_map.get(pdf_filename)
        # parse_pdf_from_url_auto: önce metin dener, sonuç yoksa OCR'e geçer
        # (hem text-based hem image-based PDF'leri destekler)
        pdf_results  = parse_pdf_from_url_auto(pdf_url, hint_event)
        raw_results.extend(pdf_results)

        if verbose and pdf_results:
            ev = pdf_results[0]
            print(f"    {pdf_filename}: {ev.gender} {ev.distance}m {ev.stroke} → {len(pdf_results)} sonuç")

    # ── 4. ProgressionDetails.pdf — opsiyonel ek doğrulama kaynağı ──────────
    # Splash Meet Manager'ın tüm sporcu × branş özetini tek PDF'de sunar.
    # include_progression=True ile etkinleştirilir (94+ sayfa OCR = yavaş).
    if include_progression:
        if verbose:
            print("  ℹ ProgressionDetails.pdf işleniyor (bu biraz zaman alabilir)...")
        prog_results = parse_progression_from_url(url)
        if prog_results:
            raw_results.extend(prog_results)
            if verbose:
                print(f"  ✓ ProgressionDetails: {len(prog_results)} ek sonuç")
        elif verbose:
            print("  ℹ ProgressionDetails.pdf bulunamadı veya ayrıştırılamadı.")

    if not raw_results:
        if verbose:
            print("  ✗ Hiç sonuç çıkarılamadı!")
        return []

    results = _enrich_all(raw_results)
    results = _merge_abbreviated_names(results)
    results = _dedup_best_time(results)
    _add_race_date(results, race_date)
    if verbose:
        _print_summary(results, source="pdf")
    return results


def scrape_direct_pdf(pdf_url: str, verbose: bool = True) -> list[dict]:
    """
    Direkt PDF URL'sinden sonuçları çeker.
    (https://dosya.tyf.gov.tr/.../1773597265.pdf gibi)

    Önce standart metin çıkarımı dener; sonuç yoksa OCR'ye geçer
    (görüntü tabanlı PDF'ler için pymupdf + rapidocr-onnxruntime gereklidir).

    Event bilgisi PDF başlığından çıkarılır (hint olmadan).
    """
    clear_missing_clubs()

    if verbose:
        print(f"\nPDF analiz ediliyor: {pdf_url}")

    pdf_content = download_pdf(pdf_url)
    if not pdf_content:
        if verbose:
            print("  ✗ PDF indirilemedi!")
        return []

    # Standart → OCR fallback
    raw_results = parse_pdf_auto(pdf_content, hint_event=None)
    if not raw_results:
        if verbose:
            print("  ✗ PDF'den sonuç çıkarılamadı (standart + OCR denendi)!")
        return []

    source_label = raw_results[0].source if raw_results else "pdf"
    results = _enrich_all(raw_results)
    results = _merge_abbreviated_names(results)
    results = _dedup_best_time(results)
    if verbose:
        _print_summary(results, source=source_label)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Tarih yardımcıları
# ─────────────────────────────────────────────────────────────────────────────

def _parse_race_date_str(date_str: str) -> str:
    """
    HTML sayfasından gelen tarih stringini YYYY.MM.DD'ye çevirir.
    Örnekler:
      "27.-30.11.2025"  → "2025.11.27"
      "1.-3.3.2026"    → "2026.03.01"
      "3.03.2026"      → "2026.03.03"
    """
    if not date_str:
        return ""
    # ISO YYYY-MM-DD (Lenex'ten gelirse)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if m:
        return f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
    # Yılı bul (4 haneli)
    year_m = re.search(r"\b(\d{4})\b", date_str)
    if not year_m:
        return ""
    year = year_m.group(1)
    # Yıldan önceki 1-2 haneli sayıları topla
    nums = re.findall(r"\b(\d{1,2})\b", date_str[:year_m.start()])
    if len(nums) >= 2:
        day, month = nums[0], nums[-1]
        return f"{year}.{int(month):02d}.{int(day):02d}"
    if len(nums) == 1:
        day = nums[0]
        # Yıldan hemen önce gelen .-ayrılmış son sayı = ay
        m2 = re.search(r"\.(\d{1,2})\.\d{4}$", date_str)
        if m2:
            return f"{year}.{int(m2.group(1)):02d}.{int(day):02d}"
    return ""


def _add_race_date(results: list[dict], race_date: str) -> None:
    """Her sonuca race_date alanı ekler."""
    for r in results:
        r["race_date"] = race_date


# ─────────────────────────────────────────────────────────────────────────────
# Aynı isimli sporcu kontrolü (her yarışta otomatik çalışır — PROSEDÜR)
# ─────────────────────────────────────────────────────────────────────────────

def _check_duplicate_display_names(results: list[dict]) -> list[dict]:
    """
    Sonuç listesinde aynı görüntüleme adına (name) sahip sporcuları bulur.

    Aynı ad + aynı kulüp + aynı YB → muhtemelen aynı yarışta tekrar (zaten dedup edilmiş)
    Aynı ad + farklı kulüp/YB → kullanıcı kontrolü gerekli

    Döndürür: [{"name": str, "entries": [dict, ...]}, ...]
    """
    from collections import defaultdict

    # Zenginleştirilmiş görüntüleme adına göre grupla
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        key = normalize_for_lookup(r.get("name") or r["name_raw"])
        groups[key].append(r)

    duplicates = []
    for key, entries in groups.items():
        if len(entries) <= 1:
            continue
        # Farklı (kulüp, YB) kombinasyonu var mı?
        # Kulüp anahtarında boşluklar kaldırılır — OCR varyantları ayrı sayılmaz.
        combos = set(
            (normalize_for_lookup(e.get("club") or "").replace(" ", ""), e.get("birth_year"))
            for e in entries
        )
        if len(combos) > 1:
            duplicates.append({
                "name":    entries[0].get("name") or entries[0]["name_raw"],
                "entries": entries,
            })

    return sorted(duplicates, key=lambda x: x["name"])


# ─────────────────────────────────────────────────────────────────────────────
# Kısaltılmış isim tespiti
# ─────────────────────────────────────────────────────────────────────────────

def _detect_abbreviated_names(results: list[dict]) -> list[dict]:
    """
    "R. Karahanoğullari" ile "Rafet Çınar Karahanoğullari" gibi
    aynı sporcu olabilecek (kısaltılmış isim + tam isim) çiftlerini bulur.

    Döndürür: [{"abbrev": ..., "full": ..., "yb_match": bool}, ...]
    """
    from modules.m1_normalize import normalize_for_lookup

    def surname_key(name: str) -> str:
        parts = name.strip().split()
        return normalize_for_lookup(parts[-1]) if parts else ""

    # İlk kelime "X." veya "X.Y." formatında mı?
    abbrev_pat = re.compile(r"^[A-ZÇĞİÖŞÜa-zçğışöüÀ-ÿ]{1,2}\.$")

    abbreviated: list[tuple] = []   # (surname, initial, result)
    full_names:  list[tuple] = []   # (surname, initial, result)

    for r in results:
        name   = r.get("name", "")
        parts  = name.split()
        if len(parts) < 2:
            continue
        sn = surname_key(name)
        first = parts[0]
        if abbrev_pat.match(first):
            initial = normalize_for_lookup(first.rstrip("."))
            abbreviated.append((sn, initial, r))
        else:
            initial = normalize_for_lookup(first[0]) if first else ""
            full_names.append((sn, initial, r))

    pairs: list[dict] = []
    seen:  set         = set()
    for (sn1, init1, r1) in abbreviated:
        for (sn2, init2, r2) in full_names:
            if sn1 != sn2 or init1 != init2:
                continue
            key = (r1["name_raw"], r2["name_raw"])
            if key in seen:
                continue
            seen.add(key)
            pairs.append({
                "abbrev_name": r1["name_raw"],
                "full_name":   r2["name_raw"],
                "abbrev_club": r1.get("club_raw", ""),
                "full_club":   r2.get("club_raw", ""),
                "yb_match":    r1.get("birth_year") == r2.get("birth_year"),
            })
    return pairs


# ─────────────────────────────────────────────────────────────────────────────
# Outlier doğrulama
# ─────────────────────────────────────────────────────────────────────────────

# (stil, mesafe) → (en hızlı makul, en yavaş makul) saniye cinsinden
_TIME_LIMITS: dict[tuple, tuple] = {
    ("Serbest",      50):  (23.0,   85.0),
    ("Serbest",     100):  (50.0,  175.0),
    ("Serbest",     200):  (110.0, 370.0),
    ("Serbest",     400):  (240.0, 750.0),
    ("Serbest",     800):  (510.0, 1500.0),
    ("Serbest",    1500):  (970.0, 2800.0),
    ("Sırtüstü",    50):  (26.0,   95.0),
    ("Sırtüstü",   100):  (58.0,  195.0),
    ("Sırtüstü",   200):  (125.0, 400.0),
    ("Kurbağalama", 50):  (27.0,  100.0),
    ("Kurbağalama",100):  (62.0,  210.0),
    ("Kurbağalama",200):  (135.0, 430.0),
    ("Kelebek",     50):  (26.0,   95.0),
    ("Kelebek",    100):  (55.0,  200.0),  # 56.25 gibi elit genç süreleri kapsamak için
    ("Kelebek",    200):  (125.0, 410.0),
    ("Karışık",    200):  (122.0, 390.0),
    ("Karışık",    400):  (265.0, 780.0),
}


def _check_outliers(results: list[dict]) -> list[str]:
    """
    Her (stil, mesafe) için en hızlı ve en yavaş süreyi kontrol eder.
    Makul aralık dışındaki kayıtları uyarı listesi olarak döndürür.
    """
    from collections import defaultdict
    events: dict[tuple, list] = defaultdict(list)
    for r in results:
        events[(r["stroke"], r["distance"])].append(r)

    warnings = []
    for (stroke, dist), rows in sorted(events.items(), key=lambda x: x[0]):
        lo, hi = _TIME_LIMITS.get((stroke, dist), (1.0, 99999.0))
        fastest = min(rows, key=lambda r: r["time_seconds"])
        slowest = max(rows, key=lambda r: r["time_seconds"])

        if fastest["time_seconds"] < lo:
            warnings.append(
                f"  ⚠ {stroke} {dist}m ÇOK HIZLI: {fastest['time_text']} "
                f"→ {fastest['name']} / {fastest['club_raw']}"
            )
        if slowest["time_seconds"] > hi:
            warnings.append(
                f"  ⚠ {stroke} {dist}m ÇOK YAVAŞ: {slowest['time_text']} "
                f"→ {slowest['name']} / {slowest['club_raw']}"
            )
    return warnings


# ─────────────────────────────────────────────────────────────────────────────
# OCR artefakt kontrolleri (PROSEDÜR: her yarışta çalışır)
# ─────────────────────────────────────────────────────────────────────────────

def _check_single_word_names(results: list[dict]) -> list[dict]:
    """
    OCR hatası tespiti: isim tek kelimeden oluşuyor ve 8+ karakter uzunluğunda.
    Bu durumda iki kelime birleşik yazılmış olabilir ("Keremeymentunc").
    Döndürür: şüpheli kayıtların listesi (tekrar içermeyen).
    """
    flagged = []
    seen: set = set()
    for r in results:
        name = r.get("name") or r["name_raw"]
        if " " not in name and len(name) >= 8:
            key = normalize_for_lookup(name)
            if key not in seen:
                seen.add(key)
                flagged.append(r)
    return sorted(flagged, key=lambda r: (r.get("name") or r["name_raw"]).lower())


def _check_punctuation_artifacts(results: list[dict]) -> list[dict]:
    """
    OCR hatası tespiti: isim veya kulüp adının başında ya da sonunda
    noktalama işareti var (ör. ".Kerem", "Kulübü-").
    Döndürür: şüpheli kayıtların listesi (tekrar içermeyen).
    """
    import string
    _PUNCT = set(string.punctuation)

    def _has_punct_artifact(text: str) -> bool:
        text = text.strip()
        return bool(text) and (text[0] in _PUNCT or text[-1] in _PUNCT)

    flagged = []
    seen: set = set()
    for r in results:
        name = r.get("name") or r["name_raw"]
        club = r.get("club") or r.get("club_raw") or ""
        if _has_punct_artifact(name) or _has_punct_artifact(club):
            key = (normalize_for_lookup(name), normalize_for_lookup(club))
            if key not in seen:
                seen.add(key)
                flagged.append(r)
    return sorted(flagged, key=lambda r: (r.get("name") or r["name_raw"]).lower())


def _check_single_letter_prefix(results: list[dict]) -> list[dict]:
    """
    OCR hatası tespiti: isim tek bir harfle başlıyor (nokta olmadan).
    Örnek: "A Alanur Eroglu" → "A" bir OCR artefaktı, gerçek isim "Alanur Eroglu"
    NOT: "A. Kerem" (noktalı kısaltma) bu kontrolde flaglenmez.
    Döndürür: şüpheli kayıtların listesi (tekrar içermeyen).
    """
    flagged = []
    seen: set = set()
    for r in results:
        name = r.get("name") or r["name_raw"]
        parts = name.strip().split()
        # İlk kelime tek harf (nokta olmadan) ve arkasında en az bir kelime daha var
        if len(parts) >= 2 and len(parts[0]) == 1 and parts[0].isalpha():
            key = normalize_for_lookup(name)
            if key not in seen:
                seen.add(key)
                flagged.append(r)
    return sorted(flagged, key=lambda r: (r.get("name") or r["name_raw"]).lower())


def _check_time_ordering(results: list[dict]) -> list[dict]:
    """
    PDF'de her branştaki sporcular süreye göre sıralı listelenir.
    Bu fonksiyon, sıralı listedeki komşu sporculardan belirgin sapan süreleri flagler.

    İki kontrol uygular:
      1. Komşu kontrolü: sıraladıktan sonra soldaki/sağdaki komşuların ortalamasından
         >35% sapan süreler şüpheli (OCR dakika hatası işareti).
      2. Grup medyan kontrolü: branştaki tüm sporcuların medyanından
         <70% veya >150% sapan süreler şüpheli.

    Kullanım durumu:
      Elif Miray Özdemir 1500m → OCR "21:21.66" yerine "15:21.66" okur.
      Grup medyanı ~1300s iken bu sporcu 921s gösterir → flaglenir.

    Döndürür: [{"stroke", "distance", "name", "club", "time_text",
                "time_seconds", "group_median", "reason"}, ...]
    """
    import statistics
    from collections import defaultdict

    NEIGHBOR_THRESHOLD = 1.45  # komşu ortalamasından >%45 sapma (2 komşu varsa)
    MEDIAN_LO         = 0.60   # grup medyanının %60'ından az  (çok hızlı = OCR dakika sildi)
    MEDIAN_HI         = 3.00   # grup medyanının %300'ünden fazla (çok yavaş = OCR ekstra dakika)

    events: dict[tuple, list] = defaultdict(list)
    for r in results:
        events[(r["stroke"], r["distance"])].append(r)

    flagged = []
    for (stroke, distance), group in events.items():
        valid = [r for r in group if r.get("time_seconds", 0) > 0]
        if len(valid) < 3:
            continue

        times        = [r["time_seconds"] for r in valid]
        group_median = statistics.median(times)
        sorted_g     = sorted(valid, key=lambda r: r["time_seconds"])

        for i, curr in enumerate(sorted_g):
            t = curr["time_seconds"]

            # Komşu kontrolü
            neighbors = []
            if i > 0:
                neighbors.append(sorted_g[i - 1]["time_seconds"])
            if i < len(sorted_g) - 1:
                neighbors.append(sorted_g[i + 1]["time_seconds"])

            neighbor_flag = False
            if neighbors:
                n_avg = sum(neighbors) / len(neighbors)
                if n_avg > 0 and (t < n_avg / NEIGHBOR_THRESHOLD or t > n_avg * NEIGHBOR_THRESHOLD):
                    neighbor_flag = True

            # Grup medyan kontrolü
            median_flag = group_median > 0 and (
                t < group_median * MEDIAN_LO or t > group_median * MEDIAN_HI
            )

            # Flagleme mantığı:
            # - Komşu sapması tek başına yeterli (iki komşu varsa güçlü sinyal)
            # - Medyan sapması: sadece çok aşırı durumlarda (<%60 veya >%300)
            if neighbor_flag or median_flag:
                reasons = []
                if neighbor_flag:
                    reasons.append("komşu sapması")
                if median_flag:
                    reasons.append("medyan sapması")
                flagged.append({
                    "stroke":       stroke,
                    "distance":     distance,
                    "name":         curr.get("name") or curr.get("name_raw", "?"),
                    "club":         curr.get("club") or curr.get("club_raw", "?"),
                    "time_text":    curr.get("time_text", "?"),
                    "time_seconds": t,
                    "group_median": group_median,
                    "reason":       " + ".join(reasons),
                })

    return flagged


def _check_clubs_with_numbers(results: list[dict]) -> list[dict]:
    """
    OCR hatası tespiti: kulüp adında rakam var.
    Genellikle OCR'ın zaman dakikasını kulüp adına yapıştırmasından kaynaklanır.
    Örnek: "Istanbul Performansyuzmesporkulubu2:" — "2:" bir zaman kalıntısı.
    NOT: "CK S.K." gibi noktalama içeren meşru kısaltmalar hariç tutulur.
    Döndürür: şüpheli kayıtların listesi (tekrar içermeyen).
    """
    flagged = []
    seen: set = set()
    for r in results:
        club = r.get("club") or r.get("club_raw") or ""
        if re.search(r"\d", club):
            key = normalize_for_lookup(club)
            if key not in seen:
                seen.add(key)
                flagged.append(r)
    return sorted(flagged, key=lambda r: (r.get("club") or r.get("club_raw") or "").lower())


# ─────────────────────────────────────────────────────────────────────────────
# Benzer isim çifti tespiti (OCR karakter hatası veya birleşik yazım)
# ─────────────────────────────────────────────────────────────────────────────

def _edit_distance(s1: str, s2: str) -> int:
    """Levenshtein edit mesafesi (iki string arasındaki min karakter değişimi)."""
    if abs(len(s1) - len(s2)) > 5:
        return 999
    m, n = len(s1), len(s2)
    dp = list(range(n + 1))
    for i in range(1, m + 1):
        prev, dp[0] = dp[0], i
        for j in range(1, n + 1):
            prev, dp[j] = dp[j], (
                prev if s1[i - 1] == s2[j - 1]
                else 1 + min(prev, dp[j], dp[j - 1])
            )
    return dp[n]


def _check_similar_name_duplicates(results: list[dict]) -> list[dict]:
    """
    Aynı kişi olabilecek isim çiftlerini tespit eder:

    Yöntem:
      1. Soyisim (son kelime normalize) aynı olanları grupla.
      2. Grup içinde iki ismin boşluksuz normalize hali arasındaki edit
         mesafesi ≤ 3 ise → şüpheli çift.
      3. Ortak branş (stil+mesafe) kontrolü:
           - Ortak branş VAR  → muhtemelen FARKLI kişi
           - Ortak branş YOK  → muhtemelen AYNI kişi (OCR hatası)

    Tespit edilen durumlar:
      "Danil Yabanci" ↔ "Danl Yabanci"         (harf eksik, dist=1)
      "Kivanc Konakli" ↔ "Kivang Konakli"       (c→g, dist=1)
      "Ahmetemirbaskonyali" ↔ zaten dedup'ta birleşir (skip)

    Döndürür: [{"name1", "name2", "club1", "club2", "yb1", "yb2",
                "shared_events", "likely_same", "edit_dist"}, ...]
    """
    from collections import defaultdict

    def _nospace(r: dict) -> str:
        name = r.get("name") or r.get("name_raw", "")
        return normalize_for_lookup(name).replace(" ", "")

    def _surname(r: dict) -> str:
        name = r.get("name") or r.get("name_raw", "")
        words = normalize_for_lookup(name).split()
        return words[-1] if words else ""

    # ── Soyisim gruplarını oluştur ────────────────────────────────────────────
    surname_groups: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        sn = _surname(r)
        if sn:
            surname_groups[sn].append(r)

    # ── Her branştaki nospace → event seti (branş çakışma kontrolü için) ──────
    events_by_nspace: dict[str, set] = defaultdict(set)
    for r in results:
        ns = _nospace(r)
        events_by_nspace[ns].add((r.get("stroke", ""), r.get("distance", 0)))

    # ── Yardımcı: çift ekle ───────────────────────────────────────────────────
    pairs: list[dict] = []
    seen_pairs: set = set()

    def _add_pair(ns1: str, r1: dict, ns2: str, r2: dict, dist: int) -> None:
        pair_key = tuple(sorted([ns1, ns2]))
        if pair_key in seen_pairs:
            return
        seen_pairs.add(pair_key)
        shared = events_by_nspace[ns1] & events_by_nspace[ns2]
        pairs.append({
            "name1":         r1.get("name") or r1.get("name_raw", "?"),
            "name2":         r2.get("name") or r2.get("name_raw", "?"),
            "club1":         r1.get("club") or r1.get("club_raw", "?"),
            "club2":         r2.get("club") or r2.get("club_raw", "?"),
            "yb1":           r1.get("birth_year"),
            "yb2":           r2.get("birth_year"),
            "shared_events": shared,
            "likely_same":   len(shared) == 0,
            "edit_dist":     dist,
        })

    # ── Pass 1: Aynı soyisim grubunda edit-distance karşılaştırması ──────────
    for surname, entries in surname_groups.items():
        seen_nspace: dict[str, dict] = {}
        for r in entries:
            ns = _nospace(r)
            if ns not in seen_nspace:
                seen_nspace[ns] = r

        nspace_list = list(seen_nspace.items())
        for i in range(len(nspace_list)):
            for j in range(i + 1, len(nspace_list)):
                ns1, r1 = nspace_list[i]
                ns2, r2 = nspace_list[j]
                if ns1 == ns2:
                    continue
                dist = _edit_distance(ns1, ns2)
                if dist <= 3:
                    _add_pair(ns1, r1, ns2, r2, dist)

    # ── Pass 2: Boşluksuz form eşit → birleşik isim tespiti ──────────────────
    # "Ayse Melek Caliskan" ve "Ayse Melekcaliskan" → nospace eşit ama
    # farklı soyisim grubuna girdikleri için Pass 1'de yakalanmaz.
    all_by_nspace: dict[str, dict] = {}
    for r in results:
        ns = _nospace(r)
        if ns not in all_by_nspace:
            all_by_nspace[ns] = r
        else:
            # Aynı nospace, farklı raw isim → birleşik OCR
            r_existing = all_by_nspace[ns]
            name_existing = r_existing.get("name") or r_existing.get("name_raw", "")
            name_new      = r.get("name") or r.get("name_raw", "")
            if normalize_for_lookup(name_existing) != normalize_for_lookup(name_new):
                _add_pair(ns, r_existing, ns + "_dup", r, 0)

    # ── Pass 3: Farklı soyisim ama yakın nospace → OCR çift-harf / eksik harf ─
    # "Ada Civvelek" ↔ "Ada Civelek": farklı soyisim, ama nospace edit dist=1
    # Sadece soyisim edit dist ≤ 2 olan çiftler karşılaştırılır (verimlilik için)
    surname_list = list(surname_groups.keys())
    for i in range(len(surname_list)):
        for j in range(i + 1, len(surname_list)):
            sn1, sn2 = surname_list[i], surname_list[j]
            if _edit_distance(sn1, sn2) > 2:
                continue
            # Bu iki soyisim birbirine yakın — içindeki sporcuları karşılaştır
            ns_group1 = {_nospace(r): r for r in surname_groups[sn1]}
            ns_group2 = {_nospace(r): r for r in surname_groups[sn2]}
            for ns1, r1 in ns_group1.items():
                for ns2, r2 in ns_group2.items():
                    if ns1 == ns2:
                        continue
                    dist = _edit_distance(ns1, ns2)
                    if dist <= 3:
                        _add_pair(ns1, r1, ns2, r2, dist)

    return sorted(pairs, key=lambda p: p["name1"])


# ─────────────────────────────────────────────────────────────────────────────
# Konsol çıktısı
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(results: list[dict], source: str) -> None:
    """Analiz özetini ve outlier kontrolünü yazdırır."""
    if not results:
        print("  ✗ Sonuç yok.")
        return

    total        = len(results)
    found_clubs  = sum(1 for r in results if r["club_found"])
    missing_list = get_missing_clubs()
    ages         = sorted(set(r["age"] for r in results if r["age"]))
    genders      = sorted(set(r["gender"] for r in results))
    events       = sorted(set(f"{r['distance']}m {r['stroke']}" for r in results))

    print(f"\n  ── Özet ({source.upper()}) ─────────────────────────────")
    print(f"  Toplam sonuç      : {total}")
    print(f"  Kulüp eşleşmesi   : {found_clubs}/{total} ({100*found_clubs//total}%)")
    print(f"  Yaş grupları      : {ages}")
    print(f"  Cinsiyet          : {genders}")
    print(f"  Yarışlar ({len(events)})     : {', '.join(events[:6])}{'...' if len(events)>6 else ''}")

    if missing_list:
        print(f"\n  ⚠ Eşleşmeyen {len(missing_list)} kulüp:")
        for club in missing_list[:10]:
            print(f"     - {club}")
        if len(missing_list) > 10:
            print(f"     ... ve {len(missing_list)-10} tane daha")

    # Outlier kontrolü (sabit eşik)
    outlier_warnings = _check_outliers(results)
    # PDF sıralama / komşu / medyan kontrolü
    ordering_flags   = _check_time_ordering(results)

    if outlier_warnings or ordering_flags:
        print(f"\n  ── Süre Doğrulama ─────────────────────────────────")
        for w in outlier_warnings:
            print(w)
        # Sıralama kontrolü — sadece outlier listesinde olmayanları göster
        outlier_names = {w.split("→")[1].strip().split(" / ")[0] for w in outlier_warnings}
        for f in ordering_flags:
            if f["name"] not in outlier_names:
                med_str = (f"{int(f['group_median']//60)}:{f['group_median']%60:05.2f}"
                           if f["group_median"] >= 60 else f"{f['group_median']:.2f}s")
                print(f"  ⚠ {f['stroke']} {f['distance']}m SIRALAMA ANOMALI [{f['reason']}]: "
                      f"{f['time_text']} → {f['name']} / {f['club'][:40]}")
                print(f"      Grup medyanı: {med_str}")
        print("  (Yukarıdaki süreler kontrol edilmeli!)")
    else:
        print(f"  Süre doğrulama    : ✓ Tüm süreler makul aralıkta")

    # Kısaltılmış isim tespiti
    abbrev_pairs = _detect_abbreviated_names(results)
    if abbrev_pairs:
        print(f"\n  ── Kontrol Edilmesi Gereken İsimler ({len(abbrev_pairs)} çift) ─────")
        for p in abbrev_pairs:
            yb_info = "✓ YB eşleşiyor" if p["yb_match"] else "? YB farklı"
            print(f"     {p['abbrev_name']:30s} ← aynı kişi mi? → {p['full_name']}")
            print(f"       Kulüp1: {p['abbrev_club'][:35]}  |  Kulüp2: {p['full_club'][:35]}  [{yb_info}]")
        print("  (Yukarıdaki çiftler Excel'de ayrı satırlarda — kontrol edin)")

    # ── Aynı görüntüleme adına sahip sporcular (PROSEDÜR: her yarışta kontrol) ──
    dup_groups = _check_duplicate_display_names(results)
    if dup_groups:
        print(f"\n  ── AYNI İSİMLİ SPORCULAR ({len(dup_groups)} grup) — Kontrol Gerekli ──")
        for grp in dup_groups:
            print(f"\n  ● {grp['name']}:")
            for entry in grp["entries"]:
                yb_str  = str(entry.get("birth_year", "?"))
                club    = entry.get("club", entry.get("club_raw", "?"))
                gender  = entry.get("gender", "?")
                print(f"      YB={yb_str}  Cinsiyet={gender}  Kulüp={club[:50]}")
        print("\n  → Yukarıdaki isimler aynı kişi mi, farklı kişi mi? Lütfen kontrol edin.")
    else:
        print(f"  Aynı isimli sporcu  : ✓ Yok")

    # ── Tek kelimeli isim kontrolü (OCR birleşik — PROSEDÜR) ──────────────────
    single_word = _check_single_word_names(results)
    if single_word:
        print(f"\n  ── TEK KELİMELİ İSİMLER ({len(single_word)}) — OCR Birleşik Olabilir ──")
        for r in single_word:
            yb_str = str(r.get("birth_year", "?"))
            club   = r.get("club") or r.get("club_raw") or "?"
            print(f"     ⚠ [{r.get('name') or r['name_raw']}]  YB={yb_str}  Kulüp={club[:50]}")
        print("  → Bu isimler OCR'da iki kelime birleşmiş olabilir, kontrol edin.")
    else:
        print(f"  Tek kelimeli isim   : ✓ Yok")

    # ── Noktalama işareti artefakt kontrolü (PROSEDÜR) ───────────────────────
    punct_artifacts = _check_punctuation_artifacts(results)
    if punct_artifacts:
        print(f"\n  ── NOKTALAMA ARTEFAKTLARI ({len(punct_artifacts)}) — Kontrol Gerekli ──")
        for r in punct_artifacts:
            name = r.get("name") or r["name_raw"]
            club = r.get("club") or r.get("club_raw") or ""
            print(f"     ⚠ İsim: [{name}]  Kulüp: [{club[:50]}]")
        print("  → Baş/son noktalama işareti temizlenmeli!")
    else:
        print(f"  Noktalama artefaktı : ✓ Yok")

    # ── Tekil harf öneki kontrolü (PROSEDÜR) ─────────────────────────────────
    single_letter = _check_single_letter_prefix(results)
    if single_letter:
        print(f"\n  ── TEKİL HARF ÖNEKİ ({len(single_letter)}) — OCR Artefakt ──")
        for r in single_letter:
            name  = r.get("name") or r["name_raw"]
            yb    = str(r.get("birth_year", "?"))
            club  = r.get("club") or r.get("club_raw") or "?"
            print(f"     ⚠ [{name}]  YB={yb}  Kulüp={club[:50]}")
        print("  → İsim başındaki tek harf OCR artefaktı olabilir (örn. 'A Alanur Eroglu'→'Alanur Eroglu')")
    else:
        print(f"  Tekil harf öneki    : ✓ Yok")

    # ── Kulüpte rakam kontrolü (PROSEDÜR) ────────────────────────────────────
    clubs_with_nums = _check_clubs_with_numbers(results)
    if clubs_with_nums:
        print(f"\n  ── KULÜPTE RAKAM ({len(clubs_with_nums)}) — OCR Zaman Kalıntısı Olabilir ──")
        for r in clubs_with_nums:
            name  = r.get("name") or r["name_raw"]
            club  = r.get("club") or r.get("club_raw") or "?"
            print(f"     ⚠ Sporcu: [{name}]  Kulüp: [{club[:60]}]")
        print("  → Kulüp adındaki rakamlar OCR'dan gelen zaman kalıntısı olabilir!")
    else:
        print(f"  Kulüpte rakam       : ✓ Yok")

    # ── Benzer isim çifti tespiti (OCR karakter/boşluk hatası — PROSEDÜR) ────
    similar_pairs = _check_similar_name_duplicates(results)
    if similar_pairs:
        print(f"\n  ── BENZER İSİM ÇİFTLERİ ({len(similar_pairs)}) — Aynı Kişi Olabilir ──")
        for p in similar_pairs:
            verdict = "✓ Ortak branş yok → muhtemelen AYNI KİŞİ" if p["likely_same"] \
                      else f"⚠ {len(p['shared_events'])} ortak branş → FARKLI KİŞİ olabilir"
            yb_info = (f"YB1={p['yb1'] or '?'} / YB2={p['yb2'] or '?'}"
                       if p["yb1"] != p["yb2"] else f"YB={p['yb1'] or '?'}")
            print(f"\n  ● [{p['name1']}]  ↔  [{p['name2']}]  (edit={p['edit_dist']})")
            print(f"      {yb_info}  |  Kulüp1: {str(p['club1'])[:35]}  |  Kulüp2: {str(p['club2'])[:35]}")
            print(f"      {verdict}")
        print("\n  → Aynı kişi ise fix script'e override ekleyin.")
    else:
        print(f"  Benzer isim çifti   : ✓ Yok")

    print()
