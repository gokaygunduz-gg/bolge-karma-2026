"""
parsers/lenex_parser.py — Lenex (.lxf) Dosyası Ayrıştırıcı

Lenex formatı: ZIP arşivi içinde XML dosyası
  - Her yarışın tam sonuçlarını içerir
  - Sporcu adı, soyadı, doğum tarihi (YYYY-MM-DD), cinsiyet, kulüp
  - Yarış stili, mesafesi, süresi (00:00:28.51 formatında)

Lenex'in avantajı: En güvenilir kaynak — PDF'e göre %100 doğru event bilgisi.
Lenex mevcut değilse sistem PDF'e düşer (m2_scraper.py yönetir).

Ham sonuç örneği:
  {
    "name_raw":    "Ali Çokçetin",
    "yb_raw":      "14",      ← 2 haneli (doğum tarihinden hesaplanır)
    "birth_year":  2014,
    "club_raw":    "Yıldız Su Sporları Spor Kulübü",
    "gender":      "M",
    "stroke":      "Kurbağalama",
    "distance":    50,
    "time_text":   "33.72",
    "time_seconds": 33.72,
    "source":      "lenex",
  }
"""

import io
import re
import zipfile
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field

import requests

from config import HTTP_TIMEOUT_LENEX, HTTP_HEADERS, STROKE_MAP
from modules.m3_age import parse_birthdate

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Veri tipi
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RawResult:
    """Bir yarış sonucu — ham, normalize edilmemiş veri."""
    name_raw:     str
    yb_raw:       str | None     # "14", "99" gibi 2 haneli string
    birth_year:   int | None     # 4 haneli: 2014, 1999
    club_raw:     str
    gender:       str            # "M" veya "F"
    stroke:       str            # "Serbest", "Sırtüstü", ...
    distance:     int            # 50, 100, 200, 400, 800, 1500
    time_text:    str            # "28.51" veya "1:02.34"
    time_seconds:     float          # saniye cinsinden, sıralama için
    source:           str = "lenex"  # "lenex" | "pdf"
    participant_type: str | None = None  # "TK" | "FD" | "TD" (okul yarışları için)
    pdf_seq:          int | None = None  # ResultList_N.pdf'deki N (PDF sıra numarası)


# ─────────────────────────────────────────────────────────────────────────────
# Zaman dönüşümü
# ─────────────────────────────────────────────────────────────────────────────

def _lenex_time_to_seconds(time_str: str) -> float | None:
    """
    Lenex zaman formatı "HH:MM:SS.cc" → saniye cinsinden float.

    Örnekler:
      "00:00:28.51" → 28.51
      "00:01:02.34" → 62.34
      "00:08:15.67" → 495.67
    """
    if not time_str or time_str in ("00:00:00.00", ""):
        return None
    try:
        parts = time_str.strip().split(":")
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
            return h * 3600 + m * 60 + s
        elif len(parts) == 2:
            m, s = int(parts[0]), float(parts[1])
            return m * 60 + s
        else:
            return float(parts[0])
    except (ValueError, IndexError):
        return None


def _seconds_to_display(seconds: float) -> str:
    """
    Saniye → görüntüleme formatı.

    Örnekler:
      28.51  → "28.51"
      62.34  → "1:02.34"
      495.67 → "8:15.67"
    """
    if seconds < 60:
        return f"{seconds:.2f}"
    minutes = int(seconds // 60)
    secs = seconds - minutes * 60
    return f"{minutes}:{secs:05.2f}"


# ─────────────────────────────────────────────────────────────────────────────
# Lenex indirme
# ─────────────────────────────────────────────────────────────────────────────

def parse_lenex_date(lenex_content: bytes) -> str:
    """
    Lenex dosyasından yarış başlangıç tarihini çıkarır.

    Önce MEET.startdate'e bakar; yoksa ilk SESSION.date'i kullanır.
    Döndürür: "YYYY.MM.DD" formatında string, bulunamazsa "".
    """
    def _fmt(datestr: str) -> str:
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", datestr)
        return f"{m.group(1)}.{m.group(2)}.{m.group(3)}" if m else ""

    try:
        with zipfile.ZipFile(io.BytesIO(lenex_content)) as zf:
            xml_content = zf.read(zf.namelist()[0]).decode("utf-8", errors="replace")
        root = ET.fromstring(xml_content)

        # 1. MEET.startdate
        meet = root.find(".//MEET")
        if meet is not None:
            startdate = meet.attrib.get("startdate", "")
            if startdate:
                result = _fmt(startdate)
                if result:
                    return result

        # 2. İlk SESSION.date (MEET.startdate yoksa)
        session = root.find(".//SESSION")
        if session is not None:
            session_date = session.attrib.get("date", "")
            if session_date:
                result = _fmt(session_date)
                if result:
                    return result

    except Exception:
        pass
    return ""


def download_lenex(race_url: str) -> bytes | None:
    """
    Yarış URL'sinden Lenex dosyasını indirir.

    Splash Meet Manager ve yaygın yarış sunucularında denenen URL pattern'leri:
      - /results.lxf          (standart Splash)
      - /results.lef          (Splash alternatif uzantı)
      - /meet.lxf             (tam yarışma dosyası)
      - /meet.lef             (alternatif uzantı)
      - /canli/results.lxf    (canli.tyf.gov.tr yapısı)
      - /canli/results.lef

    Döndürür: Lenex dosyası (bytes) veya None
    """
    base = race_url.rstrip("/")

    _LENEX_PATHS = [
        "/results.lxf",
        "/results.lef",
        "/meet.lxf",
        "/meet.lef",
        "/canli/results.lxf",
        "/canli/results.lef",
    ]

    for path in _LENEX_PATHS:
        lenex_url = base + path
        logger.info("Lenex kontrol ediliyor: %s", lenex_url)
        try:
            r = requests.get(lenex_url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT_LENEX)
            if r.status_code != 200:
                continue
            if len(r.content) < 100:
                continue
            # İçerik doğrulama: ZIP (PK header) veya XML başlangıcı
            if r.content[:2] == b"PK" or r.content[:5] in (b"<?xml", b"<LENE"):
                logger.info("Lenex bulundu (%s): %d byte", path, len(r.content))
                return r.content
        except Exception as e:
            logger.debug("Lenex yok (%s): %s", path, e)

    logger.info("Lenex bulunamadı: %s", base)
    return None


def download_lenex_direct(lxf_url: str) -> bytes | None:
    """
    Direkt .lxf URL'sinden indirir (tam URL verildiğinde).
    """
    try:
        r = requests.get(lxf_url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT_LENEX)
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.error("Lenex indirilemedi '%s': %s", lxf_url, e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Lenex ayrıştırma
# ─────────────────────────────────────────────────────────────────────────────

def parse_lenex(lenex_content: bytes) -> list[RawResult]:
    """
    Lenex dosyasını (ZIP içinde XML) ayrıştırır, RawResult listesi döner.

    Atlananlar:
      - Zaman yoksa veya 00:00:00.00 ise (DNS/DNF)
      - status="DSQ" olanlar
      - Röle sonuçları (EVENT'te relay=True veya swimstyle>stroke=MEDLEY ve >4 yüzücü)
      - Geçersiz mesafe (VALID_DISTANCES dışı)
    """
    results: list[RawResult] = []

    # ZIP'i aç
    try:
        with zipfile.ZipFile(io.BytesIO(lenex_content)) as zf:
            xml_filename = zf.namelist()[0]
            xml_content  = zf.read(xml_filename).decode("utf-8", errors="replace")
    except zipfile.BadZipFile:
        logger.error("Lenex dosyası geçerli bir ZIP değil.")
        return []
    except Exception as e:
        logger.error("Lenex ZIP açılamadı: %s", e)
        return []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error("Lenex XML parse hatası: %s", e)
        return []

    # ── Event tablosu: eventid → {gender, distance, stroke} ──────────────────
    events: dict[str, dict] = {}
    for event in root.findall(".//EVENT"):
        eid   = event.attrib.get("eventid")
        if not eid:
            continue

        # Röle yarışını atla (RELAY elementi veya 'relay' attribute)
        if event.find("RELAY") is not None:
            continue
        if event.attrib.get("relay", "").upper() == "YES":
            continue

        swimstyle = event.find("SWIMSTYLE")
        if swimstyle is None:
            continue

        distance_str = swimstyle.attrib.get("distance", "")
        stroke_code  = swimstyle.attrib.get("stroke", "")

        try:
            distance = int(distance_str)
        except ValueError:
            continue

        # Geçersiz mesafe → atla
        valid_distances = {50, 100, 200, 400, 800, 1500}
        if distance not in valid_distances:
            continue

        stroke = STROKE_MAP.get(stroke_code.upper())
        if stroke is None:
            continue  # Bilinmeyen stil → atla

        # Cinsiyet: EVENT'te veya üst AGEGROUP'ta
        gender_raw = event.attrib.get("gender", "").upper()
        gender = "F" if gender_raw in ("F", "FEMALE", "W", "WOMEN") else "M"

        events[eid] = {
            "gender":   gender,
            "distance": distance,
            "stroke":   stroke,
        }

    logger.info("Lenex: %d bireysel event bulundu.", len(events))

    # ── Kulüpler ve sporcular ─────────────────────────────────────────────────
    for club in root.findall(".//CLUB"):
        club_name = club.attrib.get("name", "").strip()
        if not club_name:
            continue

        for athlete in club.findall(".//ATHLETE"):
            firstname  = athlete.attrib.get("firstname", "").strip()
            lastname   = athlete.attrib.get("lastname", "").strip()
            full_name  = f"{firstname} {lastname}".strip()
            birthdate  = athlete.attrib.get("birthdate", "")
            gender_raw = athlete.attrib.get("gender", "").upper()
            gender     = "F" if gender_raw in ("F", "FEMALE", "W") else "M"

            birth_year = parse_birthdate(birthdate)
            yb_raw     = str(birth_year % 100).zfill(2) if birth_year else None

            for result in athlete.findall(".//RESULT"):
                # DSQ kontrolü
                status = result.attrib.get("status", "")
                if status.upper() in ("DSQ", "DQ", "DNS", "DNF"):
                    continue

                eid       = result.attrib.get("eventid")
                swim_time = result.attrib.get("swimtime", "")

                # Zaman yoksa veya sıfırsa atla
                if not swim_time or swim_time == "00:00:00.00":
                    continue

                time_seconds = _lenex_time_to_seconds(swim_time)
                if time_seconds is None or time_seconds < 1:
                    continue

                # Event bilgisi
                if eid not in events:
                    continue
                evt = events[eid]

                # Sonucu ekle
                results.append(RawResult(
                    name_raw     = full_name,
                    yb_raw       = yb_raw,
                    birth_year   = birth_year,
                    club_raw     = club_name,
                    gender       = gender,
                    stroke       = evt["stroke"],
                    distance     = evt["distance"],
                    time_text    = _seconds_to_display(time_seconds),
                    time_seconds = time_seconds,
                    source       = "lenex",
                ))

    logger.info("Lenex: %d sonuç çıkarıldı (%d kulüpten).",
                len(results), len(root.findall(".//CLUB")))
    return results


def parse_lenex_entries(lenex_content: bytes) -> list[dict]:
    """
    Lenex'teki ENTRY elemanlarını parse eder → start list.

    Döner: her kayıt için dict:
      {name_raw, birth_year, gender, stroke, distance, entry_time_sec}

    Not: entry_time_sec None olabilir (yarışa kaydolmuş ama süre girilmemiş).
    """
    entries: list[dict] = []

    try:
        with zipfile.ZipFile(io.BytesIO(lenex_content)) as zf:
            xml_content = zf.read(zf.namelist()[0]).decode("utf-8", errors="replace")
    except Exception as e:
        logger.error("Lenex entries: ZIP açılamadı: %s", e)
        return []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error("Lenex entries: XML parse hatası: %s", e)
        return []

    # Event tablosu: eventid → {gender, distance, stroke}
    events: dict[str, dict] = {}
    for event in root.findall(".//EVENT"):
        eid = event.attrib.get("eventid")
        if not eid:
            continue
        if event.find("RELAY") is not None or event.attrib.get("relay", "").upper() == "YES":
            continue
        swimstyle = event.find("SWIMSTYLE")
        if swimstyle is None:
            continue
        try:
            distance = int(swimstyle.attrib.get("distance", "0"))
        except ValueError:
            continue
        if distance not in {50, 100, 200, 400, 800, 1500}:
            continue
        stroke = STROKE_MAP.get(swimstyle.attrib.get("stroke", "").upper())
        if stroke is None:
            continue
        gender_raw = event.attrib.get("gender", "").upper()
        gender = "F" if gender_raw in ("F", "FEMALE", "W", "WOMEN") else "M"
        events[eid] = {"gender": gender, "distance": distance, "stroke": stroke}

    # Sporcular ve entry'leri
    for club in root.findall(".//CLUB"):
        for athlete in club.findall(".//ATHLETE"):
            firstname  = athlete.attrib.get("firstname", "").strip()
            lastname   = athlete.attrib.get("lastname",  "").strip()
            full_name  = f"{firstname} {lastname}".strip()
            birthdate  = athlete.attrib.get("birthdate", "")
            gender_raw = athlete.attrib.get("gender", "").upper()
            gender     = "F" if gender_raw in ("F", "FEMALE", "W") else "M"
            birth_year = parse_birthdate(birthdate)
            if not birth_year:
                continue

            for entry in athlete.findall(".//ENTRY"):
                eid = entry.attrib.get("eventid")
                if eid not in events:
                    continue
                evt = events[eid]
                entry_time_raw = entry.attrib.get("entrytime", "")
                entry_time_sec = _lenex_time_to_seconds(entry_time_raw)
                entries.append({
                    "name_raw":      full_name,
                    "birth_year":    birth_year,
                    "gender":        gender,
                    "stroke":        evt["stroke"],
                    "distance":      evt["distance"],
                    "entry_time_sec": entry_time_sec,
                    "entry_time_txt": _seconds_to_display(entry_time_sec) if entry_time_sec else None,
                })

    logger.info("Lenex entries: %d kayıt çıkarıldı.", len(entries))
    return entries
