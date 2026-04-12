"""
parsers/html_parser.py — Yarış HTML Sayfası Ayrıştırıcı

İki görev:
  1. ResultList PDF dosyalarını → (cinsiyet, mesafe, stil) bilgisine eşler
     (PDF'lerin hangi yarışa ait olduğunu anlar)
  2. Sayfadaki tüm ResultList PDF linklerini bulur

Yarış sayfası yapısı (Splash Meet Manager):
  - Ana tablo: Sıraya göre yarışlar, PDF linkleri ve yarış bilgisi
  - Satır örneği: ["5.", "Kızlar", "200m Serbest", "Timed Final", "9:35", "..."]
    Link örneği: ["StartList_5.pdf", "ResultList_5.pdf"]
  - Staf tablo (Table 3): Disipline göre matris görünümü (ikincil kaynak)

Not: Röle yarışları (4 x 50m gibi) atlanır — sadece bireysel yarışlar işlenir.
"""

import re
import logging
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup

from config import HTTP_TIMEOUT_HTML, HTTP_HEADERS

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Veri tipleri
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EventInfo:
    gender:   str    # 'M' (Erkekler) veya 'F' (Kızlar/Bayanlar)
    distance: int    # 50, 100, 200, 400, 800, 1500
    stroke:   str    # 'Serbest', 'Sırtüstü', 'Kurbağalama', 'Kelebek', 'Karışık'

@dataclass
class RacePageInfo:
    title:       str                      # Yarış başlığı
    location:    str                      # Şehir
    date:        str                      # Tarih string (ör: "27.-30.11.2025")
    event_map:   dict[str, EventInfo]     # {"ResultList_10.pdf": EventInfo(...), ...}
    pdf_links:   list[str]                # Tüm ResultList PDF URL'leri (tam URL)


# ─────────────────────────────────────────────────────────────────────────────
# Sabitler
# ─────────────────────────────────────────────────────────────────────────────

# Türkçe → standart cinsiyet
_GENDER_MAP = {
    "erkekler": "M",
    "oglanlar": "M",   # ğ → g normalized
    "kizlar":   "F",   # ı → i normalized
    "bayanlar": "F",
}

# Türkçe stil isimleri (küçük harf normalize + Türkçe → ASCII sonrası)
_STROKE_MAP = {
    "serbest":     "Serbest",
    "sirtüstü":    "Sırtüstü",    # normalize_for_lookup sonrası
    "sirtustü":    "Sırtüstü",
    "sirtüstu":    "Sırtüstü",
    "sirtustu":    "Sırtüstü",
    "kurbagalama": "Kurbağalama",
    "kelebek":     "Kelebek",
    "karisik":     "Karışık",
}

# Bireysel yarış mesafeleri
_VALID_DISTANCES = {50, 100, 200, 400, 800, 1500}


# ─────────────────────────────────────────────────────────────────────────────
# Yardımcı fonksiyonlar
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_tr(text: str) -> str:
    """Türkçe karakterleri ASCII'ye çevirir, küçük harfe indirir."""
    tr_map = str.maketrans({
        0x0131: "i", 0x0130: "i", 0x015F: "s", 0x015E: "s",
        0x011F: "g", 0x011E: "g", 0x00FC: "u", 0x00DC: "u",
        0x00F6: "o", 0x00D6: "o", 0x00E7: "c", 0x00C7: "c",
    })
    return text.translate(tr_map).lower().strip()


def _parse_gender(text: str) -> str | None:
    """Metin içinden cinsiyet çıkarır ('M' veya 'F')."""
    norm = _normalize_tr(text)
    for key, val in _GENDER_MAP.items():
        if key in norm:
            return val
    return None


def _parse_stroke(text: str) -> str | None:
    """Metin içinden stil adını çıkarır."""
    norm = _normalize_tr(text)
    for key, val in _STROKE_MAP.items():
        if key in norm:
            return val
    return None


def _parse_distance(text: str) -> int | None:
    """Metinden '200m', '50 m' gibi mesafeyi çıkarır."""
    match = re.search(r"(\d+)\s*m\b", text, re.IGNORECASE)
    if match:
        d = int(match.group(1))
        return d if d in _VALID_DISTANCES else None
    return None


def _is_relay(text: str) -> bool:
    """Röle yarışı mı? (4 x 50m gibi)"""
    return bool(re.search(r"\d\s*[xX×]\s*\d", text))


def _parse_event_text(text: str) -> EventInfo | None:
    """
    "200m Serbest", "Kızlar 100m Kelebek", "Erkekler, 50m Sırtüstü" gibi
    metinden EventInfo oluşturur. Röle yarışlarını atlar.
    """
    if _is_relay(text):
        return None

    gender   = _parse_gender(text)
    distance = _parse_distance(text)
    stroke   = _parse_stroke(text)

    if distance and stroke:
        return EventInfo(
            gender   = gender or "M",  # belirsizse varsayılan
            distance = distance,
            stroke   = stroke,
        )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Ana parser fonksiyonları
# ─────────────────────────────────────────────────────────────────────────────

def fetch_page(url: str) -> BeautifulSoup | None:
    """
    Yarış sayfasını indirir ve BeautifulSoup döner.
    Hata durumunda None döner.
    """
    url = url.rstrip("/") + "/"
    try:
        r = requests.get(url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT_HTML)
        r.raise_for_status()
        return BeautifulSoup(r.content, "html.parser")
    except Exception as e:
        logger.error("Sayfa indirilemedi '%s': %s", url, e)
        return None


def extract_event_map(soup: BeautifulSoup) -> dict[str, EventInfo]:
    """
    HTML tablosundan "ResultList_N.pdf → EventInfo" eşleştirmesini çıkarır.

    Önce yarış programı tablosuna (detaylı sıralı tablo) bakar.
    Her satırda:
      - Cinsiyet, mesafe, stil → EventInfo
      - ResultList PDF linki
    Eşleşme kurulursa event_map'e eklenir.

    Döndürür:
      {"ResultList_10.pdf": EventInfo(gender="M", distance=50, stroke="Serbest"), ...}
    """
    event_map: dict[str, EventInfo] = {}

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            links = [a["href"] for a in row.find_all("a", href=True) if ".pdf" in a["href"].lower()]

            if not cells or not links:
                continue

            # ResultList linklerini filtrele
            result_pdfs = [l for l in links if "ResultList" in l]
            if not result_pdfs:
                continue

            # Tüm hücre metnini birleştir
            row_text = " ".join(cells)

            # Röle → atla
            if _is_relay(row_text):
                continue

            # Satırdan cinsiyet/mesafe/stil çıkarmayı dene
            event = _parse_event_text(row_text)
            if event is None:
                continue

            # Bu satırda birden fazla PDF varsa (ör: Erkekler + Kızlar aynı satırda)
            # Her PDF'i ayrıca işle — cinsiyet tespiti için ek çaba gerekir
            for pdf in result_pdfs:
                pdf_filename = pdf.split("/")[-1]

                if event.gender is None:
                    # Cinsiyet belirsiz: aynı satırda birden fazla PDF varsa
                    # Start/Result çifti hangi PDF hangi cinsiyet → sırayla ata
                    # (M için ilk, F için ikinci — Erkekler her zaman önce gelir)
                    pass

                event_map[pdf_filename] = event
                logger.debug("Event map: %s → %s %dm %s",
                             pdf_filename, event.gender, event.distance, event.stroke)

    logger.info("Event map: %d PDF eşleşmesi bulundu.", len(event_map))
    return event_map


def extract_pdf_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Sayfadaki tüm ResultList PDF linklerini tam URL olarak döndürür.
    (StartList, PointScore, ProgressionDetails gibi diğer PDF'ler atlanır.)
    """
    base_url = base_url.rstrip("/") + "/"
    pdf_urls: list[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "ResultList" not in href:
            continue
        if not href.endswith(".pdf"):
            continue

        # Göreli URL'yi tam URL'ye çevir
        if href.startswith("http"):
            full_url = href
        else:
            full_url = base_url + href.lstrip("/")

        if full_url not in pdf_urls:
            pdf_urls.append(full_url)

    # Sayısal sırayla sırala: ResultList_1.pdf, ResultList_2.pdf, ...
    def _sort_key(url: str) -> int:
        m = re.search(r"(\d+)\.pdf$", url)
        return int(m.group(1)) if m else 999

    pdf_urls.sort(key=_sort_key)
    logger.info("%d ResultList PDF linki bulundu.", len(pdf_urls))
    return pdf_urls


def extract_race_meta(soup: BeautifulSoup) -> dict:
    """
    Yarış başlığı, şehir ve tarih bilgilerini HTML'den çıkarır.
    """
    meta = {"title": "", "location": "", "date": ""}

    # Başlık genellikle ilk tablonun ilk hücresinde
    tables = soup.find_all("table")
    if tables:
        first_row = tables[0].find("tr")
        if first_row:
            cells = first_row.find_all(["td", "th"])
            if cells:
                meta["title"] = cells[0].get_text(strip=True)
            if len(cells) > 1:
                meta["location"] = cells[1].get_text(strip=True)

    # Tarih genellikle ikinci satırda
    if tables:
        rows = tables[0].find_all("tr")
        if len(rows) > 1:
            cells = rows[1].find_all(["td", "th"])
            if cells:
                meta["location"] = cells[0].get_text(strip=True)
            if len(cells) > 1:
                meta["date"] = cells[1].get_text(strip=True)

    return meta


def parse_race_page(url: str) -> RacePageInfo | None:
    """
    Tam yarış sayfası analizi — fetch + event_map + pdf_links + meta.

    Döndürür: RacePageInfo veya None (sayfa indirilemezse)
    """
    soup = fetch_page(url)
    if soup is None:
        return None

    base_url = url.rstrip("/") + "/"
    event_map = extract_event_map(soup)
    pdf_urls  = extract_pdf_urls(soup, base_url)
    meta      = extract_race_meta(soup)

    return RacePageInfo(
        title     = meta.get("title", ""),
        location  = meta.get("location", ""),
        date      = meta.get("date", ""),
        event_map = event_map,
        pdf_links = pdf_urls,
    )
