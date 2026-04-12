"""
parsers/progression_parser.py — Splash Meet Manager ProgressionDetails.pdf Ayrıştırıcı

Bu PDF, bir yarışmadaki tüm sporcuların tüm branş sonuçlarını + kişisel rekorlarını
kulüp bazında listeler. Tek bir dosyada tüm veri bulunur.

Sayfa yapısı (OCR satır sırası):
  [Yarışma başlığı / tarih satırları]   → atlanır
  [Kulüp adı]                           → aktif kulüp güncellenir
  [Ad SOYAD, YYYY (XX yaş), Cinsiyet]   → aktif sporcu güncellenir
  [NNm Stil   yer.   süre   puan   PB]  → bir branş sonucu

  Örnek:
    Adalar Su Sporlari Spor Kulubu
    Yaman YILMAZ, 2012 (14 yas), Erkekler
    50m Serbest   128.  28.43  416  28.00  97%

Çıktı: RawResult listesi (pdf_parser ile aynı tip)
       Kaynak alanı: "progression_pdf"

Kullanım:
  from parsers.progression_parser import parse_progression_pdf, parse_progression_from_url
"""

import io
import re
import logging

import requests

from config import HTTP_TIMEOUT_PDF, HTTP_HEADERS
from parsers.lenex_parser import RawResult

# OCR desteği — pdf_parser ile aynı koşul
try:
    import fitz
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR as _RapidOCR
    _OCR_AVAILABLE = True
except ImportError:
    _OCR_AVAILABLE = False

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Sabitler
# ─────────────────────────────────────────────────────────────────────────────

# Progression PDF'in canli.tyf.gov.tr'deki standart yolu
_PROGRESSION_PATH = "/canli/ProgressionDetails.pdf"

# Sporcu başlık satırı: "Yaman YILMAZ, 2012 (14 yas), Erkekler"
_ATHLETE_RE = re.compile(
    r"^(.+?),\s*(\d{4})\s*\(\s*\d+\s*ya[sş]\s*\)\s*,\s*(erkekler|kizlar|bayanlar|oglanlar)",
    re.IGNORECASE,
)

# Branş satırı: "50m Serbest" veya "1500m Serbest" vb.
_EVENT_RE = re.compile(
    r"(\d+)\s*m\s+(serbest|sirtüstü|sirtustu|kurbagalama|kelebek|karisik)",
    re.IGNORECASE,
)

# Zaman kalıbı
_TIME_RE = re.compile(r"\b(\d{1,2}:\d{2}\.\d{2}|\d{2}\.\d{2})\b")

# Atlanan başlık satırları
_SKIP_RE = re.compile(
    r"(sporcularin ilerlemesi|tum yarislar|tarih|seans|verilen derece|"
    r"toplam zaman|yer\b|derece\b|^pb$|^%$|\d+m:|baraj|splash)",
    re.IGNORECASE,
)

# Türkçe cinsiyet → standart
_GENDER_MAP = {
    "erkekler": "M", "oglanlar": "M",
    "kizlar": "F",   "bayanlar": "F",
}

# Türkçe stil → standart
_STROKE_MAP = {
    "serbest":     "Serbest",
    "sirtüstü":    "Sırtüstü",
    "sirtustu":    "Sırtüstü",
    "kurbagalama": "Kurbağalama",
    "kelebek":     "Kelebek",
    "karisik":     "Karışık",
}

_VALID_DISTANCES = {50, 100, 200, 400, 800, 1500}

# YB century cutoff için import
from config import YB_CENTURY_CUTOFF, COMPETITION_YEAR


# ─────────────────────────────────────────────────────────────────────────────
# OCR yardımcı
# ─────────────────────────────────────────────────────────────────────────────

def _norm(text: str) -> str:
    """Türkçe karakterleri ASCII'ye çevir + küçük harf."""
    _tr = str.maketrans({
        0x0131: "i", 0x0130: "i", 0x015F: "s", 0x015E: "s",
        0x011F: "g", 0x011E: "g", 0x00FC: "u", 0x00DC: "u",
        0x00F6: "o", 0x00D6: "o", 0x00E7: "c", 0x00C7: "c",
    })
    return text.translate(_tr).lower().strip()


def _ocr_page_lines(doc, page_num: int, ocr_engine, zoom: int = 2) -> list[str]:
    """Sayfayı OCR yapıp satır listesi döner (pdf_parser._ocr_page_to_lines ile aynı mantık)."""
    page = doc[page_num]
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix.n == 4:
        img = img[:, :, :3]

    result, _ = ocr_engine(img)
    if not result:
        return []

    items = []
    for item in result:
        bbox = item[0]
        x1 = min(p[0] for p in bbox)
        y1 = min(p[1] for p in bbox)
        items.append((y1, x1, item[1]))
    items.sort()

    lines = []
    current_y = None
    current_row: list[tuple[float, str]] = []
    for y, x, text in items:
        if current_y is None or abs(y - current_y) > 18:
            if current_row:
                current_row.sort()
                raw_line = " ".join(t for _, t in current_row)
                # OCR zaman boşluk düzeltmesi
                raw_line = re.sub(r"(\d{1,2})\s*:\s+(\d{2}\.\d{2})", r"\1:\2", raw_line)
                raw_line = re.sub(r"(\d{1,2})\s+:\s*(\d{2}\.\d{2})", r"\1:\2", raw_line)
                raw_line = re.sub(r"([A-Za-zÀ-ÿ\u00C0-\u024F])(\d{2}\.\d{2})", r"\1 \2", raw_line)
                raw_line = re.sub(r"(\d{2}\.\d{2})([A-Za-zÀ-ÿ\u00C0-\u024F])", r"\1 \2", raw_line)
                lines.append(raw_line)
            current_row = [(x, text)]
            current_y = y
        else:
            current_row.append((x, text))
    if current_row:
        current_row.sort()
        raw_line = " ".join(t for _, t in current_row)
        raw_line = re.sub(r"(\d{1,2})\s*:\s+(\d{2}\.\d{2})", r"\1:\2", raw_line)
        raw_line = re.sub(r"(\d{1,2})\s+:\s*(\d{2}\.\d{2})", r"\1:\2", raw_line)
        raw_line = re.sub(r"([A-Za-zÀ-ÿ\u00C0-\u024F])(\d{2}\.\d{2})", r"\1 \2", raw_line)
        raw_line = re.sub(r"(\d{2}\.\d{2})([A-Za-zÀ-ÿ\u00C0-\u024F])", r"\1 \2", raw_line)
        lines.append(raw_line)
    return lines


# ─────────────────────────────────────────────────────────────────────────────
# Durum makinesi ayrıştırıcı
# ─────────────────────────────────────────────────────────────────────────────

def _is_club_line(line: str) -> bool:
    """
    Kulüp adı satırı mı?
    Sporcu satırı veya branş satırı değilse ve yeterince uzunsa kulüp adıdır.
    """
    if _ATHLETE_RE.search(line):
        return False
    if _EVENT_RE.search(_norm(line)):
        return False
    # Kulüp adı genellikle 5+ karakter, yalnızca harf/boşluk/noktalama
    # ve "Spor Kulüb" veya "SK" gibi kelime içerir
    if len(line.strip()) < 5:
        return False
    # Zaman içeriyorsa kulüp adı değil
    if _TIME_RE.search(line):
        return False
    return True


def _parse_lines(lines: list[str]) -> list[RawResult]:
    """
    OCR satırlarından RawResult listesi oluşturur (durum makinesi).
    """
    results: list[RawResult] = []

    current_club   = "Bilinmiyor"
    current_name   = None
    current_yb_str = None
    current_birth  = None
    current_gender = "M"

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        norm = _norm(line)

        # ── Atlanacak başlık satırları ──────────────────────────────────────
        if _SKIP_RE.search(norm):
            continue

        # ── Sporcu başlık satırı: "Yaman YILMAZ, 2012 (14 yas), Erkekler" ──
        athlete_m = _ATHLETE_RE.search(line)
        if athlete_m:
            raw_name = athlete_m.group(1).strip()
            # OCR artefaktı: iki sporcu adı birleşmiş olabilir
            # Örn: "Ayse Dila AKYUZ Duru OKTAY" → "Duru OKTAY"
            # Kalıp: iki ALL_CAPS kelime varsa, son isim+SOYAD bloğunu al
            tokens = raw_name.split()
            caps_pos = [i for i, t in enumerate(tokens)
                        if t.upper() == t and len(t) > 1 and t.isalpha()]
            if len(caps_pos) >= 2:
                # İkinci CAPS'ten önceki Title Case kelimelerden başla
                name_start = caps_pos[-2] + 1
                raw_name = " ".join(tokens[name_start:])
                logger.debug("Birleşik isim düzeltildi: '%s' → '%s'",
                             athlete_m.group(1).strip(), raw_name)
            current_name   = raw_name
            yb_full        = int(athlete_m.group(2))
            gender_raw     = _norm(athlete_m.group(3))
            current_gender = _GENDER_MAP.get(gender_raw, "M")
            current_birth  = yb_full
            # yb_raw: 2 haneli (2012 → "12")
            current_yb_str = str(yb_full % 100).zfill(2)
            continue

        # ── Branş satırı: "50m Serbest ... süre ..." ──────────────────────
        event_m = _EVENT_RE.search(norm)
        if event_m and current_name:
            distance   = int(event_m.group(1))
            stroke_raw = event_m.group(2)
            stroke     = _STROKE_MAP.get(_norm(stroke_raw))
            if not stroke or distance not in _VALID_DISTANCES:
                continue

            # Zamanları bul — en büyüğü gerçek sonuç zamanıdır
            time_matches = list(_TIME_RE.finditer(line))
            if not time_matches:
                continue

            best_secs = 0.0
            best_str  = None
            for tm in time_matches:
                ts = tm.group(1)
                try:
                    if ":" in ts:
                        parts = ts.split(":")
                        s = int(parts[0]) * 60 + float(parts[1])
                    else:
                        s = float(ts)
                    if s > best_secs:
                        best_secs = s
                        best_str  = ts
                except ValueError:
                    pass

            if best_str is None or best_secs < 1:
                continue

            # Yaş makullük kontrolü (üst sınır 60 — masters sporcuları için)
            age = COMPETITION_YEAR - (current_birth or 0)
            if not (8 <= age <= 60):
                continue

            results.append(RawResult(
                name_raw   = current_name,
                yb_raw     = current_yb_str or "",
                birth_year = current_birth or 0,
                club_raw   = current_club,
                gender     = current_gender,
                stroke     = stroke,
                distance   = distance,
                time_text  = best_str,
                time_seconds = best_secs,
                source     = "progression_pdf",
            ))
            continue

        # ── Kulüp adı satırı ───────────────────────────────────────────────
        # Sporcu veya branş değilse → kulüp adı (yeni bölüm başlangıcı)
        if _is_club_line(line):
            current_club = line.strip()
            # Yeni kulüp = yeni sporcu bölümü
            current_name   = None
            current_yb_str = None
            current_birth  = None

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Ana parse fonksiyonları
# ─────────────────────────────────────────────────────────────────────────────

def parse_progression_pdf(pdf_content: bytes) -> list[RawResult]:
    """
    ProgressionDetails.pdf içeriğini OCR yaparak ayrıştırır.

    Döndürür: RawResult listesi (source="progression_pdf") veya []
    """
    if not _OCR_AVAILABLE:
        logger.warning("OCR bileşenleri yok — ProgressionDetails.pdf atlanıyor.")
        return []

    try:
        doc = fitz.open(stream=io.BytesIO(pdf_content), filetype="pdf")
    except Exception as e:
        logger.error("ProgressionDetails.pdf açılamadı: %s", e)
        return []

    ocr = _RapidOCR()
    all_lines: list[str] = []

    for page_num in range(len(doc)):
        try:
            page_lines = _ocr_page_lines(doc, page_num, ocr)
            all_lines.extend(page_lines)
        except Exception as e:
            logger.warning("ProgressionDetails OCR sayfa %d hatası: %s", page_num + 1, e)

    results = _parse_lines(all_lines)
    logger.info("ProgressionDetails.pdf: %d sayfa, %d sonuç.", len(doc), len(results))
    return results


def parse_progression_from_url(base_url: str) -> list[RawResult]:
    """
    Yarış URL'sinden ProgressionDetails.pdf'i indirir ve ayrıştırır.

    base_url: "https://canli.tyf.gov.tr/istanbul/cs-1005235/" gibi yarış ana URL'si
    """
    base = base_url.rstrip("/")
    pdf_url = base + _PROGRESSION_PATH
    logger.info("ProgressionDetails.pdf indiriliyor: %s", pdf_url)
    try:
        r = requests.get(pdf_url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT_PDF)
        r.raise_for_status()
    except Exception as e:
        logger.info("ProgressionDetails.pdf bulunamadı (%s): %s", pdf_url, e)
        return []

    return parse_progression_pdf(r.content)
