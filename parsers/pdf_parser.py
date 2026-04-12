"""
parsers/pdf_parser.py — ResultList PDF Ayrıştırıcı

PDF formatı (Splash Meet Manager çıktısı):
  Her sayfada:
    - Başlık satırı: "Yarış N, Erkekler, 50m Serbest, 11 yaş"
    - Sütun başlıkları: "YB  Zaman  Derece"  (atlanır)
    - Sporcu satırları: "Eymen ÖZKAN  14  Pamukkale Olimpik Sporlar SK  28.51  394"
    - Durum satırları: "KATILIM BARAJINI GEÇTİ", "SW 10.2 ..." (atlanır)

Ayrıştırma stratejisi:
  1. Her sayfanın başlığından gender/distance/stroke çıkar (orijinal kaynak)
  2. EventInfo yoksa (HTML mapping'den gelenle) fallback olarak kullan
  3. Sporcu satırını tespit et: sonda zaman (28.51 veya 1:02.34) olan satır
  4. YB'yi bul (2 haneli sayı, isimden sonra)
  5. İsim = YB'den önce, Kulüp = YB'den sonra / zamandan önce
"""

import io
import re
import logging

import requests
import PyPDF2

# OCR desteği — opsiyonel, import hatası sessizce atlanır
try:
    import fitz          # pymupdf
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR as _RapidOCR
    _OCR_AVAILABLE = True
except ImportError:
    _OCR_AVAILABLE = False

from config import HTTP_TIMEOUT_PDF, HTTP_HEADERS
from parsers.html_parser import EventInfo

logger = logging.getLogger(__name__)

# Lenex parser'dan import — aynı RawResult tipi kullanılıyor
from parsers.lenex_parser import RawResult, _seconds_to_display


# ─────────────────────────────────────────────────────────────────────────────
# Sabitler
# ─────────────────────────────────────────────────────────────────────────────

# Geçerli bireysel mesafeler
_VALID_DISTANCES = {50, 100, 200, 400, 800, 1500}

# Atlanan durum satırı kalıpları
_SKIP_PATTERNS = re.compile(
    r"(katilim baraji|barajini gecti|sw\s+\d|dsq|diskalifiye|dns|dnf|"
    r"yb\s+zaman|sayi\s+\d|splash meet|splash|sayfa \d|registered to|"
    r"timed final|baraj|puanlar|puan|aqua|\d+\.\s*-\s*\d+\.\s*(gun|gün)|"
    r"\d+m:|;|"                    # split zaman satırları ve bariyer bilgisi
    r"\bdisk\.|"                   # diskalifiye satırları: "Disk. Sporcu Adı"
    r"^[\d:.\s]+$)",               # yalnızca zamandan oluşan satırlar (kümülatif split)
    re.IGNORECASE
)

# Zaman kalıpları (sonuç satırı tespiti için)
_TIME_PATTERN = re.compile(
    r"\b(\d{1,2}:\d{2}\.\d{2}|\d{2}\.\d{2})\b"
)

# Başlık satırı kalıpları
_HEADER_PATTERN = re.compile(
    r"yaris\s+\d+[,\s]*(erkekler|kizlar|bayanlar|oglanlar)[,\s]*"
    r"(\d+)\s*m[,\s]*(serbest|sirtüstü|sirtustu|kurbagalama|kelebek|karisik)",
    re.IGNORECASE
)

# Türkçe → normalize
_TR_NORM = str.maketrans({
    0x0131: "i", 0x0130: "i", 0x015F: "s", 0x015E: "s",
    0x011F: "g", 0x011E: "g", 0x00FC: "u", 0x00DC: "u",
    0x00F6: "o", 0x00D6: "o", 0x00E7: "c", 0x00C7: "c",
})

_STROKE_TR_MAP = {
    "serbest":     "Serbest",
    "sirtüstü":    "Sırtüstü",
    "sirtustü":    "Sırtüstü",
    "sirtüstu":    "Sırtüstü",
    "sirtustu":    "Sırtüstü",
    "kurbagalama": "Kurbağalama",
    "kelebek":     "Kelebek",
    "karisik":     "Karışık",
}

_GENDER_TR_MAP = {
    "erkekler": "M", "oglanlar": "M",
    "kizlar":   "F", "bayanlar": "F",
}

# OCR artefakt düzeltmesi: yarışma süresi eşik değerleri (yaklaşık dünya rekoru - 1s)
# Bir süre bu değerin altındaysa OCR dakika ön ekini kaçırmış demektir.
_MIN_REASONABLE_TIMES: dict[tuple[str, int], float] = {
    ("Serbest",     50):  22.0,
    ("Serbest",    100):  46.0,
    ("Serbest",    200): 100.0,
    ("Serbest",    400): 220.0,
    ("Serbest",    800): 455.0,
    ("Serbest",   1500): 870.0,
    ("Sırtüstü",   50):  24.0,
    ("Sırtüstü",  100):  52.0,
    ("Sırtüstü",  200): 112.0,
    ("Kurbağalama", 50):  26.0,
    ("Kurbağalama",100):  57.0,
    ("Kurbağalama",200): 128.0,
    ("Kelebek",    50):  23.0,
    ("Kelebek",   100):  49.0,
    ("Kelebek",   200): 113.0,
    ("Karışık",   200): 114.0,
    ("Karışık",   400): 255.0,
}


# ─────────────────────────────────────────────────────────────────────────────
# Yardımcı fonksiyonlar
# ─────────────────────────────────────────────────────────────────────────────

def _norm(text: str) -> str:
    return text.translate(_TR_NORM).lower().strip()


def _fix_ocr_time(time_text: str, time_seconds: float,
                  stroke: str, distance: int) -> tuple[str, float]:
    """
    OCR'den gelen sürede dakika öneki kaçırılmışsa düzeltir.
    Örnek: "07.37" (7.37s) → "1:07.37" (67.37s) [100m Kelebek için]

    Yalnızca "SS.cc" formatındaki (iki nokta içermeyen) sürelere uygulanır.
    Döndürür: (düzeltilmiş_time_text, düzeltilmiş_time_seconds)
    """
    min_secs = _MIN_REASONABLE_TIMES.get((stroke, distance))
    if min_secs is None or time_seconds >= min_secs or ":" in time_text:
        return time_text, time_seconds  # Zaten makul veya zaten dakika var

    for minutes in range(1, 21):
        new_secs = minutes * 60 + time_seconds
        if new_secs >= min_secs:
            logger.debug("OCR süre düzeltildi: %s → %d:%s (%.2fs, %s %dm)",
                         time_text, minutes, time_text, new_secs, stroke, distance)
            return f"{minutes}:{time_text}", new_secs

    return time_text, time_seconds  # Düzeltemedik


def _time_to_seconds(time_str: str) -> float | None:
    """
    "28.51" veya "1:02.34" → saniye.
    """
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            return int(parts[0]) * 60 + float(parts[1])
        return float(time_str)
    except ValueError:
        return None


def _parse_pdf_header(line: str) -> EventInfo | None:
    """
    "Yarış 10, Erkekler, 50m Serbest, 11 yaş" gibi başlıktan EventInfo çıkarır.
    """
    norm = _norm(line)
    match = re.search(
        r"yaris\s*\d+[,\s]*(erkekler|kizlar|bayanlar|oglanlar)?[,\s]*"
        r"(\d+)\s*m[,\s]*(serbest|sirtüstü|sirtustu|kurbagalama|kelebek|karisik)",
        norm, re.IGNORECASE
    )
    if not match:
        return None

    gender_raw = match.group(1) or ""
    distance   = int(match.group(2))
    stroke_raw = match.group(3)

    if distance not in _VALID_DISTANCES:
        return None

    gender = _GENDER_TR_MAP.get(_norm(gender_raw), "M")
    stroke = _STROKE_TR_MAP.get(_norm(stroke_raw))
    if not stroke:
        return None

    return EventInfo(gender=gender, distance=distance, stroke=stroke)


def _is_skip_line(line: str) -> bool:
    """
    Durum satırı mı? (KATILIM BARAJINI GEÇTİ, SW 10.2, başlık vs.)
    Bu satırlar sporcu verisi içermez.
    """
    norm_line = _norm(line)
    return bool(_SKIP_PATTERNS.search(norm_line))


def _parse_result_line(line: str, event: EventInfo) -> RawResult | None:
    """
    Sporcu satırını ayrıştırır.

    Satır formatları (Splash Meet Manager):
      Kısa mesafe:  "Eymen ÖZKAN 14 Pamukkale SK 28.51 394"
      Uzun mesafe:  "Ali CAN 13 Ankara SK 1:06.44 340 32.23"
                     → [sonuç_zamanı] [puan] [50m_split_zamanı]

    Strateji:
      1. Zaman eşleşmelerini bul
      2. Sonuç zamanını belirle:
         - Son zamandan SONRA puan tamsayısı → son zaman = sonuç zamanı
         - Son zamandan SONRA hiçbir şey yok VE iki zaman var VE aralarında puan var
           → ikinci son zaman = sonuç zamanı (son zaman = 50m split)
      3. YB'yi bul, isim/kulüp ayır
    """
    line = line.strip()
    if not line or len(line) < 10:
        return None

    # Sıra numarasını çıkar: "1.Buğlem" → "Buğlem", "13. Ali" → "Ali"
    line = re.sub(r"^\d+\.\s*", "", line).strip()
    if not line or len(line) < 10:
        return None

    # OCR artefakt: satır başındaki özel karakterleri temizle
    # Örnekler: "/ Ali Ulas Akkoyun", "$ Salih Yagiz Eyi", "[ Derin Kilic"
    line = re.sub(r"^[^\w\u00C0-\u024F]+\s*", "", line).strip()
    # Başta tek rakam + boşluk (ama "1." zaten üstte handle edildi): "1 Nil" → "Nil"
    line = re.sub(r"^\d+\s+(?=[A-ZÇĞİÖŞÜa-zçğışöüÀ-ÿ])", "", line).strip()
    if not line or len(line) < 10:
        return None

    # OCR artefakt: küçükharf→BÜYÜKHARF sınırında boşluk ekle (CamelCase birleştirme)
    # Kalıp 1 — 3+ ardışık küçük harf → büyük harf:
    #   "BensuhayatOYGU" → "Bensuhayat OYGU",  "GalatasaraySporKulubu" → "Galatasaray Spor Kulubu"
    # Kalıp 2 — TitleCase (B + 2+ küçük) → 2+ büyük:
    #   "EceDAGLIOGLU" → "Ece DAGLIOGLU",  "EfeDAGLIOGLU" → "Efe DAGLIOGLU"
    # Güvenli: "CiVELEK" (1 küçük) ve "AYDEMiR" (1 küçük) bölünmez.
    # Not: Zaman tokenları (4:34.91) etkilenmez — sadece harf geçişlerine bakılır.
    line = re.sub(r"([a-zçğışöü]{3,})([A-ZÇĞİÖŞÜ])", r"\1 \2", line)
    line = re.sub(r"([A-ZÇĞİÖŞÜ][a-zçğışöü]{2,})([A-ZÇĞİÖŞÜ]{2,})", r"\1 \2", line)

    # Okul yarışı katılım türü: "(Fd)", "(Tk)", "(TD)" → sporcu adından önce veya kulüpten önce
    # Örnek: "(Fd) Duru ÇETİNOĞLU 12 Uğur Okulları..."
    _PTYPE_PATTERN = re.compile(r"^\((Fd|Tk|TD|Td|FD|TK)\)\s*", re.IGNORECASE)
    ptype_match = _PTYPE_PATTERN.match(line)
    participant_type: str | None = None
    if ptype_match:
        participant_type = ptype_match.group(1).upper()
        line = line[ptype_match.end():].strip()
        if not line or len(line) < 10:
            return None

    # 1. Tüm zaman eşleşmelerini bul ve saniyeye çevir
    time_matches = list(_TIME_PATTERN.finditer(line))
    if not time_matches:
        return None  # Zaman yoksa sporcu satırı değil

    # 2. Sonuç zamanını seç: en büyük zaman = gerçek süre
    #    (toplam süre her zaman split sürelerinden büyüktür)
    best_match    = None
    best_seconds  = 0.0
    for m in time_matches:
        secs = _time_to_seconds(m.group(1))
        if secs and secs > best_seconds:
            best_seconds = secs
            best_match   = m

    if best_match is None or best_seconds < 1:
        return None

    time_str     = best_match.group(1)
    time_seconds = best_seconds

    # Sonuç zamanından önceki kısmı al (= isim + YB + kulüp)
    before_time = line[:best_match.start()].rstrip()

    # Sondaki tam sayıyı (puan) kaldır — nadir durumda puan önce gelebilir
    before_time = re.sub(r"\s+\d+$", "", before_time).rstrip()

    # 2. YB'yi bul: 2 haneli sayı (0-99)
    # (?<!\d)...(?!\d) — hem normal ayraçlı ("Ali 13 Enka") hem de
    # sona yapışık ("Daglioglu13 Enka") durumları yakalar.
    # \b başarısız olur çünkü "u13" da "u" ve "1" ikisi de \w karakter.
    yb_match = re.search(r"(?<!\d)(\d{2})(?!\d)", before_time)
    if not yb_match:
        return None

    yb_str = yb_match.group(1)

    # 3. İsim ve kulübü ayır
    yb_start = yb_match.start()
    yb_end   = yb_match.end()

    name_raw = before_time[:yb_start].strip()
    # OCR artefakt: isim başındaki çöpleri temizle
    # Örnekler: "A Alanur Eroglu" → "Alanur Eroglu", "E Efe Emir Erturk" → "Efe Emir Erturk"
    # Büyük veya küçük harf tek öneki: "A Alanur" / "k Kemal" → asıl isme geç
    name_raw = re.sub(r"^[A-ZÇĞİÖŞÜa-zçğışöü]\s+(?=[A-ZÇĞİÖŞÜ])", "", name_raw).strip()

    club_raw = before_time[yb_end:].strip()

    # Kulüp başındaki (Tk)/(Fd)/(TD) önekini de soy (bazı formatlarda YB'den sonra gelir)
    # Örnek: "13 (Tk) Ted Ankara Koleji..." → club_raw = "(Tk) Ted Ankara Koleji..."
    club_ptype = _PTYPE_PATTERN.match(club_raw) if club_raw else None
    if club_ptype:
        if participant_type is None:
            participant_type = club_ptype.group(1).upper()
        club_raw = club_raw[club_ptype.end():].strip()

    # OCR artefakt: zaman semboller/harflerle bitişik yazılmışsa _TIME_PATTERN \b yakalanamaz
    # Örnek: "Spoi5:09.45i445 1:10.61" → "5:09.45" word boundary yok, sadece 1:10.61 yakalandı
    # Kulüp adında M:SS.cc zaman varsa HER DURUMDA kulüpten çıkar (OCR artefaktı).
    # Ayrıca gömülü zaman best_seconds'tan büyükse gerçek zamandır → güncelle.
    _EMBEDDED_TIME_RE = re.compile(r"(\d{1,2}:\d{2}\.\d{2})")
    _club_emb = _EMBEDDED_TIME_RE.search(club_raw)
    if _club_emb:
        emb_time_str = _club_emb.group(1)
        emb_secs = _time_to_seconds(emb_time_str) or 0.0
        if emb_secs > best_seconds:
            # Gömülü zaman daha büyük → gerçek sonuç zamanı olabilir
            time_str      = emb_time_str
            time_seconds  = emb_secs
            best_seconds  = emb_secs
        # Zaman değeri ne olursa olsun kulüp adından her zaman çıkar
        club_raw = club_raw[:_club_emb.start()].strip()

    # OCR artefakt: kulüp sonundaki dakika ön ekini kurtar
    # Örnek: "DinamikNesiller...Kulubu3:" → recovered_minutes=3, club="DinamikNesiller...Kulubu"
    # Örnek: "Istanbul Performans...Kulubu2:" → recovered_minutes=2
    # NOT: \b çalışmaz çünkü önceki karakter alfasayısal. Sadece 1-2 rakam (tek haneli dakika).
    _club_min_m = re.search(r"(\d{1,2}):?\s*$", club_raw)
    recovered_minutes: int | None = None
    if _club_min_m:
        m_val = int(_club_min_m.group(1))
        if m_val <= 20:  # 1-20 dakika: 800m/1500m için de geçerli (Sr11:, Sp20: gibi)
            recovered_minutes = m_val
        club_raw = club_raw[:_club_min_m.start()].strip()

    # Geçerlilik kontrolleri
    if not name_raw or len(name_raw) < 2:
        return None
    # İsimde en az bir harf olmalı (salt zaman/rakam içeren OCR split satırlarını eler)
    if not re.search(r"[A-Za-zÀ-ÿ\u00C0-\u024F]", name_raw):
        return None
    if not club_raw:
        club_raw = "Bilinmiyor"

    # YB integer
    try:
        yb_int = int(yb_str)
    except ValueError:
        return None

    # Doğum yılı hesapla (m3_age import etmeden — döngüsel import'u önle)
    # config'den YB_CENTURY_CUTOFF al
    from config import YB_CENTURY_CUTOFF, COMPETITION_YEAR
    birth_year = (2000 + yb_int) if yb_int <= YB_CENTURY_CUTOFF else (1900 + yb_int)

    # Yaş makullük kontrolü — OCR rank/sıra numarasını YB olarak okuyabilir
    age = COMPETITION_YEAR - birth_year
    if not (8 <= age <= 60):
        # OCR artefaktı: "1" → "6" karışıklığı (font benzerliği)
        # Örn: YB "10" → OCR "60" → doğum 1960 → yaş 66 (geçersiz)
        # İlk basamak "6" ise "1" ile değiştirip tekrar dene: "60"→"10", "61"→"11", "63"→"13"
        if yb_str and yb_str[0] == '6':
            alt_yb_str = '1' + yb_str[1]
            alt_yb_int = int(alt_yb_str)
            alt_birth_year = (2000 + alt_yb_int) if alt_yb_int <= YB_CENTURY_CUTOFF else (1900 + alt_yb_int)
            alt_age = COMPETITION_YEAR - alt_birth_year
            if 8 <= alt_age <= 60:
                logger.debug("OCR YB düzeltmesi: '%s'→'%s' yaş %d→%d, satır: %s",
                             yb_str, alt_yb_str, age, alt_age, line[:60])
                yb_str    = alt_yb_str
                birth_year = alt_birth_year
                age        = alt_age
            else:
                logger.debug("Geçersiz yaş %d (YB=%d), satır atlanıyor: %s", age, birth_year, line)
                return None
        else:
            logger.debug("Geçersiz yaş %d (YB=%d), satır atlanıyor: %s", age, birth_year, line)
            return None

    # OCR süre düzeltmesi
    if recovered_minutes is not None and ":" not in time_str:
        # Dakika ön eki kulüpten kurtarıldı, doğrudan kullan
        time_str = f"{recovered_minutes}:{time_str}"
        time_seconds = recovered_minutes * 60 + time_seconds
    elif recovered_minutes is not None and ":" in time_str:
        # Zaman "M:SS.cc" formatında ama OCR M rakamını yanlış okumuş olabilir
        # (örn. "2:40.35" → "1:40.35" çünkü "2" kulüp adına yapıştı)
        parts = time_str.split(":", 1)
        try:
            old_m = int(parts[0])
            ss_text = parts[1]
            ss_secs = _time_to_seconds(ss_text) or 0.0
            if old_m != recovered_minutes:
                time_str = f"{recovered_minutes}:{ss_text}"
                time_seconds = recovered_minutes * 60 + ss_secs
        except (ValueError, IndexError):
            pass
    else:
        # Dakika ön eki kulüpte yoktu — standart OCR düzeltmesi
        time_str, time_seconds = _fix_ocr_time(time_str, time_seconds,
                                                event.stroke, event.distance)

    return RawResult(
        name_raw         = name_raw,
        yb_raw           = yb_str,
        birth_year       = birth_year,
        club_raw         = club_raw,
        gender           = event.gender,
        stroke           = event.stroke,
        distance         = event.distance,
        time_text        = time_str,
        time_seconds     = time_seconds,
        source           = "pdf",
        participant_type = participant_type,
    )


# ─────────────────────────────────────────────────────────────────────────────
# PDF indirme ve ayrıştırma
# ─────────────────────────────────────────────────────────────────────────────

def download_pdf(pdf_url: str) -> bytes | None:
    """PDF'i indirir. Hata durumunda None döner."""
    try:
        r = requests.get(pdf_url, headers=HTTP_HEADERS, timeout=HTTP_TIMEOUT_PDF)
        r.raise_for_status()
        return r.content
    except Exception as e:
        logger.warning("PDF indirilemedi '%s': %s", pdf_url, e)
        return None


def parse_pdf(pdf_content: bytes, hint_event: EventInfo | None = None) -> list[RawResult]:
    """
    PDF içeriğini ayrıştırır, RawResult listesi döner.

    Parametreler:
      pdf_content: PDF dosyasının byte içeriği
      hint_event: HTML event map'ten gelen ipucu (opsiyonel)
                  PDF başlığından event bilgisi okunamazsa kullanılır.

    Her sayfayı ayrı ayrı işler:
      - Sayfa başında Yarış başlığından gender/distance/stroke tespit edilir
      - Sonraki satırlar sporcu verisi olarak parse edilir
    """
    results: list[RawResult] = []
    current_event: EventInfo | None = hint_event

    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
    except Exception as e:
        logger.error("PDF okunamadı: %s", e)
        return []

    for page_num, page in enumerate(reader.pages):
        try:
            text = page.extract_text() or ""
        except Exception as e:
            logger.warning("PDF sayfa %d okunamadı: %s", page_num + 1, e)
            continue

        lines = text.splitlines()
        page_event = current_event  # Sayfa başı event'i bu sayfanın event'i olabilir

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Başlık satırı mı? (Yarış 10, Erkekler, 50m Serbest...)
            header_event = _parse_pdf_header(line)
            if header_event:
                page_event = header_event
                current_event = header_event
                continue

            # Atlanacak satır mı?
            if _is_skip_line(line):
                continue

            # Event bilgisi yoksa bu satırı işleyemeyiz
            if page_event is None:
                continue

            # Sporcu satırı parse et
            result = _parse_result_line(line, page_event)
            if result:
                results.append(result)

    logger.debug("PDF: %d satır, %d sonuç.", sum(len(p.extract_text().splitlines())
                  for p in reader.pages if p.extract_text()), len(results))
    return results


def parse_pdf_from_url(pdf_url: str, hint_event: EventInfo | None = None) -> list[RawResult]:
    """
    PDF URL'sini indirir ve ayrıştırır.
    hint_event: HTML event map'ten gelen ipucu.
    """
    pdf_content = download_pdf(pdf_url)
    if not pdf_content:
        return []
    return parse_pdf(pdf_content, hint_event)


# ─────────────────────────────────────────────────────────────────────────────
# OCR tabanlı PDF ayrıştırma (görüntü tabanlı PDF'ler için)
# ─────────────────────────────────────────────────────────────────────────────

def _ocr_page_to_lines(doc, page_num: int, ocr_engine, zoom: int = 2) -> list[str]:
    """
    pymupdf ile sayfayı görüntüye çevirir, RapidOCR ile OCR yapar.
    Metin bloklarını X,Y koordinatına göre satır satır yeniden oluşturur.
    """
    page = doc[page_num]
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat)
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    if pix.n == 4:
        img = img[:, :, :3]

    result, _ = ocr_engine(img)
    if not result:
        return []

    # Y toleransı ile satır grupla; her satırda X sıralı birleştir
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
                # OCR artefakt: "5: 11.49" → "5:11.49" (dakika-saniye arası yanlış boşluk)
                raw_line = re.sub(r"(\d{1,2})\s*:\s+(\d{2}\.\d{2})", r"\1:\2", raw_line)
                raw_line = re.sub(r"(\d{1,2})\s+:\s*(\d{2}\.\d{2})", r"\1:\2", raw_line)
                # OCR artefakt: zaman harfe yapışık — "Spo27.59ibu631" → "Spo 27.59 ibu631"
                # word boundary olmadığı için _TIME_PATTERN bulamaz; boşluk ekle
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


def parse_pdf_ocr(pdf_content: bytes,
                  hint_event: EventInfo | None = None) -> list[RawResult]:
    """
    Görüntü tabanlı PDF'leri OCR ile ayrıştırır.

    PyPDF2 metin çıkaramazsa (görüntü tabanlı PDF) bu fonksiyon kullanılır.
    Gereksinimler: pymupdf (fitz) + rapidocr-onnxruntime

    Döndürür: RawResult listesi veya [] (OCR bileşenleri yoksa)
    """
    if not _OCR_AVAILABLE:
        logger.warning("OCR bileşenleri yok (pymupdf / rapidocr-onnxruntime). "
                       "Kurulum: pip install pymupdf rapidocr-onnxruntime")
        return []

    results: list[RawResult] = []
    current_event: EventInfo | None = hint_event

    try:
        doc = fitz.open(stream=io.BytesIO(pdf_content), filetype="pdf")
    except Exception as e:
        logger.error("OCR PDF açılamadı: %s", e)
        return []

    ocr = _RapidOCR()

    for page_num in range(len(doc)):
        try:
            lines = _ocr_page_to_lines(doc, page_num, ocr)
        except Exception as e:
            logger.warning("OCR sayfa %d hatası: %s", page_num + 1, e)
            continue

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Başlık satırı mı?
            header_event = _parse_pdf_header(line)
            if header_event:
                current_event = header_event
                continue

            # Atlanacak satır mı?
            if _is_skip_line(line):
                continue

            if current_event is None:
                continue

            result = _parse_result_line(line, current_event)
            if result:
                results.append(result)

    logger.info("OCR PDF: %d sayfa, %d sonuç.", len(doc), len(results))
    return results


def parse_pdf_fitz_text(pdf_content: bytes,
                        hint_event: EventInfo | None = None) -> list[RawResult]:
    """
    PyMuPDF (fitz) ile native metin çıkarımı.
    PyPDF2'den daha iyi metin düzeni; OCR'dan çok daha hızlı.
    Gömülü metin katmanı olan PDF'lerde işe yarar.
    """
    if not _OCR_AVAILABLE:  # fitz import kontrolü için aynı bayrağı kullan
        return []
    try:
        doc = fitz.open(stream=io.BytesIO(pdf_content), filetype="pdf")
    except Exception as e:
        logger.warning("fitz PDF açılamadı: %s", e)
        return []

    results: list[RawResult] = []
    current_event: EventInfo | None = hint_event

    for page_num in range(len(doc)):
        try:
            page = doc[page_num]
            text = page.get_text("text")
        except Exception as e:
            logger.warning("fitz sayfa %d okunamadı: %s", page_num + 1, e)
            continue

        lines = text.splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            header_event = _parse_pdf_header(line)
            if header_event:
                current_event = header_event
                continue
            if _is_skip_line(line):
                continue
            if current_event is None:
                continue
            result = _parse_result_line(line, current_event)
            if result:
                result = result._replace(source="pdf_fitz")
                results.append(result)

    logger.info("fitz text PDF: %d sayfa, %d sonuç.", len(doc), len(results))
    return results


def parse_pdf_auto(pdf_content: bytes,
                   hint_event: EventInfo | None = None) -> list[RawResult]:
    """
    PDF ayrıştırmada otomatik kaynak seçimi:
      1. Önce PyPDF2 ile standart metin çıkarımı dener.
      2. PyMuPDF native metin çıkarımı dener (gömülü metin katmanı varsa).
      3. Sonuç yoksa (görüntü tabanlı PDF) OCR'ye geçer.

    Kullanım:
      results = parse_pdf_auto(pdf_bytes)
    """
    # Standart metin çıkarımı (PyPDF2)
    standard = parse_pdf(pdf_content, hint_event)
    if standard:
        return standard

    # PyMuPDF native text (PyPDF2'den daha iyi layout analizi)
    if _OCR_AVAILABLE:
        fitz_results = parse_pdf_fitz_text(pdf_content, hint_event)
        if fitz_results:
            logger.info("fitz text başarılı, OCR atlanıyor.")
            return fitz_results

    # Standart başarısız → OCR
    logger.info("Metin tabanlı parse başarısız, OCR deneniyor...")
    return parse_pdf_ocr(pdf_content, hint_event)


def parse_pdf_from_url_auto(pdf_url: str,
                             hint_event: EventInfo | None = None) -> list[RawResult]:
    """
    PDF URL'sini indirir ve otomatik kaynak seçimiyle (standart+OCR) ayrıştırır.
    """
    pdf_content = download_pdf(pdf_url)
    if not pdf_content:
        return []
    return parse_pdf_auto(pdf_content, hint_event)
