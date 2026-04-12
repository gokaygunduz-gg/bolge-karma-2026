"""
config.py — Bölge Karmaları 2026 Genel Ayarlar

Tüm sabitler, yollar ve proje genelinde kullanılan değerler bu dosyadadır.
Başka bir dosyaya hardcode değer yazmak yerine buradan import edilir.
"""

import os

# ─────────────────────────────────────────────────────────────────────────────
# Yarış yılı
# ─────────────────────────────────────────────────────────────────────────────

COMPETITION_YEAR = 2026
"""
Yaş hesabı her zaman bu yıl baz alınarak yapılır.
2025 tarihli bir yarış gönderilse dahi 2026 kullanılır (bölge karmaları kuralı).
"""

# 2 haneli YB → 4 haneli doğum yılı dönüşümü için kesim noktası
# Örn: YB <= 26 → 2000-2026, YB 27-99 → 1927-1999
YB_CENTURY_CUTOFF: int = COMPETITION_YEAR % 100  # 26

# ─────────────────────────────────────────────────────────────────────────────
# Dizin ve dosya yolları
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MAPPING_EXCEL_PATH = os.path.join(
    BASE_DIR, "Kulüp Şehir Mapping Exceli", "Kulüp Şehir Mapping.xlsx"
)
MAPPING_SHEET_NAME = "Kulüp-Bölge-Şehir"

# Mapping sayfasındaki sütunlar (0-indeksli tuple erişimi için)
COL_CLUB_ALT       = 0   # A → Kulüp Alternatif (yarış sonuçlarında görünen isim)
COL_CLUB_CANONICAL = 2   # C → Kulüp Tekil (standart, tam isim)
COL_CITY           = 4   # E → Şehir
COL_REGION         = 6   # G → Bölge (1-6)

# Mapping sayfasında veriler 5. satırdan başlar (1. satır = başlık, 4. satır = sütun adları)
MAPPING_DATA_START_ROW = 5

# Çıktı klasörleri
OUTPUT_RACE_DIR  = os.path.join(BASE_DIR, "Çıktılar", "Yarış Sonuçları Çıktı")
OUTPUT_BOLGE_DIR = os.path.join(BASE_DIR, "Çıktılar", "Bölge Karmaları Sonuç Çıktı")

# ─────────────────────────────────────────────────────────────────────────────
# Bölgeler
# ─────────────────────────────────────────────────────────────────────────────

REGIONS: dict[int, str] = {
    1: "İstanbul",
    2: "Marmara",
    3: "Ege",
    4: "İç Anadolu",
    5: "Karadeniz",
    6: "Güneydoğu",
}

# ─────────────────────────────────────────────────────────────────────────────
# Excel çıktı yapısı
# ─────────────────────────────────────────────────────────────────────────────

BASE_COLUMNS = ["ad_soyad", "yb", "kulüp", "şehir", "bölge", "yaş", "cinsiyet"]
"""Sporcu bilgisi sütunları — her zaman ilk 7 sütun"""

FIXED_EVENT_COLUMNS = [
    # Serbest (6 mesafe)
    "Serbest_50m",   "Serbest_100m",  "Serbest_200m",
    "Serbest_400m",  "Serbest_800m",  "Serbest_1500m",
    # Sırtüstü (3 mesafe)
    "Sırtüstü_50m",  "Sırtüstü_100m", "Sırtüstü_200m",
    # Kurbağalama (3 mesafe)
    "Kurbağalama_50m", "Kurbağalama_100m", "Kurbağalama_200m",
    # Kelebek (3 mesafe)
    "Kelebek_50m",   "Kelebek_100m",  "Kelebek_200m",
    # Karışık (2 mesafe)
    "Karışık_200m",  "Karışık_400m",
]
"""17 yarış sütunu — Excel'de her zaman aynı sırada, yapılmayan yarış NaN kalır"""

ALL_COLUMNS = BASE_COLUMNS + FIXED_EVENT_COLUMNS
"""Toplam 24 sütun: 7 temel + 17 yarış"""

# ─────────────────────────────────────────────────────────────────────────────
# Stil isimleri (Lenex kodu → Türkçe)
# ─────────────────────────────────────────────────────────────────────────────

STROKE_MAP: dict[str, str] = {
    "FREE":   "Serbest",
    "BACK":   "Sırtüstü",
    "BREAST": "Kurbağalama",
    "FLY":    "Kelebek",
    "MEDLEY": "Karışık",
}

# Serbest → SERBEST, SERBEST → FREE gibi ters arama için
STROKE_TR_TO_EN: dict[str, str] = {v: k for k, v in STROKE_MAP.items()}

# ─────────────────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────────────────

DB_PATH = os.path.join(BASE_DIR, "data", "bolge_karmalari.db")
"""SQLite veritabanı dosya yolu. data/ klasörü yoksa otomatik oluşturulur."""

# ─────────────────────────────────────────────────────────────────────────────
# Canlı izleme
# ─────────────────────────────────────────────────────────────────────────────

LIVE_POLL_INTERVAL_SECONDS = 600  # 10 dakika

# ─────────────────────────────────────────────────────────────────────────────
# HTTP ayarları
# ─────────────────────────────────────────────────────────────────────────────

HTTP_TIMEOUT_LENEX = 15   # saniye
HTTP_TIMEOUT_PDF   = 30   # saniye
HTTP_TIMEOUT_HTML  = 20   # saniye

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
