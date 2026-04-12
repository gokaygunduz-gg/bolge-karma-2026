"""
m3_age.py — Yaş ve Doğum Yılı Hesaplama

Çözdüğü sorunlar:
  - Yarış sonuçlarında YB (Yıl Bilgisi) 2 haneli gelir:
    2013 doğumlu → 13, 2000 doğumlu → 00, 1999 doğumlu → 99
  - 4 haneli doğum yılına çevirmek için kesim noktası gerekir.
  - Yaş hesabı: COMPETITION_YEAR − doğum_yılı  (2026 sabit)
  - Lenex dosyalarında doğum tarihi tam gelir: "2013-05-15" formatında.

Kural:
  Yarış 2025'te yapılmış olsa bile 2026 Bölge Karmaları için hesap yapıyoruz.
  Dolayısıyla age = COMPETITION_YEAR − birth_year her zaman.

Kullanım:
  from modules.m3_age import yb_to_birth_year, calc_age, parse_birthdate

  birth_year = yb_to_birth_year(13)    → 2013
  birth_year = yb_to_birth_year(99)    → 1999
  age        = calc_age(2013)          → 13   (2026 - 2013)
  birth_year = parse_birthdate("2013-05-15")  → 2013
"""

from config import COMPETITION_YEAR, YB_CENTURY_CUTOFF


# ─────────────────────────────────────────────────────────────────────────────
# 1. 2 haneli YB → 4 haneli doğum yılı
# ─────────────────────────────────────────────────────────────────────────────

def yb_to_birth_year(yb: int | str | float | None) -> int | None:
    """
    2 haneli YB değerini 4 haneli doğum yılına çevirir.

    Kural (COMPETITION_YEAR = 2026):
      YB  0 – 26  →  2000 – 2026
      YB 27 – 99  →  1927 – 1999

    Örnekler:
      yb_to_birth_year(13)  → 2013
      yb_to_birth_year(0)   → 2000
      yb_to_birth_year(26)  → 2026
      yb_to_birth_year(27)  → 1927
      yb_to_birth_year(99)  → 1999
      yb_to_birth_year(98)  → 1998

    Hata durumları:
      yb_to_birth_year(None)  → None
      yb_to_birth_year("abc") → None
    """
    if yb is None:
        return None

    try:
        yb_int = int(float(yb))  # "13", 13.0, 13 hepsini kabul et
    except (ValueError, TypeError):
        return None

    if not (0 <= yb_int <= 99):
        return None  # Geçersiz aralık

    if yb_int <= YB_CENTURY_CUTOFF:
        return 2000 + yb_int
    else:
        return 1900 + yb_int


# ─────────────────────────────────────────────────────────────────────────────
# 2. Doğum yılından yaş hesaplama
# ─────────────────────────────────────────────────────────────────────────────

def calc_age(birth_year: int | None) -> int | None:
    """
    Doğum yılından bölge karmaları yaşını hesaplar.

    Formül: COMPETITION_YEAR − birth_year (config.py'den gelir, sabit 2026)

    Örnekler (COMPETITION_YEAR=2026):
      calc_age(2013) → 13
      calc_age(2012) → 14
      calc_age(1999) → 27
      calc_age(2000) → 26
      calc_age(None) → None
    """
    if birth_year is None:
        return None
    return COMPETITION_YEAR - birth_year


def yb_to_age(yb: int | str | float | None) -> int | None:
    """
    2 haneli YB → yaş (yb_to_birth_year + calc_age birleşimi).

    Örnekler:
      yb_to_age(13) → 13
      yb_to_age(99) → 27
      yb_to_age(0)  → 26
    """
    return calc_age(yb_to_birth_year(yb))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Lenex tam tarihten doğum yılı çıkarma
# ─────────────────────────────────────────────────────────────────────────────

def parse_birthdate(birthdate: str | None) -> int | None:
    """
    Lenex formatındaki tam doğum tarihinden doğum yılını çıkarır.

    Beklenen format: "YYYY-MM-DD"  (örn: "2013-05-15")
    Sadece yılı döndürür.

    Örnekler:
      parse_birthdate("2013-05-15") → 2013
      parse_birthdate("1999-01-01") → 1999
      parse_birthdate(None)         → None
      parse_birthdate("bilinmiyor") → None
    """
    if not birthdate:
        return None

    try:
        year_str = str(birthdate).split("-")[0]
        year = int(year_str)
        if 1900 <= year <= COMPETITION_YEAR:
            return year
        return None
    except (ValueError, IndexError):
        return None


def parse_birthdate_to_age(birthdate: str | None) -> int | None:
    """
    Lenex tam tarihinden doğrudan bölge karmaları yaşını hesaplar.

    Örnek: parse_birthdate_to_age("2013-05-15") → 13
    """
    return calc_age(parse_birthdate(birthdate))


# ─────────────────────────────────────────────────────────────────────────────
# 4. Yaş kategorisi
# ─────────────────────────────────────────────────────────────────────────────

def get_age_category(age: int | None) -> str | None:
    """
    Yaştan yarışma kategorisini döndürür.
    Kriterler PDF'e göre güncellenecek (Modül 5 yapılırken).

    Şu an basit numerik kategori; Modül 5'te genişletilecek.
    """
    if age is None:
        return None
    return str(age)
