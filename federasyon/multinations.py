"""
federasyon/multinations.py
--------------------------
2026 Multinations/Yıldız Milli Takım kadrosu.
Kaynak: https://dosya.tyf.gov.tr/public/upload/0/2026-03/2026MULTIYILDIZ.pdf

Bu sporcular:
  - Karma sıralamadan ÇIKARILIR
  - Listenin EN ÜSTÜNE "Multinations" badge'i ile yazılır
  - Kendi seçimlerini bölge kotasından DÜŞÜRÜR (aynı bölge için kotadan yer açar)
"""

# (İsim tam olarak DB'deki gibi olmalı — normalize_for_lookup ile eşleştiriyoruz)
MULTINATIONS_2026 = {
    # 2011 Kadın
    ("Ayşe Nazlı Sönmez",   2011, "F"),
    ("Alara Gökalp",         2012, "F"),   # 2012 doğumlu ama Multinations'da
    ("Ayşe Kent",            2011, "F"),
    ("Beril Çağrı",          2011, "F"),
    ("Cemre İnce",           2011, "F"),
    ("Damla Maviler",        2011, "F"),
    ("Derin Anbarlı",        2011, "F"),
    ("İpek Sözer",           2011, "F"),
    ("İpek Su Ersan",        2011, "F"),
    ("Kumsal Kandemir",      2012, "F"),   # 2012 doğumlu ama Multinations'da
    ("Yaren Soysal",         2011, "F"),
    # 2011 Erkek
    ("Emir Bartu Özcan",     2011, "M"),
    ("Kaan Akca",            2011, "M"),
    ("Sarp Canlı",           2011, "M"),
    ("Sarper Taze",          2011, "M"),
    ("Toprak Topatan",       2011, "M"),
    ("Tunç Uçan",            2011, "M"),
    ("Umut Aras Özkan",      2011, "M"),
    ("Yavuz Kaan Satır",     2011, "M"),
}

# Hızlı lookup için set: (name_lower, birth_year, gender)
_MULTI_SET = {
    (name.lower().strip(), by, g)
    for name, by, g in MULTINATIONS_2026
}


def is_multinations(name: str, birth_year: int, gender: str) -> bool:
    """DB'deki sporcu Multinations listesinde mi?"""
    return (name.lower().strip(), birth_year, gender) in _MULTI_SET


def multinations_names_by_group() -> dict:
    """(birth_year, gender) → set of names (lowercase)"""
    result = {}
    for name, by, g in MULTINATIONS_2026:
        result.setdefault((by, g), set()).add(name.lower().strip())
    return result
