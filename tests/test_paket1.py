"""
tests/test_paket1.py — m1_normalize, m3_age, m4_mapping testleri

Çalıştırma:
  cd "C:/Users/Gokay/Desktop/Claude/Bölge Karmaları 2026"
  python tests/test_paket1.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.m1_normalize import normalize_for_lookup, normalize_display, names_match, club_names_match
from modules.m3_age import yb_to_birth_year, calc_age, yb_to_age, parse_birthdate
from modules.m4_mapping import lookup_club, get_missing_clubs, mapping_stats, report_missing_clubs

PASS = "✓"
FAIL = "✗"
results = []

def check(label, got, expected):
    ok = got == expected
    mark = PASS if ok else FAIL
    results.append(ok)
    print(f"  {mark}  {label}")
    if not ok:
        print(f"       Beklenen : {repr(expected)}")
        print(f"       Gelen    : {repr(got)}")


# ─────────────────────────────────────────────────────────────────────────────
print("\n── m1_normalize: Türkçe karakter normalizasyonu ─────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

check("ş → s",
      normalize_for_lookup("Şahin"),
      "sahin")

check("ğ → g",
      normalize_for_lookup("Ağaoğlu"),
      "agaoglu")

check("İ → i (Türkçe büyük İ)",
      normalize_for_lookup("İSMAİL"),
      "ismail")

check("ı → i (Türkçe küçük ı)",
      normalize_for_lookup("Kılıç"),
      "kilic")

check("ü → u",
      normalize_for_lookup("Gündüz"),
      "gunduz")

check("ö → o",
      normalize_for_lookup("Gökay"),
      "gokay")

check("ç → c",
      normalize_for_lookup("Çelik"),
      "celik")

check("Uzatma işareti: â → a",
      normalize_for_lookup("Hâki"),
      "haki")

check("Tam isim normalizasyonu",
      normalize_for_lookup("İsmail Hâki Çakar"),
      "ismail haki cakar")

check("Büyük harf tam isim",
      normalize_for_lookup("AHMET YILMAZ"),
      "ahmet yilmaz")

# ─────────────────────────────────────────────────────────────────────────────
print("\n── m1_normalize: Kısaltma açma ──────────────────────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

check("SK → spor kulubu",
      normalize_for_lookup("Ankara SK"),
      "ankara spor kulubu")

check("S.K. → spor kulubu",
      normalize_for_lookup("Ankara S.K."),
      "ankara spor kulubu")

check("Spor Kul. → spor kulubu",
      normalize_for_lookup("Ankara Spor Kul."),
      "ankara spor kulubu")

check("BB → buyuksehir belediyesi",
      normalize_for_lookup("Ankara BB"),
      "ankara buyuksehir belediyesi")

check("Bel. → belediyesi",
      normalize_for_lookup("Ankara Bel. SK"),
      "ankara belediyesi spor kulubu")

check("YK → yuzme kulubu",
      normalize_for_lookup("İstanbul YK"),
      "istanbul yuzme kulubu")

# ─────────────────────────────────────────────────────────────────────────────
print("\n── m1_normalize: names_match ve club_names_match ────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

check("İsmail = ismail = ISMAIL",
      names_match("İsmail Çelik", "ISMAIL CELIK"),
      True)

check("Hâki = Haki",
      names_match("İsmail Hâki", "Ismail Haki"),
      True)

check("Farklı isimler → False",
      names_match("Ahmet", "Mehmet"),
      False)

check("Kulüp kısaltma eşleştirme",
      club_names_match("Ankara SK", "Ankara Spor Kulübü"),
      True)

check("normalize_display: Title Case",
      normalize_display("AHMET ÇELİK"),
      "Ahmet Çelik")

# ─────────────────────────────────────────────────────────────────────────────
print("\n── m3_age: yb_to_birth_year ─────────────────────────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

check("YB=13 → 2013",    yb_to_birth_year(13),  2013)
check("YB=0  → 2000",    yb_to_birth_year(0),   2000)
check("YB=26 → 2026",    yb_to_birth_year(26),  2026)
check("YB=27 → 1927",    yb_to_birth_year(27),  1927)
check("YB=99 → 1999",    yb_to_birth_year(99),  1999)
check("YB=98 → 1998",    yb_to_birth_year(98),  1998)
check("YB='13' string",  yb_to_birth_year("13"), 2013)
check("YB=None → None",  yb_to_birth_year(None), None)
check("YB='abc' → None", yb_to_birth_year("abc"), None)

# ─────────────────────────────────────────────────────────────────────────────
print("\n── m3_age: calc_age ve yb_to_age ────────────────────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

check("calc_age(2013) → 13",  calc_age(2013), 13)
check("calc_age(2012) → 14",  calc_age(2012), 14)
check("calc_age(1999) → 27",  calc_age(1999), 27)
check("calc_age(2000) → 26",  calc_age(2000), 26)
check("calc_age(None) → None",calc_age(None),  None)
check("yb_to_age(13) → 13",  yb_to_age(13),   13)
check("yb_to_age(99) → 27",  yb_to_age(99),   27)
check("yb_to_age(0)  → 26",  yb_to_age(0),    26)

# ─────────────────────────────────────────────────────────────────────────────
print("\n── m3_age: Lenex doğum tarihi ───────────────────────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

check("parse_birthdate('2013-05-15') → 2013", parse_birthdate("2013-05-15"), 2013)
check("parse_birthdate('1999-01-01') → 1999", parse_birthdate("1999-01-01"), 1999)
check("parse_birthdate(None) → None",         parse_birthdate(None),         None)
check("parse_birthdate('hatalı') → None",     parse_birthdate("hatalı"),     None)

# ─────────────────────────────────────────────────────────────────────────────
print("\n── m4_mapping: Kulüp Excel arama ────────────────────────────────────")
# ─────────────────────────────────────────────────────────────────────────────

stats = mapping_stats()
print(f"  → Mapping yüklendi: {stats.get('total_keys', 0)} anahtar")
print(f"  → Bölge dağılımı: {stats.get('by_region', {})}")

# Bilinen birkaç kulüp — gerçek Excel'de olması bekleniyor
test_clubs = [
    "BAHÇEŞEHİR KÜLTÜR S.K.",
    "Bahçeşehir Kültür Yüzme Kulübü",
    "VAMOS SPOR KULÜBÜ",
    "Vamos Spor Kulübü",
]

print()
for club in test_clubs:
    result = lookup_club(club)
    if result:
        print(f"  {PASS}  '{club}'")
        print(f"       → {result['city']} / Bölge {result['region']} / {result['club_canonical']}")
    else:
        print(f"  {FAIL}  '{club}' → BULUNAMADI")

# Bilinmeyen kulüp testi
unknown = lookup_club("TAMAMEN BİLİNMEYEN KULÜP XYZ")
check("Bilinmeyen kulüp → None", unknown, None)
check("Missing list'te görünüyor", "TAMAMEN BİLİNMEYEN KULÜP XYZ" in get_missing_clubs(), True)

# ─────────────────────────────────────────────────────────────────────────────
# Özet
# ─────────────────────────────────────────────────────────────────────────────
print()
print(f"{'─'*60}")
passed = sum(results)
total  = len(results)
print(f"Sonuç: {passed}/{total} test geçti.")
if passed < total:
    print("⚠️  Bazı testler başarısız — yukarıdaki hataları kontrol et.")
else:
    print("✓ Tüm testler geçti.")
print()
