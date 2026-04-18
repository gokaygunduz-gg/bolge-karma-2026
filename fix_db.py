import sqlite3, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from modules.m1_normalize import normalize_for_lookup

db = 'data/bolge_karmalari.db'
if not os.path.exists(db):
    print("DB yok, atlandi")
    exit(0)

c = sqlite3.connect(db)
c.row_factory = sqlite3.Row

# i/i encoding duzeltmesi
rows = c.execute("SELECT DISTINCT athlete_name, birth_year FROM fed_results").fetchall()
fixes = []
for r in rows:
    norm = normalize_for_lookup(r["athlete_name"])
    canonical = c.execute(
        "SELECT DISTINCT athlete_name FROM fed_results WHERE birth_year=?",
        (r["birth_year"],)
    ).fetchall()
    for other in canonical:
        if other["athlete_name"] != r["athlete_name"] and normalize_for_lookup(other["athlete_name"]) == norm:
            fixes.append((other["athlete_name"], r["athlete_name"], r["birth_year"]))
            break

for canonical_name, wrong_name, by in fixes:
    c.execute("UPDATE fed_results SET athlete_name=? WHERE athlete_name=? AND birth_year=?", (canonical_name, wrong_name, by))
    c.execute("UPDATE fed_athlete_best SET athlete_name=? WHERE athlete_name=? AND birth_year=?", (canonical_name, wrong_name, by))
    print(f"Duzeltildi: {wrong_name} -> {canonical_name}")

# Bolge null duzeltmesi
null_fixes = [
    ("Uras Ozan Türkyılmaz", 1, "İstanbul"),
    ("Tusem Anastasiya Aşkar", 6, "Antalya"),
    ("Zeynep Kaya", 4, "Ankara"),
    ("Defne Ide", 4, "Ankara"),
    ("Uras Güneş", 3, "İzmir"),
    ("Egemen Karakuş", 2, "Bursa"),
]
for name, region, city in null_fixes:
    c.execute("UPDATE fed_results SET region=?, city=? WHERE athlete_name=?", (region, city, name))
    c.execute("UPDATE fed_athlete_best SET region=?, city=? WHERE athlete_name=?", (region, city, name))
    print(f"Bolge duzeltildi: {name} -> B{region} {city}")
c.commit()
c.close()
print("DB fix tamamlandi")
