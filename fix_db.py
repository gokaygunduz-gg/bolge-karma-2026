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
c.execute("UPDATE fed_results SET region=4, city='Ankara' WHERE athlete_name='Zeynep Kaya' AND birth_year=2013 AND region IS NULL")
c.execute("UPDATE fed_athlete_best SET region=4, city='Ankara' WHERE athlete_name='Zeynep Kaya' AND birth_year=2013 AND region IS NULL")

c.commit()
c.close()
print("DB fix tamamlandi")
