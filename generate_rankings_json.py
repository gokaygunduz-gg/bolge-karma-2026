"""
generate_rankings_json.py
--------------------------
fed_athlete_best'ten sıralama hesaplar ve panel/results.json + panel/results_inline.js üretir.
Hem Antalya-sonrası hem Edirne-sırasında anlık kullanılabilir.

Çalıştırma:
  python generate_rankings_json.py
"""

import sys, os, json, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from federasyon.db_fed import load_athletes_for_ranking, load_athletes_for_ranking_by_leg, get_stats, get_event_winners, init_fed_db
from federasyon.ranker import rank_all, rank_group, format_seq_display, compute_club_rankings, compute_summary_matrix
from federasyon.scorer import best_scores_sequence
from federasyon.scoring_tables import SELECTION_QUOTAS, EXCEL_COL_TO_EVENT
from federasyon.validate import print_validation_report
from federasyon.multinations import is_multinations

OUT_DIR    = os.path.join(os.path.dirname(__file__), "panel")
OUT_JSON   = os.path.join(OUT_DIR, "results.json")
OUT_INLINE = os.path.join(OUT_DIR, "results_inline.js")

AGE_LABEL = {2013: "13 Yaş (2013)", 2012: "14 Yaş (2012)", 2011: "15 Yaş (2011)"}
STROKE_ABBR = {
    "Serbest": "SB", "Sırtüstü": "SR", "Kurbağalama": "KR",
    "Kelebek": "KL", "Karışık": "KA"
}
GENDER_LABEL = {"F": "Kadın", "M": "Erkek"}
SELECTION_LABEL = {
    "TR": "TR Kadro", "BÖLGE": "Bölge", "BARAJ_YOK": "Baraj Yok",
    "MULTINATIONS": "Multinations", "-": "-"
}


def event_label(stroke: str, distance: int) -> str:
    return f"{distance}m {stroke}"


def _top3_events_constrained(event_scores: dict) -> set:
    fifties     = sorted([(s,d,p) for (s,d),p in event_scores.items() if d==50 and p>0], key=lambda x:-x[2])
    non_fifties = sorted([(s,d,p) for (s,d),p in event_scores.items() if d!=50 and p>0], key=lambda x:-x[2])
    selected = []
    used_50  = False
    i = 0
    while len(selected) < 3:
        nf = non_fifties[i] if i < len(non_fifties) else None
        f  = fifties[0] if (not used_50 and fifties) else None
        nf_pts = nf[2] if nf else -1
        f_pts  = f[2]  if f  else -1
        if nf_pts < 0 and f_pts < 0:
            break
        if f_pts > nf_pts:
            selected.append((f[0], f[1]))
            used_50 = True
        else:
            if nf:
                selected.append((nf[0], nf[1]))
                i += 1
            else:
                break
    return set(selected)


def format_event_scores(event_scores: dict, seq: list) -> list:
    sorted_events = sorted(event_scores.items(), key=lambda x: -x[1])
    top3_events = _top3_events_constrained(event_scores)
    result = []
    for (stroke, dist), pts in sorted_events:
        result.append({
            "event":   event_label(stroke, dist),
            "stroke":  stroke,
            "dist":    dist,
            "points":  pts,
            "is_50m":  dist == 50,
            "in_top3": (stroke, dist) in top3_events,
        })
    return result


def build_group_data(athletes_ranked: list, by_leg_data: dict, event_winners: dict = None) -> list:
    rows = []
    for a in athletes_ranked:
        seq   = a.get("seq", [])
        top3  = sum(seq[:3])
        sel   = a.get("selected", "-")

        # Leg bazlı event scores
        key = (a["name"], a["birth_year"])
        leg_info = by_leg_data.get(key, {})
        antalya_scores = leg_info.get("antalya", {})
        edirne_scores  = leg_info.get("edirne",  {})

        # 1. bitiş tespiti: herhangi bir leg+stil+mesafede 1. mi?
        first_place_events = []
        if event_winners:
            gender = a["gender"]
            name   = a["name"]
            for (leg, g, stroke, dist), winner in event_winners.items():
                if g == gender and winner == name:
                    first_place_events.append({"leg": leg, "stroke": stroke, "dist": dist})

        rows.append({
            "rank":           a.get("tr_rank"),
            "name":           a["name"],
            "birth_year":     a["birth_year"],
            "gender":         a["gender"],
            "club":           a.get("club", ""),
            "city":           a.get("city", ""),
            "region":         a.get("region"),
            "top3":           top3,
            "seq_display":    format_seq_display(seq),
            "region_rank":    a.get("region_rank"),
            "selected":       sel,
            "selected_slot":  a.get("selected_slot", "-"),
            "selected_label": SELECTION_LABEL.get(sel, sel),
            "qualifies":      a.get("qualifies", False),
            "tied":           a.get("tied", False),
            "first_place":    first_place_events,  # [{leg, stroke, dist}, ...]
            "events":         format_event_scores(a.get("event_scores", {}), seq),
            # Leg bazlı puanlar (panel toggle için)
            "events_antalya": format_event_scores(antalya_scores, best_scores_sequence(antalya_scores)) if antalya_scores else [],
            "events_edirne":  format_event_scores(edirne_scores,  best_scores_sequence(edirne_scores))  if edirne_scores  else [],
            "top3_antalya":   sum(best_scores_sequence(antalya_scores)[:3]) if antalya_scores else 0,
            "top3_edirne":    sum(best_scores_sequence(edirne_scores)[:3])  if edirne_scores  else 0,
        })
    return rows


def main():
    init_fed_db()
    athletes      = load_athletes_for_ranking([2013, 2012, 2011])
    by_leg_data   = load_athletes_for_ranking_by_leg([2013, 2012, 2011])
    event_winners = get_event_winners([2013, 2012, 2011])

    if not athletes:
        print("DB bos! Once process_antalya_karma.py calistirin.")
        return

    ranked = rank_all(athletes)

    # ── Doğrulama ─────────────────────────────────────────────────────────────
    print("\n=== DOGRULAMA ===")
    non_multi = [a for a in ranked if a.get("selected") != "MULTINATIONS"]
    print_validation_report(non_multi)

    # ── Kulüp sıralaması ──────────────────────────────────────────────────────
    club_rankings = compute_club_rankings(ranked)

    # ── Özet matris ───────────────────────────────────────────────────────────
    summary_matrix = compute_summary_matrix(ranked)

    # ── JSON yap ─────────────────────────────────────────────────────────────
    output = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "legs":  get_stats().get("legs", ""),
        "groups": {},
        "club_rankings": club_rankings,
        "summary_matrix": summary_matrix,
    }

    from itertools import groupby
    ranked_sorted = sorted(ranked, key=lambda a: (a["birth_year"], a["gender"]))
    for (by, gender), group_iter in groupby(ranked_sorted, key=lambda a: (a["birth_year"], a["gender"])):
        group = list(group_iter)
        key = f"{by}_{gender}"
        quota = SELECTION_QUOTAS.get(by, {})

        multi_count = sum(1 for a in group if a.get("selected") == "MULTINATIONS")
        tr_count    = sum(1 for a in group if a.get("selected") == "TR")
        bolge_count = sum(1 for a in group if a.get("selected") == "BÖLGE")

        output["groups"][key] = {
            "birth_year":         by,
            "gender":             gender,
            "gender_label":       GENDER_LABEL[gender],
            "age_label":          AGE_LABEL.get(by, str(by)),
            "tr_quota":           quota.get("tr", 0),
            "region_1_quota":     quota.get("region_1", 0),
            "region_other_quota": quota.get("region_other", 0),
            "min_points":         quota.get("min_points", 7),
            "multi_count":        multi_count,
            "tr_count":           tr_count,
            "bolge_count":        bolge_count,
            "athletes":           build_group_data(group, by_leg_data, event_winners),
        }

    # ── Missing clubs raporu (telefonda bakılabilir) ──────────────────────────
    try:
        from modules.m4_mapping import get_missing_clubs
        missing = get_missing_clubs()
        if missing:
            missing_report = {
                "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "missing_clubs": sorted(missing),
                "count": len(missing),
                "_talimat": "Bu kulüpleri manual_overrides.json'a club_aliases olarak ekleyin"
            }
            report_path = os.path.join(os.path.dirname(__file__), "data", "missing_report.json")
            with open(report_path, "w", encoding="utf-8") as f:
                json.dump(missing_report, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    # ── Yaz ──────────────────────────────────────────────────────────────────
    os.makedirs(OUT_DIR, exist_ok=True)
    json_str = json.dumps(output, ensure_ascii=False, indent=2)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        f.write(json_str)
    with open(OUT_INLINE, "w", encoding="utf-8") as f:
        f.write(f"window.RESULTS_DATA = {json_str};\n")

    print(f"\n✓ {OUT_JSON} uretildi")
    print(f"✓ {OUT_INLINE} uretildi (file:// erisimi icin)")
    print(f"  Gruplar: {list(output['groups'].keys())}")
    print(f"  Veri zamani: {output['generated_at']}")

    # ── Özet ─────────────────────────────────────────────────────────────────
    print("\n=== SIRALAMA OZETI ===")
    for key, grp in sorted(output["groups"].items()):
        print(f"\n  {grp['age_label']} {grp['gender_label']}  "
              f"(TR:{grp['tr_quota']}, B1:{grp['region_1_quota']}, diger:{grp['region_other_quota']})")
        print(f"  Multi:{grp['multi_count']}  TR:{grp['tr_count']}  Bolge:{grp['bolge_count']}")
        top5 = [a for a in grp["athletes"] if a.get("selected") not in ("MULTINATIONS",) and a["rank"] and a["rank"] <= 5]
        for a in top5:
            tied_mark = " ★" if a.get("tied") else ""
            print(f"    #{a['rank']:3d} {a['name']:30s} top3={a['top3']:2d}  {a['seq_display']:15s}  {a['selected']}{tied_mark}")

    # ── Kulüp özeti ───────────────────────────────────────────────────────────
    print("\n=== KULUP SIRALAMASI (toplam 5+) ===")
    for c in club_rankings["overall"]:
        if c["total"] >= 3:
            print(f"  {c['club'][:35]:35s} Multi:{c['multi']} TR:{c['tr']} Bolge:{c['bolge']} = {c['total']}")


if __name__ == "__main__":
    main()
