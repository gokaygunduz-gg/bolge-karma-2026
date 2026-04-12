"""
federasyon/ranker.py
--------------------
TR ve bölge sıralaması + seçim kararı.

Tie kuralı:
  - Sadece kota sınırındaki son seçilenle birebir AYNI ranking_key'e sahip,
    kotadan dışarıda kalan sporcu varsa TÜM o key sahipleri tied=True olur.
  - Başka rastgele eşitlikler işaretlenmez.
"""

from itertools import groupby as _groupby
from .scorer import best_scores_sequence, compute_ranking_key
from .scoring_tables import SELECTION_QUOTAS
from .multinations import is_multinations


def _select_with_tie(lst: list[dict], quota: int,
                     sel_value: str, slot_prefix: str,
                     slot_field: str = "selected_slot"):
    """
    lst: bölge sıralamasına göre sıralı (ranking_key ascending) sporcu listesi.
    quota: kaç kişi alınacak.

    Kural:
      1. İlk `quota` kişiyi seç.
      2. Kota sınırındaki son seçilenin key'i (boundary_key) ile
         hemen sonrasında aynı key'e sahip sporcu var mı?
         Varsa → o sporcuları da seç ve boundary_key sahiplerini tied=True yap.
         Yoksa → tied yok.
    """
    if not lst or quota <= 0:
        return

    # Kaç tane seçilebilir (top3>0 veya qualifies)?
    eligible = lst  # caller zaten qualifies filtresinden geçirdi

    effective_quota = min(quota, len(eligible))
    boundary_key = eligible[effective_quota - 1]["ranking_key"]

    # Sınırın dışında aynı key var mı?
    has_tie = any(a["ranking_key"] == boundary_key
                  for a in eligible[effective_quota:])

    slot = 0
    for a in eligible:
        in_quota = slot < quota
        at_boundary = a["ranking_key"] == boundary_key
        if in_quota or at_boundary:
            slot += 1
            a["selected"]    = sel_value
            a[slot_field]    = f"{slot_prefix}{slot}"
            if has_tie and at_boundary:
                a["tied"] = True
        else:
            break   # listede sıralı olduğu için sonrası daha kötü


def rank_group(athletes: list[dict]) -> list[dict]:
    if not athletes:
        return []

    by     = athletes[0]["birth_year"]
    quotas = SELECTION_QUOTAS.get(by, {"tr": 0, "region_1": 0, "region_other": 0, "min_points": 7})
    min_pts = quotas["min_points"]

    # ── Multinations / normal ayır ────────────────────────────────────────────
    multi_athletes  = []
    normal_athletes = []
    for a in athletes:
        if is_multinations(a["name"], a["birth_year"], a.get("gender", "")):
            multi_athletes.append(a)
        else:
            normal_athletes.append(a)

    # ── Normal: puan dizisi hesapla ───────────────────────────────────────────
    for a in normal_athletes:
        es = a.get("event_scores", {})
        a["seq"]         = best_scores_sequence(es)
        a["top3_total"]  = sum(a["seq"][:3])
        a["ranking_key"] = compute_ranking_key(es)
        a["qualifies"]   = a["top3_total"] >= min_pts
        a["tied"]        = False

    sorted_normal = sorted(normal_athletes, key=lambda a: a["ranking_key"])
    for i, a in enumerate(sorted_normal):
        a["tr_rank"]       = i + 1
        a["selected"]      = "-"
        a["selected_slot"] = "-"
        a["region_rank"]   = None

    # ── TR seçimi ────────────────────────────────────────────────────────────
    eligible_tr = [a for a in sorted_normal if a["top3_total"] > 0]
    _select_with_tie(eligible_tr, quotas["tr"], "TR", "TR-", "selected_slot")

    # ── Bölge sıralaması ──────────────────────────────────────────────────────
    remaining = [a for a in sorted_normal if a["selected"] == "-"]

    by_region: dict[int, list[dict]] = {}
    for a in remaining:
        if a["qualifies"]:
            r = a.get("region") or 0
            by_region.setdefault(r, []).append(a)

    for r, lst in by_region.items():
        lst.sort(key=lambda a: a["ranking_key"])
        for i, a in enumerate(lst):
            a["region_rank"] = i + 1

    for r in range(1, 7):
        quota = quotas["region_1"] if r == 1 else quotas["region_other"]
        lst = by_region.get(r, [])
        _select_with_tie(lst, quota, "BÖLGE", f"B{r}-", "selected_slot")

    for a in remaining:
        if not a["qualifies"]:
            a["selected"] = "BARAJ_YOK"

    # ── Multinations ─────────────────────────────────────────────────────────
    for a in multi_athletes:
        es = a.get("event_scores", {})
        a["seq"]           = best_scores_sequence(es)
        a["top3_total"]    = sum(a["seq"][:3])
        a["ranking_key"]   = compute_ranking_key(es)
        a["qualifies"]     = True
        a["tr_rank"]       = 0
        a["selected"]      = "MULTINATIONS"
        a["selected_slot"] = "MULTI"
        a["region_rank"]   = None
        a["tied"]          = False

    multi_athletes.sort(key=lambda a: a["ranking_key"])
    return multi_athletes + sorted_normal


def rank_all(athletes: list[dict]) -> list[dict]:
    groups: dict[tuple, list] = {}
    for a in athletes:
        key = (a["birth_year"], a["gender"])
        groups.setdefault(key, []).append(a)
    results = []
    for key, group in sorted(groups.items()):
        results.extend(rank_group(group))
    return results


def format_seq_display(seq: list[int], top_n: int = 6) -> str:
    if not seq:
        return "0"
    top3 = seq[:3]
    rest = seq[3:top_n]
    s = "+".join(str(p) for p in top3)
    if rest:
        s += " | " + " ".join(str(p) for p in rest)
    return s


def compute_club_rankings(ranked: list[dict]) -> dict:
    def tally(athletes):
        clubs: dict[str, dict] = {}
        for a in athletes:
            club = (a.get("club") or "").strip() or "Ferdi/Bilinmiyor"
            if club not in clubs:
                clubs[club] = {"club": club, "multi": 0, "tr": 0, "bolge": 0, "total": 0}
            sel = a.get("selected", "-")
            if sel == "MULTINATIONS":  clubs[club]["multi"] += 1
            elif sel == "TR":          clubs[club]["tr"]    += 1
            elif sel == "BÖLGE":       clubs[club]["bolge"] += 1
            else:                      continue
            clubs[club]["total"] += 1
        return sorted(clubs.values(), key=lambda x: -x["total"])

    overall  = tally(ranked)
    by_group = {}
    ranked_s = sorted(ranked, key=lambda a: (a["birth_year"], a["gender"]))
    for (by, gender), grp_iter in _groupby(ranked_s, key=lambda a: (a["birth_year"], a["gender"])):
        key = f"{by}_{'F' if gender=='F' else 'M'}"
        by_group[key] = tally(list(grp_iter))
    return {"overall": overall, "by_group": by_group}


def compute_summary_matrix(ranked: list[dict]) -> list[dict]:
    groups: dict[tuple, dict] = {}
    for a in ranked:
        key = (a["birth_year"], a["gender"])
        if key not in groups:
            groups[key] = {"birth_year": a["birth_year"], "gender": a["gender"],
                           "multi": 0, "tr": 0, "bolge": 0}
        sel = a.get("selected", "-")
        if sel == "MULTINATIONS":  groups[key]["multi"] += 1
        elif sel == "TR":          groups[key]["tr"]    += 1
        elif sel == "BÖLGE":       groups[key]["bolge"] += 1
    result = []
    for key in sorted(groups.keys()):
        g = groups[key]
        g["total"] = g["multi"] + g["tr"] + g["bolge"]
        result.append(g)
    return result
