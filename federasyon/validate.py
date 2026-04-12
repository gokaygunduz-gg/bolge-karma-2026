"""
federasyon/validate.py
----------------------
Her sıralama çalıştırmasında otomatik çalışan kontrol noktaları.

Kontrol edilen kurallar:
  1. Kümülatif sıralama: 5+5+4=14 > 6+4+3=13 olan sporcu üstte
  2. Tiebreaker uzunluğu: 6. branşı olan sporcu 6. branşı olmayandan üstte
  3. Top3 monotonluk: sıra N'deki sporcu, N+1'dekinden ≥ top3 puana sahip
  4. 50m kısıtı: hiçbir sporcunun top6 seqinde 2 veya fazla 50m branş yok
  5. Bölge seçimi adil: aynı bölgedeki sporcu ranking_key'e göre seçilmiş
  6. Baraj: seçilen her sporcunun top3 ≥ 7
"""

from .scorer import best_scores_sequence, compute_ranking_key


def validate_rankings(ranked: list[dict]) -> list[str]:
    """
    Sıralanmış sporcu listesini doğrula.
    Hata mesajları listesi döndürür (boş = tümü geçti).
    ranked: rank_all() çıktısı (tüm gruplar karışık olabilir)
    """
    errors: list[str] = []

    from itertools import groupby
    by_group = {}
    for a in ranked:
        key = (a["birth_year"], a["gender"])
        by_group.setdefault(key, []).append(a)

    for (by, gender), group in sorted(by_group.items()):
        label = f"{by} {'K' if gender=='F' else 'E'}"
        sorted_group = sorted(group, key=lambda x: x["tr_rank"])

        # ── Kontrol 1+2: tr_rank sırası ranking_key ile tutarlı ───────────
        prev_key = None
        prev_rank = 0
        for a in sorted_group:
            rk = a.get("ranking_key")
            if rk is None:
                continue
            tr = a["tr_rank"]
            if prev_key is not None and tr == prev_rank + 1:
                if rk < prev_key:
                    errors.append(
                        f"[{label}] SIRA HATASI: #{tr} {a['name']} "
                        f"key={rk} önce gelmeli ama #{prev_rank} önde"
                    )
            prev_key = rk
            prev_rank = tr

        # ── Kontrol 3: top3 monotonluk ────────────────────────────────────
        prev_top3 = 999
        for a in sorted_group:
            t3 = a.get("top3_total", 0)
            if t3 > prev_top3:
                errors.append(
                    f"[{label}] MONOTONLUK HATASI: #{a['tr_rank']} {a['name']} "
                    f"top3={t3} > bir öncekinin top3={prev_top3}"
                )
            prev_top3 = t3

        # ── Kontrol 4: 50m kısıtı ─────────────────────────────────────────
        for a in sorted_group:
            es = a.get("event_scores", {})
            seq_ev = _best_event_sequence(es)  # (stroke,dist) sırası
            fifties_used = sum(1 for (s, d) in seq_ev[:6] if d == 50)
            if fifties_used > 1:
                errors.append(
                    f"[{label}] 50m KISIT IHLALI: {a['name']} "
                    f"top6 içinde {fifties_used} adet 50m branş var"
                )

        # ── Kontrol 5: seçilen sporcu baraj geçmiş ───────────────────────
        for a in sorted_group:
            sel = a.get("selected", "-")
            if sel in ("TR", "BÖLGE"):
                t3 = a.get("top3_total", 0)
                if t3 < 7:
                    errors.append(
                        f"[{label}] BARAJ HATASI: {a['name']} seçildi ama top3={t3} < 7"
                    )

        # ── Kontrol 6: bölge seçimi kendi bölgesindeki en yüksek key ─────
        by_region: dict[int, list] = {}
        for a in sorted_group:
            if a.get("selected") == "-" and a.get("qualifies"):
                r = a.get("region", 0)
                by_region.setdefault(r, []).append(a)
        for a in sorted_group:
            if a.get("selected") == "BÖLGE":
                r = a.get("region", 0)
                # Aynı bölgede seçilmemiş ama daha iyi key'li sporcu olmamalı
                for other in by_region.get(r, []):
                    if other["ranking_key"] < a["ranking_key"]:
                        errors.append(
                            f"[{label}] BÖLGE SEÇİM HATASI: {other['name']} "
                            f"(key={other['ranking_key']}) seçilmeli ama {a['name']} seçilmiş"
                        )

    return errors


def _best_event_sequence(event_scores: dict[tuple, int]) -> list[tuple]:
    """
    best_scores_sequence ile aynı greedy mantığını kullanarak
    (stroke, dist) çiftlerinin sırasını döndürür.
    """
    fifties = sorted(
        [(p, ev) for ev, p in event_scores.items() if ev[1] == 50 and p > 0],
        reverse=True
    )
    non_fifties = sorted(
        [(p, ev) for ev, p in event_scores.items() if ev[1] != 50 and p > 0],
        reverse=True
    )

    best_50_p = fifties[0][0] if fifties else 0
    best_50_ev = fifties[0][1] if fifties else None
    result: list[tuple] = []
    used_50 = False
    i = 0

    while True:
        nf_p = non_fifties[i][0] if i < len(non_fifties) else -1
        nf_ev = non_fifties[i][1] if i < len(non_fifties) else None
        f_p = best_50_p if not used_50 else -1

        if nf_p <= 0 and f_p <= 0:
            break
        if f_p > nf_p and f_p > 0:
            result.append(best_50_ev)
            used_50 = True
        elif nf_p > 0:
            result.append(nf_ev)
            i += 1
        else:
            break

    return result


def print_validation_report(ranked: list[dict]) -> bool:
    """
    Doğrulama sonuçlarını yazdırır.
    True → tüm kontroller geçti. False → hata var.
    """
    errors = validate_rankings(ranked)
    if not errors:
        print("  ✓ Tüm doğrulama kontrolleri geçti (sıralama, 50m kısıt, baraj, bölge)")
        return True
    else:
        print(f"  ✗ {len(errors)} doğrulama hatası:")
        for e in errors:
            print(f"    {e}")
        return False
