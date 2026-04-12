"""
federasyon/scorer.py
--------------------
Puanlama motoru: süre → puan, sporcu başına en iyi sıralama dizisi.

Kural özeti:
  1. Her (stil, mesafe) için süre → puan (0-9, 8 yok)
  2. Antalya ve Edirne'den aynı branşta en iyi puan tutulur
  3. Puanlanan branşların en iyisi 3'ü toplanır (max 1 adet 50m içeren)
  4. Eşitlikte 4., 5., 6. ... branş puanları tiebreaker (aynı kısıt)
  5. Toplam ≥ 7 → bölge seçimi için "baraj geçti"
"""

import re
from typing import Optional
from .scoring_tables import TABLES, POINTS, EXCEL_COL_TO_EVENT


# ─────────────────────────────────────────────────────────────────────────────
# Süre parse
# ─────────────────────────────────────────────────────────────────────────────

def parse_time(s: str) -> Optional[float]:
    """
    Çeşitli formatlarda zaman → saniye (float) veya None.
    '29.86'       → 29.86
    '1:03.42'     → 63.42
    '2:38.04'     → 158.04
    '21:21.66'    → 1281.66
    """
    if not s or not isinstance(s, str):
        return None
    s = s.strip().replace(",", ".")
    if not s:
        return None
    # sadece rakam/nokta/virgül/iki nokta
    if not re.match(r"^\d[\d:.]*$", s):
        return None
    try:
        if ":" in s:
            parts = s.split(":")
            if len(parts) == 2:
                return int(parts[0]) * 60 + float(parts[1])
            if len(parts) == 3:
                return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        return float(s)
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Tek branş puanı
# ─────────────────────────────────────────────────────────────────────────────

def score_event(time_seconds: float, birth_year: int, gender: str,
                stroke: str, distance: int) -> int:
    """
    Verilen süreye (saniye) karşılık gelen puanı döndürür (0-9, 8 yok).
    0 → baraj geçilemedi.
    """
    table = TABLES.get(birth_year, {}).get(gender, {})
    thresholds = table.get((stroke, distance))
    if not thresholds:
        return 0

    # Puan listesini büyükten küçüğe tara, ilk eşiği geçeni bul
    for p in sorted(POINTS, reverse=True):
        if time_seconds <= thresholds[p]:
            return p
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# Sporcu başına tüm branş puanları (Excel satırından)
# ─────────────────────────────────────────────────────────────────────────────

def score_athlete_row(row: dict, birth_year: int, gender: str) -> dict[tuple, int]:
    """
    Excel satırından {(stroke, distance): puan} dict'i üretir.
    Sadece puan > 0 olan branşlar dahil edilir.
    row: {'Serbest_50m': '29.86', 'Serbest_100m': None, ...}
    """
    scores: dict[tuple, int] = {}
    for col, event in EXCEL_COL_TO_EVENT.items():
        time_str = row.get(col)
        if not time_str:
            continue
        t = parse_time(time_str)
        if t is None or t <= 0:
            continue
        p = score_event(t, birth_year, gender, event[0], event[1])
        if p > 0:
            scores[event] = p
    return scores


# ─────────────────────────────────────────────────────────────────────────────
# İki yarışın (Antalya + Edirne) puanlarını birleştir: her branşta en iyisi
# ─────────────────────────────────────────────────────────────────────────────

def merge_scores(scores_a: dict[tuple, int],
                 scores_b: dict[tuple, int]) -> dict[tuple, int]:
    """
    İki yarış sonucunu birleştir; her branşta en yüksek puan kalır.
    """
    merged = dict(scores_a)
    for event, pts in scores_b.items():
        if pts > merged.get(event, 0):
            merged[event] = pts
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# En iyi sıralama dizisi (kısıtlı greedy)
# ─────────────────────────────────────────────────────────────────────────────

def best_scores_sequence(event_scores: dict[tuple, int]) -> list[int]:
    """
    Tüm puanlanan branşlardan, "max 1 adet 50m" kısıtıyla
    en yüksekten en düşüğe sıralanmış puan listesi.

    Bu dizi sıralama için kullanılır:
      - top3_total = sum(seq[:3])
      - tiebreaker: seq[3], seq[4], ...

    Algoritma (greedy):
      - 50m branşları ayrı, 50m olmayan branşlar ayrı sıralanır
      - 50m listesinden sadece en iyisi kullanılabilir
      - Her adımda: bir sonraki 50m ile bir sonraki non-50m karşılaştır,
        büyük olanı al (50m kotası aşılmadan)
    """
    fifties = sorted(
        [p for (s, d), p in event_scores.items() if d == 50 and p > 0],
        reverse=True
    )
    non_fifties = sorted(
        [p for (s, d), p in event_scores.items() if d != 50 and p > 0],
        reverse=True
    )

    best_50 = fifties[0] if fifties else 0
    result: list[int] = []
    used_50 = False
    i = 0

    while True:
        nf = non_fifties[i] if i < len(non_fifties) else -1
        f  = best_50 if not used_50 else -1

        if nf < 0 and f < 0:
            break
        if f > nf:
            result.append(f)
            used_50 = True
        else:
            result.append(nf)
            i += 1

    return result


def compute_top3_total(event_scores: dict[tuple, int]) -> int:
    """En iyi 3 branş toplamı (50m kısıtıyla)."""
    seq = best_scores_sequence(event_scores)
    return sum(seq[:3])


_MAX_TIEBREAKER = 6   # toplam 6 branşa kadar tiebreaker bakılır


def compute_ranking_key(event_scores: dict[tuple, int]) -> tuple:
    """
    Sıralama anahtarı: kümülatif toplamlar (-top3, -top4, -top5, -top6).

    Federasyon kuralı (madde 8):
      - Önce top3 toplamına göre sırala (büyük = iyi)
      - Eşitlikte top4 toplamına bak, sonra top5, top6

    Neden lexicographic DEĞİL:
      - seq=[6,4,3] → leksik key (-6,-4,-3), seq=[5,5,4] → (-5,-5,-4)
        leksik olarak -6 < -5 → [6,4,3] daha iyi görünür, oysa 5+5+4=14 > 6+4+3=13
      - Kümülatif: top3(6+4+3)=13 vs top3(5+5+4)=14 → [5,5,4] doğru kazanır
    """
    seq = best_scores_sequence(event_scores)
    padded = seq[:_MAX_TIEBREAKER] + [0] * max(0, _MAX_TIEBREAKER - len(seq))
    cumul_key = []
    running = 0
    for i in range(_MAX_TIEBREAKER):
        running += padded[i]
        if i >= 2:   # top3'ten itibaren (indeks 2 = 3. eleman)
            cumul_key.append(-running)
    return tuple(cumul_key)   # (-top3, -top4, -top5, -top6)


def qualifies_minimum(event_scores: dict[tuple, int],
                       min_points: int = 7) -> bool:
    """
    Bölge seçimi için minimum puan barajını aşıyor mu?
    PDF madde 7: en iyi 3 branş toplamı ≥ 7 (taban puan).
    """
    return compute_top3_total(event_scores) >= min_points
