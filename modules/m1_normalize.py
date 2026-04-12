"""
m1_normalize.py — Metin ve Kulüp Adı Normalizasyonu

Çözdüğü sorunlar:
  1. Türkçe ↔ ASCII karakter uyumsuzlukları  (ş→s, ğ→g, ı→i, vs.)
  2. Uzatma/inceltme işaretleri               (Hâki→Haki, Şöhret→Sohret)
  3. Büyük/küçük harf                         (İSMAİL = Ismail = ismail)
  4. Kulüp kısaltmaları                       (SK = S.K. = Spor Kulübü)
  5. "Belediye" varyantları                   (Bel. = Belediyesi = BB)
  6. Fazla boşluk ve noktalama                (Ankara  S.K. → Ankara SK)

Kullanım:
  from modules.m1_normalize import normalize_for_lookup, normalize_display

  # Eşleştirme için (her şeyi küçük ASCII'ye indirir):
  normalize_for_lookup("FENERBAHÇE S.K.")   → "fenerbahce spor kulubu"
  normalize_for_lookup("Fenerbahçe SK")     → "fenerbahce spor kulubu"

  # Görüntüleme için (düzgün Title Case):
  normalize_display("AHMET çELİK")          → "Ahmet Çelik"
"""

import re
import unicodedata

# ─────────────────────────────────────────────────────────────────────────────
# 1. Türkçe → ASCII karakter tablosu
#    Unicode codepoint'leri kullanıyoruz — kaynak dosya encoding'inden bağımsız
# ─────────────────────────────────────────────────────────────────────────────

# Türkçe → ASCII (12 karakter çifti)
# Unicode hex codepoint → ASCII karakter
_TR_TO_ASCII = str.maketrans({
    0x0131: "i",   # ı → i  (Türkçe noktalı olmayan küçük i)
    0x0130: "I",   # İ → I  (Türkçe noktalı büyük İ)
    0x015F: "s",   # ş → s
    0x015E: "S",   # Ş → S
    0x011F: "g",   # ğ → g
    0x011E: "G",   # Ğ → G
    0x00FC: "u",   # ü → u
    0x00DC: "U",   # Ü → U
    0x00F6: "o",   # ö → o
    0x00D6: "O",   # Ö → O
    0x00E7: "c",   # ç → c
    0x00C7: "C",   # Ç → C
})

# ─────────────────────────────────────────────────────────────────────────────
# 2. Kulüp kısaltmaları → tam ifade dönüştürme tablosu
#    Regex listesi. normalize_for_lookup() içinde ardı ardına uygulanır.
#    Girdi: zaten küçük harfe çevrilmiş, noktalama kaldırılmış ASCII metni.
# ─────────────────────────────────────────────────────────────────────────────

_ABBREV_RULES: list[tuple[str, str]] = [
    # ── Büyükşehir Belediyesi ──────────────────────────────────────────────
    (r"\bbb\b",                          "buyuksehir belediyesi"),
    (r"\bbuyuksehir bel\b",              "buyuksehir belediyesi"),
    (r"\bbuyuksehir belediye\b(?!si)",   "buyuksehir belediyesi"),

    # ── Belediyesi ────────────────────────────────────────────────────────
    (r"\bbel\b(?!ediy)",                 "belediyesi"),
    (r"\bbelediye\b(?!si)",              "belediyesi"),

    # ── Spor Kulübü ───────────────────────────────────────────────────────
    (r"\bs\.k\b",                        "spor kulubu"),
    (r"\bsk\b",                          "spor kulubu"),
    (r"\bspor kul\b",                    "spor kulubu"),

    # ── Gençlik Spor Kulübü ───────────────────────────────────────────────
    (r"\bg\.s\.k\b",                     "genclik spor kulubu"),
    (r"\bgsk\b",                         "genclik spor kulubu"),
    (r"\bgenclik ve spor kulubu\b",      "genclik spor kulubu"),
    (r"\bgenclik spor kulubu\b",         "genclik spor kulubu"),

    # ── Yüzme Kulübü ─────────────────────────────────────────────────────
    (r"\by\.k\b",                        "yuzme kulubu"),
    (r"\byk\b",                          "yuzme kulubu"),
    (r"\byuzme kul\b",                   "yuzme kulubu"),

    # ── Yüzme Spor Kulübü ─────────────────────────────────────────────────
    (r"\by\.s\.k\b",                     "yuzme spor kulubu"),
    (r"\bysk\b",                         "yuzme spor kulubu"),
    (r"\byuzme spor kul\b",              "yuzme spor kulubu"),

    # ── Su Sporları Kulübü ────────────────────────────────────────────────
    (r"\bssk\b",                         "su sporlari kulubu"),
    (r"\bs\.s\.k\b",                     "su sporlari kulubu"),

    # ── "İstanbul" OCR artefaktı: l→i son harf ────────────────────────────────
    (r"\bistanbui\b",                    "istanbul"),   # OCR: İstanbul → Istanbui

    # ── "Kulübü" OCR artefaktları ─────────────────────────────────────────
    # OCR'nin "ü" ve "b" harflerini karıştırmasından oluşan varyantlar.
    # Örnekler: Kulubo, Kulubui, Kulubd, Kulib, Kulibi, Kulibo, Kulbu, Kulbo,
    #           Kuldbo, Kuldbu, Kulb, Kuluibu, Kuldbu, Kulut
    # Kural: "kul" + (u/i/d/b/l ile başlayan 1-5 karakter) → "kulubu"
    # Güvenli: "kultur" (t∉set), "kulup" (p∉set), "kulube" (e∉set) etkilenmez.
    (r"\bkul[uoidbl][uiobdt]{0,4}\b",      "kulubu"),
    # ── Kısaltılmış "Kulübü" sonları (OCR son harfleri keser) ────────────────
    # "Spor Kt", "Spor Kl", "Spor Ki" → OCR "Kulübü"'nü kısalttı
    (r"\bk[tl]\b",                       "kulubu"),
]

# Önceden derle (performans için)
_ABBREV_COMPILED: list[tuple[re.Pattern, str]] = [
    (re.compile(pattern), replacement)
    for pattern, replacement in _ABBREV_RULES
]


# ─────────────────────────────────────────────────────────────────────────────
# 3. Temel karakter normalizasyonu
# ─────────────────────────────────────────────────────────────────────────────

def _strip_diacritics(text: str) -> str:
    """
    Birleşik diakritik işaretleri kaldırır.
    Hâki → Haki, Şöhret → Sohret (â, î, û gibi uzatma işaretleri)

    Not: NFKD decomposition yapıp combining karakterleri atar.
    Türkçe'ye özgü ı/İ bu adımdan ETKİLENMEZ (kendi codepoint'leri var),
    dolayısıyla önce _TR_TO_ASCII uygulanmalı, sonra bu fonksiyon çağrılmalı.
    """
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


def _tr_to_ascii(text: str) -> str:
    """Türkçe karakterleri ASCII karşılıklarına dönüştürür."""
    return text.translate(_TR_TO_ASCII)


def _clean_punctuation(text: str) -> str:
    """
    Noktalama işaretlerini temizler.

    Sıralama önemli:
      1. Önce kısaltma noktaları kaldırılır (S.K. → SK, B.B. → BB)
         Böylece abbreviation regex'leri doğru çalışır.
      2. Kalan diğer noktalama boşluğa dönüştürülür.
      3. Fazla boşluklar birleştirilir.
    """
    # Adım 1: Harf-nokta-harf kalıplarındaki noktayı sil (S.K. → SK, Y.S.K. → YSK)
    # Sayısal ondalık (1.5) ve tarih (2026-01) dokunma
    text = re.sub(r"(?<=[a-zA-Z])\.(?=[a-zA-Z])", "", text)   # S.K → SK (orta nokta)
    text = re.sub(r"(?<=[a-zA-Z])\.\s*$", "", text)            # son harf noktası: "SK." → "SK"
    text = re.sub(r"(?<=[a-zA-Z])\.\s*(?=\s)", "", text)       # "SK. " → "SK "

    # Adım 2: Kalan noktalama → boşluk
    text = re.sub(r"(?<!\d)\.(?!\d)", " ", text)   # sayı dışı nokta
    text = re.sub(r"[,;:()/\-_#!?]", " ", text)       # diğer noktalama (! ve ? dahil)

    # Adım 3: Fazla boşluk temizle
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Dışa açık fonksiyonlar
# ─────────────────────────────────────────────────────────────────────────────

def normalize_for_lookup(text: str | None) -> str:
    """
    Eşleştirme/arama için normalize eder.
    Sonuç: küçük harf, saf ASCII, noktalama yok, kısaltmalar açıldı.

    Örnekler:
      "FENERBAHÇE S.K."        → "fenerbahce spor kulubu"
      "Fenerbahçe SK"          → "fenerbahce spor kulubu"
      "BÜYÜK ŞEHİR BEL."       → "buyuk sehir belediyesi"
      "Hâki Çakar"             → "haki cakar"
      "İSMAİL"                 → "ismail"
      "Gökay Gündüz"           → "gokay gunduz"
    """
    if not text:
        return ""

    result = str(text)

    # Adım 1: Türkçe özel harfler → ASCII (ı→i, İ→I, ş→s vs.)
    result = _tr_to_ascii(result)

    # Adım 2: Kalan diakritikler kaldır (â→a, î→i, û→u)
    result = _strip_diacritics(result)

    # Adım 3: Küçük harfe çevir
    result = result.lower()

    # Adım 4: Noktalama temizle (noktalar, virgüller, parantez)
    result = _clean_punctuation(result)

    # Adım 5: Kısaltma açma
    for pattern, replacement in _ABBREV_COMPILED:
        result = pattern.sub(replacement, result)

    # Son boşluk temizliği
    result = re.sub(r"\s+", " ", result).strip()

    return result


def normalize_display(text: str | None) -> str:
    """
    Görüntüleme için normalize eder.
    Sonuç: Title Case, Türkçe karakterler korunur, fazla boşluk temizlenir.

    Örnekler:
      "AHMET ÇELİK"     → "Ahmet Çelik"
      "mehmet  yılmaz"  → "Mehmet Yılmaz"

    Not: Python'un str.lower() metodu İ (U+0130) harfini "i\u0307" (i + birleşik
    nokta) şeklinde dönüştürür. Bu Python'un bilinen davranışıdır. Bunu önlemek
    için İ → i dönüşümü manuel yapılır.
    """
    if not text:
        return ""

    result = str(text)
    result = re.sub(r"\s+", " ", result).strip()

    # İ (U+0130, Türkçe büyük İ) → i dönüşümünü lower() öncesinde yap
    # Aksi halde Python "i" + combining dot above üretir
    _DISPLAY_LOWER = str.maketrans({
        0x0130: "i",   # İ → i (combining dot sorunu yaratmadan)
    })

    words = result.split()
    titled = []
    for word in words:
        if not word:
            continue
        first = word[0].upper()
        rest  = word[1:].translate(_DISPLAY_LOWER).lower()
        titled.append(first + rest)

    return " ".join(titled)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Türkçe isim sözlüğü — ASCII OCR çıktısını Türkçe görüntüleme adına çevirir
#
#    Anahtar  : kelime._tr_to_ascii().lower()  (tam ASCII, küçük harf)
#    Değer    : doğru Türkçe Title Case yazılışı
#
#    Kapsam   : sadece Türkçe karakter içeren (veya değiştiren) isimler.
#    "Can", "Ali" gibi değişmeyen isimler dahil edilmez.
#
#    Kullanım : restore_turkish_display("Cinar Baris") → "Çınar Barış"
# ─────────────────────────────────────────────────────────────────────────────

_TR_NAME_DICT: dict[str, str] = {
    # ── Kız isimleri ──────────────────────────────────────────────────────────
    "asli":            "Aslı",
    "ayse":            "Ayşe",
    "aysegul":         "Ayşegül",
    "beyazit":         "Beyazıt",
    "cagla":           "Çağla",
    "cigdem":          "Çiğdem",
    "cinar":           "Çınar",       # hem erkek hem kız adı
    "eylul":           "Eylül",
    "gokce":           "Gökçe",
    "gul":             "Gül",
    "gulsu":           "Gülsu",
    "gulnaz":          "Gülnaz",
    "gunes":           "Güneş",
    "inci":            "İnci",
    "ipek":            "İpek",
    "irem":            "İrem",
    "isil":            "Işıl",
    "nazli":           "Nazlı",
    "nisan":           "Nisan",
    "ozge":            "Özge",
    "sukran":          "Şükran",
    "tugba":           "Tuğba",
    "tugce":           "Tuğçe",
    "ulku":            "Ülkü",
    "yagmur":          "Yağmur",
    "zulal":           "Zülal",
    # ── Erkek isimleri ────────────────────────────────────────────────────────
    "baris":           "Barış",       # hem erkek adı hem soyisim
    "cagri":           "Çağrı",
    "cagatay":         "Çağatay",
    "dogukan":         "Doğukan",
    "gokhan":          "Gökhan",
    "gorkem":          "Görkem",
    "guney":           "Güney",
    "ilhan":           "İlhan",
    "ilker":           "İlker",
    "ibrahim":         "İbrahim",
    "ismail":          "İsmail",
    "iz":              "İz",          # kısa isim/soyisim (Önder İz gibi)
    "kagan":           "Kağan",
    "kazim":           "Kazım",
    "kivanc":          "Kıvanç",
    "meric":           "Meriç",
    "mucahit":         "Mücahit",
    "oguz":            "Oğuz",
    "oguzhan":         "Oğuzhan",
    "omer":            "Ömer",
    "onder":           "Önder",
    "ruzgar":          "Rüzgar",
    "sukru":           "Şükrü",
    "tugra":           "Tuğra",
    "tugrul":          "Tuğrul",
    "ugur":            "Uğur",
    "umit":            "Ümit",
    "yagiz":           "Yağız",
    "yigit":           "Yiğit",
    # ── Unisex / ek isimler ───────────────────────────────────────────────────
    "ayca":            "Ayça",
    "aydin":           "Aydın",
    "cakar":           "Çakar",       # hem isim hem soyisim
    "gumussuyu":       "Gümüşsuyu",
    "huma":            "Hüma",
    "ida":             "İda",
    "ilgin":           "Ilgın",       # Ilgın Olukçu (allows l→İ: "Llgin"→"Ilgın")
    "ilke":            "İlke",
    "isik":            "Işık",
    "lihan":           "İlhan",       # direct OCR-artefakt ("Lihan"→"İlhan", l→İl reversal)
    "olukcu":          "Olukçu",
    "omur":            "Ömür",
    "oyku":            "Öykü",
    "oykiu":           "Öykü",        # OCR ü→iu artefakt
    "ozim":            "Özüm",        # OCR ü→i artefakt
    "ozum":            "Özüm",
    "ozel":            "Özel",
    "ruya":            "Rüya",
    "sehiralti":       "Şehiraltı",
    # ── Soyisimler ────────────────────────────────────────────────────────────
    "acikgoz":         "Açıkgöz",
    "afatoglu":        "Afatoğlu",
    "agzikuru":        "Ağzıkuru",
    "akinci":          "Akıncı",
    "altinbas":        "Altınbaş",
    "altug":           "Altuğ",
    "arabaci":         "Arabacı",
    "aslanturk":       "Aslantürk",
    "atabas":          "Atabaş",
    "aydinlik":        "Aydınlık",
    "bas":             "Baş",
    "basoglu":         "Başoğlu",
    "baskonyali":      "Başkonyalı",
    "caglar":          "Çağlar",
    "caglayan":        "Çağlayan",
    "caldagi":         "Çaldağı",
    "cali":            "Çalı",
    "calisci":         "Çalışçı",
    "calicioglu":      "Çalıcıoğlu",
    "cavusoglu":       "Çavuşoğlu",
    "celik":           "Çelik",
    "celikdemir":      "Çelikdemir",
    "celikel":         "Çelikel",
    "cengiz":          "Cengiz",      # değişmiyor, tutarlılık için
    "cetinkaya":       "Çetinkaya",
    "cevikogullari":   "Çevikoğulları",
    "cevikkalp":       "Çevikkalp",
    "cibiroglu":       "Cıbıroğlu",
    "civici":          "Çivici",
    "coskun":          "Coşkun",
    "daglioglu":       "Dağlıoğlu",
    "darici":          "Darıcı",
    "degirmenci":      "Değirmenci",
    "doga":            "Doğa",
    "dogan":           "Doğan",
    "eksi":            "Ekşi",
    "erbas":           "Erbaş",
    "erdogan":         "Erdoğan",
    "erikcioglu":      "Erikçioğlu",
    "eroglu":          "Eroğlu",
    "erturk":          "Ertürk",
    "esmanur":         "Esmanur",
    "evliyaoglu":      "Evliyaoğlu",
    "cinarli":         "Çınarlı",
    "citir":           "Çıtır",
    "goregen":         "Göreğen",
    "guder":           "Güder",
    "gultekin":        "Gültekin",
    "gulap":           "Gülap",
    "gundogdu":        "Gündoğdu",
    "gunduz":          "Gündüz",
    "gungor":          "Güngör",
    "guner":           "Güner",
    "gunoral":         "Günoral",
    "gunsever":        "Günsever",
    "gunyol":          "Günyol",
    "gur":             "Gür",
    "gurbuz":          "Gürbüz",
    "guzel":           "Güzel",
    "hindioglu":       "Hindioğlu",
    "hoke":            "Höke",
    "ince":            "İnce",
    "inanc":           "İnanç",
    "inang":           "İnanç",       # OCR ç→g artefakt
    "ikizce":          "İkizçe",
    "izgi":            "İzgi",
    "isildak":         "Işıldak",
    "kabaoglu":        "Kabaoğlu",
    "kanberoglu":      "Kanberoğlu",
    "karaaslan":       "Karaaslan",
    "karakaya":        "Karakaya",
    "kilic":           "Kılıç",
    "kocak":           "Koçak",
    "kocadag":         "Kocadağ",
    "kocuk":           "Koçuk",
    "koken":           "Köken",
    "konakli":         "Konaklı",
    "koseogullari":    "Köseoğulları",
    "koseturk":        "Kösetürk",
    "kostekci":        "Köstekci",
    "kurtoglu":        "Kurtoğlu",
    "kursun":          "Kurşun",
    "kurtulus":        "Kurtuluş",
    "kurtuloglu":      "Kurtuloğlu",
    "lutfi":           "Lütfi",
    "luitfi":          "Lütfi",       # OCR extra-i artefakt
    "onal":            "Önal",
    "ozalp":           "Özalp",
    "ozbek":           "Özbek",
    "ozdemir":         "Özdemir",
    "ozgenc":          "Özgenç",
    "ozgok":           "Özgök",
    "ozgunduz":        "Özgündüz",
    "ozgur":           "Özgür",
    "ozkan":           "Özkan",
    "ogut":            "Öğüt",
    "ozbay":           "Özbay",
    "ozbayhan":        "Özbayhan",
    "ozsoy":           "Özsoy",
    "oztas":           "Öztaş",
    "oztaysi":         "Öztayşi",
    "ozturk":          "Öztürk",
    "ozyasar":         "Özyaşar",
    "ozyazicioglu":   "Özyazıcıoğlu",
    "pekcaglayan":     "Pekçağlayan",
    "sadoglu":         "Şadoğlu",
    "sahin":           "Şahin",
    "sahinkaya":       "Şahinkaya",
    "sarac":           "Saraç",
    "sarikas":         "Sarıkaş",
    "satiroglu":       "Satıroğlu",
    "sen":             "Şen",
    "sumer":           "Sümer",
    "surmeli":         "Sürmeli",
    "sogukpinar":      "Soğukpınar",
    "tavukcu":         "Tavukçu",
    "tekeoglu":        "Tekeoğlu",
    "tumer":           "Tümer",
    "tunc":            "Tunç",
    "turk":            "Türk",
    "turkyilmaz":      "Türkyılmaz",
    "tuzunturk":       "Tüzüntürk",
    "ulas":            "Ulaş",
    "uludag":          "Uludağ",
    "ulupinar":        "Ulupınar",
    "uzgoren":         "Uzgören",
    "yabanci":         "Yabancı",
    "yamali":          "Yamalı",
    "yazicilar":       "Yazıcılar",
    "yenturk":         "Yentürk",
    "yesil":           "Yeşil",
    "yetisgin":        "Yetişgin",
    "yildirim":        "Yıldırım",
    "yildiz":          "Yıldız",
    "yildizli":        "Yıldızlı",
    "yilmaz":          "Yılmaz",
    "yucel":           "Yücel",
    "yuksel":          "Yüksel",
    # ── Ek soyisim/isim düzeltmeleri ─────────────────────────────────────────
    "gogen":           "Gögen",
    "goncu":           "Göncü",
    "guc":             "Güç",
    "kazanci":         "Kazancı",
    "ozer":            "Özer",
    "akkus":           "Akkuş",
    "akbayir":         "Akbayır",
    "alibeyoglu":      "Alibeyoğlu",
    "ates":            "Ateş",
    "gurel":           "Gürel",
    "saygin":          "Saygın",
    "soydas":          "Soydaş",
    "suslu":           "Süslü",
    "suslo":           "Süslü",      # OCR o→ü artefakt
    "yenicag":         "Yeniçağ",
    "yalcin":          "Yalçın",
    "simsek":          "Şimşek",
    # ── Birleşik OCR → iki kelimeye bölünen soyisim/isim çiftleri ─────────────
    # OCR iki kelimeyi bitişik yazar; sözlük değeri boşluk içererek ikiye böler.
    # restore_turkish_display() bu değeri token listesine ekler → join ile doğru.
    "canoz":           "Can Öz",
    "derengur":        "Deren Gür",
    "efegines":        "Efe Güneş",   # OCR variant gines→güneş
    "efegunes":        "Efe Güneş",
    "eslemdeniz":      "Eslem Deniz",
    "izerol":          "İz Erol",
    "kaankarakaya":    "Kaan Karakaya",
    "naskostekci":     "Nas Köstekci",
    "ozumozel":        "Özüm Özel",
    "safakaracan":     "Safa Karacan",
    "suaslanturk":     "Su Aslantürk",
    "tahakocabal":     "Taha Kocabal",
    "yagmurvera":      "Yağmur Vera",
}


def restore_turkish_display(name: str | None) -> str:
    """
    İsim tokenlarını Türkçe karakter sözlüğüne göre düzeltir.

    OCR'ın dönüştürdüğü ASCII formları (Cinar, Kivanc, Ozdemir) otomatik olarak
    doğru Türkçe biçimine çevirir (Çınar, Kıvanç, Özdemir).

    Algoritma:
      Her token için: _tr_to_ascii(token).lower() → sözlük anahtarı
      Anahtar sözlükte varsa → sözlük değeriyle değiştir
      Yoksa → tokeni olduğu gibi bırak

    Örnekler:
      restore_turkish_display("Cinar Baris")         → "Çınar Barış"
      restore_turkish_display("Ipek Beyhan Yazicilar") → "İpek Beyhan Yazıcılar"
      restore_turkish_display("Mehmet Ege Kocak")    → "Mehmet Ege Koçak"
      restore_turkish_display("Yağmur Özgök")        → "Yağmur Özgök"  (değişmez)
    """
    if not name:
        return name or ""

    # Alt çizgiyi boşluğa çevir (HTML/OCR "Burak_gurbuz" → "Burak gurbuz")
    name = name.replace("_", " ")
    # Fazla boşluk temizle
    name = re.sub(r"\s+", " ", name).strip()

    words = name.split()
    result = []
    for word in words:
        # Noktalama öneki/soneki koru (örn. "Çelik," → "Çelik,")
        prefix = ""
        suffix = ""
        w = word
        while w and not w[0].isalpha():
            prefix += w[0]
            w = w[1:]
        while w and not w[-1].isalpha():
            suffix = w[-1] + suffix
            w = w[:-1]

        if w:
            # Sözlük anahtarı: Türkçe özel harfleri ASCII'ye çevir + küçük harf
            key = _tr_to_ascii(w).lower()
            if key in _TR_NAME_DICT:
                result.append(prefix + _TR_NAME_DICT[key] + suffix)
            else:
                # OCR artefakt: "l" → "İ" karışıklığı (örn. "Lpek" → "İpek")
                # OCR bazen büyük İ (noktalı) harfini küçük l olarak okur.
                # Eğer kelime küçük l ile başlıyorsa "i" ile değiştirip sözlüğe bak.
                if key and key[0] == "l" and len(key) > 1 and key[1].islower():
                    alt_key = "i" + key[1:]
                    if alt_key in _TR_NAME_DICT:
                        result.append(prefix + _TR_NAME_DICT[alt_key] + suffix)
                        continue
                # All-caps kelime dict'te yok → Title Case uygula
                # (PDF/HTML'de soyadlar ALL CAPS gelir; Türkçe harfsiz soyadlar
                #  dict'e girmeden burada kalır. Örn: BAYDAR → Baydar)
                if word.isupper() and len(word) > 1:
                    result.append(prefix + word.capitalize() + suffix)
                else:
                    result.append(word)
        else:
            result.append(word)

    return " ".join(result)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Kulüp adı Türkçe karakter sözlüğü
#
#    Anahtar  : kelime._tr_to_ascii().lower()  (tam ASCII, küçük harf)
#    Değer    : doğru Türkçe Title Case yazılışı
#
#    Kapsam   : kulüp adlarında sık geçen ve Türkçe karakter içeren kelimeler.
#    Kullanım : restore_turkish_club("Istanbul Cevre Spor Kulubu")
#               → "İstanbul Çevre Spor Kulübü"
# ─────────────────────────────────────────────────────────────────────────────

_TR_CLUB_DICT: dict[str, str] = {
    # ── Kulüp organizasyonu ───────────────────────────────────────────────────
    "kulubu":           "Kulübü",       # kulübü (ü×2 eksik → kulubu)
    "universitesi":     "Üniversitesi", # üniversitesi (ü → u)
    # ── Şehir / semt isimleri ─────────────────────────────────────────────────
    "istanbul":         "İstanbul",     # İ eksik
    "uskudar":          "Üsküdar",      # Ü, ü eksik
    "kinaliada":        "Kınalıada",    # ı×2 eksik
    # ── Sporla ilgili kelimeler ───────────────────────────────────────────────
    "yuzme":            "Yüzme",        # ü eksik
    "sporlari":         "Sporları",     # ı eksik
    "ihtisas":          "İhtisas",      # İ eksik
    # ── Kulüp / okul özel kelimeleri ─────────────────────────────────────────
    "genclik":          "Gençlik",      # ç eksik
    "istek":            "İstek",        # İ eksik
    "sevinc":           "Sevinç",       # ç eksik (aynı zamanda kişi adı)
    "cevre":            "Çevre",        # Ç eksik
    "kasirga":          "Kasırga",      # ı eksik
    # ── Büyük kulüp isimleri ─────────────────────────────────────────────────
    "besiktas":         "Beşiktaş",     # ş×2 eksik
    "fenerbahce":       "Fenerbahçe",   # ç eksik
}


def restore_turkish_club(club_name: str | None) -> str:
    """
    Kulüp adındaki Türkçe karakter eksikliklerini ve ALL CAPS yazımı düzeltir.

    OCR veya Excel'den gelen "Istanbul Cevre Spor Kulubu" veya
    "ISTANBUL CEVRE SPOR KULÜBÜ" gibi varyantları otomatik olarak
    "İstanbul Çevre Spor Kulübü" biçimine çevirir.

    Algoritma:
      1. normalize_display() ile Title Case uygula (ALL CAPS → Title Case)
      2. Her kelime için _tr_to_ascii(kelime).lower() → _TR_CLUB_DICT'e bak
         Bulunursa sözlük değeriyle değiştir, bulunmazsa olduğu gibi bırak.

    Örnekler:
      restore_turkish_club("Istanbul Cevre Spor Kulubu")
        → "İstanbul Çevre Spor Kulübü"
      restore_turkish_club("KASIRGA (TORNADO) YÜZME SPOR KULÜBÜ")
        → "Kasırga (Tornado) Yüzme Spor Kulübü"
      restore_turkish_club("İstanbul Gençlik Spor Kulübü")
        → "İstanbul Gençlik Spor Kulübü"  (değişmez)
    """
    if not club_name:
        return club_name or ""

    # Adım 1: Title Case (ALL CAPS → Title Case, boşluk normalizasyonu)
    result = normalize_display(club_name)

    # Adım 2: Türkçe karakter restorasyonu kelime kelime
    words = result.split()
    output = []
    for word in words:
        # Parantez gibi noktalama öneki/sonekini koru
        prefix = ""
        suffix = ""
        w = word
        while w and not w[0].isalpha():
            prefix += w[0]
            w = w[1:]
        while w and not w[-1].isalpha():
            suffix = w[-1] + suffix
            w = w[:-1]

        if w:
            key = _tr_to_ascii(w).lower()
            if key in _TR_CLUB_DICT:
                output.append(prefix + _TR_CLUB_DICT[key] + suffix)
            else:
                # normalize_display parantez içi kelimeleri küçük harfe düşürür;
                # dict'te yoksa da ilk harf büyük yap (örn. "(tornado)" → "(Tornado)")
                output.append(prefix + w[0].upper() + w[1:] + suffix)
        else:
            output.append(word)

    return " ".join(output)


def normalize_name(name: str | None) -> str:
    """
    Sporcu adı karşılaştırması için normalize eder.
    normalize_for_lookup() ile aynı davranışı gösterir; alias olarak var.
    """
    return normalize_for_lookup(name)


def names_match(name_a: str | None, name_b: str | None) -> bool:
    """
    İki ismin aynı kişiye ait olup olmadığını kontrol eder.
    Türkçe/ASCII farklılıkları, büyük/küçük harf ve kısaltmaları görmezden gelir.

    Örnek:
      names_match("İSMAİL HÂKI ÇAKAR", "Ismail Haki Cakar") → True
    """
    return normalize_for_lookup(name_a) == normalize_for_lookup(name_b)


def club_names_match(club_a: str | None, club_b: str | None) -> bool:
    """
    İki kulüp isminin aynı kulübü temsil edip etmediğini kontrol eder.
    Kısaltmaları açar, karakter farklarını görmezden gelir.

    Örnek:
      club_names_match("ANKARA BEL. SK", "Ankara Belediyesi Spor Kulübü") → True
    """
    return normalize_for_lookup(club_a) == normalize_for_lookup(club_b)
