"""Черновик названия товара, когда страницу распарсить не удалось (Ozon с антиботом и т.п.).

1. Текст «Поделиться» из приложений маркетплейсов обычно содержит название:
   «Посмотрите товар Кофемолка электрическая на Ozon https://ozon.ru/t/abc».
2. В полных ссылках Ozon/Я.Маркета есть транслит названия:
   ozon.ru/product/kofemolka-elektricheskaya-dlya-kofe-123456/ -> «Кофемолка электрическая для кофе».
"""
import re
from typing import Optional
from urllib.parse import unquote, urlparse

MAX_LEN = 120

SHARE_NOISE = [
    r"посмотрите\s+(?:товар|на\s+этот\s+товар)?",
    r"смотрите,?\s+что\s+я\s+нашл[аи]?",
    r"вот,?\s+что\s+я\s+нашл[аи]?",
    r"нашл[аи]?\s+(?:классный\s+)?товар",
    r"мне\s+понравился\s+товар",
    r"(?:на|в)\s+(?:ozon|озон[еау]?|wildberries|вайлдберриз|wb|яндекс\s*маркет[еу]?|market|aliexpress|алиэкспресс)\b",
    r"\b(?:ozon|wildberries|aliexpress)\s*:",
    r"по\s+ссылке",
    r"#\S+",
]

TRANSLIT = [
    ("shch", "щ"), ("sch", "щ"), ("yo", "ё"), ("zh", "ж"), ("kh", "х"), ("ts", "ц"), ("ch", "ч"),
    ("sh", "ш"), ("yu", "ю"), ("ya", "я"), ("ye", "е"),
    ("a", "а"), ("b", "б"), ("v", "в"), ("g", "г"), ("d", "д"), ("e", "е"), ("z", "з"), ("i", "и"),
    ("y", "ы"), ("k", "к"), ("l", "л"), ("m", "м"), ("n", "н"), ("o", "о"), ("p", "п"), ("r", "р"),
    ("s", "с"), ("t", "т"), ("u", "у"), ("f", "ф"), ("c", "к"),
]
LATIN_ONLY = re.compile(r"[jqwx]|^\d")  # такие слова — бренды/модели, оставляем как есть
KEEP_LATIN = {"usb", "led", "rgb", "hdmi", "pro", "max", "mini", "ultra", "plus", "se", "air", "wi", "fi"}


def _tidy(s: str) -> Optional[str]:
    s = re.sub(r"\s+", " ", s).strip(" \t\n.,:;!—-–\"'«»()[]")
    if len(s) < 3 or not re.search(r"[A-Za-zА-Яа-яЁё]{3}", s):
        return None
    if len(s) > MAX_LEN:
        s = s[:MAX_LEN].rsplit(" ", 1)[0] + "…"
    return s[0].upper() + s[1:]


FILLER_ONLY = re.compile(r"(?:хочу|вот|это|такое|такой|такую|такие|вот\s+это|глянь|смотри)[\s!.)]*", re.I)


def name_from_share_text(text: Optional[str], url: str) -> Optional[str]:
    if not text:
        return None
    s = text.replace(url, " ")
    s = re.sub(r"https?://\S+", " ", s)
    for pat in SHARE_NOISE:
        s = re.sub(pat, " ", s, flags=re.I)
    if re.fullmatch(rf"\s*(?:{FILLER_ONLY.pattern})*\s*", s, re.I):
        return None  # «хочу вот это» — не название, лучше взять транслит из ссылки
    return _tidy(s)


def _translit_word(word: str) -> str:
    if LATIN_ONLY.search(word) or word in KEEP_LATIN:
        return word.upper() if word in {"usb", "led", "rgb", "hdmi"} else word
    out, i = [], 0
    while i < len(word):
        for lat, cyr in TRANSLIT:
            if word.startswith(lat, i):
                # «e» в начале слова — почти всегда «э» (elektricheskaya -> электрическая)
                out.append("э" if lat == "e" and i == 0 else cyr)
                i += len(lat)
                break
        else:
            out.append(word[i])
            i += 1
    w = "".join(out)
    w = re.sub(r"ыы$", "ый", w)
    w = re.sub(r"иы$", "ий", w)
    w = re.sub(r"оы$", "ой", w)
    w = re.sub(r"еы$", "ей", w)
    w = re.sub(r"ы(?=[аеёиоуэюя])", "й", w)  # «yo», «ya» уже съедены выше; это «y» перед гласной
    return w


def name_from_url(url: str) -> Optional[str]:
    path = unquote(urlparse(url).path)
    m = re.search(r"/(?:product|card)/([a-z0-9\-]+?)(?:-\d{4,})?/?(?:\d+)?/?$", path, re.I)
    if not m:
        return None
    words = [w for w in m.group(1).lower().split("-") if w]
    if len(words) < 2:
        return None
    return _tidy(" ".join(_translit_word(w) for w in words))
