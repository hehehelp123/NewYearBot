"""Парсеры страниц товаров.

Опираемся на стабильные вещи — og:-теги, JSON-LD, <title>/description — а не на
хэшированные CSS-классы (`productTitle--J2W7I` и т.п.), которые маркетплейсы меняют
при каждом релизе фронта.
"""
import datetime
import html as html_lib
import json
import logging
import re
from typing import Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

EMPTY = {"name": None, "cost": None, "delivery_date": None, "image_url": None}

BLOCK_MARKERS = (
    ("нам нужно убедиться, что вы не робот", "сайт заподозрил бота и показал проверку (антибот)"),
    ("we need to make sure that you are not a robot", "сайт заподозрил бота и показал проверку (антибот)"),
    ("похоже, нет соединения", "сайт заподозрил бота и показал проверку (антибот)"),
    ("доступ ограничен", "сайт ограничил доступ"),
    ("captcha", "сайт показал капчу"),
    ("страница товара не существует", "товар не найден (404)"),
    ("такой страницы нет", "страница не найдена (404)"),
    ("товар закончился", None),  # страница есть, просто нет в наличии — не ошибка
)


# ---------------------------------------------------------------- helpers

def detect_block_reason(html: str) -> Optional[str]:
    if not html:
        return "пустая страница"
    head = html[:200_000].lower()
    title_m = re.search(r"<title[^>]*>(.*?)</title>", head, re.S)
    title = title_m.group(1) if title_m else ""
    for marker, reason in BLOCK_MARKERS:
        if reason and (marker in title or (marker in head and len(html) < 20_000)):
            return reason
    return None


def _norm_price(raw) -> Optional[str]:
    if raw is None:
        return None
    s = html_lib.unescape(str(raw))
    s = re.sub(r"[\s   ]", "", s)
    m = re.search(r"\d+(?:[.,]\d{1,2})?", s)
    if not m:
        return None
    num = m.group(0).replace(",", ".")
    try:
        val = float(num)
    except ValueError:
        return None
    if val <= 0:
        return None
    return f"{int(val):,}".replace(",", " ") + " ₽" if val == int(val) else f"{val:.2f} ₽"


def _meta(soup, *keys) -> Optional[str]:
    for k in keys:
        tag = soup.find("meta", attrs={"property": k}) or soup.find("meta", attrs={"name": k}) \
              or soup.find("meta", attrs={"itemprop": k})
        if tag and tag.get("content"):
            return html_lib.unescape(tag["content"]).strip()
    return None


def _ld_product(soup) -> dict:
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        items = data if isinstance(data, list) else data.get("@graph", [data]) if isinstance(data, dict) else []
        for it in items:
            if isinstance(it, dict) and str(it.get("@type", "")).lower() == "product":
                return it
    return {}


def _ld_price(product: dict) -> Optional[str]:
    offers = product.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    return _norm_price(offers.get("price") or offers.get("lowPrice"))


def _ld_image(product: dict) -> Optional[str]:
    img = product.get("image")
    if isinstance(img, list):
        img = img[0] if img else None
    if isinstance(img, dict):
        img = img.get("url") or img.get("contentUrl")
    return img


def _price_from_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"(?:за|цена[:\s]*|от)\s*([\d\s   ]+(?:[.,]\d{1,2})?)\s*(?:&nbsp;)?\s*(?:₽|руб)", text, re.I)
    return _norm_price(m.group(1)) if m else None


def _clean_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    t = html_lib.unescape(title).strip()
    t = re.split(r"\s+(?:—|-|\|)\s+(?:купить|buy)\b", t, flags=re.I)[0]
    t = re.split(r"\s+купить\s+(?:за|по|в)\b", t, flags=re.I)[0]
    t = t.split(" | ")[0]
    return t.strip(" ,") or None


def map_date_difference(input_date_str):
    month_map = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
        'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    day, month_str = input_date_str.split()[:2]
    month = month_map[month_str.lower()]
    now = datetime.datetime.now()
    input_date = datetime.datetime(now.year, month, int(day))
    if (input_date - now).days < -1:  # «3 января», а сейчас декабрь — это следующий год
        input_date = input_date.replace(year=now.year + 1)
    delta = (input_date - now).days
    if delta < 5:
        return "<5 дней"
    elif delta < 14:
        return "<14 дней"
    return "Китайские заначки (Больше 14 дней)"


def convert_russian_short_date(short_date):
    month_map = {"янв": "января", "фев": "февраля", "мар": "марта", "апр": "апреля", "май": "мая",
                 "июн": "июня", "июл": "июля", "авг": "августа", "сен": "сентября", "окт": "октября",
                 "ноя": "ноября", "дек": "декабря"}
    parts = short_date.strip().split()
    if len(parts) != 2:
        return short_date
    return f"{parts[0]} {month_map.get(parts[1].lower()[:3], parts[1])}"


# ---------------------------------------------------------------- parsers

def parse_generic_data(html_content: str) -> dict:
    soup = BeautifulSoup(html_content, "html.parser")
    product = _ld_product(soup)
    title_tag = soup.find("title")
    description = _meta(soup, "og:description", "description")
    return {
        "name": (product.get("name") or _meta(soup, "og:title") or _clean_title(title_tag.text if title_tag else None)),
        "cost": (_ld_price(product)
                 or _norm_price(_meta(soup, "product:price:amount", "og:price:amount", "price"))
                 or _price_from_text(description)
                 or _price_from_text(title_tag.text if title_tag else None)),
        "delivery_date": None,
        "image_url": _ld_image(product) or _meta(soup, "og:image"),
    }


def parse_ozon_data(html_content: str) -> dict:
    data = parse_generic_data(html_content)
    data["name"] = _clean_title(data["name"])
    return data


def parse_yamarket_data(html_content: str) -> dict:
    data = parse_generic_data(html_content)
    soup = BeautifulSoup(html_content, "html.parser")
    data["name"] = _clean_title(data["name"])
    cost_span = soup.find("span", {"data-auto": "snippet-price-current"})
    if cost_span:
        data["cost"] = _norm_price(cost_span.get_text(strip=True)) or data["cost"]
    m = re.search(r'"deliveryDate"\s*:\s*"([^"]+)"', html_content)
    if m:
        try:
            data["delivery_date"] = map_date_difference(convert_russian_short_date(m.group(1)))
        except Exception:
            pass
    return data


def parse_wildberries_data(html_content: str) -> dict:
    data = parse_generic_data(html_content)
    soup = BeautifulSoup(html_content, "html.parser")
    data["name"] = _clean_title(data["name"])
    # Цена с WB-кошельком — самая «честная» для покупателя; класс хэшированный, ищем по префиксу
    wallet = soup.select_one('[class*="PriceWallet"], [class*="priceBlockWalletPrice"]')
    if wallet:
        data["cost"] = _norm_price(wallet.get_text(strip=True)) or data["cost"]
    if data.get("image_url", "") and data["image_url"].endswith(".svg"):
        data["image_url"] = None
    return data


def parse_aliexpress_data(html_content: str) -> dict:
    data = parse_generic_data(html_content)
    soup = BeautifulSoup(html_content, "html.parser")
    data["name"] = _clean_title(data["name"])
    price_tag = soup.find(class_=lambda c: c and "OfferPrice__price" in c)
    if price_tag:
        data["cost"] = _norm_price(price_tag.get_text(strip=True)) or data["cost"]
    data["delivery_date"] = "Китайские заначки (>14 дней)"
    return data
