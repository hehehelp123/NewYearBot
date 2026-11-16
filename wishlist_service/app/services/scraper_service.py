import asyncio
import logging
import os
import time
from urllib.parse import urlparse

from app.services.selenium_downloader import selenium_downloader
from app.utils.parsers import (
    detect_block_reason, parse_aliexpress_data, parse_generic_data, parse_ozon_data,
    parse_wildberries_data, parse_yamarket_data,
)

logger = logging.getLogger(__name__)

DOWNLOAD_PATH = "/app/downloads"
LAST_PAGE_FILE = os.path.join(DOWNLOAD_PATH, "last_scraped_page.html")
PAGE_WAIT_SECONDS = 25

SITE_PARSERS = (
    (("ozon.ru",), parse_ozon_data, "Ozon"),
    (("market.yandex.ru",), parse_yamarket_data, "Яндекс Маркет"),
    (("wildberries.ru", "wb.ru"), parse_wildberries_data, "Wildberries"),
    (("aliexpress.ru", "aliexpress.com", "ali.click"), parse_aliexpress_data, "AliExpress"),
)


def _site_for(url: str):
    host = (urlparse(url).hostname or "").lower()
    for domains, parser, title in SITE_PARSERS:
        if any(host == d or host.endswith("." + d) for d in domains):
            return parser, title
    return parse_generic_data, host or "сайт"


def _load_page(driver, url: str) -> str:
    """Синхронно: открыть страницу и дождаться, пока появятся данные о товаре (а не спать фиксированные 20 с)."""
    driver.get(url)
    deadline = time.monotonic() + PAGE_WAIT_SECONDS
    html = ""
    while time.monotonic() < deadline:
        time.sleep(1.5)
        try:
            ready = driver.execute_script(
                "return document.readyState === 'complete' && ("
                "!!document.querySelector('meta[property=\"og:title\"]') ||"
                "!!document.querySelector('script[type=\"application/ld+json\"]'))"
            )
        except Exception:
            ready = False
        html = driver.page_source
        if ready or detect_block_reason(html):
            # даём ещё чуть-чуть дорисовать цену
            time.sleep(2)
            html = driver.page_source
            break
    return html


class ScraperService:
    def __init__(self):
        self.selenium_lock = asyncio.Lock()

    async def parse_url(self, source_url: str, user_id: int) -> dict:
        """Возвращает dict с name/cost/delivery_date/image_url или {'error': причина}."""
        parser, site = _site_for(source_url)
        logger.info(f"Фоновая задача для {source_url} ({site}) ожидает доступа к Selenium...")
        async with self.selenium_lock:
            try:
                async with selenium_downloader.get_driver_session() as driver:
                    # Selenium синхронный — уводим в поток, чтобы не вешать event loop сервиса
                    html = await asyncio.to_thread(_load_page, driver, source_url)
            except Exception as e:
                logger.error(f"Ошибка Selenium для {source_url}: {e}", exc_info=True)
                return {"error": "не удалось открыть страницу (браузер-парсер недоступен)"}

        try:
            os.makedirs(DOWNLOAD_PATH, exist_ok=True)
            with open(LAST_PAGE_FILE, "w", encoding="utf-8") as f:
                f.write(html)
        except OSError as e:
            logger.warning(f"Не удалось сохранить HTML для отладки: {e}")

        block = detect_block_reason(html)
        if block:
            logger.warning(f"{site}: страница не отдала товар ({block}) для {source_url}")
            return {"error": f"{site}: {block}"}

        try:
            data = parser(html)
        except Exception as e:
            logger.error(f"Парсер {site} упал на {source_url}: {e}", exc_info=True)
            data = {}
        if not data.get("name"):
            # специфичный парсер не справился — пробуем общий (og:/ld+json)
            generic = parse_generic_data(html)
            data = {**generic, **{k: v for k, v in data.items() if v}}
        if not data.get("name"):
            return {"error": f"{site}: не нашёл название товара на странице"}
        logger.info(f"Распарсено с {site}: {data}")
        return data


scraper_service = ScraperService()
