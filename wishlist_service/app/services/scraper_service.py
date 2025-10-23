import logging
import re
import asyncio
import os
from typing import Optional, List

from app.services.selenium_downloader import selenium_downloader
from app.core.config import settings
from app.kafka.producer import kafka_producer
from app.utils.parsers import parse_aliexpress_data, parse_ozon_data, parse_wildberries_data, parse_yamarket_data

logger = logging.getLogger(__name__)

DOWNLOAD_PATH = "/app/downloads"

class ScraperService:
    def __init__(self):
        self.selenium_lock = asyncio.Lock()

    async def parse_url(self, source_url: str, user_id: int):
        logger.info(f"Фоновая задача для {source_url} ожидает доступа к Selenium...")
        async with self.selenium_lock:
            logger.info(f"Доступ к Selenium получен. Запуск синхронизации для {source_url}")
            try:
                async with selenium_downloader.get_driver_session() as driver:
                    user_agent = driver.execute_script("return navigator.userAgent;")

                    logger.info(f"Переход на страницу: {source_url}")
                    driver.get(source_url)

                    logger.info("Ожидание загрузки страницы (20 секунд)...")
                    await asyncio.sleep(20)
                    
                    logger.info(f"Получение HTML-кода страницы...")
                    HTML_FILE = os.path.join(DOWNLOAD_PATH, f"aaaaaaa.html")
                    os.makedirs(os.path.dirname(HTML_FILE), exist_ok=True)
                    html_content = driver.page_source
                    try:
                        with open(HTML_FILE, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                        
                        logger.info(f"HTML-код успешно сохранен в: {HTML_FILE}")

                    except IOError as e:
                        logger.error(f"Ошибка при сохранении HTML-кода в файл {HTML_FILE}: {e}")

                    if "ozon.ru" in source_url:
                        return parse_ozon_data(html_content)
                    elif "https://market.yandex.ru/" in source_url:
                        return parse_yamarket_data(html_content)
                    elif "www.wildberries.ru" in source_url:
                        return parse_wildberries_data(html_content)
                    elif "aliexpress.ru" in source_url:
                        return parse_aliexpress_data(html_content)
                    else:
                        logger.warning(f"Невозможно обработать ссылку {source_url}")

            except Exception as e:
                logger.error(f"Произошла ошибка во время работы с Selenium для {source_url}: {e}", exc_info=True)


scraper_service = ScraperService()