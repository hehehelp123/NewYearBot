import logging
import os
import asyncio
import http.cookiejar
from contextlib import asynccontextmanager

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException

from app.core.config import settings

DOWNLOAD_PATH = "/app/downloads"
COOKIE_FILE = os.path.join(DOWNLOAD_PATH, "cookies.txt")

logger = logging.getLogger(__name__)


class SeleniumDownloader:

    @asynccontextmanager
    async def get_driver_session(self):
        driver: webdriver.Remote | None = None
        logger.info("Запрос новой сессии драйвера Selenium...")
        options = FirefoxOptions()
        options.profile = "/home/seluser/.mozilla/firefox/profile1.default"
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        try:
            driver = webdriver.Remote(
                command_executor=settings.SELENIUM_URL,
                options=options
            )
            logger.info("Сессия Selenium успешно создана.")
            yield driver
        except WebDriverException as e:
            logger.error(f"Не удалось создать сессию Selenium: {e}", exc_info=True)
            raise
        finally:
            if driver:
                driver.quit()
                logger.info("Сессия Selenium закрыта.")


    async def run_cookie_export_background(self):
        logger.info("BACKGROUND TASK: Starting cookie export...")
        try:
            async with self.get_driver_session() as driver:
                await self.export_cookies(driver)
            logger.info("BACKGROUND TASK: Cookie export finished successfully.")
        except Exception as e:
            logger.error(f"BACKGROUND TASK: Cookie export failed: {e}", exc_info=True)

    async def export_cookies(self, driver: webdriver.Remote):
        logger.info("Начало стандартизированного экспорта cookies...")

        cookie_jar = http.cookiejar.MozillaCookieJar()

        all_cookies_dicts = []
        domains_to_export = ["https://www.ozon.ru/", "https://market.yandex.ru/", "https://www.wildberries.ru", "https://aliexpress.ru"]

        for url in domains_to_export:
            try:
                driver.get(url)
                await asyncio.sleep(2)
                cookies_for_domain = driver.get_cookies()
                logger.info(f"Найдено {len(cookies_for_domain)} cookies для домена {url}")
                all_cookies_dicts.extend(cookies_for_domain)
            except Exception as e:
                logger.warning(f"Не удалось получить cookies для {url}: {e}")

        unique_cookies_dicts = {f"{c.get('domain', '')}_{c.get('name', '')}": c for c in all_cookies_dicts}.values()

        if not unique_cookies_dicts:
            logger.warning("Не найдено ни одного cookie для экспорта.")
            return

        for cookie_dict in unique_cookies_dicts:
            if not (cookie_dict.get('name') and cookie_dict.get('value')):
                continue

            new_cookie = http.cookiejar.Cookie(
                version=0,
                name=cookie_dict['name'],
                value=cookie_dict['value'],
                port=None,
                port_specified=False,
                domain=cookie_dict.get('domain', ''),
                domain_specified=bool(cookie_dict.get('domain')),
                domain_initial_dot=cookie_dict.get('domain', '').startswith('.'),
                path=cookie_dict.get('path', '/'),
                path_specified=bool(cookie_dict.get('path')),
                secure=cookie_dict.get('secure', False),
                expires=cookie_dict.get('expiry'),
                discard=False,
                comment=None,
                comment_url=None,
                rest={'HttpOnly': None},
                rfc2109=False,
            )
            cookie_jar.set_cookie(new_cookie)

        os.makedirs(os.path.dirname(COOKIE_FILE), exist_ok=True)
        cookie_jar.save(COOKIE_FILE, ignore_discard=True, ignore_expires=False)
        logger.info(f"Cookies успешно и стандартизированно сохранены в файл: {COOKIE_FILE}")

selenium_downloader = SeleniumDownloader()