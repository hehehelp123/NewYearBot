import logging
import os
import time
import http.cookiejar
from contextlib import asynccontextmanager

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException

from app.core.config import settings
from app.services.downloader import COOKIE_FILE, DOWNLOAD_PATH

logger = logging.getLogger(__name__)


class SeleniumUploader:

    @asynccontextmanager
    async def get_driver_session(self):
        driver: webdriver.Remote | None = None
        logger.info("Запрос новой сессии драйвера Selenium...")
        options = FirefoxOptions()
        options.add_argument("-profile")
        options.add_argument("/home/seluser/.mozilla/firefox/profile.default")
        options.add_argument("--headless")
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

    async def export_cookies(self, driver: webdriver.Remote):
        logger.info("Начало стандартизированного экспорта cookies...")

        cookie_jar = http.cookiejar.MozillaCookieJar()

        all_cookies_dicts = []
        domains_to_export = ["https://youtube.com", "https://music.yandex.ru", "https://yandex.ru"]

        for url in domains_to_export:
            try:
                driver.get(url)
                time.sleep(2)
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

    async def upload_track(self, driver: webdriver.Remote, track_path: str) -> bool:
        max_attempts = 3
        retry_delay = 1
        absolute_track_path = os.path.abspath(track_path)

        if not os.path.exists(absolute_track_path):
            logger.error(f"Файл для загрузки не найден по пути: {absolute_track_path}")
            return False

        try:
            logger.info(f"Открытие страницы плейлиста для загрузки: {settings.SELENIUM_PLAYLIST_URL}")
            driver.get(settings.SELENIUM_PLAYLIST_URL)

            for attempt in range(max_attempts):
                try:
                    wait = WebDriverWait(driver, 20)
                    upload_container_xpath = "//button[normalize-space()='Upload track']/ancestor::div[1]"
                    upload_container = wait.until(EC.presence_of_element_located((By.XPATH, upload_container_xpath)))
                    file_input = upload_container.find_element(By.CSS_SELECTOR, "input[type='file']")

                    driver.execute_script(
                        "arguments[0].style.opacity=1; arguments[0].style.transform='translate(0px, 0px) scale(1)'; "
                        "arguments[0].style.visibility='visible'; arguments[0].style.display='block';",
                        file_input
                    )

                    logger.info(f"Попытка {attempt + 1}/{max_attempts}: Отправка файла '{absolute_track_path}' в Selenium...")
                    file_input.send_keys(absolute_track_path)

                    loader_locator = (By.CSS_SELECTOR, ".file-loader_uploading")
                    wait.until(EC.presence_of_element_located(loader_locator))
                    logger.info("Процесс загрузки начался. Ожидание завершения...")
                    upload_wait = WebDriverWait(driver, 300)
                    upload_wait.until(EC.invisibility_of_element_located(loader_locator))
                    logger.info("Загрузка успешно завершена (индикатор исчез).")

                    time.sleep(5)
                    return True

                except StaleElementReferenceException:
                    logger.warning(
                        f"Попытка {attempt + 1}/{max_attempts} не удалась: StaleElementReferenceException. "
                        f"Элемент устарел. Повтор через {retry_delay} сек."
                    )
                    time.sleep(retry_delay)
                except TimeoutException:
                    logger.warning("Не удалось отследить индикатор загрузки. Возможно, трек уже загружен.")
                    time.sleep(5)
                    return True

            logger.error(f"Не удалось загрузить трек после {max_attempts} попыток.")
            return False

        except Exception as e:
            logger.error(f"Произошла критическая ошибка при загрузке через Selenium: {e}", exc_info=True)
            debug_path = DOWNLOAD_PATH
            screenshot_path = os.path.join(debug_path, f"selenium_debug_screenshot_{int(time.time())}.png")
            pagesource_path = os.path.join(debug_path, f"selenium_debug_pagesource_{int(time.time())}.html")

            try:
                logger.info(f"Текущий URL: {driver.current_url}")
                driver.save_screenshot(screenshot_path)
                logger.info(f"Скриншот сохранен в: {screenshot_path}")
                with open(pagesource_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logger.info(f"Исходный код страницы сохранен в: {pagesource_path}")
            except Exception as debug_e:
                logger.error(f"Не удалось сохранить отладочную информацию: {debug_e}")

            return False
        finally:
            if os.path.exists(track_path):
                try:
                    os.remove(track_path)
                    logger.info(f"Локальный файл '{track_path}' удален.")
                except OSError as e:
                    logger.error(f"Ошибка при удалении файла '{track_path}': {e}")


selenium_uploader = SeleniumUploader()