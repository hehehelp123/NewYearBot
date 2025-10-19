import logging
import os
import time
from pprint import pformat
import yt_dlp
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3

logger = logging.getLogger(__name__)

DOWNLOAD_PATH = "/app/downloads"
COOKIE_FILE = os.path.join(DOWNLOAD_PATH, "cookies.txt")

# Создаем кастомный логгер для yt-dlp, чтобы он использовал наш стандартный логгинг
class YtdlpLogger:
    def debug(self, msg):
        logger.debug(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)


async def download_audio(url: str, user_agent: str | None) -> list[tuple[str, str]]:
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)
        logger.info(f"Создана директория для скачивания: {DOWNLOAD_PATH}")

    def logging_progress_hook(d):
        if d['status'] == 'downloading':
            total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
            if total_bytes:
                percentage = d['downloaded_bytes'] * 100 / total_bytes
                logger.info(f"Скачивание... {percentage:.1f}% из {total_bytes / (1024*1024):.1f}МБ")
        elif d['status'] == 'finished':
            logger.info("Скачивание завершено, начинается пост-обработка...")
        elif d['status'] == 'error':
            logger.error("Ошибка в хуке yt-dlp.")

        # Уровень DEBUG для полного словаря, чтобы не засорять логи по умолчанию
        logger.debug(f"Полный словарь хука: {pformat(d)}")

    ydl_opts = {
        'format': 'bestaudio[ext=m4a]/bestaudio/best',
        'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}],
        'outtmpl': os.path.join(DOWNLOAD_PATH, '%(title)s.%(ext)s'),
        'noplaylist': False,
        'progress_hooks': [logging_progress_hook],
        'sleep_interval': 3,
        'max_sleep_interval': 10,
        'ignoreerrors': False, # Изменено на False, чтобы yt-dlp пробрасывал ошибки
        'retries': 10,
        'fragment_retries': 10,
        'socket_timeout': 120, # Увеличили до 2 минут
        'verbose': True,       # <--- ГЛАВНОЕ ИЗМЕНЕНИЕ: Включаем подробный режим
        'logger': YtdlpLogger(), # <--- Используем наш логгер
    }

    if user_agent:
        ydl_opts['http_headers'] = {'User-Agent': user_agent}

    if os.path.exists(COOKIE_FILE) and os.path.getsize(COOKIE_FILE) > 0:
        ydl_opts['cookiefile'] = COOKIE_FILE
        logger.info("Используется файл cookies для скачивания.")
    else:
        logger.warning("Файл cookies.txt не найден или пуст. Скачивание будет произведено без cookies.")

    try:
        logger.info(f"Используется yt-dlp версии: {yt_dlp.version.__version__}")

        files_before = set(os.listdir(DOWNLOAD_PATH))
        logger.info(f"Файлы в директории до скачивания: {files_before or 'пусто'}")

        logger.info(f"Начало скачивания медиа из {url}...")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        time.sleep(1)

        files_after = set(os.listdir(DOWNLOAD_PATH))
        logger.info(f"Файлы в директории после скачивания: {files_after}")

        new_files = files_after - files_before
        logger.info(f"Обнаружены новые файлы: {new_files or 'нет'}")

        new_mp3_files = [f for f in new_files if f.endswith('.mp3')]

        if not new_mp3_files:
            logger.error("Скачивание завершилось, но новые .mp3 файлы не найдены в директории.")
            return []

        logger.info(f"Найдены новые .mp3 файлы: {new_mp3_files}")

        processed_files = []
        for filename in new_mp3_files:
            filepath = os.path.join(DOWNLOAD_PATH, filename)
            title, _ = os.path.splitext(filename)
            try:
                audio = MP3(filepath, ID3=EasyID3)
                if 'title' not in audio or not audio['title']:
                    audio['title'] = title
                    audio.save()
                    logger.info(f"В файл '{filename}' добавлен тег title: '{title}'")
                processed_files.append((filepath, title))
            except Exception as tag_error:
                logger.warning(
                    f"Не удалось обработать теги для {filepath}: {tag_error}. Используется оригинальное название.")
                processed_files.append((filepath, title))

        return processed_files

    except Exception as e:
        logger.error(f"Критическая ошибка при скачивании аудио: {e}", exc_info=True)
        return []