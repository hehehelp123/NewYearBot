import logging
import os
import asyncio
from urllib.parse import urlparse, parse_qs
import yt_dlp
from mutagen.mp3 import MP3
from mutagen.easyid3 import EasyID3

logger = logging.getLogger(__name__)

DOWNLOAD_PATH = "/app/downloads"
COOKIE_FILE = os.path.join(DOWNLOAD_PATH, "cookies_music.txt")

# больше за одну ссылку не тащим: плейлист на 300 треков — это час загрузки через Selenium
MAX_PLAYLIST_ITEMS = 25


class YtdlpLogger:
    def debug(self, msg):
        if "[debug]" in msg:
            logger.debug(msg)
        else:
            logger.info(msg)

    def warning(self, msg):
        logger.warning(msg)

    def error(self, msg):
        logger.error(msg)


def is_single_video_url(url: str) -> bool:
    """watch?v=…&list=RD… — это один трек, открытый из микса. Без этой проверки
    yt-dlp выкачивает весь микс (а у YouTube Music он бесконечный)."""
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if "youtube.com" in host or host == "youtu.be":
        return host == "youtu.be" or "v" in parse_qs(p.query) or p.path.startswith("/shorts/")
    return False


def human_download_error(error: str) -> str:
    e = error.lower()
    if "unsupported url" in e:
        return "этот сайт не поддерживается"
    if "drm" in e:
        return "трек защищён DRM, скачать его нельзя"
    if "private" in e or "sign in" in e or "login" in e or "members-only" in e:
        return "доступ к треку закрыт (приватный или нужен вход)"
    if "not available" in e or "unavailable" in e or "removed" in e or "404" in e:
        return "трек недоступен или удалён"
    if "too long" in e:
        return "файл слишком длинный"
    return "не удалось скачать трек"


async def download_audio(url: str, user_agent: str | None) -> tuple[list[tuple[str, str]], str | None]:
    """Возвращает ([(путь, название)], ошибка_для_пользователя)."""
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)

    def logging_progress_hook(d):
        if d['status'] == 'finished':
            logger.info("Скачивание завершено, начинается пост-обработка...")

    ydl_opts = {
        'format': 'bestaudio[ext=m4a]/bestaudio/best',
        'postprocessors': [
            {'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'},
            # исполнитель/название из метаданных источника — так трек нормально подписан в плейлисте
            {'key': 'FFmpegMetadata', 'add_metadata': True},
        ],
        'outtmpl': os.path.join(DOWNLOAD_PATH, '%(title).150B.%(ext)s'),
        'noplaylist': is_single_video_url(url),
        'playlistend': MAX_PLAYLIST_ITEMS,
        'match_filter': yt_dlp.utils.match_filter_func("!is_live & duration <? 1800"),
        'progress_hooks': [logging_progress_hook],
        'ignoreerrors': 'only_download',
        'retries': 5,
        'fragment_retries': 5,
        'socket_timeout': 60,
        'logger': YtdlpLogger(),
        'source_address': '0.0.0.0',
    }

    if user_agent:
        ydl_opts['http_headers'] = {'User-Agent': user_agent}

    if os.path.exists(COOKIE_FILE) and os.path.getsize(COOKIE_FILE) > 0:
        ydl_opts['cookiefile'] = COOKIE_FILE

    logger.info(f"yt-dlp {yt_dlp.version.__version__}: скачивание {url}")
    files_before = set(os.listdir(DOWNLOAD_PATH))

    def sync_download():
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

    error = None
    try:
        await asyncio.to_thread(sync_download)
    except Exception as e:
        logger.error(f"Ошибка yt-dlp для {url}: {e}")
        error = human_download_error(str(e))

    new_mp3_files = sorted(f for f in set(os.listdir(DOWNLOAD_PATH)) - files_before if f.endswith('.mp3'))
    if not new_mp3_files:
        return [], error or "не удалось скачать трек (ничего не нашлось по ссылке или он длиннее 30 минут)"

    processed_files = []
    for filename in new_mp3_files:
        filepath = os.path.join(DOWNLOAD_PATH, filename)
        title, _ = os.path.splitext(filename)
        try:
            audio = MP3(filepath, ID3=EasyID3)
            if 'title' not in audio or not audio['title']:
                audio['title'] = title
                audio.save()
            artist = (audio.get('artist') or [None])[0]
            if artist and artist.lower() not in title.lower():
                title = f"{artist} — {audio['title'][0]}"
        except Exception as tag_error:
            logger.warning(f"Не удалось обработать теги для {filepath}: {tag_error}.")
        processed_files.append((filepath, title))

    return processed_files, None
