import logging
import re
import asyncio
import os
import html as html_lib
from typing import Optional, List
from urllib.parse import urlparse

import aiohttp
from yandex_music import ClientAsync, Playlist, Track
from yandex_music.utils.difference import Difference

from app.services.downloader import download_audio, COOKIE_FILE
from app.services.selenium_uploader import selenium_uploader
from app.core.config import settings
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)

URL_RE = re.compile(r"https?://[^\s<>\"']+")
YANDEX_HOST_RE = re.compile(r"(^|\.)music\.yandex\.[a-z]+$")
# со стримингов с DRM скачать нельзя — берём название со страницы и ищем трек в каталоге Яндекса
SEARCH_BY_TITLE_HOSTS = ("open.spotify.com", "spotify.link", "music.apple.com", "zvuk.com",
                         "deezer.com", "deezer.page.link", "link.deezer.com")
UUID_RE = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower().removeprefix("www.").removeprefix("m.")


def _norm(s: str) -> str:
    s = (s or "").lower().replace("ё", "е")
    s = re.sub(r"\(.*?\)|\[.*?\]", " ", s)  # (feat. …), [Remastered]
    s = re.sub(r"\s[-–—]\s.*(remaster|version|edit|mix|live).*$", " ", s)
    return " ".join(re.findall(r"\w+", s))


def track_label(track: Track) -> str:
    artists = ", ".join(a.name for a in (track.artists or []) if a.name)
    return f"{artists} — {track.title}" if artists else (track.title or str(track.id))


class YandexMusicManager:
    def __init__(self, token: str, user_login: str):
        self.client = ClientAsync(token)
        self.user_login = user_login
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        await self.client.init()
        self._initialized = True
        logger.info("Клиент Яндекс Музыки успешно инициализирован.")

    async def find_playlist_by_name(self, playlist_name: str) -> Optional[Playlist]:
        try:
            playlists = await self.client.users_playlists_list(user_id=self.user_login)
            for playlist in playlists:
                if playlist.title == playlist_name:
                    # список отдаёт плейлист без треков — берём полный, со свежей ревизией
                    return await self.client.users_playlists(playlist.kind, self.user_login)
            return None
        except Exception as e:
            logger.error(f"Ошибка при поиске плейлиста: {e}")
            return None

    async def add_tracks_to_playlist(self, playlist: Playlist, items: List[dict]) -> bool:
        if not items:
            return True
        try:
            diff = Difference().add_insert(at=playlist.track_count, tracks=items)
            await self.client.users_playlists_change(
                kind=playlist.kind, diff=diff.to_json(), revision=playlist.revision, user_id=self.user_login
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка при пакетном добавлении треков: {e}")
            return False

    async def tracks_from_url(self, url: str) -> tuple[list[Track], Optional[str]]:
        """Все форматы ссылок: старые (album/X/track/Y, users/U/playlists/K) и нового веба
        (track/Y, album/X, playlists/<uuid>, playlists/lk.<uuid>)."""
        c = self.client
        path = urlparse(url).path.rstrip("/")

        if m := re.search(r"/album/(\d+)/track/(\d+)", path):
            album_id, track_id = m.groups()
            return list(await c.tracks([f"{track_id}:{album_id}"]) or []), None
        if m := re.search(r"/track/(\d+)$", path):
            return list(await c.tracks([m.group(1)]) or []), None
        if m := re.search(r"/album/(\d+)$", path):
            album = await c.albums_with_tracks(m.group(1))
            tracks = [t for volume in (album.volumes or []) for t in volume] if album else []
            for t in tracks:
                if not t.albums:
                    t.albums = [album]
            return tracks, None
        if m := re.search(r"/users/([^/]+)/playlists/(\d+)", path):
            owner, kind = m.groups()
            return await self._playlist_tracks(kind, owner), None
        if m := re.search(rf"/playlists?/(?:lk\.)?({UUID_RE})", path):
            data = await c._request.get(f"{c.base_url}/playlist/{m.group(1)}",
                                        params={"resumeStream": "false", "richTracks": "false"})
            owner = (data or {}).get("owner") or {}
            if not data or not data.get("kind") or not owner.get("uid"):
                return [], "плейлист не найден (возможно, он приватный)"
            return await self._playlist_tracks(data["kind"], owner["uid"]), None
        if "/artist/" in path:
            return [], "ссылки на исполнителя не поддерживаются — пришли трек, альбом или плейлист"
        return [], "не понял ссылку Яндекс Музыки — пришли трек, альбом или плейлист"

    async def _playlist_tracks(self, kind, owner) -> list[Track]:
        playlist = await self.client.users_playlists(kind, owner)
        if not playlist or not playlist.tracks:
            return []
        tracks = [ts.track for ts in playlist.tracks if ts.track]
        missing = [ts.track_id for ts in playlist.tracks if not ts.track]
        if missing:
            tracks += list(await self.client.tracks(missing) or [])
        return tracks

    async def search_track(self, query: str, artist_hint: str | None = None,
                           title_hint: str | None = None) -> Optional[Track]:
        result = await self.client.search(query, type_="track")
        candidates = (result.tracks.results if result and result.tracks else [])[:10]
        if not candidates:
            return None
        if not title_hint:
            return candidates[0]
        want_title, want_artist = _norm(title_hint), _norm(artist_hint or "")
        for t in candidates:
            got_title = _norm(t.title)
            got_artists = [_norm(a.name) for a in (t.artists or [])]
            title_ok = bool(got_title) and (got_title in want_title or want_title in got_title)
            artist_ok = not want_artist or any(a and (a in want_artist or want_artist in a) for a in got_artists)
            if title_ok and artist_ok:
                return t
        return None


class MusicSyncService:
    def __init__(self):
        self.yandex_manager = YandexMusicManager(
            token=settings.YANDEX_MUSIC_TOKEN,
            user_login=settings.YANDEX_USER_LOGIN
        )
        self.selenium_lock = asyncio.Lock()
        # изменения плейлиста идут по ревизии — параллельные вставки затирали бы друг друга
        self.playlist_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()

    async def start(self):
        await self.yandex_manager.initialize()
        logger.info("MusicSyncService запущен и готов к работе.")

    def stop(self):
        logger.info("MusicSyncService остановлен.")

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _fail(self, topic: str, user_id: int, source: str, reason: str, **extra):
        logger.warning(f"[{user_id}] {source}: {reason}")
        await kafka_producer.send(topic, {"user_id": user_id, "telegram_id": user_id,
                                          "source_url": source, "reason": reason, **extra})

    async def process_sync_request(self, payload: dict):
        text = (payload.get("source_url") or "").strip()
        user_id = payload.get("telegram_id")
        if not text or not user_id:
            logger.error(f"Некорректные данные для синхронизации: {payload}")
            return

        logger.info(f"Запрос на добавление музыки от {user_id}: {text}")
        url_match = URL_RE.search(text)
        if not url_match:
            # просто «Исполнитель — Название»: ищем в каталоге
            self._spawn(self._add_by_search(user_id, text, query=text))
            return

        url = url_match.group(0).rstrip(".,;)")
        host = _host(url)
        if YANDEX_HOST_RE.search(host):
            self._spawn(self._sync_with_yandex_api(url, user_id))
        elif any(host == h or host.endswith("." + h) for h in SEARCH_BY_TITLE_HOSTS):
            self._spawn(self._add_from_streaming_page(url, user_id))
        elif host in ("vk.com", "vk.ru") and re.search(r"/(audio|music)|[?&]z=audio|act=audio", url):
            await self._fail("yandex.upload.failed", user_id, url,
                             "музыку ВКонтакте без входа в аккаунт не скачать. Пришли ссылку на Яндекс, "
                             "YouTube или просто «Исполнитель — Название»")
        else:
            self._spawn(self._sync_with_selenium(url, user_id))

    # ---------- каталог Яндекса ----------

    async def _add_tracks(self, user_id: int, source: str, tracks: list[Track], matched_query: str | None = None):
        async with self.playlist_lock:
            destination = await self.yandex_manager.find_playlist_by_name(settings.YANDEX_DESTINATION_PLAYLIST_NAME)
            if not destination:
                await self._fail("yandex.upload.failed", user_id, source, "не найден общий плейлист")
                return
            existing = {str(ts.track.id if ts.track else ts.id) for ts in (destination.tracks or [])}
            seen, to_add, items = set(), [], []
            tracks = [t for t in tracks if t]
            for t in tracks:
                tid = str(t.id)
                if tid in existing or tid in seen or not t.albums:
                    continue
                seen.add(tid)
                to_add.append(t)
                items.append({"id": tid, "album_id": str(t.albums[0].id)})

            payload = {"user_id": user_id, "telegram_id": user_id, "source_url": source,
                       "destination_playlist": destination.title,
                       "added_count": len(to_add), "already_count": len(tracks) - len(to_add),
                       "titles": [track_label(t) for t in to_add][:15], "matched_query": matched_query}
            if not to_add:
                payload["titles"] = [track_label(t) for t in tracks][:15]
                await kafka_producer.send("yandex.upload.success", payload)
                return
            if await self.yandex_manager.add_tracks_to_playlist(destination, items):
                logger.info(f"[{user_id}] добавлено {len(to_add)} трек(ов) из {source}")
                await kafka_producer.send("yandex.upload.success", payload)
            else:
                await self._fail("yandex.upload.failed", user_id, source, "Яндекс не дал изменить плейлист, попробуй ещё раз")

    async def _sync_with_yandex_api(self, source_url: str, user_id: int):
        try:
            tracks, error = await self.yandex_manager.tracks_from_url(source_url)
        except Exception as e:
            logger.error(f"Ошибка Yandex API для {source_url}: {e}", exc_info=True)
            tracks, error = [], "не удалось получить треки по ссылке"
        if not tracks:
            await self._fail("yandex.upload.failed", user_id, source_url, error or "по ссылке не нашлось треков")
            return
        await self._add_tracks(user_id, source_url, tracks)

    async def _add_by_search(self, user_id: int, source: str, query: str,
                             artist: str | None = None, title: str | None = None):
        try:
            track = await self.yandex_manager.search_track(query, artist_hint=artist, title_hint=title)
        except Exception as e:
            logger.error(f"Ошибка поиска '{query}': {e}", exc_info=True)
            track = None
        if not track:
            await self._fail("yandex.upload.failed", user_id, source, f"в Яндекс Музыке не нашлось «{query}»")
            return
        await self._add_tracks(user_id, source, [track], matched_query=query)

    async def _add_from_streaming_page(self, url: str, user_id: int):
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "en"}) as s:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    final_url, page = str(r.url), await r.text()
        except Exception as e:
            logger.warning(f"Не открылась страница {url}: {e}")
            await self._fail("yandex.upload.failed", user_id, url, "не удалось открыть страницу трека")
            return

        def meta(prop):
            m = re.search(rf'<meta[^>]+property="og:{prop}"[^>]+content="([^"]*)"', page)
            return html_lib.unescape(m.group(1)).strip() if m else ""

        title, description = meta("title"), meta("description")
        path = urlparse(final_url).path
        if "spotify.com" in final_url:
            if "/track/" not in path:
                await self._fail("yandex.upload.failed", user_id, url,
                                 "со Spotify добавляю только отдельные треки, не альбомы и плейлисты")
                return
            artist = description.split(" · ")[0] if description else None  # «Wham! · LAST CHRISTMAS · Song · 1984»
        else:
            artist = None
            for sep in (" — ", " - ", " – ", " by "):
                if sep in title:
                    left, right = title.split(sep, 1)
                    title, artist = (right, left) if "zvuk" in final_url else (left, right)
                    break
        if not title:
            await self._fail("yandex.upload.failed", user_id, url, "не понял, что это за трек")
            return
        query = f"{artist} {title}" if artist else title
        await self._add_by_search(user_id, url, query=query, artist=artist, title=title)

    # ---------- скачивание + загрузка файлом ----------

    async def _playlist_count(self) -> int | None:
        pl = await self.yandex_manager.find_playlist_by_name(settings.YANDEX_DESTINATION_PLAYLIST_NAME)
        return pl.track_count if pl else None

    async def _wait_count_above(self, count: int | None, timeout: int = 90) -> bool:
        """Загруженный файл появляется в плейлисте не сразу — ждём, пока API его покажет."""
        if count is None:
            return False
        for _ in range(timeout // 5):
            current = await self._playlist_count()
            if current is not None and current > count:
                return True
            await asyncio.sleep(5)
        return False

    @staticmethod
    def _split_title(label: str) -> tuple[str | None, str]:
        for sep in (" — ", " - ", " – "):
            if sep in label:
                artist, title = label.split(sep, 1)
                return artist.strip(), title.strip()
        return None, label

    async def _catalog_fallback(self, titles: list[str]) -> list[Track]:
        """Файл в плейлист не попал — пробуем найти тот же трек в каталоге (строгое совпадение
        исполнителя и названия, чтобы не подсунуть кавер)."""
        found = []
        for label in titles:
            artist, title = self._split_title(label)
            if not artist:
                continue
            try:
                track = await self.yandex_manager.search_track(f"{artist} {_norm(title)}",
                                                               artist_hint=artist, title_hint=title)
            except Exception as e:
                logger.warning(f"Поиск в каталоге '{label}' не удался: {e}")
                track = None
            if track:
                found.append(track)
        return found

    async def _sync_with_selenium(self, source_url: str, user_id: int):
        logger.info(f"Фоновая задача для {source_url} ожидает доступа к Selenium...")
        async with self.selenium_lock:
            try:
                async with selenium_uploader.get_driver_session() as driver:
                    user_agent = await asyncio.to_thread(driver.execute_script, "return navigator.userAgent;")
                    if not os.path.exists(COOKIE_FILE) or os.path.getsize(COOKIE_FILE) == 0:
                        await selenium_uploader.export_cookies(driver)

                    downloaded, error = await download_audio(source_url, user_agent)
                    if not downloaded:
                        await self._fail("selenium.upload.failed", user_id, source_url, error)
                        return

                    not_ready = await selenium_uploader.check_ready(driver)
                    if not_ready:
                        logger.error(f"Загрузка файлов недоступна: {not_ready}")
                    uploaded, failed = [], []
                    for path, title in downloaded:
                        if not_ready:
                            failed.append(title)
                            if os.path.exists(path):
                                os.remove(path)
                            continue
                        before = await self._playlist_count()
                        sent = await selenium_uploader.upload_track(driver, path)
                        if sent and await self._wait_count_above(before):
                            uploaded.append(title)
                        else:
                            failed.append(title)

                if uploaded:
                    await kafka_producer.send("selenium.upload.success", {
                        "user_id": user_id, "telegram_id": user_id, "source_url": source_url,
                        "titles": uploaded, "failed_titles": failed, "title": uploaded[0]})
                if failed:
                    fallback = await self._catalog_fallback(failed)
                    if fallback:
                        await self._add_tracks(user_id, source_url, fallback, matched_query=", ".join(failed))
                    elif not uploaded:
                        reason = ("загрузка файлов сейчас не работает — на сервере слетел вход в Яндекс Музыку "
                                  "(напиши админу)") if not_ready == "login" else \
                            "скачал, но не смог загрузить в Яндекс Музыку, и в каталоге такого трека нет"
                        await self._fail("selenium.upload.failed", user_id, source_url, reason,
                                         failed_titles=failed)
            except Exception as e:
                logger.critical(f"Критическая ошибка в задаче синхронизации Selenium: {e}", exc_info=True)
                await self._fail("selenium.upload.failed", user_id, source_url, "внутренняя ошибка загрузчика")
        logger.info(f"Задача для {source_url} завершена, доступ к Selenium освобожден.")


music_sync_service = MusicSyncService()
