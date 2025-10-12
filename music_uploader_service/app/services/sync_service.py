import logging
import re
import asyncio
from typing import Optional, List

from app.services.downloader import download_audio
from app.services.selenium_uploader import selenium_uploader
from yandex_music import ClientAsync, Playlist, Track
from yandex_music.utils.difference import Difference
from app.core.config import settings
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)


class YandexMusicManager:
    def __init__(self, token: str, user_login: str):
        self.client = ClientAsync(token)
        self.user_login = user_login
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        try:
            await self.client.init()
            self._initialized = True
            logger.info("Клиент Яндекс Музыки успешно инициализирован.")
        except Exception as e:
            logger.error(f"Не удалось инициализировать клиент Яндекс Музыки: {e}")
            raise

    async def find_playlist_by_name(self, playlist_name: str) -> Optional[Playlist]:
        try:
            playlists = await self.client.users_playlists_list(user_id=self.user_login)
            for playlist in playlists:
                if playlist.title == playlist_name:
                    return playlist
            return None
        except Exception as e:
            logger.error(f"Ошибка при поиске плейлиста: {e}")
            return None

    async def add_tracks_to_playlist(self, playlist: Playlist, tracks: List[Track]) -> bool:
        if not tracks:
            return True
        try:
            diff = Difference()
            tracks_to_insert = [{'id': track.id, 'album_id': track.albums[0].id} for track in tracks if track.albums]
            if not tracks_to_insert:
                logger.warning("Нет треков для добавления (возможно, отсутствуют альбомы).")
                return False

            diff.add_insert(at=playlist.track_count, tracks=tracks_to_insert)
            await self.client.users_playlists_change(
                kind=playlist.kind, diff=diff.to_json(), revision=playlist.revision, user_id=self.user_login
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка при пакетном добавлении треков: {e}")
            return False


class MusicSyncService:
    def __init__(self):
        self.yandex_manager = YandexMusicManager(
            token=settings.YANDEX_MUSIC_TOKEN,
            user_login=settings.YANDEX_USER_LOGIN
        )
        self.selenium_lock = asyncio.Lock()

    async def start(self):
        await self.yandex_manager.initialize()
        logger.info("MusicSyncService запущен и готов к работе.")

    def stop(self):
        logger.info("MusicSyncService остановлен.")

    async def _sync_with_selenium(self, source_url: str, user_id: int):
        logger.info(f"Фоновая задача для {source_url} ожидает доступа к Selenium...")
        async with self.selenium_lock:
            logger.info(f"Доступ к Selenium получен. Запуск синхронизации для {source_url}")
            try:
                async with selenium_uploader.get_driver_session() as driver:
                    user_agent = driver.execute_script("return navigator.userAgent;")
                    await selenium_uploader.export_cookies(driver)

                    downloaded_data = await download_audio(source_url, user_agent)
                    if not downloaded_data:
                        logger.error(f"Фоновая задача: не удалось скачать аудио с {source_url}")
                        await kafka_producer.send("selenium.upload.failed",
                                                  {"user_id": user_id, "telegram_id": user_id, "source_url": source_url,
                                                   "reason": "Download failed"})
                        return

                    for path, title in downloaded_data:
                        success = await selenium_uploader.upload_track(driver, path)
                        event_payload = {"user_id": user_id, "telegram_id": user_id, "title": title,
                                         "source_url": source_url}
                        if success:
                            logger.info(f"Фоновая задача: трек '{title}' успешно загружен через Selenium.")
                            await kafka_producer.send("selenium.upload.success", event_payload)
                        else:
                            logger.error(f"Фоновая задача: ошибка при загрузке '{title}' через Selenium.")
                            await kafka_producer.send("selenium.upload.failed",
                                                      {"reason": "Selenium upload error", **event_payload})
            except Exception as e:
                logger.critical(f"Критическая ошибка в задаче синхронизации Selenium: {e}", exc_info=True)
                await kafka_producer.send("selenium.upload.failed",
                                          {"user_id": user_id, "telegram_id": user_id, "source_url": source_url,
                                           "reason": "Critical error in sync task"})
        logger.info(f"Задача для {source_url} завершена, доступ к Selenium освобожден.")

    async def _sync_with_yandex_api(self, source_url: str) -> dict:
        logger.info(f"Запуск синхронизации через Yandex API для {source_url}")

        destination_playlist = await self.yandex_manager.find_playlist_by_name(
            settings.YANDEX_DESTINATION_PLAYLIST_NAME)
        if not destination_playlist:
            return {"status": "error", "detail": "Плейлист назначения не найден."}

        track_match = re.search(r"music\.yandex\.ru/album/(\d+)/track/(\d+)", source_url)
        user_playlist_match = re.search(r"music\.yandex\.ru/users/(.+)/playlists/(\d+)", source_url)
        editorial_playlist_match = re.search(r"music\.yandex\.ru/playlists/", source_url)

        source_playlist = None
        source_tracks = []

        if track_match:
            album_id, track_id = track_match.groups()
            source_tracks = await self.yandex_manager.client.tracks([f"{track_id}:{album_id}"])
        elif user_playlist_match:
            owner, kind = user_playlist_match.groups()
            source_playlist = await self.yandex_manager.client.users_playlists(kind, owner)
        elif editorial_playlist_match:
            try:
                chart_info = await self.yandex_manager.client.chart()
                if chart_info and chart_info.chart:
                    source_playlist = chart_info.chart
            except Exception as e:
                logger.error(f"Не удалось получить чарт: {e}")

        if source_playlist and not source_tracks:
            if hasattr(source_playlist, 'tracks') and source_playlist.tracks:
                source_tracks = [t.track for t in source_playlist.tracks if t.track]
            elif hasattr(source_playlist, 'track_ids'):
                source_tracks = await self.yandex_manager.client.tracks(source_playlist.track_ids)

        if not source_tracks:
            return {"status": "error", "detail": "URL не распознан или не удалось получить треки."}

        await destination_playlist.fetch_tracks_async()
        dest_track_ids = {track.track.id for track in destination_playlist.tracks if track.track}

        tracks_to_add = [track for track in source_tracks if track and track.id not in dest_track_ids]

        if not tracks_to_add:
            return {"status": "success", "added_tracks": 0, "detail": "Синхронизация не требуется."}

        success = await self.yandex_manager.add_tracks_to_playlist(destination_playlist, tracks_to_add)

        if success:
            event_payload = {
                "source_url": source_url,
                "added_count": len(tracks_to_add),
                "destination_playlist": destination_playlist.title
            }
            await kafka_producer.send("yandex_music_synced", event_payload)
            return {"status": "success", "added_tracks": len(tracks_to_add)}
        else:
            return {"status": "error", "detail": "Ошибка при добавлении треков."}


music_sync_service = MusicSyncService()