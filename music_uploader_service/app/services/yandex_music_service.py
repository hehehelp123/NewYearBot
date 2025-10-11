import logging
import re
from typing import Optional, List

from yandex_music import ClientAsync, Playlist, Track
from yandex_music.utils.difference import Difference
from app.core.config import settings
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)


class YandexMusicManager:
    def __init__(self, token: str, user_login: str):
        self.client = ClientAsync(token)
        self.user_login = user_login

    async def initialize(self) -> None:
        await self.client.init()
        logger.info("Клиент Яндекс Музыки успешно инициализирован")

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


class YandexMusicSyncService:
    def __init__(self):
        self.manager = YandexMusicManager(
            token=settings.YANDEX_MUSIC_TOKEN,
            user_login=settings.YANDEX_USER_LOGIN
        )

    async def start(self):
        await self.manager.initialize()
        # Проверяем наличие плейлиста при старте, но не кэшируем его надолго
        destination_playlist = await self.manager.find_playlist_by_name(settings.YANDEX_DESTINATION_PLAYLIST_NAME)
        if not destination_playlist:
            raise RuntimeError(f"Плейлист назначения '{settings.YANDEX_DESTINATION_PLAYLIST_NAME}' не найден.")
        logger.info("YandexMusicSyncService готов к работе.")

    async def sync_from_url(self, source_url: str) -> dict:
        # Получаем актуальную версию плейлиста перед КАЖДОЙ операцией
        destination_playlist = await self.manager.find_playlist_by_name(settings.YANDEX_DESTINATION_PLAYLIST_NAME)
        if not destination_playlist:
            return {"status": "error", "detail": "Плейлист назначения не найден."}

        source_url_str = str(source_url)
        track_match = re.search(r"music\.yandex\.ru/album/(\d+)/track/(\d+)", source_url_str)
        user_playlist_match = re.search(r"music\.yandex\.ru/users/(.+)/playlists/(\d+)", source_url_str)
        editorial_playlist_match = re.search(r"music\.yandex\.ru/playlists/", source_url_str)

        source_playlist = None
        source_tracks = []

        if track_match:
            album_id, track_id = track_match.groups()
            source_tracks = await self.manager.client.tracks([f"{track_id}:{album_id}"])
        elif user_playlist_match:
            owner, kind = user_playlist_match.groups()
            source_playlist = await self.manager.client.users_playlists(kind, owner)
        elif editorial_playlist_match:
            try:
                chart_info = await self.manager.client.chart()
                if chart_info and chart_info.chart:
                    source_playlist = chart_info.chart
            except Exception as e:
                logger.error(f"Не удалось получить чарт: {e}")

        if source_playlist and not source_tracks:
            if hasattr(source_playlist, 'tracks') and source_playlist.tracks:
                source_tracks = [t.track for t in source_playlist.tracks if t.track]
            elif hasattr(source_playlist, 'track_ids'):
                source_tracks = await self.manager.client.tracks(source_playlist.track_ids)

        if not source_tracks:
            return {"status": "error", "detail": "URL не распознан или не удалось получить треки."}

        await destination_playlist.fetch_tracks_async()
        dest_track_ids = {track.track.id for track in destination_playlist.tracks if track.track}

        tracks_to_add = [track for track in source_tracks if track and track.id not in dest_track_ids]

        if not tracks_to_add:
            return {"status": "success", "added_tracks": 0, "detail": "Синхронизация не требуется."}

        success = await self.manager.add_tracks_to_playlist(destination_playlist, tracks_to_add)

        if success:
            event_payload = {
                "source_url": source_url_str,
                "added_count": len(tracks_to_add),
                "destination_playlist": destination_playlist.title
            }
            await kafka_producer.send("yandex_music_synced", event_payload)
            return {"status": "success", "added_tracks": len(tracks_to_add)}
        else:
            return {"status": "error", "detail": "Ошибка при добавлении треков."}


yandex_music_sync_service = YandexMusicSyncService()