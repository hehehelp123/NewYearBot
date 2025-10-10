import asyncio
import httpx
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)


class MenuService:
    def __init__(self):
        self.service_urls = [
            settings.USER_SERVICE_URL,
            settings.WISHLIST_SERVICE_URL,
            settings.TICKET_SERVICE_URL,
        ]
        self.menu_tree = {}
        self.client = httpx.AsyncClient()

    async def _fetch_features(self, url: str):
        try:
            response = await self.client.get(f"{url}/api/v1/features")
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as e:
            logger.error(f"Failed to fetch features from {url}: {e}")
            return None

    def _merge_trees(self, tree_list: list):
        merged_tree = {"name": "Главное меню", "type": "menu", "items": []}
        menu_map = {}

        for tree in tree_list:
            if not tree:
                continue
            for item in tree:
                if item["type"] == "menu":
                    if item["name"] not in menu_map:
                        menu_map[item["name"]] = {"name": item["name"], "type": "menu", "items": []}
                        merged_tree["items"].append(menu_map[item["name"]])
                    menu_map[item["name"]]["items"].extend(item.get("items", []))
                else:
                    merged_tree["items"].append(item)
        return merged_tree

    async def build_menu_tree(self):
        logger.info("Building menu tree...")
        tasks = [self._fetch_features(url) for url in self.service_urls]
        feature_lists = await asyncio.gather(*tasks)

        self.menu_tree = self._merge_trees(feature_lists)
        logger.info("Menu tree built successfully.")
        return self.menu_tree

    def get_menu_tree(self):
        return self.menu_tree

    async def close(self):
        await self.client.aclose()


menu_service = MenuService()