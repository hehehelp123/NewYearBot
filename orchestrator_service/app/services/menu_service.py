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
            settings.MUSIC_UPLOADER_SERVICE_URL,
        ]
        self.menu_tree = {}
        self.command_map = {}
        self.client = httpx.AsyncClient()

    async def _fetch_features(self, url: str):
        try:
            response = await self.client.get(f"{url}/api/v1/features")
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as e:
            logger.error(f"Failed to fetch features from {url}: {e}")
            return None

    def _process_item_for_command_map(self, item):
        if item.get("type") == "action" and "kafka_topic" in item:
            topic = item["kafka_topic"]
            command_path = topic.replace('.', '_')
            self.command_map[command_path] = topic

        if "items" in item:
            for sub_item in item["items"]:
                self._process_item_for_command_map(sub_item)

    def _process_item_for_bot_menu(self, item):
        if item.get("type") == "action" and "kafka_topic" in item:
            command_path = item["kafka_topic"].replace('.', '_')
            item["url"] = f"/api/v1/commands/{command_path}"
            item["method"] = "POST"

        if "items" in item:
            for sub_item in item["items"]:
                self._process_item_for_bot_menu(sub_item)

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
        logger.info("Building menu tree and command map...")
        tasks = [self._fetch_features(url) for url in self.service_urls]
        feature_responses = await asyncio.gather(*tasks)

        all_menu_parts = []
        all_command_parts = []

        for response in feature_responses:
            if not response:
                continue

            if isinstance(response, dict) and "menu" in response:
                all_menu_parts.append(response["menu"])
                all_command_parts.extend(response.get("commands", []))
            elif isinstance(response, list):
                all_menu_parts.append(response)

        self.command_map = {}
        for menu_part in all_menu_parts:
            for item in menu_part:
                self._process_item_for_command_map(item)
        for command in all_command_parts:
            self._process_item_for_command_map(command)

        self.menu_tree = self._merge_trees(all_menu_parts)

        self._process_item_for_bot_menu(self.menu_tree)

        logger.info(f"Menu tree built. Command map: {self.command_map}")
        return self.menu_tree, self.command_map

    async def close(self):
        await self.client.aclose()


menu_service = MenuService()