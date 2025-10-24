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
        except Exception as e: logger.error(f"Failed features from {url}: {e}"); return None

    def _process_item_for_command_map(self, item):
        if not isinstance(item, dict): return
        if item.get("type") == "action" and "kafka_topic" in item:
            topic = item["kafka_topic"]; command_path = topic.replace('.', '_')
            self.command_map[command_path] = topic
            logger.debug(f"Mapped {command_path} to {topic}")
        if "items" in item and isinstance(item["items"], list):
            for sub_item in item["items"]: self._process_item_for_command_map(sub_item)

    async def build_menu_tree(self):
        logger.info("Building menu tree...")
        tasks = [self._fetch_features(url) for url in self.service_urls]
        feature_responses = await asyncio.gather(*tasks)
        all_menu_parts, all_command_parts = [], []
        for response in feature_responses:
            if not response: continue
            if isinstance(response, dict):
                if "menu" in response: all_menu_parts.append({"items": response["menu"]})
                if "commands" in response: all_command_parts.extend(response["commands"])
            elif isinstance(response, list): all_menu_parts.append({"items": response})

        admin_menu_part = {
            "items": [
                {
                    "name": "🔑 Добавить юзера по ID", "type": "action",
                    "kafka_topic": "user.user.allow_request", "admin_only": True,
                    "payload": {"target_user_id": { "type": "integer", "description": "Telegram ID" }}
                },
                {
                    "name": "🚫 Удалить юзера", "type": "action",
                    "kafka_topic": "user.user.list_request", "admin_only": True,
                    "payload": {}
                }
            ]
        }
        all_menu_parts.append(admin_menu_part)

        self.command_map = {}
        for menu_part in all_menu_parts:
             if isinstance(menu_part.get("items"), list):
                for item in menu_part["items"]: self._process_item_for_command_map(item)
        for command in all_command_parts: self._process_item_for_command_map(command)

        self.menu_tree = self._merge_trees(all_menu_parts)
        logger.info(f"Menu built. Commands: {len(self.command_map)}.")
        return self.menu_tree, self.command_map

    def _merge_trees(self, trees: list) -> dict:
        merged_tree = {"items": []}; menu_map = {}
        for tree in trees:
             if not isinstance(tree, dict) or not isinstance(tree.get("items"), list): continue
             for item in tree["items"]:
                if not isinstance(item, dict) or "name" not in item: continue
                if item["name"] in menu_map:
                    existing = menu_map[item["name"]]
                    if existing.get("type") == "menu" and item.get("type") == "menu":
                        existing.setdefault("items", []).extend(item.get("items", []))
                    else: logger.warning(f"Duplicate item '{item['name']}' skip merge.")
                else: merged_tree["items"].append(item); menu_map[item["name"]] = item
        return merged_tree

    async def close(self): await self.client.aclose()

menu_service = MenuService()