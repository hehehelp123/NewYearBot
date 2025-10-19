import logging
from typing import Dict, List, Optional

from aiogram import F, Bot
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, Document, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.core.config import settings
from app.kafka.producer import kafka_producer
from app.services.storage_service import storage_service
from app.services.bot_service import bot_service
from app.core.http_client import http_client


class ActionForm(StatesGroup):
    waiting_for_field = State()


START_BUTTON = "🔄 Старт"


def get_schema_loader():
    schema_cache: Dict = {}

    async def load_schema() -> Dict:
        if schema_cache: return schema_cache
        try:
            response = await http_client.client.get(f"{settings.ORCHESTRATOR_URL}/api/v1/menu")
            response.raise_for_status()
            schema_cache.update(response.json())
            return schema_cache
        except Exception as exc:
            logging.exception("Не удалось загрузить schema.json: %s", exc)
            schema_cache.clear()
            return schema_cache

    return load_schema


load_schema = get_schema_loader()


async def get_root_items() -> List[str]:
    schema = await load_schema()
    items = schema.get("items", [])
    return [item.get("name") for item in items if isinstance(item, Dict) and isinstance(item.get("name"), str)]


def find_item_by_name(target_name: str, node: Dict) -> Optional[Dict]:
    if not isinstance(node, Dict): return None
    children = node.get("items") or []
    for child in children:
        if isinstance(child, Dict) and child.get("name") == target_name:
            return child
    return None


def build_menu_keyboard(item_names: List[str], add_start: bool = False) -> ReplyKeyboardMarkup:
    row, rows = [], []
    for i, name in enumerate(item_names, start=1):
        row.append(KeyboardButton(text=name))
        if i % 2 == 0:
            rows.append(row)
            row = []
    if row: rows.append(row)
    if add_start: rows.append([KeyboardButton(text=START_BUTTON)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def start_button_handler(message: Message, state: FSMContext) -> None:
    if message.text == START_BUTTON:
        await state.clear()
        item_names = await get_root_items()
        kb = build_menu_keyboard(item_names)
        await state.update_data(current_node=await load_schema())
        await message.answer("Выберите пункт меню:", reply_markup=kb)
        return


async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = message.from_user
    if user:
        user_data = {"telegram_id": user.id, "username": user.username or user.full_name}
        await kafka_producer.send("user.user.create", user_data)
    item_names = await get_root_items()
    if item_names:
        await state.update_data(current_node=await load_schema())
        kb = build_menu_keyboard(item_names)
        await message.answer("Добро пожаловать! Выберите пункт меню:", reply_markup=kb)
    else:
        await message.answer("Схема меню пуста или не найдена.")


async def menu_handler(message: Message, state: FSMContext) -> None:
    if not message.text or not message.from_user: return
    data = await state.get_data()
    current_node = data.get("current_node", await load_schema())
    selected = find_item_by_name(message.text, current_node)
    if selected is None:
        await message.answer(f"Пункт '{message.text}' не найден. Попробуйте снова.")
        return

    if selected.get("type") == "menu":
        sub_items = selected.get("items") or []
        sub_names = [i.get("name") for i in sub_items if isinstance(i, Dict) and isinstance(i.get("name"), str)]
        if sub_names:
            await state.update_data(current_node=selected)
            kb = build_menu_keyboard(sub_names, add_start=True)
            await message.answer("Выберите пункт меню:", reply_markup=kb)
        else:
            await message.answer("В этом меню нет пунктов.")
        return

    if selected.get("type") == "action":
        await start_form_action(selected, message, state)


async def start_form_action(action: dict, message: Message, state: FSMContext):
    payload_schema = action.get("payload", {})
    fields = list(payload_schema.keys())

    if not fields:
        collected_data = {}
        if message.from_user:
            collected_data["telegram_id"] = message.from_user.id

        await message.answer("Выполняю запрос...")

        try:
            kafka_topic = action.get("kafka_topic")
            if not kafka_topic:
                raise ValueError("В схеме не указан kafka_topic для этого действия")

            await kafka_producer.send(kafka_topic, collected_data)
        except Exception as e:
            logging.error(f"Ошибка отправки в Kafka: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            await reset_to_main_menu(message, state)
    else:
        await state.set_state(ActionForm.waiting_for_field)
        await state.update_data(action=action, fields=fields, current_field_index=0, collected_data={})
        field_name = fields[0]
        field_info = payload_schema[field_name]
        await message.answer(
            f"Введите '{field_info['description']}' ({field_info['type']}):",
            reply_markup=build_menu_keyboard([], add_start=True)
        )


async def process_action_field(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    action = data["action"]
    fields = data["fields"]
    current_field_idx = data["current_field_index"]
    collected_data = data["collected_data"]
    current_field_name = fields[current_field_idx]
    field_info = action["payload"][current_field_name]
    field_type = field_info.get("type")

    if field_type == "file":
        if not message.document:
            await message.answer("Пожалуйста, прикрепите файл.")
            return
        doc = message.document
        await message.answer(f"Загружаю файл '{doc.file_name}' на сервер...")
        file_info = await bot.get_file(doc.file_id)
        file_bytes = await bot.download_file(file_info.file_path)

        try:
            object_name = storage_service.upload_file(file_bytes.read(), doc.file_name)
            collected_data[current_field_name] = object_name
            await message.answer("Файл успешно загружен.")
        except Exception as e:
            logging.error(f"Ошибка загрузки файла в MinIO: {e}", exc_info=True)
            await message.answer("Произошла ошибка при загрузке файла. Попробуйте еще раз.")
            return

    else:
        if not message.text:
            await message.answer("Пожалуйста, введите текст.")
            return
        collected_data[current_field_name] = message.text

    next_field_idx = current_field_idx + 1
    if next_field_idx < len(fields):
        await state.update_data(current_field_index=next_field_idx, collected_data=collected_data)
        next_field_name = fields[next_field_idx]
        next_field_info = action["payload"][next_field_name]
        await message.answer(f"Введите '{next_field_info['description']}' ({next_field_info['type']}):")
    else:
        if message.from_user: collected_data["telegram_id"] = message.from_user.id
        await message.answer("Все данные собраны! Отправляю запрос на обработку...")

        try:
            kafka_topic = action.get("kafka_topic")
            if not kafka_topic:
                raise ValueError("В схеме не указан kafka_topic для этого действия")

            await kafka_producer.send(kafka_topic, collected_data)
            await message.answer("Ваш запрос принят в обработку!")
        except Exception as e:
            logging.error(f"Ошибка отправки в Kafka: {e}", exc_info=True)
            await message.answer(f"Ошибка: {e}")
        finally:
            await reset_to_main_menu(message, state)


async def callback_query_handler(query: CallbackQuery, bot: Bot, state: FSMContext):
    action, value = query.data.split(":", 1)
    ticket_id = int(value)
    user_id = query.from_user.id

    if action == "download_ticket":
        await query.answer("Запрос на скачивание отправлен...")
        command_path = "ticket_download_request"
        action_schema = {"method": "POST", "url": f"/api/v1/commands/{command_path}"}
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action(action_schema, payload)

    elif action == "delete_ticket":
        builder = InlineKeyboardBuilder()
        builder.button(text="Да, удалить", callback_data=f"confirm_delete:{ticket_id}")
        builder.button(text="Нет, отмена", callback_data=f"cancel_delete:{ticket_id}")
        await query.message.edit_text(
            f"Вы уверены, что хотите удалить билет **{query.message.text.splitlines()[0]}**?",
            reply_markup=builder.as_markup(),
            parse_mode="MarkdownV2"
        )
        await query.answer()

    elif action == "confirm_delete":
        await query.answer("Запрос на удаление отправлен...")
        command_path = "ticket_delete_request"
        action_schema = {"method": "POST", "url": f"/api/v1/commands/{command_path}"}
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action(action_schema, payload)
        await query.message.delete()

    elif action == "cancel_delete":
        await query.message.edit_text(query.message.text, entities=query.message.entities, reply_markup=None)
        await query.answer("Удаление отменено.")


async def reset_to_main_menu(message: Message, state: FSMContext):
    await state.clear()
    item_names = await get_root_items()
    kb = build_menu_keyboard(item_names)
    await state.update_data(current_node=await load_schema())
    await message.answer("Выберите пункт меню:", reply_markup=kb)