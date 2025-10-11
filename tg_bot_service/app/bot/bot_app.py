import logging
from pathlib import Path
from typing import Dict, List, Optional
from app.core.http_client import http_client
from app.core.config import settings

from aiogram import Bot, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

class ActionForm(StatesGroup):
    waiting_for_field = State()

START_BUTTON = "🔄 Старт"

def get_schema_loader():
    schema_cache: Dict = {}

    async def load_schema() -> Dict:
        if schema_cache:
            return schema_cache
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
    if not isinstance(node, Dict):
        return None
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
    if row:
        rows.append(row)
    if add_start:
        rows.append([KeyboardButton(text=START_BUTTON)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

async def start_button_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    item_names = await get_root_items() 
    kb = build_menu_keyboard(item_names)
    await state.update_data(current_node=await load_schema())
    await message.answer("Выберите пункт меню:", reply_markup=kb)
    return

async def start_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    item_names = await get_root_items()  
    if item_names:
        await state.update_data(current_node=await load_schema())
        kb = build_menu_keyboard(item_names)
        await message.answer("Выберите пункт меню:", reply_markup=kb)
    else:
        await message.answer("Схема меню пуста или не найдена.")

async def menu_handler(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    current_node = data.get("current_node", await load_schema())

    selected = find_item_by_name(message.text, current_node)
    if selected is not None:
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
        elif selected.get("type") == "action":
            await state.clear()
            await state.update_data(current_node=current_node)
            payload = selected.get("payload", {})
            if not payload:
                await message.answer(f"Действие '{selected.get('name')}' не требует ввода данных.")
                return
            fields = list(payload.keys())
            await state.update_data(
                action=selected,
                fields=fields,
                current_field=0,
                collected_data={}
            )
            await message.answer(
                f"Введите значение для поля '{fields[0]}' ({payload[fields[0]]['type']}):",
                reply_markup=build_menu_keyboard([], add_start=True)
            )
            await state.set_state(ActionForm.waiting_for_field)
            return

    await message.answer(f"Пункт '{message.text}' не найден в текущем меню. Попробуйте снова.")

async def process_action_field(message: Message, state: FSMContext) -> None:
    if not message.text:
        await message.answer("Пожалуйста, введите текстовое значение.")
        return

    data = await state.get_data()
    fields = data.get("fields", [])
    current_field_idx = data.get("current_field", 0)
    collected_data = data.get("collected_data", {})
    action = data.get("action", {})
    current_node = data.get("current_node", await load_schema())

    if current_field_idx >= len(fields):
        await message.answer("Все данные собраны!")
        response = f"Действие: {action.get('name')}\nСобранные данные: {collected_data}"
        await message.answer(response)
        item_names = await get_root_items()
        kb = build_menu_keyboard(item_names, add_start=True)
        await state.clear()
        await start_handler(message, state)

    current_field = fields[current_field_idx]
    collected_data[current_field] = message.text
    await state.update_data(collected_data=collected_data)

    next_field_idx = current_field_idx + 1
    if next_field_idx < len(fields):
        next_field = fields[next_field_idx]
        await state.update_data(current_field=next_field_idx)
        await message.answer(
            f"Введите значение для поля '{next_field}' ({action['payload'][next_field]['type']}):",
            reply_markup=build_menu_keyboard([], add_start=True)
        )
    else:
        await state.update_data(current_field=next_field_idx)
        await process_action_field(message, state)