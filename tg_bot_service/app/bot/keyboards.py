from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import List

from app.bot.constants import START_BUTTON, BACK_TO_WELCOME_BUTTON

def build_menu_keyboard(item_names: List[str], add_start: bool = True,
                        add_back_to_welcome: bool = False) -> ReplyKeyboardMarkup:
    rows = []
    row = []
    all_items = list(item_names)
    if add_start: all_items.append(START_BUTTON)
    if add_back_to_welcome: all_items.append(BACK_TO_WELCOME_BUTTON)
    for i, name in enumerate(all_items):
        row.append(KeyboardButton(text=name))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


async def welcome_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔑 Хочу WiFi", callback_data="info:wifi")
    builder.button(text="🆘 Кто тут у нас?", callback_data="info:admins")
    builder.button(text="🚀 Ну давай не томи", callback_data="info:go_to_main_menu")
    builder.adjust(2, 1)
    return builder.as_markup()