import logging
from aiogram import Bot
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.services.bot_service import bot_service

logger = logging.getLogger(__name__)


async def tickets_callback_handler(query: CallbackQuery, bot: Bot, state: FSMContext):
    logger.debug(f"CBQ: {query.data} from {query.from_user.id}")
    action, value = query.data.split(":", 1)
    ticket_id = int(value)
    user_id = query.from_user.id
    if action == "download_ticket":
        await query.answer("Ща поищу...")
        command_path = "ticket_download_request"
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action({"method": "POST", "url": f"/api/v1/commands/{command_path}"}, payload)
        logger.info(f"Download req sent for {ticket_id}")
    elif action == "delete_ticket":
        builder = InlineKeyboardBuilder()
        builder.button(text="Да, нахер билеты", callback_data=f"confirm_delete:{ticket_id}")
        builder.button(text="Не, погодь", callback_data=f"cancel_delete:{ticket_id}")
        await query.message.edit_text(f"Ты че, решил не приезжать с **{query.message.text.splitlines()[0]}**?",
                                      reply_markup=builder.as_markup(), parse_mode="MarkdownV2")
        await query.answer()
    elif action == "confirm_delete":
        await query.answer("Очистка!..")
        command_path = "ticket_delete_request"
        payload = {"telegram_id": user_id, "ticket_id": ticket_id}
        await bot_service.execute_action({"method": "POST", "url": f"/api/v1/commands/{command_path}"}, payload)
        await query.message.delete()
        logger.info(f"Delete req sent for {ticket_id}")
    elif action == "cancel_delete":
        await query.message.edit_text(query.message.text, entities=query.message.entities, reply_markup=None)
        await query.answer("Вернул как было!")