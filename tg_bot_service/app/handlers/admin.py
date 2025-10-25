import logging
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from app.kafka.producer import kafka_producer

logger = logging.getLogger(__name__)

async def handle_remove_user_confirm(query: CallbackQuery, state: FSMContext):
    user_id_to_remove = int(query.data.split(":")[-1])
    await state.update_data(user_id_to_remove=user_id_to_remove)
    await state.set_state(UserRemoval.confirming_delete)
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text=f"✅ Да, удалить {user_id_to_remove}",
                                     callback_data=f"remove_user_delete:{user_id_to_remove}"))
    builder.row(InlineKeyboardButton(text="❌ Нет", callback_data="remove_user_cancel"))
    await query.message.edit_text(f"Точно удалить {user_id_to_remove}?", reply_markup=builder.as_markup())
    await query.answer()


async def handle_remove_user_delete(query: CallbackQuery, state: FSMContext):
    user_id_to_remove = int(query.data.split(":")[-1])
    admin_id = query.from_user.id
    logger.info(f"Admin {admin_id} confirms removal of {user_id_to_remove}")
    payload = {"admin_id": admin_id, "target_user_id": user_id_to_remove}
    try:
        await kafka_producer.send("user.user.disallow_request", payload)
        await query.message.edit_text(f"Запрос на удаление {user_id_to_remove} отправлен.")
    except Exception as e:
        logger.error(f"Kafka error disallow_request: {e}")
        await query.message.edit_text("Ошибка отправки запроса.")
    await state.clear()
    await query.answer()


async def handle_remove_user_cancel(query: CallbackQuery, state: FSMContext):
    logger.info(f"User removal cancelled by {query.from_user.id}")
    await query.message.edit_text("Удаление отменено.")
    await state.clear()
    await query.answer("Отменено")