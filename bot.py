from __future__ import annotations

import os
import asyncio
import logging
import sys
from datetime import date
from typing import Optional

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

from config import SETTINGS
import db
from utils import normalize_phone, now_dt
import services
import keyboards
from texts import welcome_text, contacts_text, booking_created_text

BANNER_URL = "https://cdn-icons-png.flaticon.com/512/3202/3202926.png"

# !!! ВСТАВЬ СЮДА СВОЙ ID ЦИФРАМИ !!!
MY_ADMIN_ID = 1517303554  # <--- ЗАМЕНИ 0 НА СВОЙ ID

# --- FSM ---
class BookingFSM(StatesGroup):
    choosing_date = State()
    waiting_car = State()
    waiting_issue = State()
    waiting_phone = State()

# --- HELPERS ---
def get_admin_id() -> int:
    return MY_ADMIN_ID

def is_admin(user_id: int) -> bool:
    return user_id == MY_ADMIN_ID

async def delete_msg(message: Message):
    try: await message.delete()
    except: pass

async def safe_edit(message: Message, text: str, reply_markup=None):
    try: await message.edit_text(text, reply_markup=reply_markup)
    except: pass

async def go_home(message: Message | CallbackQuery, state: FSMContext):
    await state.clear()
    load = await db.get_shop_load(now_dt().date())
    txt = welcome_text(load)
    kb = keyboards.main_menu_kb(is_admin(message.from_user.id))
    
    if isinstance(message, CallbackQuery):
        try: await message.message.edit_caption(caption=txt, reply_markup=kb)
        except: 
            await message.message.delete()
            await message.message.answer_photo(BANNER_URL, caption=txt, reply_markup=kb)
    else:
        await message.answer_photo(BANNER_URL, caption=txt, reply_markup=kb)

# --- COMMANDS ---
async def cmd_start(message: Message, state: FSMContext):
    # Если ты не вписал ID, бот подскажет его
    if MY_ADMIN_ID == 0:
        await message.answer(f"⚠️ <b>ВНИМАНИЕ:</b> Админ ID не настроен.\nВаш ID: <code>{message.from_user.id}</code>\nВпишите его в bot.py в строку 33.")
    
    await state.clear()
    load = await db.get_shop_load(now_dt().date())
    await message.answer_photo(BANNER_URL, caption=welcome_text(load), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
    await delete_msg(message)

async def cmd_menu(message: Message, state: FSMContext):
    await delete_msg(message)
    await go_home(message, state)

async def cmd_admin(message: Message):
    await delete_msg(message)
    if not is_admin(message.from_user.id): return
    await message.answer("🛠 <b>Панель администратора</b>", reply_markup=keyboards.admin_menu_kb())

async def cmd_id(message: Message):
    await message.answer(f"ID: <code>{message.from_user.id}</code>")

# --- MENU CALLBACKS ---
async def menu_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()
    if call.data == "menu:home": await go_home(call, state)
    
    elif call.data == "menu:contacts":
        try: await call.message.edit_caption(caption=contacts_text(), reply_markup=keyboards.contacts_kb())
        except: pass

    elif call.data == "menu:admin":
        if is_admin(call.from_user.id):
            await safe_edit(call.message, "🛠 <b>Админка</b>", reply_markup=keyboards.admin_menu_kb())
            
    elif call.data == "menu:book_static":
        # ГЕНЕРАЦИЯ КАЛЕНДАРЯ
        await state.set_state(BookingFSM.choosing_date)
        await state.update_data(mode="static")
        
        # Получаем дни
        items = await services.get_month_calendar()
        
        # Показываем клавиатуру
        await call.message.answer("📅 <b>Выберите свободную дату:</b>\n(✅ - свободно, ❌ - занято)", reply_markup=keyboards.dates_kb(items))
        # Можно удалить старое сообщение с фото, чтобы не мешало
        await delete_msg(call.message)

    elif call.data == "menu:book_live":
        today = now_dt().date()
        await state.set_state(BookingFSM.waiting_car)
        await state.update_data(mode="live", day=today.isoformat())
        await call.message.answer("⚡ <b>Срочный заезд</b>\n━━━━━━━━━━━━━━━━━━\n\n🚗 <b>Напишите марку и год авто:</b>", reply_markup=keyboards.inline_home_kb())
        await delete_msg(call.message)

# --- CALENDAR CALLBACK ---
async def date_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()
    # format: date:YYYY-MM-DD
    day_s = call.data.split(":")[1]
    day = date.fromisoformat(day_s)
    
    # Проверка доступности
    ok, reason = await db.is_day_available(day)
    if not ok:
        await call.answer(f"⛔ {reason}", show_alert=True)
        return

    await state.update_data(day=day_s)
    await state.set_state(BookingFSM.waiting_car)
    await safe_edit(call.message, f"📅 Выбрано: <b>{day.strftime('%d.%m.%Y')}</b>\n\n🚗 <b>Напишите марку и год авто:</b>", reply_markup=keyboards.inline_home_kb())

# --- BOOKING FLOW ---
async def car_handler(message: Message, state: FSMContext):
    await delete_msg(message)
    car = message.text.strip()
    await state.update_data(car_text=car)
    await state.set_state(BookingFSM.waiting_issue)
    await message.answer(f"🚗 <b>{car}</b>\n\n🛠 <b>Опишите проблему:</b>", reply_markup=keyboards.reply_nav_kb())

async def issue_handler(message: Message, state: FSMContext):
    await delete_msg(message)
    issue = message.text.strip()
    await state.update_data(issue_text=issue)
    await state.set_state(BookingFSM.waiting_phone)
    await message.answer(f"🛠 <b>Проблема принята.</b>\n\n📞 <b>Отправьте ваш телефон:</b>", reply_markup=keyboards.reply_nav_kb(with_contact=True))

async def phone_handler(message: Message, state: FSMContext, bot: Bot):
    if not message.contact: await delete_msg(message)
    phone = normalize_phone(message.contact.phone_number if message.contact else message.text)
    if not phone:
        msg = await message.answer("⛔ Неверный формат номера.")
        await asyncio.sleep(2)
        await msg.delete()
        return
    
    data = await state.get_data()
    day = date.fromisoformat(data['day'])
    
    if data['mode'] == 'static':
        try:
            bid, seq = await db.add_static_booking(day, message.from_user.id, message.from_user.full_name, phone, data['car_text'], data['issue_text'])
            await message.answer(booking_created_text(day, seq, data['car_text'], data['issue_text'], phone), reply_markup=keyboards.inline_home_kb())
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=keyboards.inline_home_kb())
    else:
        # Live
        try:
            bid, seq = await db.add_live_booking_today(day, message.from_user.id, message.from_user.full_name, phone, data['car_text'], data['issue_text'])
            await message.answer(f"⚡ Заявка №{seq} создана!\nЖдите вызова.", reply_markup=keyboards.inline_home_kb())
        except Exception as e:
            await message.answer(f"Ошибка: {e}", reply_markup=keyboards.inline_home_kb())

    await state.clear()

# --- CLIENT & ADMIN ---
async def client_cb(call: CallbackQuery, state: FSMContext, bot: Bot):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) < 3: return
    action = parts[1]
    bid = int(parts[2])
    
    if action == "arrived":
        ok, msg = await services.client_arrived(bot, get_admin_id(), bid, call.from_user.id)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
    elif action == "offer_yes":
        ok, msg = await services.client_offer_yes(bot, bid, call.from_user.id)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())

async def review_cb(call: CallbackQuery, bot: Bot):
    await call.answer()
    parts = call.data.split(":")
    if len(parts) < 3: return
    stars = int(parts[1])
    bid = int(parts[2])
    await services.handle_review_stars(bot, get_admin_id(), bid, stars, call.from_user.id)
    await safe_edit(call.message, "Спасибо за отзыв!", reply_markup=keyboards.inline_home_kb())

async def admin_cb(call: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(call.from_user.id): return
    await call.answer()
    parts = call.data.split(":")
    action = parts[1]
    
    if action == "auto_now":
        await services.auto_fill_to_max(bot, get_admin_id(), reason="Кнопка")
        await call.message.answer("🚀 Автодобор запущен!")
    elif action == "done" and len(parts) > 2:
        bid = int(parts[2])
        ok, msg = await services.admin_done(bot, get_admin_id(), bid)
        await safe_edit(call.message, msg, reply_markup=keyboards.admin_menu_kb())
    elif action == "accept" and len(parts) > 2:
        bid = int(parts[2])
        ok, msg = await services.admin_force_accept(bot, get_admin_id(), bid)
        await safe_edit(call.message, msg, reply_markup=keyboards.admin_menu_kb())

# --- MAIN ---
async def main():
    load_dotenv()
    token = os.getenv("BOT_TOKEN")
    if not token: 
        logger.error("NO TOKEN")
        return
    
    bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_id, Command("id"))
    dp.message.register(cmd_menu, Command("menu"))
    dp.message.register(cmd_admin, Command("admin"))
    
    dp.message.register(car_handler, BookingFSM.waiting_car)
    dp.message.register(issue_handler, BookingFSM.waiting_issue)
    dp.message.register(phone_handler, BookingFSM.waiting_phone)
    
    dp.callback_query.register(menu_cb, F.data.startswith("menu:"))
    dp.callback_query.register(date_cb, F.data.startswith("date:")) # ВАЖНО: Календарь
    dp.callback_query.register(client_cb, F.data.startswith("cli:"))
    dp.callback_query.register(review_cb, F.data.startswith("review:"))
    dp.callback_query.register(admin_cb, F.data.startswith("adm:"))

    await db.init_db()
    services.start_background_tasks(bot, get_admin_id())
    logger.info("BOT STARTED")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
