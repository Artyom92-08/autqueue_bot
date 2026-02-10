# bot.py
from __future__ import annotations

import os
import asyncio
from datetime import date, timedelta
from collections import defaultdict
from typing import Optional

import aiosqlite
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardRemove,
    BotCommand, MenuButtonCommands,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest

from config import SETTINGS
import db
from db import DB_PATH, KIND_LIVE
from utils import (
    is_admin as is_admin_fn,
    is_work_time, is_working_day, next_working_day,
    normalize_phone, parse_year,
    parse_arrival_minutes,
    format_day_label, now_dt, short,
)
import services
from texts import (
    welcome_text, contacts_text,
    booking_created_text, live_created_need_eta_text,
    duplicate_active_text, eta_request_text,
    live_no_today_suggest_text,
    time_passed_text,
    STATUS_VIEW,
)
import keyboards


# ===================== FSM =====================
class BookingFSM(StatesGroup):
    choosing_date = State()
    waiting_car = State()
    waiting_issue = State()
    waiting_phone = State()


class ETAFSM(StatesGroup):
    waiting_eta = State()  # ввод “когда подъедете” текстом
    # data: bid


class RescheduleFSM(StatesGroup):
    choosing_day = State()
    # data: bid


class AdminAddFSM(StatesGroup):
    choosing_date = State()
    waiting_name = State()
    waiting_phone = State()
    waiting_car = State()
    waiting_issue = State()


BOT_USERNAME: Optional[str] = None


# ===================== HELPERS =====================
def _project_root_env_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, ".env")


def get_admin_id() -> int:
    return int(os.getenv("ADMIN_ID", "0") or "0")


def is_admin(user_id: int) -> bool:
    return is_admin_fn(user_id, get_admin_id())


async def safe_edit(message: Message, text: str, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


async def setup_bot_menu(bot: Bot):
    commands = [
        BotCommand(command="start", description="Старт"),
        BotCommand(command="menu", description="Главное меню"),
        BotCommand(command="my", description="Мои записи"),
        BotCommand(command="contacts", description="Контакты"),
        BotCommand(command="admin", description="Админ‑панель (админ)"),
    ]
    try:
        await bot.set_my_commands(commands)
        await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception:
        pass


async def go_home(message: Message | CallbackQuery, state: FSMContext):
    await state.clear()
    if isinstance(message, CallbackQuery):
        await safe_edit(
            message.message,
            welcome_text(),
            reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id))
        )
    else:
        await message.answer("🏠 Главное меню:", reply_markup=ReplyKeyboardRemove())
        await message.answer(
            welcome_text(),
            reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id))
        )


async def build_dates_items(start: Optional[date] = None) -> list[tuple[date, bool]]:
    """
    Показываем SETTINGS.DAYS_AHEAD рабочих дней.
    """
    d = start or now_dt().date()
    out: list[tuple[date, bool]] = []
    while len(out) < SETTINGS.DAYS_AHEAD:
        if is_working_day(d):
            ok, _ = await db.is_day_available(d)
            out.append((d, ok))
        d += timedelta(days=1)
    return out


async def find_nearest_available_day(start: date, limit_days: int = 180) -> Optional[date]:
    d = start
    for _ in range(limit_days):
        if is_working_day(d):
            ok, _ = await db.is_day_available(d)
            if ok:
                return d
        d += timedelta(days=1)
    return None

from aiogram.exceptions import TelegramBadRequest

async def safe_answer(call: CallbackQuery, text: str | None = None, show_alert: bool = False):
    try:
        await call.answer(text, show_alert=show_alert)
    except TelegramBadRequest as e:
        if "query is too old" in str(e) or "query ID is invalid" in str(e):
            return
        raise

# ===================== BACK NAV =====================
async def booking_back(message: Message, state: FSMContext):
    st = await state.get_state()

    if st == BookingFSM.waiting_phone.state:
        await state.set_state(BookingFSM.waiting_issue)
        await message.answer("🛠 <b>Шаг 2/3</b>\nОпишите проблему ещё раз.", reply_markup=keyboards.reply_nav_kb())
        return

    if st == BookingFSM.waiting_issue.state:
        await state.set_state(BookingFSM.waiting_car)
        await message.answer(
            "🚗 <b>Шаг 1/3</b>\nНапишите авто: марка/модель и год.\nПример: <i>Toyota Camry 2012</i>",
            reply_markup=keyboards.reply_nav_kb()
        )
        return

    data = await state.get_data()
    mode = data.get("mode")

    if mode == "live":
        await go_home(message, state)
        return

    await state.set_state(BookingFSM.choosing_date)
    await message.answer("📅 Выберите день:", reply_markup=ReplyKeyboardRemove())
    items = await build_dates_items()
    await message.answer("Доступные даты:", reply_markup=keyboards.dates_kb(items, cb_prefix="date"))


async def adminadd_back(message: Message, state: FSMContext):
    st = await state.get_state()

    if st == AdminAddFSM.waiting_issue.state:
        await state.set_state(AdminAddFSM.waiting_car)
        await message.answer("🚗 Авто (пример: Camry 2012):", reply_markup=keyboards.reply_nav_kb())
        return

    if st == AdminAddFSM.waiting_car.state:
        await state.set_state(AdminAddFSM.waiting_phone)
        await message.answer("📞 Телефон (+7...):", reply_markup=keyboards.reply_nav_kb())
        return

    if st == AdminAddFSM.waiting_phone.state:
        await state.set_state(AdminAddFSM.waiting_name)
        await message.answer("👤 Имя клиента (можно «-»):", reply_markup=keyboards.reply_nav_kb())
        return

    await state.set_state(AdminAddFSM.choosing_date)
    await message.answer("📅 Выберите день для ручной записи:", reply_markup=ReplyKeyboardRemove())
    items = await build_dates_items()
    await message.answer("Даты:", reply_markup=keyboards.dates_kb(items, cb_prefix="admadd"))


# ===================== COMMANDS =====================
async def cmd_start(message: Message, state: FSMContext, bot: Bot):
    await state.clear()

    payload = None
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) == 2:
            payload = parts[1].strip()

    if payload and payload.startswith("claim_"):
        token = payload[len("claim_"):]  # <-- ВАЖНО: с отступом

        # если db.claim_booking возвращает (ok, msg) или (ok, msg, bid) — обработаем оба варианта
        try:
            ok, msg, bid = await db.claim_booking(token, message.from_user.id, (message.from_user.full_name or "").strip())
        except ValueError:
            ok, msg = await db.claim_booking(token, message.from_user.id, (message.from_user.full_name or "").strip())
            bid = None

        await message.answer(msg)

        if ok:
            await services.notify_admin(
                bot, get_admin_id(),
                f"🔗 Клиент привязал ручную запись: {message.from_user.full_name} ({message.from_user.id})"
            )
            # если ты добавлял функцию "после привязки" — отправим статус/кнопки
            if bid and hasattr(services, "client_after_claim_send_status"):
                await services.client_after_claim_send_status(bot, get_admin_id(), int(bid))

    await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))

async def cmd_menu(message: Message, state: FSMContext):
    await go_home(message, state)


async def cmd_contacts(message: Message):
    await message.answer(contacts_text(), reply_markup=keyboards.contacts_kb())


async def cmd_my(message: Message):
    rows = await db.get_my_active_bookings(message.from_user.id)
    if not rows:
        await message.answer("📌 Активных записей нет.", reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
        return

    lines = ["📌 <b>Ваши активные записи</b>\n"]
    for _bid, day_s, seq, car, _issue, st, kind, eta in rows:
        d = date.fromisoformat(day_s)
        ico, st_h = STATUS_VIEW.get(st, ("•", st))
        k = "⚡" if kind == KIND_LIVE else "📅"
        eta_s = f" • через {eta} мин" if eta else ""
        lines.append(f"• {k} {d.strftime('%d.%m')} №{seq} {ico} {short(car, 18)} — <i>{st_h}</i>{eta_s}")

    await message.answer("\n".join(lines), reply_markup=keyboards.my_bookings_kb(rows))


async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа.", reply_markup=keyboards.main_menu_kb(False))
        return

    await message.answer(
        "🛠 <b>Админ‑панель</b>\n\n"
        "• «Очередь сегодня» — управление кнопками по каждой записи\n"
        "• «Автодобор сейчас» — добрать поток до лимита / предложить «сегодня»\n",
        reply_markup=keyboards.admin_menu_kb()
    )


# ===================== MENU CALLBACKS =====================
async def menu_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()

    if call.data == "menu:home":
        await go_home(call, state)
        return

    if call.data == "menu:contacts":
        await safe_edit(call.message, contacts_text(), reply_markup=keyboards.contacts_kb())
        return

    if call.data == "menu:my":
        rows = await db.get_my_active_bookings(call.from_user.id)
        if not rows:
            await safe_edit(call.message, "📌 Активных записей нет.", reply_markup=keyboards.main_menu_kb(is_admin(call.from_user.id)))
            return

        lines = ["📌 <b>Ваши активные записи</b>\n"]
        for _bid, day_s, seq, car, _issue, st, kind, eta in rows:
            d = date.fromisoformat(day_s)
            ico, st_h = STATUS_VIEW.get(st, ("•", st))
            k = "⚡" if kind == KIND_LIVE else "📅"
            eta_s = f" • через {eta} мин" if eta else ""
            lines.append(f"• {k} {d.strftime('%d.%m')} №{seq} {ico} {short(car, 18)} — <i>{st_h}</i>{eta_s}")

        await safe_edit(call.message, "\n".join(lines), reply_markup=keyboards.my_bookings_kb(rows))
        return

    if call.data == "menu:admin":
        if not is_admin(call.from_user.id):
            await safe_edit(call.message, "⛔ Нет доступа.", reply_markup=keyboards.main_menu_kb(False))
            return
        await safe_edit(call.message, "🛠 <b>Админ‑панель</b>\nВыберите действие:", reply_markup=keyboards.admin_menu_kb())
        return

    if call.data == "menu:book_static":
        await state.set_state(BookingFSM.choosing_date)
        await state.update_data(mode="static")
        items = await build_dates_items()
        await safe_edit(
            call.message,
            "📅 <b>Запись на день</b>\n✅ — свободно, ⛔ — занято.\n\nВыберите день:",
            reply_markup=keyboards.dates_kb(items, cb_prefix="date")
        )
        return

    if call.data == "menu:book_live":
        now = now_dt()
        today = now.date()

        can_today = False
        if is_work_time(now):
            ok_day, _ = await db.is_day_available(today)
            if ok_day and (await db.get_shop_load(today) < SETTINGS.MAX_AT_SHOP):
                can_today = True

        if can_today:
            await state.set_state(BookingFSM.waiting_car)
            await state.update_data(mode="live", day=today.isoformat())
            await call.message.answer(
                "⚡ <b>Ближайшее время</b>\n\n"
                "🚗 <b>Шаг 1/3</b>\nНапишите авто: марка/модель и год.\nПример: <i>Toyota Camry 2012</i>",
                reply_markup=keyboards.reply_nav_kb()
            )
            return

        start = next_working_day(today)
        suggest_day = await find_nearest_available_day(start)
        if not suggest_day:
            items = await build_dates_items()
            await safe_edit(call.message, "⛔ Нет свободных дат. Выберите вручную:", reply_markup=keyboards.dates_kb(items, cb_prefix="date"))
            await state.set_state(BookingFSM.choosing_date)
            await state.update_data(mode="static")
            return

        await safe_edit(
            call.message,
            live_no_today_suggest_text(suggest_day),
            reply_markup=keyboards.live_suggest_day_kb(suggest_day)
        )
        return


# ===================== LIVE SUGGEST CALLBACKS =====================
async def live_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()

    if call.data == "live:choose_day":
        await state.set_state(BookingFSM.choosing_date)
        await state.update_data(mode="static")
        items = await build_dates_items()
        await safe_edit(call.message, "📅 Выберите день:", reply_markup=keyboards.dates_kb(items, cb_prefix="date"))
        return

    if call.data.startswith("live:accept_day:"):
        day = date.fromisoformat(call.data.split(":")[2])

        ok, reason = await db.is_day_available(day)
        if not ok:
            await call.answer(reason, show_alert=True)
            items = await build_dates_items()
            await safe_edit(call.message, "⛔ Эта дата уже занята. Выберите другую:", reply_markup=keyboards.dates_kb(items, cb_prefix="date"))
            await state.set_state(BookingFSM.choosing_date)
            await state.update_data(mode="static")
            return

        await state.update_data(mode="static", day=day.isoformat())
        await state.set_state(BookingFSM.waiting_car)
        await call.message.answer(
            "🚗 <b>Шаг 1/3</b>\nНапишите авто: марка/модель и год.\nПример: <i>Toyota Camry 2012</i>",
            reply_markup=keyboards.reply_nav_kb()
        )
        return


# ===================== DATE CALLBACK (STATIC) =====================
async def date_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()
    day = date.fromisoformat(call.data.split(":")[1])

    ok, reason = await db.is_day_available(day)
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    await state.update_data(mode="static", day=day.isoformat())
    await state.set_state(BookingFSM.waiting_car)

    await call.message.answer(
        "🚗 <b>Шаг 1/3</b>\nНапишите авто: марка/модель и год.\nПример: <i>Toyota Camry 2012</i>",
        reply_markup=keyboards.reply_nav_kb()
    )


# ===================== BOOKING FSM =====================
async def car_handler(message: Message, state: FSMContext):
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await booking_back(message, state)
        return

    car_text = (message.text or "").strip()
    if len(car_text) < 4 or parse_year(car_text) is None:
        await message.answer(
            "⛔ Не понял.\nНапишите так: <b>Марка Модель Год</b>\nПример: <i>Toyota Camry 2012</i>",
            reply_markup=keyboards.reply_nav_kb()
        )
        return

    await state.update_data(car_text=car_text)
    await state.set_state(BookingFSM.waiting_issue)
    await message.answer("🛠 <b>Шаг 2/3</b>\nОпишите проблему/что нужно сделать.", reply_markup=keyboards.reply_nav_kb())


async def issue_handler(message: Message, state: FSMContext):
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await booking_back(message, state)
        return

    issue = (message.text or "").strip()
    if len(issue) < 5:
        await message.answer("⛔ Опишите чуть подробнее (минимум 5 символов).", reply_markup=keyboards.reply_nav_kb())
        return

    await state.update_data(issue_text=issue)
    await state.set_state(BookingFSM.waiting_phone)
    await message.answer(
        "📞 <b>Шаг 3/3</b>\nОтправьте телефон кнопкой ниже или напишите +7XXXXXXXXXX.",
        reply_markup=keyboards.reply_nav_kb(with_contact=True)
    )


async def phone_handler(message: Message, state: FSMContext, bot: Bot):
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await booking_back(message, state)
        return

    data = await state.get_data()
    mode = data.get("mode")
    if mode not in ("static", "live"):
        await state.clear()
        await message.answer("⛔ Сессия сбилась. Нажмите /start и начните заново.", reply_markup=ReplyKeyboardRemove())
        return

    raw_phone = message.contact.phone_number if message.contact else (message.text or "")
    phone = normalize_phone(raw_phone)
    if not phone:
        await message.answer("⛔ Не смог распознать телефон. Формат: +7XXXXXXXXXX", reply_markup=keyboards.reply_nav_kb(with_contact=True))
        return

    car_text = data.get("car_text")
    issue_text = data.get("issue_text")
    day_s = data.get("day")
    if not (car_text and issue_text and day_s):
        await state.clear()
        await message.answer("⛔ Сессия сбилась. Нажмите /start и начните заново.", reply_markup=ReplyKeyboardRemove())
        return

    user_name = (message.from_user.full_name or "").strip()
    day = date.fromisoformat(day_s)

    dup = await db.get_active_booking_brief_by_user_or_phone(message.from_user.id, phone)
    if dup:
        await state.clear()
        await message.answer(duplicate_active_text(), reply_markup=ReplyKeyboardRemove())
        await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
        return

    if mode == "static":
        ok, reason = await db.is_day_available(day)
        if not ok:
            await state.clear()
            await message.answer(f"⛔ Не получилось записать: {reason}", reply_markup=ReplyKeyboardRemove())
            await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
            return

        try:
            bid, seq = await db.add_static_booking(day, message.from_user.id, user_name, phone, car_text, issue_text)
        except RuntimeError as e:
            await state.clear()
            if str(e) == "duplicate_active":
                await message.answer(duplicate_active_text(), reply_markup=ReplyKeyboardRemove())
            elif str(e) == "day_full":
                await message.answer("⛔ На этот день уже нет мест.", reply_markup=ReplyKeyboardRemove())
            else:
                await message.answer("⛔ Не получилось создать запись.", reply_markup=ReplyKeyboardRemove())
            await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
            return

        await state.clear()
        await message.answer(booking_created_text(day, seq, car_text, issue_text, phone), reply_markup=ReplyKeyboardRemove())
        await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))

        await services.notify_admin(
            bot, get_admin_id(),
            "🆕 <b>Новая запись</b>\n"
            f"{day.strftime('%d.%m.%Y')} №{seq}\n"
            f"🚗 {car_text}\n"
            f"🛠 {issue_text}\n"
            f"👤 {user_name}\n"
            f"📞 {phone}"
        )
        return

    # live today
    now = now_dt()
    if not is_work_time(now):
        await state.clear()
        await message.answer("⛔ Сейчас не рабочее время. Запишитесь на день через меню.", reply_markup=ReplyKeyboardRemove())
        await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
        return

    try:
        bid, seq = await db.add_live_booking_today(day, message.from_user.id, user_name, phone, car_text, issue_text)
    except RuntimeError as e:
        await state.clear()
        if str(e) == "duplicate_active":
            await message.answer(duplicate_active_text(), reply_markup=ReplyKeyboardRemove())
        elif str(e) == "day_full":
            await message.answer("⛔ Сегодня мест уже нет. Запишитесь на день.", reply_markup=ReplyKeyboardRemove())
        else:
            await message.answer("⛔ Не получилось создать запись.", reply_markup=ReplyKeyboardRemove())
        await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
        return

    await state.clear()
    await message.answer(live_created_need_eta_text(seq), reply_markup=ReplyKeyboardRemove())
    await message.answer(eta_request_text(), reply_markup=keyboards.eta_kb(bid))

    await services.notify_admin(
        bot, get_admin_id(),
        "⚡ <b>Ближайшее время (сегодня)</b>\n"
        f"Сегодня №{seq}\n"
        f"🚗 {car_text}\n"
        f"🛠 {issue_text}\n"
        f"👤 {user_name}\n"
        f"📞 {phone}\n\n"
        "Ждём, когда клиент сможет подъехать (потребуется подтверждение)."
    )


# ===================== “OTHER TIME” TEXT INPUT =====================
async def eta_text_handler(message: Message, state: FSMContext, bot: Bot):
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await state.clear()
        await message.answer("Ок. Возвращаю в меню.", reply_markup=ReplyKeyboardRemove())
        await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))
        return

    data = await state.get_data()
    bid = int(data.get("bid") or 0)
    if not bid:
        await state.clear()
        await message.answer("⛔ Сессия сбилась. Нажмите /menu.", reply_markup=ReplyKeyboardRemove())
        return

    minutes, err = parse_arrival_minutes(message.text or "", now_dt())
    if err == "time_passed":
        await message.answer(time_passed_text(), reply_markup=keyboards.reply_nav_kb())
        return
    if minutes is None:
        await message.answer(
            "⛔ Не понял. Примеры: <b>40</b>, <b>40 мин</b>, <b>1.5ч</b>, <b>18:30</b>",
            reply_markup=keyboards.reply_nav_kb()
        )
        return

    ok, msg = await services.client_set_eta_from_text(bot, get_admin_id(), bid, message.from_user.id, minutes)
    await state.clear()
    await message.answer(msg, reply_markup=ReplyKeyboardRemove())
    await message.answer(welcome_text(), reply_markup=keyboards.main_menu_kb(is_admin(message.from_user.id)))


# ===================== CANCEL (from “My bookings” list) =====================
async def cancel_cb(call: CallbackQuery, bot: Bot):
    await call.answer()
    bid = int(call.data.split(":")[1])
    ok, msg = await services.client_cancel(bot, get_admin_id(), bid, call.from_user.id)
    await safe_edit(call.message, msg, reply_markup=keyboards.main_menu_kb(is_admin(call.from_user.id)))


# ===================== CLIENT CALLBACKS =====================
async def client_cb(call: CallbackQuery, state: FSMContext, bot: Bot):
    await call.answer()
    parts = call.data.split(":")
    action = parts[1]

    if action == "cancel":
        bid = int(parts[2])
        ok, msg = await services.client_cancel(bot, get_admin_id(), bid, call.from_user.id)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "edit_time":
        bid = int(parts[2])
        await call.message.answer(eta_request_text(), reply_markup=keyboards.eta_kb(bid))
        return

    if action == "arrived":
        bid = int(parts[2])
        ok, msg = await services.client_arrived(bot, get_admin_id(), bid, call.from_user.id)
        await call.message.answer(msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "offer_yes":
        bid = int(parts[2])
        ok, msg = await services.client_offer_yes(bot, bid, call.from_user.id)
        if ok:
            await safe_edit(call.message, msg, reply_markup=keyboards.eta_kb(bid))
        else:
            await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "offer_no":
        bid = int(parts[2])
        ok, msg = await services.client_offer_no(bot, get_admin_id(), bid, call.from_user.id)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "eta":
        bid = int(parts[2])
        minutes = int(parts[3])
        ok, msg = await services.client_set_eta_from_button(bot, get_admin_id(), bid, call.from_user.id, minutes)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "eta_other":
        bid = int(parts[2])
        await state.set_state(ETAFSM.waiting_eta)
        await state.update_data(bid=bid)
        await call.message.answer(
            "✍️ Напишите, когда сможете подъехать (например 40 мин или 18:30):",
            reply_markup=keyboards.reply_nav_kb()
        )
        return

    # === NEW: confirm callbacks ===
    if action == "confirm_yes":
        bid = int(parts[2])
        ok, msg = await services.client_confirm_yes(bot, get_admin_id(), bid, call.from_user.id)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "confirm_move":
        bid = int(parts[2])
        ok, msg = await services.client_confirm_move(bot, get_admin_id(), bid, call.from_user.id)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "resched_accept":
        bid = int(parts[2])
        day = date.fromisoformat(parts[3])
        ok, msg = await services.client_reschedule_accept(bot, get_admin_id(), bid, call.from_user.id, day)
        await safe_edit(call.message, msg, reply_markup=keyboards.inline_home_kb())
        return

    if action == "resched_choose":
        bid = int(parts[2])
        await state.set_state(RescheduleFSM.choosing_day)
        await state.update_data(bid=bid)
        items = await build_dates_items(start=next_working_day(now_dt().date()))
        await call.message.answer("📅 Выберите новую дату:", reply_markup=keyboards.dates_kb(items, cb_prefix="resched"))
        return


# ===================== RESCHEDULE DATE PICKER CALLBACK =====================
async def resched_date_cb(call: CallbackQuery, state: FSMContext, bot: Bot):
    await call.answer()
    if await state.get_state() != RescheduleFSM.choosing_day.state:
        await call.answer("⛔ Сессия переноса сбилась. Нажмите /menu.", show_alert=True)
        return

    data = await state.get_data()
    bid = int(data.get("bid") or 0)
    if not bid:
        await state.clear()
        await call.answer("⛔ Сессия переноса сбилась.", show_alert=True)
        return

    new_day = date.fromisoformat(call.data.split(":")[2])
    ok, msg = await services.client_reschedule_move(bot, get_admin_id(), bid, call.from_user.id, new_day)
    if ok:
        await state.clear()
        await call.message.answer(msg, reply_markup=keyboards.inline_home_kb())
    else:
        await call.answer(msg, show_alert=True)


# ===================== ADMIN VIEWS =====================
async def admin_queue_text(day: date):
    rows = await db.get_queue_for_day(day)
    in_serv = await db.get_in_service_all()
    parts = await db.get_parts_wait_all()
    load = await db.get_shop_load(now_dt().date())

    lines = [
        f"📋 <b>Очередь на {day.strftime('%d.%m.%Y')}</b>\n"
        f"🕒 {SETTINGS.WORK_INFO}\n"
        f"🚗 <b>В потоке:</b> {load}/{SETTINGS.MAX_AT_SHOP}  (в работе: {await db.get_in_service_count()}/{SETTINGS.MAX_IN_SERVICE})\n"
    ]

    if not rows:
        lines.append("\n— записей на этот день нет —")
    else:
        lines.append("\n<b>Список на день:</b>")
        for _bid, seq, car, issue, phone, status, uid, kind, eta, mco, needs_ok in rows:
            ico, st_h = STATUS_VIEW.get(status, ("•", status))
            tail = "" if uid else " (без бота)"
            k = "⚡" if kind == KIND_LIVE else "📅"
            eta_s = f" • через {eta}м" if eta else ""
            mco_s = " • 🛑ручн." if int(mco) == 1 else ""
            nok_s = " • ⏳ждём✅/❌" if int(needs_ok) == 1 else ""
            lines.append(f"{seq}. {k} {ico} {short(car, 18)} — {short(issue, 24)} — {phone}{tail}{eta_s}{mco_s}{nok_s} • <i>{st_h}</i>")

    return "\n".join(lines), rows, in_serv, parts


async def admin_month_text():
    today = now_dt().date()
    days = []
    d = today
    while len(days) < SETTINGS.DAYS_AHEAD:
        if is_working_day(d):
            days.append(d)
        d += timedelta(days=1)

    by_day = defaultdict(list)
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            SELECT day, seq, car_text, issue_text, phone, status, user_id, kind, eta_minutes, needs_admin_ok
            FROM bookings
            WHERE status IN ('waiting','called','arrived','in_service','parts_wait')
            ORDER BY day, seq, id
        """)
        rows = await cur.fetchall()

    for day_s, seq, car, issue, phone, status, uid, kind, eta, needs_ok in rows:
        by_day[day_s].append((seq, car, issue, phone, status, uid, kind, eta, needs_ok))

    out = [f"📆 <b>Записи на месяц</b>\n🕒 {SETTINGS.WORK_INFO}\n"]
    for dd in days:
        ds = dd.isoformat()
        out.append(f"\n<b>{format_day_label(dd)}:</b>")
        items = by_day.get(ds, [])
        if not items:
            out.append("— нет —")
        else:
            for seq, car, issue, phone, st, uid, kind, eta, needs_ok in items:
                ico, st_h = STATUS_VIEW.get(st, ("•", st))
                tail = "" if uid else " (без бота)"
                k = "⚡" if kind == KIND_LIVE else "📅"
                eta_s = f" • через {eta}м" if eta else ""
                nok_s = " • ⏳ждём✅/❌" if int(needs_ok) == 1 else ""
                out.append(f"{seq}. {k} {ico} {short(car, 18)} — {short(issue, 22)} — {phone}{tail}{eta_s}{nok_s} • <i>{st_h}</i>")
    return "\n".join(out)


# ===================== ADMIN CALLBACKS =====================
async def admin_cb(call: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    await call.answer()

    if call.data == "adm:queue_today":
        day = now_dt().date()
        txt, rows, in_serv, parts = await admin_queue_text(day)
        await call.message.answer(txt, reply_markup=keyboards.admin_queue_keyboard(day, True, rows, in_serv, parts))
        return

    if call.data.startswith("adm:queue_day:"):
        day = date.fromisoformat(call.data.split(":")[2])
        txt, rows, in_serv, parts = await admin_queue_text(day)
        await safe_edit(call.message, txt, reply_markup=keyboards.admin_queue_keyboard(day, day == now_dt().date(), rows, in_serv, parts))
        return

    if call.data in ("adm:month", "adm:week"):
        txt = await admin_month_text()
        await call.message.answer(txt, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data == "adm:auto_now":
        res = await services.admin_auto_now(bot, get_admin_id())
        await call.message.answer(res, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data == "adm:add":
        await admin_add_start(call, state)
        return

    if call.data.startswith("adm:approve_yes:"):
        bid = int(call.data.split(":")[2])
        res = await services.admin_approve_yes(bot, get_admin_id(), bid)
        await call.message.answer(res, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    # NEW
    if call.data.startswith("adm:approve_next:"):
        bid = int(call.data.split(":")[2])
        res = await services.admin_approve_next(bot, get_admin_id(), bid)
        await call.message.answer(res, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:approve_no:"):
        bid = int(call.data.split(":")[2])
        res = await services.admin_approve_no(bot, get_admin_id(), bid)
        await call.message.answer(res, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:call_bid:"):
        bid = int(call.data.split(":")[2])
        res = await services.admin_call_bid(bot, get_admin_id(), bid)
        await call.message.answer(res, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:force_accept:"):
        bid = int(call.data.split(":")[2])
        ok, msg = await services.admin_force_accept(bot, get_admin_id(), bid)
        await call.message.answer(msg, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:accept:"):
        bid = int(call.data.split(":")[2])
        ok, msg = await services.admin_accept_to_service(bot, get_admin_id(), bid)
        await call.message.answer(msg, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:done:"):
        bid = int(call.data.split(":")[2])
        ok, msg = await services.admin_done(bot, get_admin_id(), bid)
        await call.message.answer(msg, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:noshow:"):
        bid = int(call.data.split(":")[2])
        ok, msg = await services.admin_no_show(bot, get_admin_id(), bid)
        await call.message.answer(msg, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:parts_wait:"):
        bid = int(call.data.split(":")[2])
        ok, msg = await services.admin_wait_parts(bot, get_admin_id(), bid)
        await call.message.answer(msg, reply_markup=keyboards.inline_admin_back_home_kb())
        return

    if call.data.startswith("adm:parts_ok:"):
        bid = int(call.data.split(":")[2])
        ok, msg = await services.admin_parts_ok(bot, get_admin_id(), bid)
        await call.message.answer(msg, reply_markup=keyboards.inline_admin_back_home_kb())
        return


# ===================== ADMIN MANUAL ADD =====================
async def admin_add_start(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.answer()
    await state.set_state(AdminAddFSM.choosing_date)
    items = await build_dates_items()
    await safe_edit(call.message, "➕ <b>Ручная запись</b>\nВыберите день:", reply_markup=keyboards.dates_kb(items, cb_prefix="admadd"))


async def admin_add_choose_date(call: CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return
    await call.answer()

    day = date.fromisoformat(call.data.split(":")[2])
    ok, reason = await db.is_day_available(day)
    if not ok:
        await call.answer(reason, show_alert=True)
        return

    await state.update_data(day=day.isoformat())
    await state.set_state(AdminAddFSM.waiting_name)
    await call.message.answer("👤 Имя клиента (можно «-»):", reply_markup=keyboards.reply_nav_kb())


async def admin_add_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await adminadd_back(message, state)
        return

    name = (message.text or "").strip() or "-"
    await state.update_data(client_name=name)
    await state.set_state(AdminAddFSM.waiting_phone)
    await message.answer("📞 Телефон клиента (+7...):", reply_markup=keyboards.reply_nav_kb())


async def admin_add_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await adminadd_back(message, state)
        return

    phone = normalize_phone(message.text or "")
    if not phone:
        await message.answer("⛔ Не понял номер. Формат: +7XXXXXXXXXX", reply_markup=keyboards.reply_nav_kb())
        return

    await state.update_data(phone=phone)
    await state.set_state(AdminAddFSM.waiting_car)
    await message.answer("🚗 Авто (пример: Camry 2012):", reply_markup=keyboards.reply_nav_kb())


async def admin_add_car(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await adminadd_back(message, state)
        return

    car = (message.text or "").strip()
    if len(car) < 3:
        await message.answer("⛔ Слишком коротко. Напишите авто и год.", reply_markup=keyboards.reply_nav_kb())
        return

    await state.update_data(car_text=car)
    await state.set_state(AdminAddFSM.waiting_issue)
    await message.answer("🛠 Проблема/задача:", reply_markup=keyboards.reply_nav_kb())


async def admin_add_issue(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    if message.text == SETTINGS.BTN_HOME:
        await go_home(message, state)
        return
    if message.text == SETTINGS.BTN_BACK:
        await adminadd_back(message, state)
        return

    issue = (message.text or "").strip()
    if len(issue) < 5:
        await message.answer("⛔ Опишите чуть подробнее (минимум 5 символов).", reply_markup=keyboards.reply_nav_kb())
        return

    data = await state.get_data()
    day = date.fromisoformat(data["day"])
    client_name = data.get("client_name", "-")
    phone = data["phone"]
    car = data["car_text"]

    try:
        bid, seq, token = await db.add_booking_admin_manual(day, client_name, phone, car, issue)
    except RuntimeError:
        await message.answer("⛔ На этот день уже нет мест.", reply_markup=ReplyKeyboardRemove())
        await state.clear()
        return

    await state.clear()

    link = None
    if BOT_USERNAME:
        link = f"https://t.me/{BOT_USERNAME}?start=claim_{token}"

    text = (
        "✅ <b>Ручная запись создана</b>\n\n"
        f"📅 {day.strftime('%d.%m.%Y')} • №{seq}\n"
        f"👤 {client_name}\n"
        f"📞 {phone}\n"
        f"🚗 {car}\n"
        f"🛠 {issue}\n\n"
        "⚠️ <b>Важно:</b> бот не может написать человеку первым, если он не запускал бота.\n"
    )
    if link:
        text += f"\nСсылка для привязки записи:\n{link}"

    await message.answer(text, reply_markup=ReplyKeyboardRemove())
    await message.answer("🛠 Админ‑панель:", reply_markup=keyboards.admin_menu_kb())
    await services.notify_admin(
        bot, get_admin_id(),
        f"➕ Ручная запись #{bid}: {day.strftime('%d.%m.%Y')} №{seq} — {phone} — {short(car, 22)}"
    )


# ===================== NOOP =====================
async def noop_cb(call: CallbackQuery):
    await call.answer()


# ===================== REGISTER HANDLERS =====================
def register_handlers(dp: Dispatcher) -> None:
    # commands
    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_menu, Command("menu"))
    dp.message.register(cmd_my, Command("my"))
    dp.message.register(cmd_contacts, Command("contacts"))
    dp.message.register(cmd_admin, Command("admin"))

    # callbacks
    dp.callback_query.register(noop_cb, F.data == "noop")

    dp.callback_query.register(menu_cb, F.data.startswith("menu:"))
    dp.callback_query.register(live_cb, F.data.startswith("live:"))
    dp.callback_query.register(date_cb, F.data.startswith("date:"))
    dp.callback_query.register(resched_date_cb, F.data.startswith("resched:date:"))
    dp.callback_query.register(cancel_cb, F.data.startswith("cancel:"))
    dp.callback_query.register(client_cb, F.data.startswith("cli:"))
    dp.callback_query.register(admin_add_choose_date, F.data.startswith("admadd:date:"))
    dp.callback_query.register(admin_cb, F.data.startswith("adm:"))

    # FSM messages (booking)
    dp.message.register(car_handler, BookingFSM.waiting_car)
    dp.message.register(issue_handler, BookingFSM.waiting_issue)
    dp.message.register(phone_handler, BookingFSM.waiting_phone)

    # FSM messages (ETA)
    dp.message.register(eta_text_handler, ETAFSM.waiting_eta)

    # FSM messages (admin add)
    dp.message.register(admin_add_name, AdminAddFSM.waiting_name)
    dp.message.register(admin_add_phone, AdminAddFSM.waiting_phone)
    dp.message.register(admin_add_car, AdminAddFSM.waiting_car)
    dp.message.register(admin_add_issue, AdminAddFSM.waiting_issue)


# ===================== MAIN =====================
async def main():
    load_dotenv(_project_root_env_path())

    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN not set in .env")

    admin_id = get_admin_id()

    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    register_handlers(dp)

    await db.init_db()

    global BOT_USERNAME
    try:
        me = await bot.get_me()
        BOT_USERNAME = me.username
    except Exception:
        BOT_USERNAME = None

    await setup_bot_menu(bot)

    # background tasks
    services.start_background_tasks(bot, admin_id)

    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
