from __future__ import annotations

from datetime import date
from typing import Sequence

from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)

from config import SETTINGS, YANDEX_MAP_URL, GOOGLE_MAP_URL, TWO_GIS_URL
from utils import short


# ===================== CLIENT =====================
def main_menu_kb(is_admin: bool) -> InlineKeyboardMarkup:
    """
    По вашему требованию: админу показываем только админку.
    """
    if is_admin:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛠 Админ‑панель", callback_data="menu:admin")],
        ])

    rows = [
        [
            InlineKeyboardButton(text="📅 Записаться (на день)", callback_data="menu:book_static"),
            InlineKeyboardButton(text="⚡ Ближайшее время", callback_data="menu:book_live"),
        ],
        [
            InlineKeyboardButton(text="📌 Мои записи", callback_data="menu:my"),
            InlineKeyboardButton(text="📍 Контакты", callback_data="menu:contacts"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def inline_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def inline_admin_back_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Админ‑меню", callback_data="menu:admin")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def contacts_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗺 Яндекс Карты", url=YANDEX_MAP_URL)],
        [InlineKeyboardButton(text="🗺 Google Maps", url=GOOGLE_MAP_URL)],
        [InlineKeyboardButton(text="🗺 2ГИС", url=TWO_GIS_URL)],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def reply_nav_kb(with_contact: bool = False) -> ReplyKeyboardMarkup:
    rows = []
    if with_contact:
        rows.append([KeyboardButton(text="📱 Отправить телефон", request_contact=True)])
    rows.append([KeyboardButton(text=SETTINGS.BTN_BACK), KeyboardButton(text=SETTINGS.BTN_HOME)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def arrived_kb(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📍 Я подъехал", callback_data=f"cli:arrived:{booking_id}")],
        [
            InlineKeyboardButton(text="✏️ Изменить время", callback_data=f"cli:edit_time:{booking_id}"),
            InlineKeyboardButton(text="❌ Я не приеду", callback_data=f"cli:cancel:{booking_id}"),
        ],
        [
            InlineKeyboardButton(text="🗺 Яндекс", url=YANDEX_MAP_URL),
            InlineKeyboardButton(text="🗺 Google", url=GOOGLE_MAP_URL),
        ],
        [InlineKeyboardButton(text="🗺 2ГИС", url=TWO_GIS_URL)],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def offer_today_kb(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, могу сегодня", callback_data=f"cli:offer_yes:{booking_id}")],
        [InlineKeyboardButton(text="❌ Нет, оставьте как было", callback_data=f"cli:offer_no:{booking_id}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def eta_kb(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="10 мин", callback_data=f"cli:eta:{booking_id}:10"),
            InlineKeyboardButton(text="20 мин", callback_data=f"cli:eta:{booking_id}:20"),
            InlineKeyboardButton(text="30 мин", callback_data=f"cli:eta:{booking_id}:30"),
        ],
        [
            InlineKeyboardButton(text="40 мин", callback_data=f"cli:eta:{booking_id}:40"),
            InlineKeyboardButton(text="60 мин", callback_data=f"cli:eta:{booking_id}:60"),
            InlineKeyboardButton(text="Другое", callback_data=f"cli:eta_other:{booking_id}"),
        ],
        [InlineKeyboardButton(text="❌ Я не приеду", callback_data=f"cli:cancel:{booking_id}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def called_confirm_kb(bid: int) -> InlineKeyboardMarkup:
    """
    Переспрос клиенту после наступления ETA.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, приеду", callback_data=f"cli:confirm_yes:{bid}")],
        [InlineKeyboardButton(text="↩️ Не успеваю — перенести", callback_data=f"cli:confirm_move:{bid}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cli:cancel:{bid}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def live_suggest_day_kb(day: date) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"✅ Записаться на {day.strftime('%d.%m')}",
            callback_data=f"live:accept_day:{day.isoformat()}"
        )],
        [InlineKeyboardButton(text="📅 Выбрать другую дату", callback_data="live:choose_day")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def reschedule_suggest_kb(bid: int, day: date) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"✅ Оставить {day.strftime('%d.%m')}",
            callback_data=f"cli:resched_accept:{bid}:{day.isoformat()}"
        )],
        [InlineKeyboardButton(
            text="📅 Выбрать другую дату",
            callback_data=f"cli:resched_choose:{bid}"
        )],
        [InlineKeyboardButton(text="❌ Отменить запись", callback_data=f"cli:cancel:{bid}")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def my_bookings_kb(rows: Sequence[tuple]) -> InlineKeyboardMarkup:
    kb = []
    for bid, day_s, seq, car, _issue, _st, _kind, _eta in rows:
        d = date.fromisoformat(day_s)
        kb.append([InlineKeyboardButton(
            text=f"❌ Отменить • {d.strftime('%d.%m')} №{seq} • {short(car, 18)}",
            callback_data=f"cancel:{bid}"
        )])
    kb.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def dates_kb(items: Sequence[tuple[date, bool]], cb_prefix: str = "date") -> InlineKeyboardMarkup:
    """
    3 колонки.
    cb_prefix:
      - "date"    -> date:YYYY-MM-DD
      - "admadd"  -> admadd:date:YYYY-MM-DD
      - "resched" -> resched:date:YYYY-MM-DD
    """
    btns = []
    wds = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

    for d, ok in items:
        icon = "✅" if ok else "⛔"
        text = f"{icon} {d.strftime('%d.%m')} {wds[d.weekday()]}"
        if cb_prefix == "date":
            cb = f"date:{d.isoformat()}"
        else:
            cb = f"{cb_prefix}:date:{d.isoformat()}"
        btns.append(InlineKeyboardButton(text=text, callback_data=cb))

    rows = []
    for i in range(0, len(btns), 3):
        rows.append(btns[i:i + 3])

    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ===================== ADMIN =====================
def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Очередь (сегодня)", callback_data="adm:queue_today")],
        [InlineKeyboardButton(text="📆 Записи на месяц", callback_data="adm:month")],
        [InlineKeyboardButton(text="🚀 Автодобор сейчас", callback_data="adm:auto_now")],
        [InlineKeyboardButton(text="➕ Записать вручную", callback_data="adm:add")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def accept_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟦 Взять в работу", callback_data=f"adm:accept:{bid}")],
        [InlineKeyboardButton(text="⬅️ Админ‑меню", callback_data="menu:admin")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")],
    ])


def admin_time_approve_kb(bid: int) -> InlineKeyboardMarkup:
    """
    ВАЖНО: по твоему ТЗ тут только 2 кнопки.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Согласен", callback_data=f"adm:approve_yes:{bid}")],
        [InlineKeyboardButton(text="⏭ Предложить следующему", callback_data=f"adm:approve_next:{bid}")],
    ])


def admin_queue_keyboard(day: date, is_today: bool, day_rows, in_service_rows, parts_rows) -> InlineKeyboardMarkup:
    """
    day_rows: (id, seq, car, issue, phone, status, user_id, kind, eta, manual_call_only, needs_admin_ok)
    """
    kb = []

    if is_today:
        kb.append([InlineKeyboardButton(text="🚀 Автодобор сейчас", callback_data="adm:auto_now")])

    kb.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=f"adm:queue_day:{day.isoformat()}")])
    kb.append([InlineKeyboardButton(text="⬅️ Админ‑меню", callback_data="menu:admin")])
    kb.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu:home")])

    # ---- Кнопки по каждой записи дня ----
    for bid, seq, car, _issue, _phone, status, _uid, _kind, _eta, _mco, needs_ok in day_rows:
        car_s = short(car, 16)

        # если ждёт решения мастера — кнопки решения прямо тут
        if is_today and status in ("waiting", "called") and int(needs_ok) == 1:
            kb.append([InlineKeyboardButton(
                text=f"⏳ Решение • №{seq} • {car_s}",
                callback_data="noop"
            )])
            kb.append([InlineKeyboardButton(text=f"✅ Принять • №{seq}", callback_data=f"adm:approve_yes:{bid}")])
            kb.append([InlineKeyboardButton(text=f"⏭ Следующий • №{seq}", callback_data=f"adm:approve_next:{bid}")])
            kb.append([InlineKeyboardButton(text=f"❌ Не подходит • №{seq}", callback_data=f"adm:approve_no:{bid}")])
            continue

        # waiting: можно позвать и можно взять в работу
        if is_today and status == "waiting":
            kb.append([InlineKeyboardButton(
                text=f"📞 Позвать • №{seq} • {car_s}",
                callback_data=f"adm:call_bid:{bid}"
            )])
            kb.append([InlineKeyboardButton(
                text=f"🟦 В работу • №{seq} • {car_s}",
                callback_data=f"adm:force_accept:{bid}"
            )])

        # called: можно взять в работу + неявка
        if is_today and status == "called":
            kb.append([InlineKeyboardButton(
                text=f"🟦 В работу • №{seq} • {car_s}",
                callback_data=f"adm:force_accept:{bid}"
            )])
            kb.append([InlineKeyboardButton(
                text=f"🚫 Неявка • №{seq} • {car_s}",
                callback_data=f"adm:noshow:{bid}"
            )])

        # arrived: взять в работу + неявка
        if is_today and status == "arrived":
            kb.append([InlineKeyboardButton(
                text=f"🟦 В работу • №{seq} • {car_s}",
                callback_data=f"adm:accept:{bid}"
            )])
            kb.append([InlineKeyboardButton(
                text=f"🚫 Неявка • №{seq} • {car_s}",
                callback_data=f"adm:noshow:{bid}"
            )])

        # in_service: готово / ждёт запчасть
        if is_today and status == "in_service":
            kb.append([InlineKeyboardButton(
                text=f"✅ Готово • №{seq} • {car_s}",
                callback_data=f"adm:done:{bid}"
            )])
            kb.append([InlineKeyboardButton(
                text=f"🧩 Ждёт запчасть • №{seq} • {car_s}",
                callback_data=f"adm:parts_wait:{bid}"
            )])

    # В работе сейчас
    if in_service_rows:
        kb.append([InlineKeyboardButton(text="— 🛠 В работе сейчас —", callback_data="noop")])
        for bid, d_s, seq, car, _issue, _phone, _uid, _kind, _eta in in_service_rows:
            kb.append([InlineKeyboardButton(
                text=f"✅ Готово • {d_s[8:10]}.{d_s[5:7]} №{seq} • {short(car, 16)}",
                callback_data=f"adm:done:{bid}"
            )])
            kb.append([InlineKeyboardButton(
                text=f"🧩 Ждёт запчасть • {d_s[8:10]}.{d_s[5:7]} №{seq} • {short(car, 16)}",
                callback_data=f"adm:parts_wait:{bid}"
            )])

    # Запчасть пришла
    if parts_rows:
        kb.append([InlineKeyboardButton(text="— 🧩 Ждут запчасть —", callback_data="noop")])
        for bid, d_s, _seq, car, _issue, _phone, _uid, _kind, _eta, _upd in parts_rows:
            kb.append([InlineKeyboardButton(
                text=f"📦 Запчасть пришла • {d_s[8:10]}.{d_s[5:7]} • {short(car, 18)}",
                callback_data=f"adm:parts_ok:{bid}"
            )])

    return InlineKeyboardMarkup(inline_keyboard=kb)
