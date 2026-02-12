from __future__ import annotations

from datetime import date
from typing import Sequence
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from config import SETTINGS, YANDEX_MAP_URL, GOOGLE_MAP_URL, TWO_GIS_URL
from utils import short

# ===================== CLIENT =====================
def main_menu_kb(is_admin: bool) -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton(text="📅 Записаться на день", callback_data="menu:book_static"),
            InlineKeyboardButton(text="⚡ Срочный заезд", callback_data="menu:book_live"),
        ],
        [
            InlineKeyboardButton(text="📌 Мои записи", callback_data="menu:my"),
            InlineKeyboardButton(text="📍 Как проехать", callback_data="menu:contacts"),
        ]
    ]
    if is_admin:
        kb.insert(0, [InlineKeyboardButton(text="🛠 Панель администратора", callback_data="menu:admin")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def inline_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="menu:home")]
    ])

def contacts_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗺 Яндекс Карты", url=YANDEX_MAP_URL)],
        [InlineKeyboardButton(text="🗺 Google Maps", url=GOOGLE_MAP_URL)],
        [InlineKeyboardButton(text="🗺 2ГИС", url=TWO_GIS_URL)],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="menu:home")],
    ])

def reply_nav_kb(with_contact: bool = False) -> ReplyKeyboardMarkup:
    rows = []
    if with_contact:
        rows.append([KeyboardButton(text="📱 Отправить мой номер", request_contact=True)])
    rows.append([KeyboardButton(text=SETTINGS.BTN_BACK), KeyboardButton(text=SETTINGS.BTN_HOME)])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def arrived_kb(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📍 Я ПОДЪЕХАЛ", callback_data=f"cli:arrived:{booking_id}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cli:cancel:{booking_id}")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:home")],
    ])

def offer_today_kb(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, приеду сегодня!", callback_data=f"cli:offer_yes:{booking_id}")],
        [InlineKeyboardButton(text="🙅‍♂️ Нет", callback_data=f"cli:offer_no:{booking_id}")],
    ])

def eta_kb(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="30 мин", callback_data=f"cli:eta:{booking_id}:30")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:home")],
    ])

def called_confirm_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выезжаю", callback_data=f"cli:confirm_yes:{bid}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cli:cancel:{bid}")],
    ])

def reschedule_suggest_kb(bid: int, day: date) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Согласен на {day.strftime('%d.%m')}", callback_data=f"cli:resched_accept:{bid}:{day.isoformat()}")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cli:cancel:{bid}")],
    ])

def my_bookings_kb(rows: Sequence[tuple]) -> InlineKeyboardMarkup:
    kb = []
    for bid, day_s, seq, car, _issue, st, _kind, _eta in rows:
        d = date.fromisoformat(day_s)
        kb.append([InlineKeyboardButton(
            text=f"❌ Отменить {d.strftime('%d.%m')} ({short(car, 15)})",
            callback_data=f"cancel:{bid}"
        )])
    kb.append([InlineKeyboardButton(text="🏠 В меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# --- ГЛАВНАЯ ФУНКЦИЯ ДЛЯ КАЛЕНДАРЯ ---
def dates_kb(items: Sequence[tuple[date, bool]], cb_prefix: str = "date") -> InlineKeyboardMarkup:
    """
    Строит сетку 3 колонки.
    items: [(date, is_free), ...]
    """
    btns = []
    wds = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    
    for d, ok in items:
        # Если свободно ✅, если занято ❌
        icon = "✅" if ok else "❌"
        # Если занято, кнопка будет, но мы обработаем нажатие как "занято"
        # Или можно делать текст "❌ 25.10 Сб"
        text = f"{icon} {d.strftime('%d.%m')} {wds[d.weekday()]}"
        
        if cb_prefix == "date":
            cb = f"date:{d.isoformat()}"
        else:
            cb = f"{cb_prefix}:date:{d.isoformat()}"
            
        btns.append(InlineKeyboardButton(text=text, callback_data=cb))
    
    # Разбиваем на 3 колонки
    rows = [btns[i:i + 3] for i in range(0, len(btns), 3)]
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="menu:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def review_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⭐️", callback_data=f"review:1:{bid}"),
            InlineKeyboardButton(text="⭐️⭐️", callback_data=f"review:2:{bid}"),
            InlineKeyboardButton(text="⭐️⭐️⭐️", callback_data=f"review:3:{bid}"),
        ],
        [
            InlineKeyboardButton(text="⭐️⭐️⭐️⭐️", callback_data=f"review:4:{bid}"),
            InlineKeyboardButton(text="⭐️⭐️⭐️⭐️⭐️", callback_data=f"review:5:{bid}"),
        ]
    ])

# ===================== ADMIN =====================
def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Очередь (Сегодня)", callback_data="adm:queue_today")],
        [InlineKeyboardButton(text="🚀 Автодобор", callback_data="adm:auto_now")],
        [InlineKeyboardButton(text="🏠 В меню", callback_data="menu:home")],
    ])

def accept_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏁 Взять в работу", callback_data=f"adm:accept:{bid}")],
        [InlineKeyboardButton(text="⬅️ В админку", callback_data="menu:admin")],
    ])

def admin_time_approve_kb(bid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"adm:approve_yes:{bid}")],
        [InlineKeyboardButton(text="⏭ Отказать", callback_data=f"adm:approve_next:{bid}")],
    ])
