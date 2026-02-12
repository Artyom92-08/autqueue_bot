from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Optional, List, Tuple

import aiosqlite
from aiogram import Bot

from config import SETTINGS
import db
from db import (
    DB_PATH, STATUS_WAITING, STATUS_CALLED, STATUS_ARRIVED, STATUS_IN_SERVICE, 
    KIND_STATIC, KIND_LIVE
)
from utils import is_work_time, is_working_day, next_working_day, short, now_dt
from keyboards import (
    arrived_kb, offer_today_kb, accept_kb,
    admin_time_approve_kb, reschedule_suggest_kb,
    called_confirm_kb, eta_kb, review_kb,
    inline_home_kb
)
from texts import review_request_text

OFFER_COOLDOWN_MINUTES = 365 * 24 * 60

# ==================== УВЕДОМЛЕНИЯ ====================
async def notify_admin(bot: Bot, admin_id: int, text: str, reply_markup=None) -> None:
    if not admin_id: return
    try: await bot.send_message(int(admin_id), text, reply_markup=reply_markup)
    except: pass

async def try_send(bot: Bot, user_id: int, text: str, reply_markup=None) -> bool:
    try:
        await bot.send_message(int(user_id), text, reply_markup=reply_markup)
        return True
    except: return False

# ==================== КАЛЕНДАРЬ (НОВОЕ) ====================
async def get_month_calendar() -> List[Tuple[date, bool]]:
    """
    Возвращает список дней на месяц вперед.
    (Дата, Свободно ли)
    """
    today = now_dt().date()
    # Начинаем с завтрашнего дня (или сегодня, если еще утро)
    # Но обычно запись идет на след. дни. Давай со следующего рабочего.
    start_date = today
    
    items = []
    current = start_date
    
    # Ищем 30 рабочих дней
    while len(items) < 30:
        if is_working_day(current):
            # Проверяем базу: есть ли места?
            ok, _ = await db.is_day_available(current)
            items.append((current, ok))
        current += timedelta(days=1)
            
    return items

async def _get_booking_brief(bid: int) -> Optional[tuple]:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT id, day, seq, user_id, car_text, issue_text, phone, status, kind, eta_minutes, manual_call_only, needs_admin_ok, offer_stage FROM bookings WHERE id=?", (int(bid),))
        return await cur.fetchone()

async def _find_nearest_available_day(start: date, limit_days: int = 120) -> Optional[date]:
    d = start
    for _ in range(limit_days):
        if is_working_day(d):
            ok, _ = await db.is_day_available(d)
            if ok: return d
        d += timedelta(days=1)
    return None

# ==================== ОТЗЫВЫ ====================
async def request_review_later(bot: Bot, user_id: int, bid: int):
    await asyncio.sleep(60)
    await try_send(bot, user_id, review_request_text(), reply_markup=review_kb(bid))

async def handle_review_stars(bot: Bot, admin_id: int, bid: int, stars: int, user_id: int):
    await db.add_review(bid, user_id, stars)
    await try_send(bot, user_id, f"✅ Спасибо за оценку ({stars}⭐️)!", reply_markup=inline_home_kb())
    await notify_admin(bot, admin_id, f"⭐️ <b>Новый отзыв!</b>\nОценка: {stars}\nЗапись #{bid}")

# ==================== АВТОДОБОР ====================
async def auto_fill_to_max(bot: Bot, admin_id: int, reason: str = "tick") -> None:
    now = now_dt()
    today = now.date()
    if not is_work_time(now): return

    current_load = await db.get_shop_load(today)
    needed = SETTINGS.MAX_AT_SHOP - current_load

    if needed <= 0: return

    # 1. Зовем тех, кто записан на сегодня
    while needed > 0:
        result = await call_next_today(bot, admin_id)
        if result == "OK":
            needed -= 1
        else:
            break
    
    # 2. Если все еще пусто - зовем из будущего
    if needed > 0:
        active_offer = await db.get_active_offer_row(db.now_ts())
        if not active_offer:
            await offer_send_next_candidate(bot, admin_id)

async def call_next_today(bot: Bot, admin_id: int) -> str:
    now = now_dt()
    today = now.date()
    ts = db.now_iso()
    now_epoch = db.now_ts()
    grace_sec = SETTINGS.CALL_CONFIRM_GRACE_MINUTES * 60

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        cur = await conn.execute("""
            SELECT id, seq, user_id, car_text, issue_text, phone
            FROM bookings
            WHERE day=? AND status='waiting' AND manual_call_only=0 AND needs_admin_ok=0
            ORDER BY seq ASC LIMIT 1
        """, (today.isoformat(),))
        row = await cur.fetchone()
        
        if not row:
            await conn.commit()
            return "EMPTY"

        bid, seq, user_id, car, issue, phone = row
        
        await conn.execute("""
            UPDATE bookings
            SET status='called', called_at=?, eta_due_at=?+(COALESCE(eta_minutes,30)*60), confirm_expires_at=?+(COALESCE(eta_minutes,30)*60)+?, confirm_tries=0, updated_at=?
            WHERE id=?
        """, (now_epoch, now_epoch, now_epoch, grace_sec, ts, int(bid)))
        await conn.commit()

    if int(user_id) > 0:
        await try_send(bot, int(user_id), 
            "📞 <b>Освободилось место! Вас ожидают.</b>\n"
            "Пожалуйста, подъезжайте.\n\n"
            f"Когда будете на месте — нажмите кнопку:",
            reply_markup=arrived_kb(int(bid))
        )
    return "OK"

async def offer_send_next_candidate(bot: Bot, admin_id: int) -> str:
    now = now_dt()
    today = now.date()
    cand = await db.pick_future_candidate_for_offer(today, db.now_ts())
    if not cand: return "Нет кандидатов"

    bid, user_id, day_s, seq, car, issue, phone = cand
    user_id = int(user_id or 0)
    expires_at = db.now_ts() + SETTINGS.OFFER_EXPIRE_MINUTES * 60

    if user_id <= 0:
        await db.clear_offer(int(bid), cooldown_until=db.now_ts() + OFFER_COOLDOWN_MINUTES * 60)
        return "Без ТГ"

    await db.set_offer_pending(int(bid), today, int(expires_at))

    await try_send(
        bot, user_id,
        f"🔥 <b>Появилось место СЕГОДНЯ!</b>\n\n"
        f"Вместо записи {date.fromisoformat(day_s).strftime('%d.%m')} мы можем принять вас прямо сейчас.\n"
        f"🚗 {car}\n\n"
        "Сможете подъехать в ближайшее время?",
        reply_markup=offer_today_kb(int(bid))
    )
    return "Отправлено"

# ==================== ДЕЙСТВИЯ АДМИНА ====================
async def admin_done(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT user_id FROM bookings WHERE id=? AND status='in_service'", (int(bid),))
        row = await cur.fetchone()
        if not row: return False, "⛔ Ошибка статуса."
        user_id = int(row[0] or 0)
        cur = await conn.execute("UPDATE bookings SET status='done', updated_at=? WHERE id=? AND status='in_service'", (ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0:
        if user_id > 0:
            await try_send(bot, user_id, "✅ <b>Ваше авто готово!</b>\nМожно забирать.")
            asyncio.create_task(request_review_later(bot, user_id, bid))
        await auto_fill_to_max(bot, admin_id, reason="Авто уехало")
        return True, "✅ Готово."
    return False, "⛔ Ошибка."

async def admin_force_accept(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        cnt = (await (await conn.execute("SELECT COUNT(*) FROM bookings WHERE status='in_service'")).fetchone())[0]
        if cnt >= SETTINGS.MAX_IN_SERVICE:
            await conn.commit()
            return False, f"⛔ Бокс занят ({SETTINGS.MAX_IN_SERVICE} авто)."
        cur = await conn.execute("UPDATE bookings SET status='in_service', updated_at=? WHERE id=?", (ts, int(bid)))
        await conn.commit()
    
    b = await db.get_booking(bid)
    if b and int(b.user_id) > 0:
        await try_send(bot, int(b.user_id), "🛠 <b>Начинаем работу с вашим авто.</b>")
    return True, "✅ В работе."

# ==================== КЛИЕНТ ====================
async def client_offer_yes(bot: Bot, bid: int, user_id: int) -> tuple[bool, str]:
    now = now_dt()
    today = now.date()
    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        load = await db.get_shop_load(today)
        if load >= SETTINGS.MAX_AT_SHOP:
            await conn.commit()
            await db.clear_offer(int(bid), cooldown_until=db.now_ts()+3600)
            return False, "😔 Извините, место заняли."
        
        cur = await conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (today.isoformat(),))
        (new_seq,) = await cur.fetchone()
        now_epoch = db.now_ts()
        grace = SETTINGS.CALL_CONFIRM_GRACE_MINUTES * 60
        
        await conn.execute("""
            UPDATE bookings
            SET day=?, seq=?, status='called', kind='live',
                offer_day=NULL, offer_stage=NULL, offer_expires_at=NULL,
                called_at=?, eta_due_at=?+1800, confirm_expires_at=?+1800+?, confirm_tries=0,
                updated_at=?
            WHERE id=?
        """, (today.isoformat(), int(new_seq), now_epoch, now_epoch, now_epoch, grace, ts, int(bid)))
        await conn.commit()
    return True, f"✅ Перенесли вас на СЕГОДНЯ (№{new_seq}). Ждем!"

async def client_offer_no(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    await db.clear_offer(int(bid), cooldown_until=db.now_ts() + OFFER_COOLDOWN_MINUTES * 60)
    await auto_fill_to_max(bot, admin_id, reason="Отказ")
    return True, "👌 Хорошо."

async def client_arrived(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("UPDATE bookings SET status='arrived', updated_at=? WHERE id=? AND status IN ('called','arrived','no_show')", (ts, int(bid)))
        await conn.commit()
    if cur.rowcount > 0:
        await notify_admin(bot, admin_id, f"📍 <b>Клиент подъехал!</b> (Запись #{bid})", reply_markup=accept_kb(int(bid)))
        return True, "📍 Мастер видит вас. Ожидайте."
    return False, "⛔ Ошибка."

# ==================== ФОНОВЫЕ ЗАДАЧИ ====================
async def auto_fill_loop(bot: Bot, admin_id: int):
    while True:
        try:
            if is_work_time(now_dt()): await auto_fill_to_max(bot, admin_id, reason="Таймер")
            await asyncio.sleep(SETTINGS.AUTO_TICK_SECONDS)
        except: await asyncio.sleep(10)

async def offer_cleaner_loop(bot: Bot, admin_id: int):
    while True:
        try:
            if await db._clear_expired_offers() > 0: await auto_fill_to_max(bot, admin_id)
            await asyncio.sleep(60)
        except: await asyncio.sleep(10)

async def rollover_loop(bot: Bot, admin_id: int):
    while True:
        await asyncio.sleep(300)

def start_background_tasks(bot: Bot, admin_id: int) -> None:
    asyncio.create_task(auto_fill_loop(bot, admin_id))
    asyncio.create_task(offer_cleaner_loop(bot, admin_id))
    asyncio.create_task(rollover_loop(bot, admin_id))
