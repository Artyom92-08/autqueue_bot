# services.py
from __future__ import annotations

import asyncio
from datetime import date, timedelta
from typing import Optional

import aiosqlite
from aiogram import Bot

from config import SETTINGS
import db
from db import (
    DB_PATH,
    STATUS_WAITING, STATUS_CALLED, STATUS_ARRIVED, STATUS_IN_SERVICE, STATUS_PARTS_WAIT,
    KIND_STATIC, KIND_LIVE,
)
from utils import is_work_time, is_working_day, next_working_day, short, now_dt
from keyboards import (
    arrived_kb, offer_today_kb, accept_kb,
    admin_time_approve_kb, reschedule_suggest_kb,
    called_confirm_kb, eta_kb,
)

OFFER_COOLDOWN_MINUTES = 365 * 24 * 60  # "не повторять" фактически навсегда (можно уменьшить)


# -------------------- notify helpers --------------------
async def notify_admin(bot: Bot, admin_id: int, text: str, reply_markup=None) -> None:
    if not admin_id:
        return
    try:
        await bot.send_message(int(admin_id), text, reply_markup=reply_markup)
    except Exception:
        pass


async def try_send(bot: Bot, user_id: int, text: str, reply_markup=None) -> bool:
    try:
        await bot.send_message(int(user_id), text, reply_markup=reply_markup)
        return True
    except Exception:
        return False
async def client_after_claim_send_status(bot: Bot, admin_id: int, bid: int) -> None:
    b = await db.get_booking(int(bid))
    if not b or int(b.user_id or 0) <= 0:
        return

    today = now_dt().date().isoformat()

    # Если админ уже позвал клиента (called) на сегодня — сразу отправим “можно подъезжать” + кнопку "Я подъехал"
    if b.day == today and b.status == "called":
        await try_send(
            bot,
            int(b.user_id),
            "📞 <b>Вас уже позвали</b> — можно подъезжать.\n\n"
            f"🔢 Номер на сегодня: <b>№{b.seq}</b>\n"
            f"🚗 Авто: {b.car_text}\n"
            f"🛠 Задача: {b.issue_text}\n\n"
            "Когда будете на месте — нажмите «📍 Я подъехал».",
            reply_markup=arrived_kb(int(bid))
        )
        return

    # Иначе просто “сводка”, чтобы человек точно видел № и дату
    await try_send(
        bot,
        int(b.user_id),
        "ℹ️ <b>Запись активна</b>.\n\n"
        f"📅 Дата: <b>{date.fromisoformat(b.day).strftime('%d.%m.%Y')}</b>\n"
        f"🔢 Номер: <b>№{b.seq}</b>\n"
        f"📌 Статус: <b>{b.status}</b>\n\n"
        "Ожидайте — мастер напишет/позовёт."
    )

# -------------------- helpers --------------------
async def _get_booking_brief(bid: int) -> Optional[tuple]:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            SELECT id, day, seq, user_id, car_text, issue_text, phone,
                   status, kind, eta_minutes, manual_call_only, needs_admin_ok,
                   offer_stage
            FROM bookings
            WHERE id=?
        """, (int(bid),))
        return await cur.fetchone()


async def _set_called_if_waiting_today(bid: int, today: date) -> bool:
    """
    Ставит called + армирует таймеры confirm.
    По ТЗ: eta_due_at = called_at + (eta_minutes or 30)*60
    """
    ts = db.now_iso()
    now_epoch = db.now_ts()
    today_s = today.isoformat()
    grace_sec = SETTINGS.CALL_CONFIRM_GRACE_MINUTES * 60

    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            UPDATE bookings
            SET status='called',
                called_at=?,
                eta_due_at=? + (COALESCE(eta_minutes, 30) * 60),
                confirm_expires_at=? + (COALESCE(eta_minutes, 30) * 60) + ?,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=?
              AND day=?
              AND status='waiting'
              AND manual_call_only=0
              AND needs_admin_ok=0
        """, (int(now_epoch), int(now_epoch), int(now_epoch), int(grace_sec), ts, int(bid), today_s))
        await conn.commit()
        return cur.rowcount > 0


async def _find_nearest_available_day(start: date, limit_days: int = 120) -> Optional[date]:
    d = start
    for _ in range(limit_days):
        if is_working_day(d):
            ok, _ = await db.is_day_available(d)
            if ok:
                return d
        d += timedelta(days=1)
    return None


# -------------------- OFFER expiry + next candidate --------------------
async def _clear_expired_offers() -> int:
    """
    Если клиент не ответил за OFFER_EXPIRE_MINUTES:
    - снимаем offer_stage
    - ставим offer_cooldown_until, чтобы больше не предлагать
    """
    now_epoch = db.now_ts()
    cooldown_until = now_epoch + OFFER_COOLDOWN_MINUTES * 60
    ts = db.now_iso()

    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            UPDATE bookings
            SET offer_day=NULL,
                offer_stage=NULL,
                offer_expires_at=NULL,
                offer_cooldown_until=?,
                updated_at=?
            WHERE offer_stage IN ('pending','awaiting_eta')
              AND offer_expires_at IS NOT NULL
              AND offer_expires_at <= ?
        """, (int(cooldown_until), ts, int(now_epoch)))
        await conn.commit()
        return int(cur.rowcount or 0)


# -------------------- AUTO NOW (one button) --------------------
async def admin_auto_now(bot: Bot, admin_id: int) -> str:
    """
    1) добираем на сегодня до MAX_AT_SHOP
    2) если поток всё ещё < MAX_AT_SHOP и сегодня звать некого -> отправляем 1 offer "сегодня"
       (если нет активного оффера; истёкшие офферы снимаем)
    """
    now = now_dt()
    today = now.date()

    before = await db.get_shop_load(today)
    called = await auto_fill_to_max(bot, admin_id, reason="adm:auto_now")
    after = await db.get_shop_load(today)

    tail = ""
    if is_work_time(now) and after < SETTINGS.MAX_AT_SHOP:
        await _clear_expired_offers()

        today_s = today.isoformat()
        async with aiosqlite.connect(DB_PATH) as conn:
            cur = await conn.execute("""
                SELECT 1
                FROM bookings
                WHERE day=? AND status='waiting' AND manual_call_only=0 AND needs_admin_ok=0
                LIMIT 1
            """, (today_s,))
            exists_today_waiting = await cur.fetchone() is not None

        if not exists_today_waiting:
            tail = "\n" + await offer_send_next_candidate(bot, admin_id)

    return f"🚀 Автодобор: было {before}/{SETTINGS.MAX_AT_SHOP}, стало {after}/{SETTINGS.MAX_AT_SHOP}, позвал {called}.{tail}"


# -------------------- CALL NEXT / AUTOFILL --------------------
async def call_next_today(bot: Bot, admin_id: int) -> str:
    now = now_dt()
    today = now.date()

    if not is_work_time(now):
        return "⛔ Работает только в рабочее время."

    if await db.get_shop_load(today) >= SETTINGS.MAX_AT_SHOP:
        return f"⛔ Уже {SETTINGS.MAX_AT_SHOP}/{SETTINGS.MAX_AT_SHOP} в потоке."

    today_s = today.isoformat()
    ts = db.now_iso()
    now_epoch = db.now_ts()
    grace_sec = SETTINGS.CALL_CONFIRM_GRACE_MINUTES * 60

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        cur = await conn.execute("""
            SELECT id, seq, user_id, car_text, issue_text, phone
            FROM bookings
            WHERE day=?
              AND status='waiting'
              AND manual_call_only=0
              AND needs_admin_ok=0
            ORDER BY CASE WHEN user_id=0 THEN 1 ELSE 0 END, seq, id
            LIMIT 1
        """, (today_s,))
        row = await cur.fetchone()

        if not row:
            await conn.commit()
            return "⛔ На сегодня нет ожидающих (или ждут решения/ручного вызова)."

        bid, seq, user_id, car, issue, phone = row

        await conn.execute("""
            UPDATE bookings
            SET status='called',
                called_at=?,
                eta_due_at=? + (COALESCE(eta_minutes, 30) * 60),
                confirm_expires_at=? + (COALESCE(eta_minutes, 30) * 60) + ?,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND status='waiting'
        """, (int(now_epoch), int(now_epoch), int(now_epoch), int(grace_sec), ts, int(bid)))

        await conn.commit()

    user_id = int(user_id or 0)
    if user_id > 0:
        await try_send(
            bot,
            user_id,
            "📞 <b>Можно подъезжать</b>.\n"
            "Пожалуйста, подъедьте в течение ~30 минут.\n\n"
            f"🔢 Номер на сегодня: <b>№{seq}</b>\n"
            f"🚗 Авто: {car}\n"
            f"🛠 Задача: {issue}\n\n"
            "Когда будете на месте — нажмите «📍 Я подъехал».",
            reply_markup=arrived_kb(int(bid))
        )
        return f"✅ Позвал №{seq}"
    else:
        await notify_admin(
            bot, admin_id,
            "☎️ <b>Клиент без бота</b> (нужно звонить вручную)\n"
            f"Вызов №{seq}\n"
            f"Тел: {phone}\n"
            f"{car} — {issue}"
        )
        return f"✅ Вызов №{seq} (клиент без бота)"


async def auto_fill_to_max(bot: Bot, admin_id: int, reason: str = "tick") -> int:
    now = now_dt()
    if not is_work_time(now):
        return 0

    called = 0
    while True:
        if await db.get_shop_load(now.date()) >= SETTINGS.MAX_AT_SHOP:
            break
        res = await call_next_today(bot, admin_id)
        if res.startswith("⛔"):
            break
        called += 1

    if called and reason != "tick":
        await notify_admin(
            bot, admin_id,
            f"🤖 <b>Добор очереди</b> ({reason}): позвал {called}, чтобы было {SETTINGS.MAX_AT_SHOP} в потоке."
        )
    return called


# -------------------- OFFER TODAY --------------------
async def offer_send_next_candidate(bot: Bot, admin_id: int) -> str:
    now = now_dt()
    today = now.date()

    if not is_work_time(now):
        return "⛔ Предложение «сегодня» имеет смысл только в рабочее время."

    ok_day, _ = await db.is_day_available(today)
    if not ok_day:
        return f"⛔ На сегодня уже набрано {SETTINGS.MAX_CARS_PER_DAY} записей."

    if await db.get_shop_load(today) >= SETTINGS.MAX_AT_SHOP:
        return f"⛔ Поток уже заполнен {SETTINGS.MAX_AT_SHOP}/{SETTINGS.MAX_AT_SHOP}."

    # если есть активный оффер — ждём
    active = await db.get_active_offer_row(db.now_ts())
    if active:
        return "⏳ Уже есть активное предложение — ждём ответ."

    # берём кандидата из будущих дней по очереди
    cand = await db.pick_future_candidate_for_offer(today, db.now_ts())
    if not cand:
        return "⛔ Нет подходящих кандидатов в будущих днях."

    bid, user_id, day_s, seq, car, issue, phone = cand
    user_id = int(user_id or 0)

    expires_at = db.now_ts() + SETTINGS.OFFER_EXPIRE_MINUTES * 60

    # если без бота — просто сообщаем админу и ставим cooldown, чтобы не повторять
    if user_id <= 0:
        await notify_admin(
            bot, admin_id,
            "☎️ <b>Клиент без бота</b>: предложите «сегодня» вручную.\n"
            f"Дата записи: {day_s} №{seq}\n"
            f"Тел: {phone}\n"
            f"{car} — {issue}"
        )
        await db.clear_offer(int(bid), cooldown_until=db.now_ts() + OFFER_COOLDOWN_MINUTES * 60)
        return "✅ Кандидат без бота — предложите по телефону."

    await db.set_offer_pending(int(bid), today, int(expires_at))

    await try_send(
        bot,
        user_id,
        f"📅 <b>Есть возможность принять сегодня</b> вместо {date.fromisoformat(day_s).strftime('%d.%m.%Y')}.\n\n"
        f"🚗 Авто: {car}\n"
        f"🛠 Задача: {issue}\n\n"
        "Если вам удобно — нажмите кнопку:",
        reply_markup=offer_today_kb(int(bid))
    )
    return "✅ Предложение отправлено."


# -------------------- CLIENT ACTIONS --------------------
async def client_cancel(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    brief = await _get_booking_brief(int(bid))
    if not brief:
        return False, "⛔ Запись не найдена."

    _id, day_s, seq, uid, car, issue, phone, status, kind, minutes, mco, needs_ok, offer_stage = brief
    if int(uid or 0) != int(user_id):
        return False, "⛔ Нет доступа к этой записи."

    if status == STATUS_IN_SERVICE:
        return False, "⛔ Машина уже в работе. Для отмены свяжитесь с мастером."

    ok = await db.cancel_booking(int(bid), int(user_id))
    if not ok:
        return False, "⛔ Не удалось отменить."

    await notify_admin(
        bot, admin_id,
        "❌ <b>Клиент отменил запись</b>\n"
        f"{day_s} №{seq}\n"
        f"{car}\n{issue}\n{phone}"
    )

    today = now_dt().date()
    if day_s == today.isoformat() and status in (STATUS_CALLED, STATUS_ARRIVED):
        await auto_fill_to_max(bot, admin_id, reason="после отмены клиентом")

    return True, "✅ Запись отменена."


async def client_arrived(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    """
    По ТЗ: разрешаем подъехал также из no_show.
    """
    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        cur = await conn.execute("""
            SELECT status, day, seq, car_text, issue_text, phone
            FROM bookings
            WHERE id=? AND user_id=?
        """, (int(bid), int(user_id)))
        row = await cur.fetchone()
        if not row:
            await conn.commit()
            return False, "⛔ Запись не найдена."

        status, day_s, seq, car, issue, phone = row
        if status not in (STATUS_CALLED, STATUS_ARRIVED, "no_show"):
            await conn.commit()
            return False, "⛔ Сейчас это действие неактуально."

        cur = await conn.execute("""
            UPDATE bookings
            SET status='arrived',
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND user_id=? AND status IN ('called','arrived','no_show')
        """, (ts, int(bid), int(user_id)))
        await conn.commit()

    if cur.rowcount > 0:
        await notify_admin(
            bot, admin_id,
            "📍 <b>Клиент подъехал</b>\n"
            f"{date.fromisoformat(day_s).strftime('%d.%m.%Y')} №{seq}\n"
            f"{short(car, 30)} — {short(issue, 40)}\n"
            f"{phone}\n\n"
            "Нажмите кнопку ниже, чтобы <b>взять в работу</b>:",
            reply_markup=accept_kb(int(bid))
        )
        return True, "📍 Принято. Мастер видит, что вы на месте. Подождите — вас позовут."

    return False, "⛔ Не получилось."


async def client_offer_yes(bot: Bot, bid: int, user_id: int) -> tuple[bool, str]:
    # клиент согласился — ждём его время; оффер истечёт через 10 минут автоматически
    now = now_dt()
    today = now.date()

    ok_day, _ = await db.is_day_available(today)
    if not ok_day:
        await db.clear_offer(int(bid), cooldown_until=db.now_ts() + OFFER_COOLDOWN_MINUTES * 60)
        return False, "⛔ На сегодня мест уже нет. Оставили вашу запись на исходный день."

    expires_at = db.now_ts() + SETTINGS.OFFER_EXPIRE_MINUTES * 60
    ok = await db.set_offer_awaiting_eta(int(bid), today, int(expires_at))
    if not ok:
        return False, "⛔ Предложение уже не актуально."

    return True, "✅ Отлично! Напишите, когда сможете подъехать (например 40 мин или 18:30)."


async def client_offer_no(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    await db.clear_offer(int(bid), cooldown_until=db.now_ts() + OFFER_COOLDOWN_MINUTES * 60)
    await notify_admin(bot, admin_id, f"❌ Клиент отказался от «сегодня». Запись #{bid}")
    return True, "Хорошо, оставили на исходный день."


# -------- client ETA -> admin approve --------
async def client_set_eta_from_button(bot: Bot, admin_id: int, bid: int, user_id: int, minutes: int) -> tuple[bool, str]:
    return await _client_submit_minutes(bot, admin_id, bid, user_id, int(minutes))


async def client_set_eta_from_text(bot: Bot, admin_id: int, bid: int, user_id: int, minutes: int) -> tuple[bool, str]:
    return await _client_submit_minutes(bot, admin_id, bid, user_id, int(minutes))


async def _client_submit_minutes(bot: Bot, admin_id: int, bid: int, user_id: int, minutes: int) -> tuple[bool, str]:
    minutes = max(SETTINGS.ETA_MIN, min(SETTINGS.ETA_MAX, int(minutes)))
    manual_call_only = 0 if minutes <= 30 else 1
    ts = db.now_iso()

    brief = await _get_booking_brief(int(bid))
    if not brief:
        return False, "⛔ Запись не найдена."

    _id, day_s, seq, uid, car, issue, phone, status, kind, old_minutes, mco, needs_ok, offer_stage = brief
    if int(uid or 0) != int(user_id):
        return False, "⛔ Нет доступа."

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        # если запись была called — снимаем в waiting, чтобы не держать слот
        new_status = "waiting" if status == "called" else status

        cur = await conn.execute("""
            UPDATE bookings
            SET status=?,
                eta_minutes=?,
                manual_call_only=?,
                needs_admin_ok=1,

                called_at=NULL,
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,

                updated_at=?
            WHERE id=? AND user_id=? AND status IN ('waiting','called')
        """, (new_status, int(minutes), int(manual_call_only), ts, int(bid), int(user_id)))

        await conn.commit()

        if cur.rowcount <= 0:
            return False, "⛔ Сейчас нельзя сохранить время."

    await notify_admin(
        bot, admin_id,
        "⏱ <b>Клиент написал время прибытия</b>\n"
        f"Запись #{bid}\n"
        f"Дата: {day_s} №{seq}\n"
        f"Через: <b>{minutes} мин</b>\n"
        f"{short(car, 30)} — {short(issue, 40)}\n"
        f"{phone}\n\n"
        "Принять?",
        reply_markup=admin_time_approve_kb(int(bid))
    )

    return True, (
        f"✅ Спасибо! Записал: вы сможете подъехать примерно через <b>{minutes} минут</b>.\n"
        "Передал мастеру — он подтвердит."
    )


# -------------------- ADMIN: approve time --------------------
async def admin_approve_yes(bot: Bot, admin_id: int, bid: int) -> str:
    b = await db.get_booking(int(bid))
    if not b:
        return "⛔ Запись не найдена."
    if int(b.needs_admin_ok) != 1:
        return "ℹ️ Решение уже не требуется."

    now = now_dt()
    today = now.date()
    ts = db.now_iso()

    # Если это оффер "сегодня" — переносим на сегодня только после решения админа
    if b.offer_stage == "awaiting_eta":
        ok_day, _ = await db.is_day_available(today)
        if not ok_day:
            return await admin_approve_no(bot, admin_id, int(bid), reason="лимит на сегодня заполнен")

        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("BEGIN IMMEDIATE")
            cur = await conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (today.isoformat(),))
            (new_seq,) = await cur.fetchone()
            await conn.execute("""
                UPDATE bookings
                SET day=?, seq=?, kind='live', updated_at=?
                WHERE id=? AND status='waiting'
            """, (today.isoformat(), int(new_seq), ts, int(bid)))
            await conn.commit()

        await db.clear_offer(int(bid), cooldown_until=None)

    # Снимаем ожидание решения
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            UPDATE bookings
            SET needs_admin_ok=0, updated_at=?
            WHERE id=?
        """, (ts, int(bid)))
        await conn.commit()

    # Если <=30 и есть место — можно сразу позвать
    b2 = await db.get_booking(int(bid))
    if not b2:
        return "✅ Принято."

    if (
        b2.day == today.isoformat()
        and b2.status == STATUS_WAITING
        and int(b2.manual_call_only) == 0
        and is_work_time(now)
        and await db.get_shop_load(today) < SETTINGS.MAX_AT_SHOP
    ):
        if await _set_called_if_waiting_today(int(bid), today):
            await try_send(
                bot,
                int(b2.user_id),
                "📞 <b>Можно подъезжать</b>.\n"
                "Пожалуйста, подъедьте в течение ~30 минут.\n\n"
                f"🔢 Номер на сегодня: <b>№{b2.seq}</b>\n"
                f"🚗 Авто: {b2.car_text}\n"
                f"🛠 Задача: {b2.issue_text}\n\n"
                "Когда будете на месте — нажмите «📍 Я подъехал».",
                reply_markup=arrived_kb(int(bid))
            )
            return f"✅ Принял и позвал (№{b2.seq})."

    await try_send(bot, int(b2.user_id), "✅ Мастер подтвердил. Ждите сообщения «Можно подъезжать».")
    return "✅ Принято. Оставил в ожидании."


async def admin_approve_next(bot: Bot, admin_id: int, bid: int) -> str:
    """
    По ТЗ:
    - снять needs_admin_ok
    - сбросить eta_minutes
    - поставить manual_call_only=1 (чтобы автодобор не позвал его снова)
    - уведомить клиента "укажите другое время"
    - auto_fill_to_max() чтобы позвать следующего
    """
    b = await db.get_booking(int(bid))
    if not b:
        return "⛔ Запись не найдена."
    if int(b.needs_admin_ok) != 1:
        return "ℹ️ Решение уже не требуется."

    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("""
            UPDATE bookings
            SET status='waiting',
                needs_admin_ok=0,
                eta_minutes=NULL,
                manual_call_only=1,

                called_at=NULL,
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,

                updated_at=?
            WHERE id=? AND needs_admin_ok=1 AND status IN ('waiting','called')
        """, (ts, int(bid)))
        await conn.commit()

    if int(b.user_id) > 0:
        await try_send(
            bot,
            int(b.user_id),
            "⏭ Сейчас беру следующего клиента.\n"
            "Пожалуйста, укажите <b>другое время</b>, когда сможете подъехать:",
            reply_markup=eta_kb(int(bid))
        )

    await auto_fill_to_max(bot, admin_id, reason="adm:approve_next")
    return "⏭ Ок, предложил следующему. Клиенту отправил запрос другого времени."


async def admin_approve_no(bot: Bot, admin_id: int, bid: int, reason: str = "") -> str:
    b = await db.get_booking(int(bid))
    if not b:
        return "⛔ Запись не найдена."

    now = now_dt()
    today = now.date()
    ts = db.now_iso()

    # Если это оффер "сегодня" — просто убираем оффер и оставляем на исходной дате
    if b.offer_stage == "awaiting_eta":
        await db.clear_offer(int(bid), cooldown_until=db.now_ts() + OFFER_COOLDOWN_MINUTES * 60)
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("""
                UPDATE bookings
                SET needs_admin_ok=0,
                    eta_minutes=NULL,
                    manual_call_only=0,
                    updated_at=?
                WHERE id=?
            """, (ts, int(bid)))
            await conn.commit()
        await try_send(bot, int(b.user_id), "⛔ Сегодня не получится. Оставили вашу запись на исходный день.")
        return "↩️ Оставил на исходной дате."

    # Если запись на сегодня — переносим на ближайший свободный день и даём клиенту выбор
    if b.day == today.isoformat() and b.status in (STATUS_WAITING, STATUS_CALLED):
        suggest = await _find_nearest_available_day(next_working_day(today))
        if not suggest:
            async with aiosqlite.connect(DB_PATH) as conn:
                await conn.execute("""
                    UPDATE bookings
                    SET status='canceled', needs_admin_ok=0, updated_at=?
                    WHERE id=?
                """, (ts, int(bid)))
                await conn.commit()
            await try_send(bot, int(b.user_id), "⛔ Не нашли свободную дату. Запись отменена, пожалуйста запишитесь заново.")
            await notify_admin(bot, admin_id, f"⛔ Не нашёл свободную дату для переноса. Запись #{bid} отменена.")
            return "⛔ Не нашёл свободную дату — отменил."

        new_seq = await db.move_booking_to_day_append_seq(int(bid), suggest, new_kind=KIND_STATIC)
        if new_seq is None:
            return "⛔ Не удалось перенести."

        await try_send(
            bot,
            int(b.user_id),
            f"⛔ Сегодня не получится по времени.\n"
            f"Ближайшая свободная дата: <b>{suggest.strftime('%d.%m.%Y')}</b> (№{new_seq}).\n\n"
            "Если нужно — выберите другую дату или отмените.",
            reply_markup=reschedule_suggest_kb(int(bid), suggest)
        )

        await notify_admin(
            bot, admin_id,
            f"📅 Отклонил по времени и перенёс запись #{bid} на {suggest.strftime('%d.%m.%Y')} №{new_seq}."
        )
        return f"📅 Перенёс на {suggest.strftime('%d.%m.%Y')} №{new_seq}."

    # иначе просто снимаем флаг
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("UPDATE bookings SET needs_admin_ok=0, updated_at=? WHERE id=?", (ts, int(bid)))
        await conn.commit()

    await try_send(bot, int(b.user_id), "⛔ Пока не получится. Мастер позовёт позже.")
    return "❌ Отклонил."


# -------------------- ADMIN: per booking actions --------------------
async def admin_call_bid(bot: Bot, admin_id: int, bid: int) -> str:
    """
    Админ вручную зовёт конкретную запись на сегодня (waiting/called).
    + армирует called confirm таймеры.
    """
    now = now_dt()
    today = now.date()

    if not is_work_time(now):
        return "⛔ Только в рабочее время."
    if await db.get_shop_load(today) >= SETTINGS.MAX_AT_SHOP:
        return "⛔ Поток уже заполнен."

    b = await db.get_booking(int(bid))
    if not b:
        return "⛔ Запись не найдена."
    if b.day != today.isoformat():
        return "⛔ Можно позвать вручную только запись на сегодня."
    if int(b.needs_admin_ok) == 1:
        return "⛔ Ждёт решения по времени (✅/❌)."
    if b.status not in (STATUS_WAITING, STATUS_CALLED):
        return "⛔ Сейчас нельзя позвать (не waiting/called)."

    ts = db.now_iso()
    now_epoch = db.now_ts()
    grace_sec = SETTINGS.CALL_CONFIRM_GRACE_MINUTES * 60

    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("""
            UPDATE bookings
            SET status='called',
                called_at=?,
                eta_due_at=? + (COALESCE(eta_minutes, 30) * 60),
                confirm_expires_at=? + (COALESCE(eta_minutes, 30) * 60) + ?,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND status IN ('waiting','called')
        """, (int(now_epoch), int(now_epoch), int(now_epoch), int(grace_sec), ts, int(bid)))
        await conn.commit()

    if cur.rowcount <= 0:
        return "⛔ Не получилось."

    if int(b.user_id) > 0:
        await try_send(
            bot,
            int(b.user_id),
            "📞 <b>Можно подъезжать</b>.\n"
            "Пожалуйста, подъедьте в течение ~30 минут.\n\n"
            f"🔢 Номер на сегодня: <b>№{b.seq}</b>\n"
            f"🚗 Авто: {b.car_text}\n"
            f"🛠 Задача: {b.issue_text}\n\n"
            "Когда будете на месте — нажмите «📍 Я подъехал».",
            reply_markup=arrived_kb(int(bid))
        )
        return f"✅ Позвал №{b.seq}."
    return f"✅ Позвал №{b.seq} (клиент без бота)."


async def admin_force_accept(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    """
    По ТЗ: разрешить в работу также из no_show.
    """
    ts = db.now_iso()

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        cur = await conn.execute("SELECT COUNT(*) FROM bookings WHERE status='in_service'")
        (cnt,) = await cur.fetchone()
        if int(cnt) >= SETTINGS.MAX_IN_SERVICE:
            await conn.commit()
            return False, f"⛔ Уже {SETTINGS.MAX_IN_SERVICE} машина в работе."

        cur = await conn.execute("""
            SELECT user_id
            FROM bookings
            WHERE id=? AND status IN ('waiting','called','arrived','no_show')
        """, (int(bid),))
        row = await cur.fetchone()
        if not row:
            await conn.commit()
            return False, "⛔ Можно взять в работу только если статус waiting/called/arrived/no_show."

        user_id = int(row[0] or 0)

        cur = await conn.execute("""
            UPDATE bookings
            SET status='in_service',
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND status IN ('waiting','called','arrived','no_show')
        """, (ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0 and user_id > 0:
        await try_send(bot, user_id, "🛠 <b>Машина принята в работу</b>. Начинаю заниматься.")

    return (cur.rowcount > 0), ("✅ Взято в работу." if cur.rowcount > 0 else "⛔ Не получилось.")


# -------------------- CLIENT: called confirm buttons --------------------
async def client_confirm_yes(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    ts = db.now_iso()
    now_epoch = db.now_ts()

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        cur = await conn.execute("""
            SELECT day, seq, car_text, issue_text, phone
            FROM bookings
            WHERE id=? AND user_id=? AND status='called'
        """, (int(bid), int(user_id)))
        row = await cur.fetchone()
        if not row:
            await conn.commit()
            return False, "⛔ Запись не найдена или действие неактуально."

        day_s, seq, car, issue, phone = row

        await conn.execute("""
            UPDATE bookings
            SET confirm_expires_at=NULL,
                confirm_last_sent_at=?,
                updated_at=?
            WHERE id=? AND user_id=? AND status='called'
        """, (int(now_epoch), ts, int(bid), int(user_id)))

        await conn.commit()

    await notify_admin(
        bot, admin_id,
        "✅ <b>Клиент подтвердил, что приедет</b>\n"
        f"{day_s} №{seq}\n"
        f"{short(car, 30)} — {short(issue, 40)}\n"
        f"{phone}"
    )
    return True, "✅ Ок, ждём вас."


async def client_confirm_move(bot: Bot, admin_id: int, bid: int, user_id: int) -> tuple[bool, str]:
    """
    По твоему ТЗ в bot.py: возвращаем (ok,msg) и показываем home.
    Переносим автоматически на ближайший свободный день.
    """
    b = await db.get_booking(int(bid))
    if not b or int(b.user_id) != int(user_id):
        return False, "⛔ Запись не найдена."
    if b.status != STATUS_CALLED:
        return False, "⛔ Сейчас перенос неактуален."

    today = now_dt().date()
    suggest = await _find_nearest_available_day(next_working_day(today))
    if not suggest:
        return False, "⛔ Не нашёл ближайшую свободную дату. Лучше отмените и запишитесь заново."

    new_seq = await db.move_booking_to_day_append_seq(int(bid), suggest, new_kind=KIND_STATIC)
    if new_seq is None:
        return False, "⛔ Не удалось перенести."

    await notify_admin(
        bot, admin_id,
        "↩️ <b>Клиент попросил перенос</b>\n"
        f"#{bid}: {today.strftime('%d.%m.%Y')} №{b.seq} → {suggest.strftime('%d.%m.%Y')} №{new_seq}\n"
        f"{short(b.car_text, 30)} — {short(b.issue_text, 40)}\n"
        f"{b.phone}"
    )

    await auto_fill_to_max(bot, admin_id, reason="после переноса клиентом")

    return True, f"↩️ Перенёс на <b>{suggest.strftime('%d.%m.%Y')}</b> • №{new_seq}."


# -------------------- CLIENT: reschedule result --------------------
async def client_reschedule_accept(bot: Bot, admin_id: int, bid: int, user_id: int, day: date) -> tuple[bool, str]:
    b = await db.get_booking(int(bid))
    if not b or int(b.user_id) != int(user_id):
        return False, "⛔ Запись не найдена."
    await notify_admin(bot, admin_id, f"✅ Клиент подтвердил дату {day.strftime('%d.%m.%Y')} (запись #{bid}).")
    return True, "✅ Хорошо. Запись остаётся на этой дате."


async def client_reschedule_move(bot: Bot, admin_id: int, bid: int, user_id: int, new_day: date) -> tuple[bool, str]:
    b = await db.get_booking(int(bid))
    if not b or int(b.user_id) != int(user_id):
        return False, "⛔ Запись не найдена."

    ok, reason = await db.is_day_available(new_day)
    if not ok:
        return False, f"⛔ Эта дата занята: {reason}"

    new_seq = await db.move_booking_to_day_append_seq(int(bid), new_day, new_kind=KIND_STATIC)
    if new_seq is None:
        return False, "⛔ Не удалось перенести."

    await notify_admin(bot, admin_id, f"📅 Клиент выбрал новую дату: #{bid} → {new_day.strftime('%d.%m.%Y')} №{new_seq}.")
    return True, f"✅ Перенёс: <b>{new_day.strftime('%d.%m.%Y')}</b> • №{new_seq}."


# -------------------- ADMIN: normal flow actions --------------------
async def admin_accept_to_service(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    """
    Стандартно: arrived -> in_service
    """
    ts = db.now_iso()

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        cur = await conn.execute("SELECT COUNT(*) FROM bookings WHERE status='in_service'")
        (cnt,) = await cur.fetchone()
        if int(cnt) >= SETTINGS.MAX_IN_SERVICE:
            await conn.commit()
            return False, f"⛔ Уже {SETTINGS.MAX_IN_SERVICE} машина в работе."

        cur = await conn.execute("SELECT user_id FROM bookings WHERE id=? AND status='arrived'", (int(bid),))
        row = await cur.fetchone()
        if not row:
            await conn.commit()
            return False, "⛔ Можно взять в работу только «подъехал»."

        user_id = int(row[0] or 0)

        cur = await conn.execute("""
            UPDATE bookings
            SET status='in_service',
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND status='arrived'
        """, (ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0 and user_id > 0:
        await try_send(bot, user_id, "🛠 <b>Машина принята в работу</b>.")
    return (cur.rowcount > 0), ("✅ Взято в работу." if cur.rowcount > 0 else "⛔ Не получилось.")


async def admin_done(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    ts = db.now_iso()

    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT user_id FROM bookings WHERE id=? AND status='in_service'", (int(bid),))
        row = await cur.fetchone()
        if not row:
            return False, "⛔ Можно отметить «готово» только если «в работе»."
        user_id = int(row[0] or 0)

        cur = await conn.execute("""
            UPDATE bookings
            SET status='done', updated_at=?
            WHERE id=? AND status='in_service'
        """, (ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0 and user_id > 0:
        await try_send(bot, user_id, "✅ <b>Готово</b>. Можно забирать авто.")

    if cur.rowcount > 0:
        await auto_fill_to_max(bot, admin_id, reason="после готово")

    return (cur.rowcount > 0), ("✅ Готово." if cur.rowcount > 0 else "⛔ Не получилось.")


async def admin_no_show(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    ts = db.now_iso()
    today_s = now_dt().date().isoformat()

    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute("SELECT user_id, day FROM bookings WHERE id=?", (int(bid),))
        row = await cur.fetchone()
        if not row:
            return False, "⛔ Запись не найдена."
        user_id = int(row[0] or 0)
        day_s = row[1]

        cur = await conn.execute("""
            UPDATE bookings
            SET status='no_show',
                eta_minutes=NULL,
                manual_call_only=0,
                needs_admin_ok=0,
                called_at=NULL,
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND status IN ('called','arrived')
        """, (ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0 and user_id > 0:
        await try_send(bot, user_id, "🚫 Отмечено: <b>неявка</b>.")

    if cur.rowcount > 0 and day_s == today_s:
        await auto_fill_to_max(bot, admin_id, reason="после неявки")

    return (cur.rowcount > 0), ("🚫 Неявка." if cur.rowcount > 0 else "⛔ Не получилось.")


async def admin_wait_parts(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    ts = db.now_iso()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        cur = await conn.execute("""
            SELECT user_id, car_text, phone
            FROM bookings
            WHERE id=? AND status='in_service'
        """, (int(bid),))
        row = await cur.fetchone()
        if not row:
            await conn.commit()
            return False, "⛔ Можно только если «в работе»."

        user_id = int(row[0] or 0)
        car, phone = row[1], row[2]

        cur = await conn.execute("""
            UPDATE bookings
            SET status='parts_wait', updated_at=?
            WHERE id=? AND status='in_service'
        """, (ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0 and user_id > 0:
        await try_send(bot, user_id, "🧩 <b>Ждём запчасть</b>.")

    if cur.rowcount > 0:
        await notify_admin(bot, admin_id, f"🧩 Ждёт запчасть: {short(car, 24)} • {phone}")
        await auto_fill_to_max(bot, admin_id, reason="после ждёт запчасть")

    return (cur.rowcount > 0), ("🧩 Ждёт запчасть." if cur.rowcount > 0 else "⛔ Не получилось.")


async def admin_parts_ok(bot: Bot, admin_id: int, bid: int) -> tuple[bool, str]:
    now = now_dt()
    today_s = now.date().isoformat()
    ts = db.now_iso()

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")
        cur = await conn.execute("""
            SELECT user_id, car_text, phone
            FROM bookings
            WHERE id=? AND status='parts_wait'
        """, (int(bid),))
        row = await cur.fetchone()
        if not row:
            await conn.commit()
            return False, "⛔ Сейчас не «ждёт запчасть»."

        user_id = int(row[0] or 0)
        car, phone = row[1], row[2]

        cur = await conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (today_s,))
        (new_seq,) = await cur.fetchone()

        cur = await conn.execute("""
            UPDATE bookings
            SET day=?, seq=?, status='arrived', updated_at=?
            WHERE id=? AND status='parts_wait'
        """, (today_s, int(new_seq), ts, int(bid)))
        await conn.commit()

    if cur.rowcount > 0 and user_id > 0:
        await try_send(bot, user_id, "📦 <b>Запчасть пришла</b>. Скоро возьму в работу.")
    if cur.rowcount > 0:
        await notify_admin(bot, admin_id, f"📦 Запчасть пришла: сегодня №{int(new_seq)} • {short(car, 26)} • {phone}", reply_markup=accept_kb(int(bid)))
    return (cur.rowcount > 0), ("📦 Запчасть пришла." if cur.rowcount > 0 else "⛔ Не получилось.")


# -------------------- LOOPS --------------------
async def called_confirm_loop(bot: Bot, admin_id: int):
    """
    По ТЗ:
    - когда now >= eta_due_at и пока now < confirm_expires_at:
        отправляем переспрос максимум 2 раза
        с интервалом >= 5 минут
    - когда now >= confirm_expires_at:
        ставим no_show, чистим таймеры/ETA, уведомляем админа, делаем auto_fill_to_max()
    """
    while True:
        try:
            now = now_dt()
            today = now.date()
            if not is_working_day(today):
                await asyncio.sleep(SETTINGS.CALL_CONFIRM_TICK_SECONDS)
                continue

            now_epoch = db.now_ts()
            retry_sec = SETTINGS.CALL_CONFIRM_RETRY_MINUTES * 60
            max_tries = SETTINGS.CALL_CONFIRM_MAX_TRIES
            ts = db.now_iso()

            # 1) переспрос
            async with aiosqlite.connect(DB_PATH) as conn:
                cur = await conn.execute("""
                    SELECT id, user_id, confirm_tries, confirm_last_sent_at
                    FROM bookings
                    WHERE day=?
                      AND status='called'
                      AND eta_due_at IS NOT NULL
                      AND confirm_expires_at IS NOT NULL
                      AND eta_due_at <= ?
                      AND confirm_expires_at > ?
                      AND confirm_tries < ?
                      AND (confirm_last_sent_at IS NULL OR confirm_last_sent_at <= ?)
                    ORDER BY eta_due_at ASC, id ASC
                    LIMIT 50
                """, (
                    today.isoformat(),
                    int(now_epoch),
                    int(now_epoch),
                    int(max_tries),
                    int(now_epoch - retry_sec),
                ))
                rows = await cur.fetchall()

            for bid, user_id, tries, last_sent in rows:
                uid = int(user_id or 0)
                if uid > 0:
                    await try_send(
                        bot,
                        uid,
                        "❓ <b>Вы точно приедете?</b>\n"
                        "Если не успеваете — перенесём, чтобы не держать очередь.",
                        reply_markup=called_confirm_kb(int(bid))
                    )

                async with aiosqlite.connect(DB_PATH) as conn:
                    await conn.execute("""
                        UPDATE bookings
                        SET confirm_tries=confirm_tries+1,
                            confirm_last_sent_at=?,
                            updated_at=?
                        WHERE id=? AND status='called'
                    """, (int(now_epoch), ts, int(bid)))
                    await conn.commit()

            # 2) истёк confirm -> no_show
            async with aiosqlite.connect(DB_PATH) as conn:
                cur = await conn.execute("""
                    SELECT id, user_id, day, seq, car_text, issue_text, phone
                    FROM bookings
                    WHERE day=?
                      AND status='called'
                      AND confirm_expires_at IS NOT NULL
                      AND confirm_expires_at <= ?
                    ORDER BY confirm_expires_at ASC, id ASC
                    LIMIT 50
                """, (today.isoformat(), int(now_epoch)))
                expired = await cur.fetchall()

            for bid, user_id, day_s, seq, car, issue, phone in expired:
                async with aiosqlite.connect(DB_PATH) as conn:
                    await conn.execute("""
                        UPDATE bookings
                        SET status='no_show',
                            eta_minutes=NULL,
                            manual_call_only=0,
                            needs_admin_ok=0,

                            called_at=NULL,
                            eta_due_at=NULL,
                            confirm_expires_at=NULL,
                            confirm_tries=0,
                            confirm_last_sent_at=NULL,

                            updated_at=?
                        WHERE id=? AND status='called'
                    """, (ts, int(bid)))
                    await conn.commit()

                await notify_admin(
                    bot, admin_id,
                    "🚫 <b>Неявка по таймеру</b>\n"
                    f"{day_s} №{seq}\n"
                    f"{short(car, 30)} — {short(issue, 40)}\n"
                    f"{phone}"
                )

                await auto_fill_to_max(bot, admin_id, reason="после авто-неявки")

            await asyncio.sleep(SETTINGS.CALL_CONFIRM_TICK_SECONDS)

        except Exception as e:
            await notify_admin(bot, admin_id, f"⛔ Ошибка called_confirm_loop: <code>{e}</code>")
            await asyncio.sleep(5)


async def auto_fill_loop(bot: Bot, admin_id: int):
    while True:
        try:
            now = now_dt()
            if is_working_day(now.date()):
                await auto_fill_to_max(bot, admin_id, reason="tick")
            await asyncio.sleep(SETTINGS.AUTO_TICK_SECONDS)
        except Exception as e:
            await notify_admin(bot, admin_id, f"⛔ Ошибка auto_fill_loop: <code>{e}</code>")
            await asyncio.sleep(5)


async def offer_loop(bot: Bot, admin_id: int):
    while True:
        try:
            now = now_dt()
            if is_working_day(now.date()):
                # 1) снимаем истёкшие предложения каждые OFFER_TICK_SECONDS (у вас 60)
                await _clear_expired_offers()

                # 2) если поток недозаполнен — и сегодня некого звать — пробуем отправить следующее предложение
                if is_work_time(now) and await db.get_shop_load(now.date()) < SETTINGS.MAX_AT_SHOP:
                    today_s = now.date().isoformat()
                    async with aiosqlite.connect(DB_PATH) as conn:
                        cur = await conn.execute("""
                            SELECT 1
                            FROM bookings
                            WHERE day=? AND status='waiting' AND manual_call_only=0 AND needs_admin_ok=0
                            LIMIT 1
                        """, (today_s,))
                        exists_today_waiting = await cur.fetchone() is not None

                    if not exists_today_waiting:
                        await offer_send_next_candidate(bot, admin_id)

            await asyncio.sleep(SETTINGS.OFFER_TICK_SECONDS)
        except Exception as e:
            await notify_admin(bot, admin_id, f"⛔ Ошибка offer_loop: <code>{e}</code>")
            await asyncio.sleep(5)


async def rollover_loop(bot: Bot, admin_id: int):
    """
    Перенос в конце дня + уведомление админу.
    """
    while True:
        try:
            now = now_dt()
            if is_working_day(now.date()) and now.time() >= SETTINGS.WORK_END:
                today_s = now.date().isoformat()
                last = await db.get_meta("last_rollover_day")
                if last != today_s:
                    await db.set_meta("last_rollover_day", today_s)
                    await notify_admin(bot, admin_id, "🔁 <b>Конец дня</b>: выполняю перенос активных записей на следующий рабочий день.")
                    await rollover_at_end_of_day(bot, admin_id)
            await asyncio.sleep(60)
        except Exception as e:
            await notify_admin(bot, admin_id, f"⛔ Ошибка rollover_loop: <code>{e}</code>")
            await asyncio.sleep(5)


async def rollover_at_end_of_day(bot: Bot, admin_id: int) -> None:
    """
    Переносим активные записи сегодняшнего дня на следующий рабочий:
    waiting/called/arrived/in_service/parts_wait -> следующий рабочий
    called -> waiting
    """
    today = now_dt().date()
    if not is_working_day(today):
        return

    next_day = next_working_day(today)
    ts = db.now_iso()

    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute("BEGIN IMMEDIATE")

        cur = await conn.execute("""
            SELECT id, user_id, status
            FROM bookings
            WHERE day=? AND status IN ('waiting','called','arrived','in_service','parts_wait')
            ORDER BY seq, id
        """, (today.isoformat(),))
        rows = await cur.fetchall()

        if not rows:
            await conn.commit()
            return

        for bid, uid, st in rows:
            new_status = "waiting" if st == "called" else st
            await conn.execute("""
                UPDATE bookings
                SET day=?, status=?, kind='static',
                    eta_minutes=NULL, manual_call_only=0, needs_admin_ok=0,
                    offer_day=NULL, offer_stage=NULL, offer_expires_at=NULL,

                    called_at=NULL,
                    eta_due_at=NULL,
                    confirm_expires_at=NULL,
                    confirm_tries=0,
                    confirm_last_sent_at=NULL,

                    updated_at=?
                WHERE id=?
            """, (next_day.isoformat(), new_status, ts, int(bid)))

        # пересчёт seq на next_day
        cur = await conn.execute("""
            SELECT id
            FROM bookings
            WHERE day=? AND status IN ('waiting','called','arrived','in_service')
            ORDER BY seq, id
        """, (next_day.isoformat(),))
        ids = [r[0] for r in await cur.fetchall()]
        for i, bid in enumerate(ids, start=1):
            await conn.execute("UPDATE bookings SET seq=? WHERE id=?", (int(i), int(bid)))

        await conn.commit()

    await notify_admin(
        bot, admin_id,
        f"✅ Перенос выполнен: {len(rows)} записей → {next_day.strftime('%d.%m.%Y')}."
    )


def start_background_tasks(bot: Bot, admin_id: int) -> None:
    asyncio.create_task(auto_fill_loop(bot, admin_id))
    asyncio.create_task(offer_loop(bot, admin_id))
    asyncio.create_task(rollover_loop(bot, admin_id))
    asyncio.create_task(called_confirm_loop(bot, admin_id))
