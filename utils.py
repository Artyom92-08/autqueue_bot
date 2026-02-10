# utils.py
from __future__ import annotations

import re
from datetime import datetime, date, timedelta
from typing import Optional, Tuple

from config import SETTINGS, TZ


# ---------- admin ----------
def is_admin(user_id: int, admin_id: int) -> bool:
    return int(admin_id or 0) != 0 and int(user_id) == int(admin_id)


# ---------- work time / days ----------
def is_working_day(d: date) -> bool:
    return d.weekday() != 6  # воскресенье выходной


def next_working_day(d: date) -> date:
    x = d + timedelta(days=1)
    while not is_working_day(x):
        x += timedelta(days=1)
    return x


def is_work_time(dt: datetime) -> bool:
    return is_working_day(dt.date()) and (SETTINGS.WORK_START <= dt.time() < SETTINGS.WORK_END)


def format_day_label(d: date) -> str:
    wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][d.weekday()]
    return f"{wd} {d.strftime('%d.%m')}"


def short(text: str, n: int = 28) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1] + "…"


# ---------- phone ----------
def normalize_phone(text: str) -> Optional[str]:
    digits = re.sub(r"\D+", "", text or "")
    if len(digits) < 10:
        return None
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if len(digits) == 10:
        digits = "7" + digits
    if digits.startswith("7") and len(digits) == 11:
        return "+" + digits
    return None


# ---------- car year ----------
def parse_year(text: str) -> Optional[int]:
    m = re.search(r"(19\d{2}|20\d{2})", text or "")
    if not m:
        return None
    y = int(m.group(1))
    if 1980 <= y <= 2035:
        return y
    return None


# ---------- now helpers ----------
def now_dt() -> datetime:
    return datetime.now(TZ)


# ---------- arrival time parsing ----------
def clamp_minutes(minutes: int) -> int:
    if minutes < SETTINGS.ETA_MIN:
        return SETTINGS.ETA_MIN
    if minutes > SETTINGS.ETA_MAX:
        return SETTINGS.ETA_MAX
    return minutes


def parse_arrival_minutes(text: str, now: datetime) -> tuple[Optional[int], Optional[str]]:
    """
    Парсит “через сколько минут приеду” ИЛИ “в 18:30”.

    Возвращает:
      (minutes, None)                     если успешно
      (None, "time_passed")               если указано время (18:30), но оно уже прошло сегодня
      (None, "invalid")                   если не распознали

    Поддержка:
      - 25 / 40 мин / через 60
      - 1ч / 1.5ч
      - 18:30 / 18.30
    """
    t = (text or "").strip().lower()
    if not t:
        return None, "invalid"

    # 1) точное время HH:MM или HH.MM
    #    Вариант "18:30" или "18.30"
    m = re.search(r"\b([01]?\d|2[0-3])[:.](\d{2})\b", t)
    if m:
        hh = int(m.group(1))
        mm = int(m.group(2))
        if mm > 59:
            return None, "invalid"

        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        delta = (target - now).total_seconds()
        if delta <= 0:
            return None, "time_passed"

        minutes = int((delta + 59) // 60)  # округляем вверх
        return clamp_minutes(minutes), None

    # 2) если есть часы "1ч", "1.5ч"
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*ч", t)
    if m:
        h = float(m.group(1).replace(",", "."))
        minutes = int(round(h * 60))
        return clamp_minutes(minutes), None

    # 3) просто число минут
    m = re.search(r"(\d{1,3})", t)
    if m:
        minutes = int(m.group(1))
        return clamp_minutes(minutes), None

    return None, "invalid"


# Совместимость со старым кодом (если где-то осталось)
def parse_eta_minutes(text: str) -> Optional[int]:
    minutes, err = parse_arrival_minutes(text, now_dt())
    return minutes
