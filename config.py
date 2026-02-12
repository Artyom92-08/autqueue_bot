# config.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


@dataclass(frozen=True)
class Settings:
    # ====== ВРЕМЯ / ЧАСОВОЙ ПОЯС ======
    # Важно: указывайте IANA timezone (как в Linux): Asia/Krasnoyarsk, Asia/Novosibirsk, Europe/Moscow ...
    TZ_NAME: str = "Asia/Krasnoyarsk"

    # Рабочее время (локальное время TZ_NAME)
    WORK_START: time = time(0, 0)
    WORK_END: time = time(23, 59 , 59)
    WORK_INFO: str = "Пн–Сб 13:00–21:00 (Красноярск)"

    # ====== ОТОБРАЖЕНИЕ ДАТ ДЛЯ ЗАПИСИ ======
    DAYS_AHEAD: int = 30  # сколько рабочих дней показывать в кнопках
    
    # ====== ЛИМИТ "В ДЕНЬ" ======
    # ВАЖНО: это общий лимит активных записей на дату (и для обычной записи, и для “ближайшего времени”)
    MAX_CARS_PER_DAY: int = 3

    # ====== ЛИМИТЫ ЗАПИСЕЙ ======
    # Лимит обычных записей на день (статических): 3
    MAX_STATIC_PER_DAY: int = 3

    # Поток “в сервисе сейчас” (в мастерской):
    MAX_AT_SHOP: int = 3       # всего в потоке: called + arrived + in_service
    MAX_IN_SERVICE: int = 1    # одновременно в работе

    # ====== АВТО-ЛОГИКА ======
    AUTO_TICK_SECONDS: int = 300        # автодобор раз в 5 минут
    OFFER_TICK_SECONDS: int = 60        # офферы проверяем раз в 1 минуту 
    OFFER_EXPIRE_MINUTES: int = 10      # ждём ответа 10 минут

    OFFER_EXPIRE_MINUTES: int = 30     # предложение действует 30 минут
    ETA_MIN: int = 5                  # минимальный ETA, который принимаем
    ETA_MAX: int = 180                # максимальный ETA (3 часа)

    # ====== КНОПКИ (текст) ======
    BTN_BACK: str = "⬅️ Назад"
    BTN_HOME: str = "🏠 Главное меню"

    # ====== КОНТАКТЫ ======
    ADDRESS_TEXT: str = "Красноярск, (Метолургов 2в/13 /Автосервис Чемпион)"
    PHONE_TEXT: str = "+79333346444"
    MAP_LAT: float = 56.064253
    MAP_LON: float = 92.974059


def get_tz(settings: Settings):
    """
    Возвращает ZoneInfo если доступно, иначе фиксированный UTC+offset.
    (на некоторых системах zoneinfo базы может не быть)
    """
    try:
        return ZoneInfo(settings.TZ_NAME)
    except ZoneInfoNotFoundError:
        # fallback для Красноярска (UTC+7) — поменяйте если у вас другой регион
        return timezone(timedelta(hours=7))


SETTINGS = Settings()
TZ = get_tz(SETTINGS)

YANDEX_MAP_URL = f"https://yandex.ru/maps/?pt={SETTINGS.MAP_LON},{SETTINGS.MAP_LAT}&z=17&l=map"
GOOGLE_MAP_URL = f"https://maps.google.com/?q={SETTINGS.MAP_LAT},{SETTINGS.MAP_LON}"
TWO_GIS_URL = f"https://2gis.ru/?m={SETTINGS.MAP_LON},{SETTINGS.MAP_LAT}/17"

CONTACTS_TEXT = (
    "📍 <b>Контакты</b>\n\n"
    f"Адрес: {SETTINGS.ADDRESS_TEXT}\n"
    f"Телефон: {SETTINGS.PHONE_TEXT}\n\n"
    f"🕒 График: {SETTINGS.WORK_INFO}\n"
)
