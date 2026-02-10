from __future__ import annotations

import secrets
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

import aiosqlite

from config import SETTINGS, TZ

DB_PATH = "queue.sqlite3"

# -------------------- STATUSES --------------------
STATUS_WAITING = "waiting"
STATUS_CALLED = "called"
STATUS_ARRIVED = "arrived"
STATUS_IN_SERVICE = "in_service"
STATUS_PARTS_WAIT = "parts_wait"
STATUS_DONE = "done"
STATUS_CANCELED = "canceled"
STATUS_NO_SHOW = "no_show"

FLOW_STATUSES = (STATUS_WAITING, STATUS_CALLED, STATUS_ARRIVED, STATUS_IN_SERVICE)

CLIENT_ACTIVE_STATUSES = (
    STATUS_WAITING, STATUS_CALLED, STATUS_ARRIVED, STATUS_IN_SERVICE, STATUS_PARTS_WAIT
)

LIMIT_STATUSES = (STATUS_WAITING, STATUS_CALLED, STATUS_ARRIVED, STATUS_IN_SERVICE)

# -------------------- KINDS --------------------
KIND_STATIC = "static"
KIND_LIVE = "live"


def now_iso() -> str:
    return datetime.now(TZ).isoformat()


def now_ts() -> int:
    return int(datetime.now(TZ).timestamp())


@dataclass(frozen=True)
class BookingRow:
    id: int
    day: str
    seq: int
    user_id: int
    user_name: str | None
    phone: str
    car_text: str
    issue_text: str
    status: str

    kind: str
    eta_minutes: int | None
    manual_call_only: int
    needs_admin_ok: int

    offer_day: str | None
    offer_stage: str | None
    offer_expires_at: int | None
    offer_cooldown_until: int | None

    claim_token: str | None

    # --- timers for called confirm (new) ---
    called_at: int | None
    eta_due_at: int | None
    confirm_expires_at: int | None
    confirm_tries: int
    confirm_last_sent_at: int | None

    created_at: str
    updated_at: str


# -------------------- INIT / MIGRATIONS --------------------
async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        # 1) базовая таблица (для новых установок)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT NOT NULL,
            seq INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            user_name TEXT,
            phone TEXT NOT NULL,
            car_text TEXT NOT NULL,
            issue_text TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'waiting',

            kind TEXT NOT NULL DEFAULT 'static',
            eta_minutes INTEGER DEFAULT NULL,
            manual_call_only INTEGER NOT NULL DEFAULT 0,
            needs_admin_ok INTEGER NOT NULL DEFAULT 0,

            offer_day TEXT DEFAULT NULL,
            offer_stage TEXT DEFAULT NULL,
            offer_expires_at INTEGER DEFAULT NULL,
            offer_cooldown_until INTEGER DEFAULT NULL,

            claim_token TEXT DEFAULT NULL,

            -- new columns for "called" confirm logic
            called_at INTEGER DEFAULT NULL,
            eta_due_at INTEGER DEFAULT NULL,
            confirm_expires_at INTEGER DEFAULT NULL,
            confirm_tries INTEGER NOT NULL DEFAULT 0,
            confirm_last_sent_at INTEGER DEFAULT NULL,

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """)

        # 2) миграции (для старых баз): сначала добавляем колонки
        cur = await db.execute("PRAGMA table_info(bookings)")
        cols = {r[1] for r in await cur.fetchall()}

        async def add_col(name: str, ddl: str):
            if name not in cols:
                await db.execute(ddl)

        await add_col("kind", "ALTER TABLE bookings ADD COLUMN kind TEXT NOT NULL DEFAULT 'static'")
        await add_col("eta_minutes", "ALTER TABLE bookings ADD COLUMN eta_minutes INTEGER DEFAULT NULL")
        await add_col("manual_call_only", "ALTER TABLE bookings ADD COLUMN manual_call_only INTEGER NOT NULL DEFAULT 0")
        await add_col("needs_admin_ok", "ALTER TABLE bookings ADD COLUMN needs_admin_ok INTEGER NOT NULL DEFAULT 0")

        await add_col("offer_day", "ALTER TABLE bookings ADD COLUMN offer_day TEXT DEFAULT NULL")
        await add_col("offer_stage", "ALTER TABLE bookings ADD COLUMN offer_stage TEXT DEFAULT NULL")
        await add_col("offer_expires_at", "ALTER TABLE bookings ADD COLUMN offer_expires_at INTEGER DEFAULT NULL")
        await add_col("offer_cooldown_until", "ALTER TABLE bookings ADD COLUMN offer_cooldown_until INTEGER DEFAULT NULL")

        await add_col("claim_token", "ALTER TABLE bookings ADD COLUMN claim_token TEXT DEFAULT NULL")

        # --- new columns for "called" confirm logic ---
        await add_col("called_at", "ALTER TABLE bookings ADD COLUMN called_at INTEGER DEFAULT NULL")
        await add_col("eta_due_at", "ALTER TABLE bookings ADD COLUMN eta_due_at INTEGER DEFAULT NULL")
        await add_col("confirm_expires_at", "ALTER TABLE bookings ADD COLUMN confirm_expires_at INTEGER DEFAULT NULL")
        await add_col("confirm_tries", "ALTER TABLE bookings ADD COLUMN confirm_tries INTEGER NOT NULL DEFAULT 0")
        await add_col("confirm_last_sent_at", "ALTER TABLE bookings ADD COLUMN confirm_last_sent_at INTEGER DEFAULT NULL")

        await db.commit()

        # 3) индексы — только после миграций
        async def safe_index(sql: str):
            try:
                await db.execute(sql)
            except Exception:
                # если вдруг индекс на старой схеме не создаётся — не валим весь старт
                pass

        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_day_seq ON bookings(day, seq)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(status)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_kind ON bookings(kind)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_user_id ON bookings(user_id)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_phone ON bookings(phone)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_offer_stage ON bookings(offer_stage)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_offer_expires ON bookings(offer_expires_at)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_claim_token ON bookings(claim_token)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_needs_admin_ok ON bookings(needs_admin_ok)")

        # new indexes
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_called_at ON bookings(called_at)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_eta_due_at ON bookings(eta_due_at)")
        await safe_index("CREATE INDEX IF NOT EXISTS idx_bookings_confirm_expires_at ON bookings(confirm_expires_at)")

        await db.commit()


# -------------------- META --------------------
async def get_meta(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM meta WHERE key=?", (key,))
        row = await cur.fetchone()
    return row[0] if row else None


async def set_meta(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO meta(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))
        await db.commit()


# -------------------- COUNTS / LOAD --------------------
async def get_in_service_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM bookings WHERE status=?", (STATUS_IN_SERVICE,))
        (cnt,) = await cur.fetchone()
    return int(cnt)


async def get_arrived_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM bookings WHERE status=?", (STATUS_ARRIVED,))
        (cnt,) = await cur.fetchone()
    return int(cnt)


async def get_called_today_count(today: date) -> int:
    today_s = today.isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM bookings WHERE day=? AND status=?", (today_s, STATUS_CALLED))
        (cnt,) = await cur.fetchone()
    return int(cnt)


async def get_shop_load(today: date) -> int:
    return (await get_in_service_count()) + (await get_arrived_count()) + (await get_called_today_count(today))


# -------------------- DUPLICATES --------------------
async def get_active_booking_brief_by_user_or_phone(user_id: int, phone: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"""
            SELECT id, day, seq, status
            FROM bookings
            WHERE status IN ({",".join(["?"] * len(CLIENT_ACTIVE_STATUSES))})
              AND (user_id=? OR phone=?)
            ORDER BY day, seq, id
            LIMIT 1
        """, (*CLIENT_ACTIVE_STATUSES, int(user_id), phone))
        return await cur.fetchone()


# -------------------- DAY LIMIT --------------------
async def get_day_count(day: date) -> int:
    day_s = day.isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"""
            SELECT COUNT(*)
            FROM bookings
            WHERE day=?
              AND status IN ({",".join(["?"] * len(LIMIT_STATUSES))})
        """, (day_s, *LIMIT_STATUSES))
        (cnt,) = await cur.fetchone()
    return int(cnt)


async def is_day_available(day: date) -> tuple[bool, str]:
    cnt = await get_day_count(day)
    if cnt >= SETTINGS.MAX_CARS_PER_DAY:
        return False, f"На этот день уже {SETTINGS.MAX_CARS_PER_DAY} записи"
    return True, "OK"


async def is_static_day_available(day: date) -> tuple[bool, str]:
    return await is_day_available(day)


async def next_seq_for_day(day: date) -> int:
    day_s = day.isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (day_s,))
        (seq,) = await cur.fetchone()
    return int(seq)


# -------------------- CRUD create/read/update --------------------
async def add_static_booking(day: date, user_id: int, user_name: str, phone: str, car_text: str, issue_text: str) -> tuple[int, int]:
    day_s = day.isoformat()
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute(f"""
            SELECT 1
            FROM bookings
            WHERE status IN ({",".join(["?"] * len(CLIENT_ACTIVE_STATUSES))})
              AND (user_id=? OR phone=?)
            LIMIT 1
        """, (*CLIENT_ACTIVE_STATUSES, int(user_id), phone))
        if await cur.fetchone():
            await db.commit()
            raise RuntimeError("duplicate_active")

        cur = await db.execute(f"""
            SELECT COUNT(*)
            FROM bookings
            WHERE day=? AND status IN ({",".join(["?"] * len(LIMIT_STATUSES))})
        """, (day_s, *LIMIT_STATUSES))
        (cnt,) = await cur.fetchone()
        if int(cnt) >= SETTINGS.MAX_CARS_PER_DAY:
            await db.commit()
            raise RuntimeError("day_full")

        cur = await db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (day_s,))
        (seq,) = await cur.fetchone()

        cur = await db.execute("""
            INSERT INTO bookings(day, seq, user_id, user_name, phone, car_text, issue_text,
                                 status, kind, eta_minutes, manual_call_only, needs_admin_ok,
                                 offer_day, offer_stage, offer_expires_at, offer_cooldown_until,
                                 claim_token, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'waiting', 'static', NULL, 0, 0,
                    NULL, NULL, NULL, NULL,
                    NULL, ?, ?)
        """, (day_s, int(seq), int(user_id), user_name, phone, car_text, issue_text, ts, ts))

        bid = int(cur.lastrowid)
        await db.commit()
    return bid, int(seq)


async def add_live_booking_today(today: date, user_id: int, user_name: str, phone: str, car_text: str, issue_text: str) -> tuple[int, int]:
    day_s = today.isoformat()
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute(f"""
            SELECT 1
            FROM bookings
            WHERE status IN ({",".join(["?"] * len(CLIENT_ACTIVE_STATUSES))})
              AND (user_id=? OR phone=?)
            LIMIT 1
        """, (*CLIENT_ACTIVE_STATUSES, int(user_id), phone))
        if await cur.fetchone():
            await db.commit()
            raise RuntimeError("duplicate_active")

        cur = await db.execute(f"""
            SELECT COUNT(*)
            FROM bookings
            WHERE day=? AND status IN ({",".join(["?"] * len(LIMIT_STATUSES))})
        """, (day_s, *LIMIT_STATUSES))
        (cnt,) = await cur.fetchone()
        if int(cnt) >= SETTINGS.MAX_CARS_PER_DAY:
            await db.commit()
            raise RuntimeError("day_full")

        cur = await db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (day_s,))
        (seq,) = await cur.fetchone()

        cur = await db.execute("""
            INSERT INTO bookings(day, seq, user_id, user_name, phone, car_text, issue_text,
                                 status, kind, eta_minutes, manual_call_only, needs_admin_ok,
                                 offer_day, offer_stage, offer_expires_at, offer_cooldown_until,
                                 claim_token, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'waiting', 'live', NULL, 1, 0,
                    NULL, NULL, NULL, NULL,
                    NULL, ?, ?)
        """, (day_s, int(seq), int(user_id), user_name, phone, car_text, issue_text, ts, ts))

        bid = int(cur.lastrowid)
        await db.commit()
    return bid, int(seq)


async def get_booking(bid: int) -> Optional[BookingRow]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, day, seq, user_id, user_name, phone, car_text, issue_text,
                   status,
                   kind, eta_minutes, manual_call_only, needs_admin_ok,
                   offer_day, offer_stage, offer_expires_at, offer_cooldown_until,
                   claim_token,
                   called_at, eta_due_at, confirm_expires_at, confirm_tries, confirm_last_sent_at,
                   created_at, updated_at
            FROM bookings
            WHERE id=?
        """, (int(bid),))
        row = await cur.fetchone()
    return BookingRow(*row) if row else None


async def get_my_active_bookings(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"""
            SELECT id, day, seq, car_text, issue_text, status, kind, eta_minutes
            FROM bookings
            WHERE user_id=? AND status IN ({",".join(["?"] * len(CLIENT_ACTIVE_STATUSES))})
            ORDER BY day, seq, id
        """, (int(user_id), *CLIENT_ACTIVE_STATUSES))
        return await cur.fetchall()


async def cancel_booking(bid: int, user_id: int) -> bool:
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"""
            UPDATE bookings
            SET status='canceled',
                offer_day=NULL, offer_stage=NULL, offer_expires_at=NULL,
                needs_admin_ok=0,
                eta_minutes=NULL,
                called_at=NULL, eta_due_at=NULL,
                confirm_expires_at=NULL, confirm_tries=0, confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND user_id=? AND status IN ({",".join(["?"] * len(CLIENT_ACTIVE_STATUSES))})
        """, (ts, int(bid), int(user_id), *CLIENT_ACTIVE_STATUSES))
        await db.commit()
        return cur.rowcount > 0


# -------------------- QUEUE LISTS --------------------
async def get_queue_for_day(day: date):
    day_s = day.isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(f"""
            SELECT id, seq, car_text, issue_text, phone, status, user_id,
                   kind, eta_minutes, manual_call_only, needs_admin_ok
            FROM bookings
            WHERE day=? AND status IN ({",".join(["?"] * len(FLOW_STATUSES))})
            ORDER BY seq, id
        """, (day_s, *FLOW_STATUSES))
        return await cur.fetchall()


async def get_in_service_all():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, day, seq, car_text, issue_text, phone, user_id, kind, eta_minutes
            FROM bookings
            WHERE status='in_service'
            ORDER BY day, seq, id
        """)
        return await cur.fetchall()


async def get_parts_wait_all():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, day, seq, car_text, issue_text, phone, user_id, kind, eta_minutes, updated_at
            FROM bookings
            WHERE status='parts_wait'
            ORDER BY updated_at, id
        """)
        return await cur.fetchall()


# -------------------- ADMIN MANUAL ADD + CLAIM --------------------
async def add_booking_admin_manual(day: date, client_name: str, phone: str, car_text: str, issue_text: str) -> tuple[int, int, str]:
    day_s = day.isoformat()
    ts = now_iso()
    token = secrets.token_urlsafe(10)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute(f"""
            SELECT COUNT(*)
            FROM bookings
            WHERE day=? AND status IN ({",".join(["?"] * len(LIMIT_STATUSES))})
        """, (day_s, *LIMIT_STATUSES))
        (cnt,) = await cur.fetchone()
        if int(cnt) >= SETTINGS.MAX_CARS_PER_DAY:
            await db.commit()
            raise RuntimeError("day_full")

        cur = await db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (day_s,))
        (seq,) = await cur.fetchone()

        cur = await db.execute("""
            INSERT INTO bookings(day, seq, user_id, user_name, phone, car_text, issue_text,
                                 status, kind, eta_minutes, manual_call_only, needs_admin_ok,
                                 offer_day, offer_stage, offer_expires_at, offer_cooldown_until,
                                 claim_token, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?, ?, ?, 'waiting', 'static', NULL, 0, 0,
                    NULL, NULL, NULL, NULL,
                    ?, ?, ?)
        """, (day_s, int(seq), client_name, phone, car_text, issue_text, token, ts, ts))

        bid = int(cur.lastrowid)
        await db.commit()
    return bid, int(seq), token


async def claim_booking(token: str, user_id: int, user_name: str) -> tuple[bool, str, int | None]:
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute("""
            SELECT id, day, seq, car_text, issue_text, status
            FROM bookings
            WHERE claim_token=? AND user_id=0
        """, (token,))
        row = await cur.fetchone()
        if not row:
            await db.commit()
            return False, "⛔ Ссылка неактуальна или запись уже привязана.", None

        bid, day_s, seq, car, issue, status = row

        await db.execute("""
            UPDATE bookings
            SET user_id=?, user_name=?, claim_token=NULL, updated_at=?
            WHERE id=? AND user_id=0
        """, (int(user_id), user_name, ts, int(bid)))

        await db.commit()

    msg = (
        "✅ <b>Запись привязана</b> к вашему Telegram.\n\n"
        f"📅 Дата: <b>{date.fromisoformat(day_s).strftime('%d.%m.%Y')}</b>\n"
        f"🔢 Номер: <b>№{int(seq)}</b>\n"
        f"🚗 Авто: {car}\n"
        f"🛠 Задача: {issue}\n"
        f"📌 Статус: <b>{status}</b>\n\n"
        "Дальше статусы будут приходить сюда."
    )
    return True, msg, int(bid)


# -------------------- OFFER TODAY HELPERS --------------------
async def get_active_offer_row(now_epoch: int) -> Optional[tuple]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, user_id, day, seq, car_text, issue_text, phone, offer_stage, offer_day, offer_expires_at
            FROM bookings
            WHERE offer_stage IN ('pending','awaiting_eta')
              AND offer_expires_at IS NOT NULL
              AND offer_expires_at > ?
            ORDER BY offer_expires_at ASC
            LIMIT 1
        """, (int(now_epoch),))
        return await cur.fetchone()


async def pick_future_candidate_for_offer(today: date, now_epoch: int) -> Optional[tuple]:
    today_s = today.isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id, user_id, day, seq, car_text, issue_text, phone
            FROM bookings
            WHERE day > ?
              AND kind = 'static'
              AND status = 'waiting'
              AND (offer_stage IS NULL)
              AND (offer_cooldown_until IS NULL OR offer_cooldown_until <= ?)
            ORDER BY day ASC,
                     CASE WHEN user_id=0 THEN 1 ELSE 0 END,
                     seq ASC,
                     id ASC
            LIMIT 1
        """, (today_s, int(now_epoch)))
        return await cur.fetchone()


async def set_offer_pending(bid: int, offer_day: date, expires_at: int) -> None:
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE bookings
            SET offer_day=?, offer_stage='pending', offer_expires_at=?, updated_at=?
            WHERE id=? AND status='waiting'
        """, (offer_day.isoformat(), int(expires_at), ts, int(bid)))
        await db.commit()


async def set_offer_awaiting_eta(bid: int, offer_day: date, expires_at: int) -> bool:
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            UPDATE bookings
            SET offer_day=?, offer_stage='awaiting_eta', offer_expires_at=?, updated_at=?
            WHERE id=? AND status='waiting' AND offer_stage='pending'
        """, (offer_day.isoformat(), int(expires_at), ts, int(bid)))
        await db.commit()
        return cur.rowcount > 0


async def clear_offer(bid: int, cooldown_until: Optional[int] = None) -> None:
    ts = now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE bookings
            SET offer_day=NULL,
                offer_stage=NULL,
                offer_expires_at=NULL,
                offer_cooldown_until=COALESCE(?, offer_cooldown_until),
                updated_at=?
            WHERE id=?
        """, (cooldown_until, ts, int(bid)))
        await db.commit()


async def move_booking_to_day_append_seq(bid: int, new_day: date, new_kind: str = KIND_STATIC) -> Optional[int]:
    """
    Перенос записи на другую дату и постановка в конец очереди этой даты.
    Сбрасывает ETA и ожидание решения мастера, а также таймеры "called".
    """
    ts = now_iso()
    new_day_s = new_day.isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE")

        cur = await db.execute("SELECT COALESCE(MAX(seq), 0) + 1 FROM bookings WHERE day=?", (new_day_s,))
        (new_seq,) = await cur.fetchone()

        cur = await db.execute("""
            UPDATE bookings
            SET day=?, seq=?, kind=?,
                status='waiting',
                eta_minutes=NULL,
                manual_call_only=0,
                needs_admin_ok=0,
                offer_day=NULL,
                offer_stage=NULL,
                offer_expires_at=NULL,
                called_at=NULL,
                eta_due_at=NULL,
                confirm_expires_at=NULL,
                confirm_tries=0,
                confirm_last_sent_at=NULL,
                updated_at=?
            WHERE id=? AND status IN ('waiting','called')
        """, (new_day_s, int(new_seq), new_kind, ts, int(bid)))

        await db.commit()

    return int(new_seq) if cur.rowcount > 0 else None
