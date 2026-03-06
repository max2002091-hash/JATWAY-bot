import os
import re
import json
import math
import csv
import sqlite3
import asyncio
import time
import aiohttp
from io import StringIO
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Dict, List

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.constants import ChatType
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    CallbackQueryHandler,
    ChatMemberHandler,
    filters,
)

# =========================
# ===== ENV =====
# =========================
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

COURIER_GROUP_ID = int(os.getenv("COURIER_GROUP_ID", "0"))   # диспетчерська група
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0"))         # овнер група/чат

# (залишаємо для сумісності; тепер не використовуємо як основний)
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# ✅ Фіксовані номери підтримки (клієнтська)
SUPPORT_PHONE_1 = "+380968130807"

# Робочі години
WORK_HOURS_START = os.getenv("WORK_HOURS_START", "10:00").strip()
WORK_HOURS_END = os.getenv("WORK_HOURS_END", "22:00").strip()

# Оплата / гарантія
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "4441111026102602").strip()
PAYMENT_RECEIVER_NAME = os.getenv("PAYMENT_RECEIVER_NAME", "Jetway Delivery").strip()

REMINDER_BEFORE_CANCEL_MIN = int(os.getenv("REMINDER_BEFORE_CANCEL_MIN", "5"))

WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip().rstrip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()  # опціонально
PORT = int(os.getenv("PORT", "8000"))

# =========================
# ===== Tariffs =====
# =========================
PRICE_SCHEDULED_BASE = 120     # ✅ було 110
PRICE_URGENT_BASE = 170
BASE_KM = 2
EXTRA_KM_PRICE = 20            # ✅ було 23
EXTRA_OUTSIDE_OBUKHIV_PER_KM = 8

JETWAY_FEE_RATE_DEFAULT = 0.25  # базово 25%
DIST_SAFETY_MULT = 1.0   # ✅ без +10%

# Баланс: прогноз після fee не може бути нижче
MIN_NEGATIVE_BALANCE = -50

# =========================
# ===== Timezone =====
# =========================
try:
    TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    TZ = timezone(timedelta(hours=2))

# =========================
# ===== Data dir (Railway Volume friendly) =====
# =========================
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "bot.sqlite3")

# =========================
# ===== In-memory state =====
# =========================
COURIERS: set[int] = set()
BLACKLISTED_COURIERS: set[int] = set()
COURIER_BALANCES: Dict[int, int] = {}
ACTIVE_ORDERS_COUNT: Dict[int, int] = {}       # courier_id -> count
ORDERS_DB: Dict[str, dict] = {}                # order_id -> data (active/in-flight)

OWNER_PENDING: Dict[int, str] = {}             # owner_id -> "add"/"del"/"topup"/"bal"/"setbal"
SUPPORT_CONTACT_PENDING: Dict[int, str] = {}   # courier_id -> order_id
FINAL_KM_PENDING: Dict[int, str] = {}          # courier_id -> order_id

# Purchase flow pending stages after owner confirms guarantee
PENDING_CUSTOMER_STAGE: Dict[int, dict] = {}   # customer_id -> {"stage": "...", "order_id": "...."}

# =========================
# ===== States =====
# =========================
(
    ROLE_CHOICE,
    CHOICE,

    # after "🚚 Доставка"
    DELIVERY_MODE,          # 🛒 / 📦
    DELIV_TYPE,             # urgent/scheduled
    WHEN_INPUT,
    WHEN_CONFIRM,

    FROM_ADDR,
    CONFIRM_FROM,
    TO_ADDR,
    CONFIRM_TO,

    # pickup basic
    ITEM,
    PHONE,
    COMMENT,
    CONFIRM_ORDER,

    # support call-me
    CALLME_PHONE,
) = range(15)

# =========================
# ===== Purchase (buy) inline steps inside conv =====
# =========================
(
    BUY_LIST,
    BUY_APPROX_SUM,
    BUY_WAIT_PAID_CLICK,   # customer clicks "I paid" (inline)
) = range(15, 18)

# =========================
# ===== SQL helpers (SQLite) =====
# =========================
def db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def db_init():
    con = db_connect()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS couriers (
        courier_id INTEGER PRIMARY KEY,
        created_at TEXT NOT NULL,
        username TEXT,
        full_name TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS courier_balances (
        courier_id INTEGER PRIMARY KEY,
        balance INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders_closed (
        order_id TEXT PRIMARY KEY,
        closed_at TEXT NOT NULL,
        delivery_mode TEXT NOT NULL,   -- pickup / buy
        delivery_key TEXT NOT NULL,    -- urgent / scheduled
        total INTEGER NOT NULL,
        fee INTEGER NOT NULL,
        courier_id INTEGER,
        courier_name TEXT,
        customer_id INTEGER,
        customer_name TEXT,
        customer_username TEXT,
        from_addr TEXT,
        to_addr TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ratings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE,
        courier_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        rating INTEGER NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS courier_blacklist (
        courier_id INTEGER PRIMARY KEY,
        reason TEXT,
        added_at TEXT NOT NULL
    )
    """)


    cur.execute("""
    CREATE TABLE IF NOT EXISTS purchase_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT NOT NULL,
        file_id TEXT NOT NULL,
        media_type TEXT NOT NULL DEFAULT 'photo',
        file_name TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL
    )
    """)

    # М'яка міграція старої таблиці без зламу існуючої БД
    try:
        cur.execute("ALTER TABLE purchase_checks ADD COLUMN media_type TEXT NOT NULL DEFAULT 'photo'")
    except Exception:
        pass
    try:
        cur.execute("ALTER TABLE purchase_checks ADD COLUMN file_name TEXT NOT NULL DEFAULT ''")
    except Exception:
        pass

    con.commit()
    con.close()


def db_load_couriers_and_balances():
    global COURIERS, COURIER_BALANCES
    con = db_connect()
    cur = con.cursor()

    COURIERS = set()
    for r in cur.execute("SELECT courier_id FROM couriers"):
        COURIERS.add(int(r["courier_id"]))

    COURIER_BALANCES = {}
    for r in cur.execute("SELECT courier_id, balance FROM courier_balances"):
        COURIER_BALANCES[int(r["courier_id"])] = int(r["balance"])

    con.close()




def db_get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else default


def db_set_setting(key: str, value: str):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO settings(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    con.commit()
    con.close()


def db_load_settings_into_globals():
    global WORK_HOURS_START, WORK_HOURS_END
    ws = db_get_setting("work_hours_start")
    we = db_get_setting("work_hours_end")
    if ws:
        WORK_HOURS_START = ws.strip()
    if we:
        WORK_HOURS_END = we.strip()


def db_blacklist_load_cache():
    BLACKLISTED_COURIERS.clear()
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT courier_id FROM courier_blacklist")
    for (cid,) in cur.fetchall():
        try:
            BLACKLISTED_COURIERS.add(int(cid))
        except Exception:
            pass
    con.close()


def db_blacklist_add(courier_id: int, reason: str = ""):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO courier_blacklist(courier_id, reason, added_at) VALUES(?,?,?) "
        "ON CONFLICT(courier_id) DO UPDATE SET reason=excluded.reason, added_at=excluded.added_at",
        (int(courier_id), (reason or "").strip(), datetime.now(TZ).isoformat()),
    )
    con.commit()
    con.close()
    BLACKLISTED_COURIERS.add(int(courier_id))


def db_blacklist_remove(courier_id: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("DELETE FROM courier_blacklist WHERE courier_id=?", (int(courier_id),))
    con.commit()
    con.close()
    BLACKLISTED_COURIERS.discard(int(courier_id))


def db_blacklist_list() -> List[Tuple[int, str, str]]:
    con = db_connect()
    cur = con.cursor()
    cur.execute("SELECT courier_id, reason, added_at FROM courier_blacklist ORDER BY added_at DESC")
    rows = cur.fetchall()
    con.close()
    out: List[Tuple[int, str, str]] = []
    for cid, reason, added_at in rows:
        try:
            cid = int(cid)
        except Exception:
            continue
        out.append((cid, reason or "", added_at or ""))
    return out


def is_blacklisted_courier(courier_id: int) -> bool:
    return int(courier_id) in BLACKLISTED_COURIERS
def db_add_courier(courier_id: int, username: str = "", full_name: str = ""):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO couriers(courier_id, created_at, username, full_name) VALUES (?, ?, ?, ?)",
        (int(courier_id), datetime.now(TZ).isoformat(), username or "", full_name or ""),
    )
    cur.execute(
        "INSERT OR IGNORE INTO courier_balances(courier_id, balance) VALUES (?, 0)",
        (int(courier_id),),
    )
    con.commit()
    con.close()
    COURIERS.add(int(courier_id))
    COURIER_BALANCES.setdefault(int(courier_id), 0)


def db_del_courier(courier_id: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute("DELETE FROM couriers WHERE courier_id=?", (int(courier_id),))
    con.commit()
    con.close()
    COURIERS.discard(int(courier_id))


def db_get_balance(courier_id: int) -> int:
    return int(COURIER_BALANCES.get(int(courier_id), 0))


def db_add_balance(courier_id: int, delta: int):
    cid = int(courier_id)
    new_bal = int(COURIER_BALANCES.get(cid, 0)) + int(delta)
    COURIER_BALANCES[cid] = new_bal

    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO courier_balances(courier_id, balance) VALUES (?, ?) "
        "ON CONFLICT(courier_id) DO UPDATE SET balance=excluded.balance",
        (cid, int(new_bal)),
    )
    con.commit()
    con.close()


def db_set_balance(courier_id: int, new_balance: int):
    cid = int(courier_id)
    COURIER_BALANCES[cid] = int(new_balance)
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO courier_balances(courier_id, balance) VALUES (?, ?) "
        "ON CONFLICT(courier_id) DO UPDATE SET balance=excluded.balance",
        (cid, int(new_balance)),
    )
    con.commit()
    con.close()


def db_insert_closed_order(order: dict, final_total: int, final_fee: int):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO orders_closed(
            order_id, closed_at, delivery_mode, delivery_key,
            total, fee, courier_id, courier_name,
            customer_id, customer_name, customer_username,
            from_addr, to_addr
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            order.get("order_id"),
            datetime.now(TZ).isoformat(),
            order.get("delivery_mode", "pickup"),
            order.get("delivery_key", "-"),
            int(final_total),
            int(final_fee),
            order.get("courier_id"),
            order.get("courier_name"),
            order.get("customer_id"),
            order.get("customer_name"),
            order.get("customer_username"),
            order.get("from_addr"),
            order.get("to_addr"),
        ),
    )
    con.commit()
    con.close()


def db_try_add_rating(order_id: str, courier_id: int, customer_id: int, rating: int) -> bool:
    con = db_connect()
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO ratings(order_id, courier_id, customer_id, rating, created_at) VALUES(?,?,?,?,?)",
            (order_id, int(courier_id), int(customer_id), int(rating), datetime.now(TZ).isoformat()),
        )
        con.commit()
        return True
    except Exception:
        return False
    finally:
        con.close()


def db_get_courier_rating(courier_id: int, year_month: Optional[str] = None) -> Tuple[float, int]:
    """
    If year_month provided like '2026-03' -> stats for that month, else all-time.
    """
    con = db_connect()
    cur = con.cursor()

    if year_month:
        start = f"{year_month}-01"
        # next month
        y, m = map(int, year_month.split("-"))
        if m == 12:
            end = f"{y+1}-01-01"
        else:
            end = f"{y}-{m+1:02d}-01"
        row = cur.execute(
            "SELECT AVG(rating) AS a, COUNT(*) AS c FROM ratings WHERE courier_id=? AND created_at>=? AND created_at<?",
            (int(courier_id), start, end),
        ).fetchone()
    else:
        row = cur.execute(
            "SELECT AVG(rating) AS a, COUNT(*) AS c FROM ratings WHERE courier_id=?",
            (int(courier_id),),
        ).fetchone()

    con.close()
    avg = float(row["a"]) if row and row["a"] is not None else 0.0
    cnt = int(row["c"]) if row and row["c"] is not None else 0
    return avg, cnt


def db_calc_next_month_commission(courier_id: int) -> float:
    """
    Rule:
    - Current month is 25% always.
    - Next month = 20% if current month avg>=4.8 AND count>=5, else 25%.
    """
    now = datetime.now(TZ)
    ym = f"{now.year}-{now.month:02d}"
    avg, cnt = db_get_courier_rating(int(courier_id), ym)
    if cnt >= 5 and avg >= 4.8:
        return 0.20
    return 0.25



async def show_my_rating(update: Update, context: ContextTypes.DEFAULT_TYPE, courier_id: int):
    """Show courier rating + bonus rule info."""
    avg_all, cnt_all = db_get_courier_rating(int(courier_id), None)
    ym = _month_key()
    avg_m, cnt_m = db_get_courier_rating(int(courier_id), ym)
    next_fee = db_calc_next_month_commission(int(courier_id))
    next_fee_pct = int(round(next_fee * 100))

    bonus_text = (
        "Бонус комісії:\n"
        "Якщо за місяць рейтинг не нижче 4.80 і є мінімум 5 оцінок, то комісія наступного місяця буде 20%.\n"
        "Якщо менше 5 оцінок — комісія залишається 25%."
    )

    msg = (
        f"⭐ **Ваш рейтинг**\n\n"
        f"За весь час: **{avg_all:.2f}** (оцінок: {cnt_all})\n"
        f"Цей місяць ({ym}): **{avg_m:.2f}** (оцінок: {cnt_m})\n\n"
        f"📉 Поточна комісія: **25%**\n"
        f"📌 Комісія наступного місяця (за правилами): **{next_fee_pct}%**\n\n"
        f"{bonus_text}"
    )

    if update.message:
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=courier_menu())
    elif update.callback_query:
        await update.callback_query.message.reply_text(msg, parse_mode="Markdown", reply_markup=courier_menu())


def db_insert_purchase_check(order_id: str, file_id: str, media_type: str = "photo", file_name: str = ""):
    con = db_connect()
    cur = con.cursor()
    cur.execute(
        "INSERT INTO purchase_checks(order_id, file_id, media_type, file_name, created_at) VALUES(?,?,?,?,?)",
        (order_id, file_id, (media_type or "photo"), (file_name or ""), datetime.now(TZ).isoformat()),
    )
    con.commit()
    con.close()


def db_get_checks(limit: int = 20) -> List[sqlite3.Row]:
    con = db_connect()
    cur = con.cursor()
    rows = cur.execute(
        "SELECT order_id, file_id, media_type, file_name, created_at FROM purchase_checks ORDER BY id DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    con.close()
    return rows


def db_get_closed_orders(limit: int = 30) -> List[sqlite3.Row]:
    con = db_connect()
    cur = con.cursor()
    rows = cur.execute(
        "SELECT order_id, closed_at, delivery_mode, delivery_key, total, fee, "
        "courier_id, courier_name, customer_id, customer_name, customer_username, "
        "from_addr, to_addr "
        "FROM orders_closed ORDER BY closed_at DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    con.close()
    return rows



def _parse_hhmm(s: str) -> Tuple[int, int]:
    m = re.match(r"^\s*(\d{1,2}):(\d{2})\s*$", s or "")
    if not m:
        return (10, 0)
    hh = max(0, min(23, int(m.group(1))))
    mm = max(0, min(59, int(m.group(2))))
    return hh, mm


def is_within_work_hours(dt: Optional[datetime] = None) -> bool:
    dt = dt or datetime.now(TZ)
    sh, sm = _parse_hhmm(WORK_HOURS_START)
    eh, em = _parse_hhmm(WORK_HOURS_END)
    start = dt.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = dt.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= dt <= end


def is_owner_chat(update: Update) -> bool:
    try:
        return update.effective_chat and update.effective_chat.id == OWNER_CHAT_ID
    except Exception:
        return False


def is_dispatcher_chat_id(chat_id: int) -> bool:
    return (chat_id == COURIER_GROUP_ID) or (ADMIN_CHAT_ID and chat_id == ADMIN_CHAT_ID)


def is_private_chat(update: Update) -> bool:
    try:
        return update.effective_chat and update.effective_chat.type == "private"
    except Exception:
        return False


# =========================
# ===== Address rules =====
# =========================
def has_locality(addr: str) -> bool:
    a = (addr or "").strip()
    if not a:
        return False
    low = a.lower()
    marker_pattern = r"(^|,|\s)(м\.|с\.|смт|селище|місто|село)\s*[A-Za-zА-Яа-яІіЇїЄєҐґ'\-]{3,}"
    if re.search(marker_pattern, low):
        return True
    parts = [p.strip() for p in a.split(",") if p.strip()]
    if len(parts) >= 2:
        locality_part = parts[-1]
        if re.search(r"[A-Za-zА-Яа-яІіЇїЄєҐґ'\-]{3,}", locality_part) and not re.search(r"\d", locality_part):
            return True
    return False


def locality_hint() -> str:
    return (
        "❗ Адреса має містити населений пункт (місто/село/смт).\n\n"
        "Приклади правильного формату:\n"
        "• м. Обухів, вул. Київська 1\n"
        "• с. Германівка, вул. Шевченка 5\n"
        "• вул. Київська 1, Обухів\n\n"
        "Спробуй ще раз 👇"
    )


def addr_prompt_text(kind: str) -> str:
    return (
        f"{kind}\n\n"
        "✍️ Напиши адресу у форматі:\n"
        "• м. Обухів, вул. Київська 1\n"
        "• с. Германівка, вул. Шевченка 5\n"
        "або\n"
        "• вул. Київська 1, Обухів\n\n"
        "📍 Можна також натиснути «📍 Поділитись точкою» для точнішого розрахунку.\n"
        "⚠️ У Telegram Desktop (ПК) кнопка геолокації може не відкриватися. Якщо так — надішли лінк Google Maps або координати (lat, lon). На телефоні кнопка працює."
    )


# =========================
# ===== Menus / Texts =====
# =========================
def role_menu():
    return ReplyKeyboardMarkup(
        [["🙋 Я клієнт", "🚚 Я кур'єр"]],
        resize_keyboard=True
    )


def tariff_text() -> str:
    return (
        "💳 Наші тарифи\n\n"
        f"⏰ На певний час: {PRICE_SCHEDULED_BASE} грн до {BASE_KM} км\n"
        f"➕ кожен додатковий 1 км: +{EXTRA_KM_PRICE} грн\n\n"
        f"⚡ Термінова: {PRICE_URGENT_BASE} грн до {BASE_KM} км\n"
        f"➕ кожен додатковий 1 км: +{EXTRA_KM_PRICE} грн\n\n"
        f"🏙️ Поза містом Обухів: додатково +{EXTRA_OUTSIDE_OBUKHIV_PER_KM} грн/км."
    )


def support_text() -> str:
    return (
        "🛠 Підтримка\n"
        f"📞 Наші номери:\n"
        f"• {SUPPORT_PHONE_1}\n\n"
        "Натисни «📞 Зателефонуйте мені» або напиши свій номер — ми передзвонимо."
    )


def main_menu():
    return ReplyKeyboardMarkup(
        [["🚚 Доставка", "💳 Тариф", "🛠 Підтримка"]],
        resize_keyboard=True
    )


def courier_menu():
    # ✅ Додано "🆔 Мій ID"
    return ReplyKeyboardMarkup(
        [
            ["📦 Мої активні", "💳 Мій баланс"],
            ["⭐ Мій рейтинг", "🆔 Мій ID"],
            ["🔄 Закрити замовлення"],
            ["🆘 Техпідтримка"],
            ["⬅️ Курєрське меню"],
        ],
        resize_keyboard=True
    )



def courier_active_order_menu(is_buy: bool = False) -> ReplyKeyboardMarkup:
    rows = []
    if is_buy:
        rows.append(["📸 Надіслати чек (фото)"])
    rows.append(["✅ Доставлено", "🆘 Техпідтримка"])
    rows.append(["📦 Мої активні", "⬅️ Курєрське меню"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def customer_active_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["📞 Зв’язатись з кур’єром", "🆘 Техпідтримка (замовлення)"],
            ["⬅️ Головне меню"],
        ],
        resize_keyboard=True
    )

def owner_quick_kb():
    return ReplyKeyboardMarkup([["/panel"]], resize_keyboard=True)


def delivery_mode_menu():
    return ReplyKeyboardMarkup(
        [
            ["🛒 Купити / викуп кур’єром", "📦 Забрати готове (оплачено)"],
            ["⬅️ Назад в меню"]
        ],
        resize_keyboard=True
    )


def delivery_type_menu():
    return ReplyKeyboardMarkup(
        [["⏰ На певний час", "⚡ Термінова"],
         ["⬅️ Назад в меню"]],
        resize_keyboard=True
    )


def back_only_kb():
    return ReplyKeyboardMarkup([["⬅️ Назад в меню"]], resize_keyboard=True)


def addr_input_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["⬅️ Назад в меню"],
        ],
        resize_keyboard=True
    )

def phone_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📱 Поділитись контактом", request_contact=True)],
            ["Пропустити"],
            ["⬅️ Назад в меню"]
        ],
        resize_keyboard=True
    )


def callme_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📞 Зателефонуйте мені", request_contact=True)],
            ["⬅️ Назад в меню"]
        ],
        resize_keyboard=True
    )


def addr_confirm_kb():
    return ReplyKeyboardMarkup(
        [["✅ Підтвердити адресу", "✏️ Змінити адресу"],
         ["⬅️ Назад в меню"]],
        resize_keyboard=True
    )


def when_confirm_kb():
    return ReplyKeyboardMarkup(
        [["✅ Підтвердити час", "✏️ Змінити час"],
         ["⬅️ Назад в меню"]],
        resize_keyboard=True
    )


def order_confirm_kb():
    return ReplyKeyboardMarkup(
        [
            ["✅ Підтвердити", "❌ Скасувати"],
            ["📍 Змінити звідки", "🎯 Змінити куди"],
            ["💬 Змінити коментар"],
            ["⬅️ Назад в меню"],
        ],
        resize_keyboard=True
    )


def owner_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика: сьогодні", callback_data="stats:today")],
        [InlineKeyboardButton("📊 Статистика: вчора", callback_data="stats:yesterday")],
        [InlineKeyboardButton("📊 Статистика: тиждень", callback_data="stats:week")],
        [InlineKeyboardButton("📊 Статистика: місяць", callback_data="stats:month")],
        [InlineKeyboardButton("📤 Експорт CSV за місяць", callback_data="export:month")],
        [InlineKeyboardButton("🟢 Активні замовлення", callback_data="active:list")],
        [InlineKeyboardButton("📚 Закриті замовлення", callback_data="closed:list")],
        [InlineKeyboardButton("⛔ Чорний список кур’єрів", callback_data="bl:list")],
        [InlineKeyboardButton("⏰ Графік роботи", callback_data="wh:view")],
        [InlineKeyboardButton("⭐ Рейтинги кур’єрів", callback_data="ratings:list")],
        [InlineKeyboardButton("🧾 Архів чеків (викуп)", callback_data="checks:list")],
        [
            InlineKeyboardButton("➕ Додати кур’єра", callback_data="owner:add"),
            InlineKeyboardButton("➖ Видалити кур’єра", callback_data="owner:del"),
        ],
        [InlineKeyboardButton("👥 Список кур’єрів", callback_data="owner:list")],
        [
            InlineKeyboardButton("💳 Поповнити баланс", callback_data="owner:topup"),
            InlineKeyboardButton("🔍 Баланс кур’єра", callback_data="owner:bal"),
        ],
        [InlineKeyboardButton("⬇️ Зняти баланс (встановити)", callback_data="owner:setbal")],
    ])


# =========================
# ===== Geocoding =====
# =========================
_GEOCODE_CACHE: Dict[str, Tuple[float, float]] = {}
_LAST_GOOGLE_TS = 0.0


def _is_ua_result(result: dict) -> bool:
    comps = result.get("address_components") or []
    for c in comps:
        if "country" in (c.get("types") or []) and (c.get("short_name") or "").upper() == "UA":
            return True
    return False


def ensure_ua_suffix(addr: str) -> str:
    a = (addr or "").strip()
    low = a.lower()
    if "укра" in low or "ukraine" in low:
        return a
    return f"{a}, Україна"


async def geocode_address_google(address: str, session: aiohttp.ClientSession) -> Optional[Tuple[float, float]]:
    global _LAST_GOOGLE_TS

    addr = (address or "").strip()
    if not addr or not GOOGLE_MAPS_API_KEY:
        return None

    addr = ensure_ua_suffix(addr)

    if addr in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[addr]

    now = time.time()
    wait = 0.12 - (now - _LAST_GOOGLE_TS)  # ~8 req/s
    if wait > 0:
        await asyncio.sleep(wait)

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "address": addr,
        "key": GOOGLE_MAPS_API_KEY,
        "language": "uk",
        "region": "ua",
        "components": "country:UA",
    }

    try:
        async with session.get(url, params=params, timeout=12) as resp:
            _LAST_GOOGLE_TS = time.time()
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None

    if (data or {}).get("status") != "OK":
        return None

    results = data.get("results") or []
    if not results:
        return None

    ua_result = None
    for r in results:
        if _is_ua_result(r):
            ua_result = r
            break

    if not ua_result:
        return None

    loc = (ua_result.get("geometry") or {}).get("location") or {}
    try:
        lat = float(loc["lat"])
        lon = float(loc["lng"])
    except Exception:
        return None

    _GEOCODE_CACHE[addr] = (lat, lon)
    return lat, lon


def gmaps_link_from_coords(lat: float, lon: float) -> str:
    return f"https://www.google.com/maps?q={lat},{lon}"


def get_distance_km(c1: Optional[Tuple[float, float]], c2: Optional[Tuple[float, float]]) -> float:
    if not c1 or not c2:
        return 0.0
    R = 6371.0
    phi1 = math.radians(c1[0])
    phi2 = math.radians(c2[0])
    dphi = math.radians(c2[0] - c1[0])
    dlambda = math.radians(c2[1] - c1[1])
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def is_outside_obukhiv(from_addr: str, to_addr: str) -> bool:
    return ("обухів" not in (from_addr or "").lower()) or ("обухів" not in (to_addr or "").lower())


def calculate_finance_auto(from_addr: str, to_addr: str, dist_km: float, is_urgent: bool, fee_rate: float):
    outside = is_outside_obukhiv(from_addr, to_addr)
    safe_dist = dist_km * DIST_SAFETY_MULT
    base = PRICE_URGENT_BASE if is_urgent else PRICE_SCHEDULED_BASE

    km_price = EXTRA_KM_PRICE + (EXTRA_OUTSIDE_OBUKHIV_PER_KM if outside else 0)
    extra_km = max(0.0, safe_dist - BASE_KM)
    total = base + (extra_km * km_price)

    fee = total * fee_rate
    cut = total - fee
    return int(round(total)), int(round(fee)), int(round(cut)), outside, safe_dist


def calculate_finance_final(from_addr: str, to_addr: str, dist_km_real: float, is_urgent: bool, fee_rate: float):
    outside = is_outside_obukhiv(from_addr, to_addr)
    base = PRICE_URGENT_BASE if is_urgent else PRICE_SCHEDULED_BASE
    km_price = EXTRA_KM_PRICE + (EXTRA_OUTSIDE_OBUKHIV_PER_KM if outside else 0)

    extra_km = max(0.0, dist_km_real - BASE_KM)
    total = base + (extra_km * km_price)

    fee = total * fee_rate
    cut = total - fee
    return int(round(total)), int(round(fee)), int(round(cut)), outside


# =========================
# ===== Order keyboards ===
# =========================
def kb_accept(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚚 Взяти замовлення", callback_data=f"accept:{order_id}")]
    ])





async def repost_order_to_dispatcher(context: ContextTypes.DEFAULT_TYPE, order_id: str):
    """Repost an order back to dispatcher chat so another courier can take it."""
    order = ORDERS_DB.get(order_id)
    if not order:
        return

    outside = bool(order.get("outside"))
    outside_text = "🌆 За містом\n" if outside else "🏢 По місту\n"

    when_line = order.get("when_label") or order.get("when") or "-"
    from_addr_v = order.get("from_addr") or "-"
    to_addr_v = order.get("to_addr") or "-"
    from_coords = order.get("from_coords")
    to_coords = order.get("to_coords")

    total = int(order.get("total") or 0)

    if order.get("delivery_mode") == "buy":
        title = f"🟡 **ВИКУП • ПОТРІБЕН ЧЕК** №{order_id}"
        body = (
            outside_text
            + f"🕒 Час: {when_line}\n"
            + f"📍 Звідки: {from_addr_v}\n"
            + f"📍 Гео (ЗВІДКИ): {fmt_link(from_coords)}\n"
            + f"🎯 Куди: {to_addr_v}\n"
            + f"📍 Гео (КУДИ): {fmt_link(to_coords)}\n"
            + f"🧾 Список: {order.get('buy_list','-')}\n"
            + f"💰 Орієнт. сума покупок: {int(order.get('buy_approx_sum') or 0)} грн\n"
            + f"🔒 Гарантія: {int(order.get('guarantee_amount') or 0)} грн\n"
            + f"📞 Телефон клієнта: {order.get('phone') or 'не вказано'}\n"
            + f"💬 Коментар: {order.get('comment','-')}\n"
            + "------------------------\n"
            + f"💰 Доставка (авто): {total} грн\n"
        )
    else:
        title = f"🚚 **НОВЕ ЗАМОВЛЕННЯ №{order_id}**"
        body = (
            outside_text
            + f"🕒 Час: {when_line}\n"
            + f"📍 Звідки: {from_addr_v}\n"
            + f"🧭 Коорд. звідки: {fmt_coords(from_coords)}\n"
            + f"📍 Гео (ЗВІДКИ): {fmt_link(from_coords)}\n"
            + f"🎯 Куди: {to_addr_v}\n"
            + f"🧭 Коорд. куди: {fmt_coords(to_coords)}\n"
            + f"📍 Гео (КУДИ): {fmt_link(to_coords)}\n"
            + f"📦 Що: {order.get('item','-')}\n"
            + f"📞 Тел: {order.get('phone') or 'не вказано'}\n"
            + f"💬 Коментар: {order.get('comment','-')}\n"
            + "------------------------\n"
            + f"💰 До сплати клієнтом: {total} грн\n"
        )

    msg_to_couriers = f"{title}\n{body}"
    dispatcher_chat = order.get("admin_chat_id") or dispatcher_chat_id()
    if dispatcher_chat:
        sent = await context.bot.send_message(
            chat_id=dispatcher_chat,
            text=msg_to_couriers,
            reply_markup=kb_accept(order_id),
            parse_mode="Markdown"
        )
        order["admin_msg_id"] = sent.message_id

def kb_courier_active_list(order_ids: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for oid in order_ids[:10]:
        rows.append([InlineKeyboardButton(f"📦 №{oid}", callback_data=f"courier_open:{oid}")])
    if not rows:
        rows = [[InlineKeyboardButton("—", callback_data="noop")]]
    return InlineKeyboardMarkup(rows)


def kb_courier_controls_pickup(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Доставлено", callback_data=f"delivered:{order_id}")],
        [InlineKeyboardButton("🆘 Техпідтримка", callback_data=f"support:{order_id}")]
    ])


def kb_courier_controls_buy(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📸 Надіслати чек (фото)", callback_data=f"sendcheck:{order_id}")],
        [InlineKeyboardButton("✅ Доставлено", callback_data=f"delivered:{order_id}")],
        [InlineKeyboardButton("🆘 Техпідтримка", callback_data=f"support:{order_id}")]
    ])


def kb_customer_done(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отримав", callback_data=f"done:{order_id}")]
    ])


def tme_url(username: str) -> Optional[str]:
    u = (username or "").strip().lstrip("@")
    if not u or u == "-":
        return None
    return f"https://t.me/{u}"

def tg_user_url(user_id: int, username: str = "") -> str:
    # Вимога: відкривати контакт через t.me/username
    u = (username or "").lstrip("@").strip()
    if u and u != "-":
        return f"https://t.me/{u}"
    return ""



def kb_contact_customer(order_id: str, customer_id: int, customer_username: str = "") -> Optional[InlineKeyboardMarkup]:
    url = tg_user_url(customer_id, customer_username)
    if not url:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("📞 Написати клієнту", url=url)]])


def kb_contact_courier(order_id: str, courier_id: int, courier_username: str = "") -> Optional[InlineKeyboardMarkup]:
    url = tg_user_url(courier_id, courier_username)
    if not url:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("📞 Написати кур’єру", url=url)]])


def kb_close_after_manual(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Закрити замовлення", callback_data=f"close_manual:{order_id}")]
    ])


def finalize_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Ціна вірна", callback_data=f"finish_auto:{order_id}")],
        [InlineKeyboardButton("✏️ Ввести фінальний км", callback_data=f"finish_manual:{order_id}")]
    ])


def kb_owner_force_close(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛠 Закрити вручну", callback_data=f"force:{order_id}")]
    ])


def kb_owner_payment_confirm(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Підтвердити гарантію", callback_data=f"gpay_ok:{order_id}")],
        [InlineKeyboardButton("❌ Відхилити/скасувати", callback_data=f"gpay_no:{order_id}")],
    ])


def kb_customer_paid(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Я оплатив гарантію", callback_data=f"gpaid:{order_id}")],
        [InlineKeyboardButton("❌ Не оплатив — скасувати", callback_data=f"gcancel:{order_id}")],
    ])


def kb_rate(order_id: str, courier_id: int) -> InlineKeyboardMarkup:
    # rating goes as text too, but inline is faster
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1", callback_data=f"rate:{order_id}:{courier_id}:1"),
            InlineKeyboardButton("2", callback_data=f"rate:{order_id}:{courier_id}:2"),
            InlineKeyboardButton("3", callback_data=f"rate:{order_id}:{courier_id}:3"),
            InlineKeyboardButton("4", callback_data=f"rate:{order_id}:{courier_id}:4"),
            InlineKeyboardButton("5", callback_data=f"rate:{order_id}:{courier_id}:5"),
        ]
    ])


def support_share_contact_kb(has_active_order: bool = False) -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton("📱 Поділитись номером", request_contact=True)]]
    if has_active_order:
        rows.append(["↩️ Повернутись до замовлення"])
        rows.append(["⬅️ Курєрське меню"])
    else:
        rows.append(["⬅️ Назад в меню"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def kb_support_pick(orders: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for oid in orders[:10]:
        rows.append([InlineKeyboardButton(f"🆘 №{oid}", callback_data=f"support_pick:{oid}")])
    rows.append([InlineKeyboardButton("❌ Скасувати", callback_data="support_pick:cancel")])
    return InlineKeyboardMarkup(rows)


# =========================
# ===== Utils =============
# =========================
def gen_order_id() -> str:
    return str(int(time.time() * 1000))[-8:]


def fmt_coords(coords: Optional[Tuple[float, float]]) -> str:
    if not coords:
        return "не знайдено"
    return f"{coords[0]:.6f}, {coords[1]:.6f}"


def fmt_link(coords: Optional[Tuple[float, float]]) -> str:
    if not coords:
        return "-"
    return gmaps_link_from_coords(coords[0], coords[1])


def extract_coords_from_text(text: str) -> Optional[Tuple[float, float]]:
    """Extract coordinates from plain 'lat, lon' or Google Maps links."""
    t = (text or "").strip()
    if not t:
        return None

    # plain coords: 50.123, 30.456
    m = re.search(r"(-?\d{1,2}\.\d{3,})\s*,\s*(-?\d{1,3}\.\d{3,})", t)
    if m:
        try:
            lat = float(m.group(1))
            lon = float(m.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                return (lat, lon)
        except Exception:
            pass

    # Google Maps variants
    # ...maps?q=lat,lon  OR  ...maps/@lat,lon,zoom
    m = re.search(r"[?&]q=(-?\d{1,2}\.\d+),(-?\d{1,3}\.\d+)", t)
    if m:
        try:
            return (float(m.group(1)), float(m.group(2)))
        except Exception:
            pass

    m = re.search(r"/@(-?\d{1,2}\.\d+),(-?\d{1,3}\.\d+)", t)
    if m:
        try:
            return (float(m.group(1)), float(m.group(2)))
        except Exception:
            pass

    return None

def dispatcher_chat_id() -> int:
    return COURIER_GROUP_ID or ADMIN_CHAT_ID


def schedule_once(context: ContextTypes.DEFAULT_TYPE, callback, delay_sec: float, data: Optional[dict] = None, name: str = ""):
    """Run a one-shot delayed task even when JobQueue is unavailable (e.g., PTB installed without job-queue extras).

    - If JobQueue exists, uses job_queue.run_once.
    - Otherwise falls back to asyncio.sleep + direct callback invocation with a minimal context-like object.
    """
    jq = getattr(context, "job_queue", None)
    if jq:
        try:
            return jq.run_once(callback, when=delay_sec, data=data, name=(name or None))
        except Exception:
            # fallback to asyncio below
            pass

    async def _runner():
        await asyncio.sleep(max(0.0, float(delay_sec)))

        class _Job:
            def __init__(self, d):
                self.data = d

        class _Ctx:
            def __init__(self, application, bot, job):
                self.application = application
                self.bot = bot
                self.job = job

        try:
            await callback(_Ctx(context.application, context.bot, _Job(data)))
        except Exception:
            pass

    asyncio.create_task(_runner())
    return None


def courier_active_order_ids(courier_id: int) -> List[str]:
    ids = []
    for oid, o in ORDERS_DB.items():
        if o.get("courier_id") == courier_id and o.get("status") not in ("closed",):
            if o.get("status") in ("accepted", "await_customer", "await_finish", "searching"):
                ids.append(oid)
    return ids


def get_single_active_order_for_courier(courier_id: int) -> Optional[dict]:
    active = [o for o in ORDERS_DB.values() if o.get("courier_id") == courier_id and o.get("status") in ("accepted", "await_customer", "await_finish", "searching") and o.get("status") != "closed"]
    if len(active) == 1:
        return active[0]
    return None


def get_any_active_order_for_courier(courier_id: int) -> Optional[dict]:
    for o in ORDERS_DB.values():
        if o.get("courier_id") == courier_id and o.get("status") not in ("closed",):
            return o
    return None


async def send_courier_active_order_details(context: ContextTypes.DEFAULT_TYPE, courier_id: int, order: Optional[dict]):
    if not order:
        await context.bot.send_message(courier_id, "📭 Активне замовлення не знайдено.", reply_markup=courier_menu())
        return

    order_id = str(order.get("order_id") or "-")
    when_val = order.get("when_label") or order.get("scheduled_when") or order.get("when") or "-"

    if order.get("delivery_mode") == "buy":
        info = (
            f"📦 **Активне замовлення №{order_id}**\n"
            f"🟡 ВИКУП • ПОТРІБЕН ЧЕК\n"
            f"🕒 Час: {when_val}\n"
            f"📍 Звідки: {order.get('from_addr','-')}\n"
            f"🎯 Куди: {order.get('to_addr','-')}\n"
            f"🧾 Список: {order.get('buy_list','-')}\n"
            f"🔒 Гарантія: {order.get('guarantee_amount','-')} грн\n"
            f"📞 Телефон клієнта: {order.get('phone','-')}\n"
        )
    else:
        info = (
            f"📦 **Активне замовлення №{order_id}**\n"
            f"🕒 Час: {when_val}\n"
            f"📍 Звідки: {order.get('from_addr','-')}\n"
            f"🎯 Куди: {order.get('to_addr','-')}\n"
            f"📦 Що: {order.get('item','-')}\n"
            f"📞 Телефон клієнта: {order.get('phone','-')}\n"
        )

    await context.bot.send_message(
        chat_id=courier_id,
        text=info,
        parse_mode="Markdown",
        reply_markup=courier_active_order_menu(bool(order.get("delivery_mode") == "buy"))
    )

    kb_contact = kb_contact_customer(order_id, int(order.get("customer_id") or 0), str(order.get("customer_username") or "-"))
    if kb_contact:
        await context.bot.send_message(chat_id=courier_id, text="📞 Зв’язок з клієнтом:", reply_markup=kb_contact)
    else:
        await context.bot.send_message(chat_id=courier_id, text="📞 Зв’язок з клієнтом: у клієнта немає username в Telegram.")


def _money_int_from_text(txt: str) -> Optional[int]:
    s = (txt or "").strip().replace(",", ".")
    m = re.search(r"(\d+(\.\d+)?)", s)
    if not m:
        return None
    try:
        v = float(m.group(1))
        if v < 0:
            return None
        return int(round(v))
    except Exception:
        return None


def calc_guarantee(approx_sum: int) -> int:
    # 25%, min 150, max 500
    g = int(round(approx_sum * 0.25))
    return max(150, min(500, g))


def _month_key(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(TZ)
    return f"{dt.year}-{dt.month:02d}"


def _range_start_end(kind: str) -> Tuple[str, str]:
    """
    returns iso strings [start, end)
    """
    now = datetime.now(TZ)
    if kind == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif kind == "yesterday":
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
    elif kind == "week":
        # last 7 days including today
        end = now + timedelta(seconds=1)
        start = now - timedelta(days=7)
    elif kind == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            end = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        end = now + timedelta(seconds=1)
        start = now - timedelta(days=1)
    return start.isoformat(), end.isoformat()


def stats_closed(kind: str) -> Dict[str, int]:
    start, end = _range_start_end(kind)
    con = db_connect()
    cur = con.cursor()
    row = cur.execute(
        "SELECT COUNT(*) AS c, COALESCE(SUM(total),0) AS s, COALESCE(SUM(fee),0) AS f "
        "FROM orders_closed WHERE closed_at>=? AND closed_at<?",
        (start, end),
    ).fetchone()
    con.close()
    return {
        "orders": int(row["c"]),
        "revenue": int(row["s"]),
        "fee": int(row["f"]),
    }


def build_csv_for_month(year_month: str) -> Tuple[str, str]:
    # returns (filename, csv_text)
    start = f"{year_month}-01"
    y, m = map(int, year_month.split("-"))
    if m == 12:
        end = f"{y+1}-01-01"
    else:
        end = f"{y}-{m+1:02d}-01"

    con = db_connect()
    cur = con.cursor()
    rows = cur.execute(
        "SELECT order_id, closed_at, delivery_mode, delivery_key, total, fee, courier_id, courier_name, "
        "customer_id, customer_name, customer_username, from_addr, to_addr "
        "FROM orders_closed WHERE closed_at>=? AND closed_at<? ORDER BY closed_at ASC",
        (start, end),
    ).fetchall()
    con.close()

    out = StringIO()
    w = csv.writer(out)
    w.writerow([
        "order_id", "closed_at", "delivery_mode", "delivery_key",
        "total", "fee", "courier_id", "courier_name",
        "customer_id", "customer_name", "customer_username",
        "from_addr", "to_addr"
    ])
    for r in rows:
        w.writerow([
            r["order_id"], r["closed_at"], r["delivery_mode"], r["delivery_key"],
            r["total"], r["fee"], r["courier_id"], r["courier_name"],
            r["customer_id"], r["customer_name"], r["customer_username"],
            r["from_addr"], r["to_addr"]
        ])

    filename = f"jetway_{year_month}.csv"
    return filename, out.getvalue()


# =========================
# ===== Owner panel =======
# =========================
async def panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if update.effective_chat.id != OWNER_CHAT_ID:
        return
    await update.message.reply_text("🧩 Адмін-панель", reply_markup=owner_panel_kb())


async def owner_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if update.effective_chat.id != OWNER_CHAT_ID:
        return

    owner_id = update.effective_user.id
    mode = OWNER_PENDING.get(owner_id)
    if not mode:
        return

    txt = update.message.text.strip()

    if mode == "wh_set":
        m = re.match(r"^\s*(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})\s*$", txt)
        if not m:
            await update.message.reply_text("❌ Формат невірний. Приклад: 10:00-22:00")
            return
        ws, we = m.group(1), m.group(2)
        db_set_setting("work_hours_start", ws)
        db_set_setting("work_hours_end", we)
        db_load_settings_into_globals()
        await update.message.reply_text(f"✅ Графік оновлено: {WORK_HOURS_START}–{WORK_HOURS_END}")
        OWNER_PENDING.pop(owner_id, None)
        return

    if mode == "bl_add":
        parts = txt.split(maxsplit=1)
        try:
            cid = int(parts[0])
        except ValueError:
            await update.message.reply_text("❌ Невірний ID.")
            return
        reason = parts[1] if len(parts) > 1 else ""
        db_blacklist_add(cid, reason)
        await update.message.reply_text(f"✅ Кур’єра {cid} додано в чорний список.")
        OWNER_PENDING.pop(owner_id, None)
        return

    if mode == "bl_del":
        try:
            cid = int(txt.split()[0])
        except ValueError:
            await update.message.reply_text("❌ Невірний ID.")
            return
        db_blacklist_remove(cid)
        await update.message.reply_text(f"✅ Кур’єра {cid} прибрано з чорного списку.")
        OWNER_PENDING.pop(owner_id, None)
        return

    if mode in ("add", "del", "bal", "setbal"):
        try:
            cid = int(txt.split()[0])
        except ValueError:
            return await update.message.reply_text("ID має бути числом. Приклад: `123456789`", parse_mode="Markdown")

        if mode == "add":
            db_add_courier(cid, update.effective_user.username or "", "")
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(f"✅ Додано кур’єра: `{cid}`", parse_mode="Markdown")
            try:
                await context.bot.send_message(cid, "✅ Вас додано як кур’єра. Тепер ви можете приймати замовлення.")
            except Exception:
                pass
            return

        if mode == "del":
            removed = cid in COURIERS
            db_del_courier(cid)
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(
                f"✅ Видалено кур’єра: `{cid}`" if removed else f"ℹ️ Кур’єра `{cid}` не було у списку.",
                parse_mode="Markdown")
            return

        if mode == "bal":
            bal = db_get_balance(cid)
            is_c = "✅" if cid in COURIERS else "❌"
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(
                f"🔍 Кур’єр: `{cid}`\n"
                f"Статус у списку: {is_c}\n"
                f"Баланс: **{bal} грн**",
                parse_mode="Markdown"
            )
            return

        if mode == "setbal":
            # format: ID new_balance
            parts = txt.replace(",", ".").split()
            if len(parts) != 2:
                return await update.message.reply_text(
                    "Формат: `ID новий_баланс`\nПриклад: `123456789 -200`",
                    parse_mode="Markdown"
                )
            try:
                cid = int(parts[0])
                nb = int(float(parts[1]))
            except Exception:
                return await update.message.reply_text("Помилка. Приклад: `123456789 -200`", parse_mode="Markdown")

            db_set_balance(cid, nb)
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(
                f"✅ Баланс `{cid}` встановлено: **{nb} грн**",
                parse_mode="Markdown"
            )
            try:
                await context.bot.send_message(cid, f"💳 Ваш баланс встановлено адміністратором: {nb} грн.")
            except Exception:
                pass
            return

    if mode == "topup":
        parts = txt.replace(",", ".").split()
        if len(parts) != 2:
            return await update.message.reply_text(
                "Формат: `ID сума`\nПриклад: `123456789 500`",
                parse_mode="Markdown"
            )
        try:
            cid = int(parts[0])
            amount = int(float(parts[1]))
        except Exception:
            return await update.message.reply_text("Помилка. Приклад: `123456789 500`", parse_mode="Markdown")

        db_add_balance(cid, amount)
        OWNER_PENDING.pop(owner_id, None)

        await update.message.reply_text(
            f"✅ Баланс `{cid}` поповнено на **{amount} грн**.\n"
            f"Поточний баланс: **{db_get_balance(cid)} грн**",
            parse_mode="Markdown"
        )

        try:
            await context.bot.send_message(
                cid,
                f"💳 Баланс поповнено на {amount} грн.\nПоточний баланс: {db_get_balance(cid)} грн."
            )
        except Exception:
            pass
        return


async def owner_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = (query.data or "").strip()
    await query.answer()

    if query.message.chat.id != OWNER_CHAT_ID:
        return await query.answer("Недостатньо прав.", show_alert=True)

    owner_id = query.from_user.id

    if data.startswith("stats:"):
        kind = data.split(":", 1)[1]
        s = stats_closed(kind)
        label = {
            "today": "сьогодні",
            "yesterday": "вчора",
            "week": "тиждень (останні 7 днів)",
            "month": "місяць (поточний)",
        }.get(kind, kind)
        msg = (
            f"📊 **Статистика за {label}**\n\n"
            f"✅ Завершених замовлень: {s['orders']}\n"
            f"💰 Оборот (доставка): {s['revenue']} грн\n"
            f"📈 Комісія кур’єрів (сума fee): {s['fee']} грн"
        )
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")
        return

    if data == "export:month":
        ym = _month_key()
        filename, csv_text = build_csv_for_month(ym)
        await context.bot.send_document(
            chat_id=OWNER_CHAT_ID,
            document=csv_text.encode("utf-8"),
            filename=filename,
            caption=f"📤 CSV за {ym}"
        )
        return

    if data == "ratings:list":
        if not COURIERS:
            await context.bot.send_message(OWNER_CHAT_ID, "👥 Список кур’єрів порожній.")
            return

        ym = _month_key()
        today_str = datetime.now(TZ).strftime("%m.%d.%Y")

        lines: List[str] = []
        lines.append("⭐ Рейтинги кур’єрів")
        lines.append(f"📅 {today_str}")
        lines.append("ℹ️ Умова бонусу: рейтинг ≥4.80 та мінімум 5 оцінок за місяць")
        lines.append("")

        for cid in sorted(COURIERS):
            avg_all, cnt_all = db_get_courier_rating(cid, None)
            avg_m, cnt_m = db_get_courier_rating(cid, ym)
            next_rate = db_calc_next_month_commission(cid)

            lines.append(f"🚚 Кур’єр: {cid}")
            lines.append(f"⭐ За весь час: {avg_all:.2f} (оцінок: {cnt_all})")
            lines.append(f"📊 За цей місяць ({ym[5:7]}.{ym[0:4]}): {avg_m:.2f} (оцінок: {cnt_m})")
            lines.append(f"💰 Комісія наступного місяця: {int(next_rate*100)}%")
            lines.append("")

        await context.bot.send_message(OWNER_CHAT_ID, "\n".join(lines).strip())
        return

    
    if data == "closed:list":
        rows = db_get_closed_orders(40)
        if not rows:
            await context.bot.send_message(OWNER_CHAT_ID, "📚 Закритих замовлень ще немає.")
            return

        # Показуємо детальну інформацію (останні 40)
        parts: List[str] = []
        for r in rows[:40]:
            try:
                dt = datetime.fromisoformat(r["closed_at"])
                dt_str = dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")
            except Exception:
                dt_str = str(r["closed_at"] or "-")

            mode = "🛒 ВИКУП" if r["delivery_mode"] == "buy" else "📦 ГОТОВЕ"
            courier = r["courier_name"] or str(r["courier_id"] or "-")
            customer = r["customer_name"] or "-"
            cuser = r["customer_username"] or "-"

            parts.append(
                f"🗓 {dt_str}\n"
                f"№{r['order_id']} | {mode} | {r['delivery_key'] or '-'}\n"
                f"👤 Клієнт: {customer} (@{cuser})\n"
                f"🚚 Кур’єр: {courier} (ID: {r['courier_id'] or '-'})\n"
                f"📍 Звідки: {r['from_addr'] or '-'}\n"
                f"🎯 Куди: {r['to_addr'] or '-'}\n"
                f"💰 Сума: {r['total']} грн | 📈 Комісія: {r['fee']} грн\n"
                f"--------------------------"
            )

        header = "📚 Закриті замовлення (останні)\n\n"
        chunk = ""
        for part in parts:
            if len(header) + len(chunk) + len(part) + 2 > 3800:
                await context.bot.send_message(OWNER_CHAT_ID, header + chunk)
                chunk = ""
            chunk += part + "\n"
        if chunk:
            await context.bot.send_message(OWNER_CHAT_ID, header + chunk)
        return

    if data == "active:list":
        # показати активні (не закриті) замовлення
        active = [o for o in ORDERS_DB.values() if o and o.get("status") != "closed"]
        if not active:
            await context.bot.send_message(OWNER_CHAT_ID, "🟢 Активних замовлень зараз немає.")
            return

        # покажемо останні/актуальні 15
        active = sorted(active, key=lambda x: str(x.get("order_id") or ""), reverse=True)[:15]

        lines = []
        buttons = []
        for o in active:
            oid = o.get("order_id")
            mode = "ВИКУП" if o.get("delivery_mode") == "buy" else "ГОТОВЕ"
            st = o.get("status") or "-"
            courier = o.get("courier_name") or str(o.get("courier_id") or "-")
            dist = o.get("dist_km")
            dist_s = f"{float(dist):.1f}км" if dist is not None else "-"
            total = o.get("total")
            fee = o.get("fee")
            lines.append(f"• №{oid} | {mode} | {st} | {dist_s} | {total} грн | fee {fee} | кур'єр: {courier}")
            if oid:
                buttons.append([
                    InlineKeyboardButton(f"✅ З комісією №{oid}", callback_data=f"active:close_fee:{oid}"),
                    InlineKeyboardButton(f"🆓 Без комісії №{oid}", callback_data=f"active:close_nofee:{oid}"),
                ])

        await context.bot.send_message(
            OWNER_CHAT_ID,
            "🟢 Активні замовлення\n\n" + "\n".join(lines) + "\n\nОберіть дію: закрити із комісією або без комісії.",
            reply_markup=InlineKeyboardMarkup(buttons[:10]) if buttons else None,
        )
        return

    
    # Закриття активного замовлення owner-ом:
    # - з комісією (списує fee з курʼєра)
    # - без комісії (fee=0, баланс не змінюємо)
    if data.startswith("active:close_fee:") or data.startswith("active:close_nofee:") or data.startswith("active:close:"):
        parts = data.split(":")
        # active:close_fee:<id>  | active:close_nofee:<id> | active:close:<id> (legacy)
        oid = parts[-1]
        order = ORDERS_DB.get(oid)
        if not order or order.get("status") == "closed":
            await context.bot.send_message(OWNER_CHAT_ID, f"⚠️ Замовлення №{oid} не знайдено або вже закрито.")
            return

        final_total = int(order.get("manual_total") or order.get("total") or 0)
        if data.startswith("active:close_nofee:"):
            final_fee = 0
            closed_by = "owner (no fee)"
        else:
            final_fee = int(order.get("manual_fee") or order.get("fee") or 0)
            closed_by = "owner"

        courier_id = order.get("courier_id")
        courier_balance_before = db_get_balance(int(courier_id)) if courier_id else None

        # списання комісії (або ні)
        if courier_id and final_fee:
            db_add_balance(int(courier_id), -final_fee)

        courier_balance_after = db_get_balance(int(courier_id)) if courier_id else None

        # mark closed & save
        order["status"] = "closed"
        order["closed_at"] = datetime.now(TZ).isoformat()
        order["closed_by"] = closed_by
        order["final_total"] = final_total
        order["final_fee"] = final_fee
        order["final_dist"] = order.get("manual_dist") or order.get("dist_km") or order.get("distance_km")

        db_insert_closed_order(order, final_total, final_fee)

        # повідомлення курʼєру
        if courier_id:
            try:
                txt_c = (
                    f"✅ №{oid} закрито owner-ом.\n"
                    f"📉 Комісія: {final_fee} грн.\n"
                    f"💰 Баланс: {courier_balance_after} грн"
                )
                await context.bot.send_message(int(courier_id), txt_c, reply_markup=courier_menu())
            except Exception:
                pass

        # повідомлення owner
        try:
            dt_str = datetime.fromisoformat(order["closed_at"]).astimezone(TZ).strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            dt_str = order["closed_at"]

        text_owner = (
            f"🏁 **ЗАМОВЛЕННЯ №{oid} ЗАКРИТО**\n"
            "--------------------------\n"
            f"👤 Клієнт: {order.get('customer_name','-')} (@{order.get('customer_username') or '-'})\n"
            f"🚚 Кур'єр: {order.get('courier_name','-')} (ID: {courier_id or '-'})\n"
            f"🧾 Тип: {'🛒 ВИКУП' if order.get('delivery_mode')=='buy' else '📦 ГОТОВЕ'}\n"
            f"📏 Дистанція: {order.get('final_dist','-')} км\n"
            f"💰 Каса (клієнт сплатив): {final_total} грн\n"
            f"📈 Комісія: {final_fee} грн\n"
            f"💳 Баланс кур'єра: {(courier_balance_after if courier_id else '-')} грн\n"
            f"🔒 Закрив: {closed_by}\n"
            f"🕒 Час: {dt_str}\n"
        )

        await context.bot.send_message(
            OWNER_CHAT_ID,
            text_owner,
            parse_mode="Markdown",
        )
        return

    if data == "checks:list":
        rows = db_get_checks(30)
        if not rows:
            await context.bot.send_message(OWNER_CHAT_ID, "🧾 Архів чеків порожній.")
            return

        items: List[str] = []
        for r in rows[:30]:
            try:
                dt = datetime.fromisoformat(r["created_at"])
                dt_str = dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")
            except Exception:
                dt_str = str(r["created_at"] or "-")
            media_label = {"photo": "Фото", "document": "Файл", "video": "Відео"}.get((r["media_type"] or "photo"), "Медіа")
            name_part = f" | {r['file_name']}" if r["file_name"] else ""
            items.append(f"• №{r['order_id']} | {media_label} | {dt_str}{name_part}")

        await context.bot.send_message(
            OWNER_CHAT_ID,
            "🧾 **Останні чеки / медіа**\n\n" + "\n".join(items),
            parse_mode="Markdown",
        )

        for r in rows[:10]:
            file_id = r["file_id"]
            if not file_id:
                continue
            try:
                try:
                    dt = datetime.fromisoformat(r["created_at"])
                    dt_str = dt.astimezone(TZ).strftime("%d.%m.%Y %H:%M")
                except Exception:
                    dt_str = str(r["created_at"] or "-")

                caption = f"🧾 Архів медіа по замовленню №{r['order_id']}\n🗓 {dt_str}"
                media_type = (r["media_type"] or "photo").lower()
                if media_type == "photo":
                    await context.bot.send_photo(chat_id=OWNER_CHAT_ID, photo=file_id, caption=caption)
                elif media_type == "video":
                    await context.bot.send_video(chat_id=OWNER_CHAT_ID, video=file_id, caption=caption)
                else:
                    await context.bot.send_document(chat_id=OWNER_CHAT_ID, document=file_id, caption=caption)
            except Exception:
                try:
                    await context.bot.send_document(
                        chat_id=OWNER_CHAT_ID,
                        document=file_id,
                        caption=f"🧾 Архів медіа по замовленню №{r['order_id']}",
                    )
                except Exception:
                    pass
        return

    if data == "owner:add":

        OWNER_PENDING[owner_id] = "add"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "➕ Надішли **ID кур’єра** одним числом.\nНаприклад: `123456789`",
            parse_mode="Markdown"
        )
        return

    if data == "owner:del":
        OWNER_PENDING[owner_id] = "del"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "➖ Надішли **ID кур’єра**, якого треба видалити.\nНаприклад: `123456789`",
            parse_mode="Markdown"
        )
        return

    if data == "owner:list":
        if not COURIERS:
            await context.bot.send_message(OWNER_CHAT_ID, "👥 Список порожній.")
            return
        lines = "\n".join(f"• `{cid}`" for cid in sorted(COURIERS))
        await context.bot.send_message(OWNER_CHAT_ID, "👥 **Кур’єри:**\n" + lines, parse_mode="Markdown")
        return

    if data == "owner:topup":
        OWNER_PENDING[owner_id] = "topup"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "💳 Введи **ID і суму** через пробіл.\nПриклад: `123456789 500`",
            parse_mode="Markdown"
        )
        return

    if data == "owner:bal":
        OWNER_PENDING[owner_id] = "bal"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "🔍 Введи **ID кур’єра**.\nПриклад: `123456789`",
            parse_mode="Markdown"
        )
        return

    if data == "owner:setbal":
        OWNER_PENDING[owner_id] = "setbal"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "⬇️ Введи **ID і НОВИЙ баланс** через пробіл.\nПриклад: `123456789 -200`",
            parse_mode="Markdown"
        )
        return



    if data == "wh:view":
        msg = f"⏰ Графік роботи\n\nПоточний: {WORK_HOURS_START}–{WORK_HOURS_END}\n\nНатисни «✏️ Змінити», щоб встановити новий."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("✏️ Змінити", callback_data="wh:set")]])
        await context.bot.send_message(OWNER_CHAT_ID, msg, reply_markup=kb)
        return

    if data == "wh:set":
        OWNER_PENDING[owner_id] = "wh_set"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "✏️ Введи новий графік у форматі `HH:MM-HH:MM`.\nПриклад: `10:00-22:00`",
            parse_mode="Markdown"
        )
        return

    if data == "bl:list":
        rows = db_blacklist_list()
        if not rows:
            await context.bot.send_message(OWNER_CHAT_ID, "⛔ Чорний список порожній.")
        else:
            lines = ["⛔ Чорний список кур’єрів\n"]
            for cid, reason, added_at in rows[:50]:
                ds = added_at
                try:
                    dt = datetime.fromisoformat(added_at)
                    ds = dt.strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pass
                r = (reason or "").strip()
                lines.append(f"• {cid} | {ds}" + (f" | {r}" if r else ""))
            await context.bot.send_message(OWNER_CHAT_ID, "\n".join(lines))
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("➕ Додати", callback_data="bl:add"), InlineKeyboardButton("➖ Видалити", callback_data="bl:del")]])
        await context.bot.send_message(OWNER_CHAT_ID, "Керування чорним списком:", reply_markup=kb)
        return

    if data == "bl:add":
        OWNER_PENDING[owner_id] = "bl_add"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "➕ Введи `ID` або `ID причина`.\nПриклад: `123456789 спам/порушення`",
            parse_mode="Markdown"
        )
        return

    if data == "bl:del":
        OWNER_PENDING[owner_id] = "bl_del"
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "➖ Введи `ID` кур’єра, щоб прибрати з чорного списку.\nПриклад: `123456789`",
            parse_mode="Markdown"
        )
        return
# =========================
# ===== Group join notify =
# =========================
async def on_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not COURIER_GROUP_ID or not OWNER_CHAT_ID:
        return
    if update.effective_chat.id != COURIER_GROUP_ID:
        return

    for m in update.message.new_chat_members:
        if m.is_bot:
            continue
        text = (
            "👤 Новий учасник у диспетчерській\n"
            f"Імʼя: {m.full_name}\n"
            f"Username: @{m.username if m.username else '-'}\n"
            f"ID: {m.id}\n\n"
            "Додати курʼєром: /panel → ➕ Додати"
        )
        await context.bot.send_message(OWNER_CHAT_ID, text)


async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not COURIER_GROUP_ID or not OWNER_CHAT_ID:
        return
    cmu = update.chat_member
    if not cmu or cmu.chat.id != COURIER_GROUP_ID:
        return

    new_status = cmu.new_chat_member.status
    old_status = cmu.old_chat_member.status

    joined = old_status in ("left", "kicked") and new_status in ("member", "administrator", "restricted")
    if not joined:
        return

    u = cmu.new_chat_member.user
    if u.is_bot:
        return

    text = (
        "👤 Новий учасник у диспетчерській\n"
        f"Імʼя: {u.full_name}\n"
        f"Username: @{u.username if u.username else '-'}\n"
        f"ID: {u.id}\n\n"
        "Додати курʼєром: /panel → ➕ Додати"
    )
    await context.bot.send_message(OWNER_CHAT_ID, text)


# =========================
# ===== Finalize & close ==
# =========================
async def ask_customer_rating_if_needed(context: ContextTypes.DEFAULT_TYPE, order: dict):
    try:
        courier_id = order.get("courier_id")
        customer_id = order.get("customer_id")
        order_id = order.get("order_id")
        # ✅ дозволяємо оцінку текстом 1–5
        pending = context.application.bot_data.get("rating_pending", {})
        pending[int(customer_id)] = (str(order_id), int(courier_id))
        context.application.bot_data["rating_pending"] = pending
        if not courier_id or not customer_id or not order_id:
            return

        await context.bot.send_message(
            chat_id=customer_id,
            text=(
                f"⭐ Замовлення №{order_id} завершено.\n"
                "Оцініть кур’єра від 1 до 5 (надішліть цифру або натисніть кнопку):"
            ),
            reply_markup=kb_rate(order_id, int(courier_id))
        )
    except Exception:
        pass


async def finalize_and_close_order(
    context: ContextTypes.DEFAULT_TYPE,
    order_id: str,
    final_total: int,
    final_fee: int,
    manual_dist: Optional[float],
    closed_by: str,
):
    order = ORDERS_DB.get(order_id)
    if not order:
        return

    courier_id = order.get("courier_id")

    new_balance = None
    if courier_id:
        db_add_balance(courier_id, -int(final_fee))
        new_balance = db_get_balance(courier_id)
        ACTIVE_ORDERS_COUNT[courier_id] = max(0, ACTIVE_ORDERS_COUNT.get(courier_id, 1) - 1)

    # write closed stats to SQL
    db_insert_closed_order(order, final_total, final_fee)

    # (якщо ще існує) видаляємо повідомлення з диспетчерки при закритті (на випадок force)
    try:
        if order.get("admin_chat_id") and order.get("admin_msg_id"):
            await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
    except Exception:
        pass

    dist_line = f"{manual_dist:.1f} км (вручну)" if manual_dist is not None else f"{order.get('dist_km', 0.0):.1f} км (авто)"

    now_str = datetime.now(TZ).strftime("%H:%M:%S")

    if OWNER_CHAT_ID:
        report = (
            f"🏁 ЗАМОВЛЕННЯ №{order_id} ЗАКРИТО\n"
            f"--------------------------\n"
            f"👤 Клієнт: {order.get('customer_name','-')} (@{order.get('customer_username','-')})\n"
            f"🚚 Кур'єр: {order.get('courier_name','-')} (ID: {courier_id})\n"
            f"🧾 Тип: {'🛒 ВИКУП' if order.get('delivery_mode')=='buy' else '📦 ГОТОВЕ'}\n"
            f"📏 Дистанція: {dist_line}\n"
            f"💰 Каса (клієнт сплатив): {final_total} грн\n"
            f"📈 Комісія: {final_fee} грн\n"
            + (f"💳 Баланс кур'єра: {new_balance} грн\n" if new_balance is not None else "")
            + f"🔒 Закрив: {closed_by}\n"
            + f"🕒 Час: {now_str}\n"
        )
        try:
            await context.bot.send_message(OWNER_CHAT_ID, report)
        except Exception:
            pass

    if courier_id:
        try:
            await context.bot.send_message(
                courier_id,
                f"✅ №{order_id} закрито.\n📉 Списано {final_fee} грн.\n💰 Баланс: {new_balance} грн",
                reply_markup=courier_menu()
            )
        except Exception:
            pass

    # ask rating
    await ask_customer_rating_if_needed(context, order)

    order["status"] = "closed"
    ORDERS_DB.pop(order_id, None)


# =========================
# ===== Final KM input ====
# =========================
async def final_km_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if not is_private_chat(update):
        return

    courier_id = update.effective_user.id
    if courier_id not in FINAL_KM_PENDING:
        return

    order_id = FINAL_KM_PENDING.get(courier_id)
    order = ORDERS_DB.get(order_id)
    if not order:
        FINAL_KM_PENDING.pop(courier_id, None)
        return await update.message.reply_text("Замовлення не знайдено або вже закрите.", reply_markup=courier_menu())

    txt = update.message.text.strip().replace(",", ".")
    try:
        dist_real = float(txt)
        if dist_real <= 0 or dist_real > 300:
            raise ValueError
    except ValueError:
        return await update.message.reply_text("Введи тільки число км (наприклад: 5.7)", reply_markup=courier_menu())

    FINAL_KM_PENDING.pop(courier_id, None)

    fee_rate = float(order.get("fee_rate") or JETWAY_FEE_RATE_DEFAULT)

    total, fee, _, _ = calculate_finance_final(
        order.get("from_addr", ""),
        order.get("to_addr", ""),
        dist_real,
        bool(order.get("is_urgent")),
        fee_rate
    )

        # НЕ закриваємо одразу. Зберігаємо ручну фіналізацію і даємо кнопку "Закрити замовлення"
    order["manual_pending"] = True
    order["manual_total"] = int(total)
    order["manual_fee"] = int(fee)
    order["manual_dist"] = float(dist_real)
    order["status"] = "await_manual_close"

    await update.message.reply_text(
        f"✅ Замовлення №{order_id} фіналізовано.\n"
        f"📏 Фінальний км: {dist_real:.1f}\n"
        f"💰 До сплати: {total} грн\n"
        f"📉 Комісія: {fee} грн\n\n"
        "Скажи клієнту нову суму. Після оплати натисни «✅ Закрити замовлення».",
        reply_markup=kb_close_after_manual(order_id)
    )



# =========================
# ===== Support contact ====
# =========================
async def support_contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private_chat(update):
        return
    if not update.message:
        return

    courier_id = update.effective_user.id
    if courier_id not in SUPPORT_CONTACT_PENDING:
        return

    order_id = SUPPORT_CONTACT_PENDING.pop(courier_id, None)

    phone = "-"
    if update.message.contact and update.message.contact.phone_number:
        phone = update.message.contact.phone_number
    elif update.message.text:
        phone = update.message.text.strip()

    u = update.effective_user
    msg = (
        "🆘 Запит техпідтримки від кур'єра\n\n"
        f"Кур'єр: {u.full_name} (@{u.username or '-'})\n"
        f"ID: {u.id}\n"
        f"Телефон: {phone}\n"
        f"Замовлення: №{order_id}\n"
    )

    if OWNER_CHAT_ID:
        try:
            rm = kb_owner_force_close(order_id) if order_id else None
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=msg,
                reply_markup=rm,
            )
        except Exception:
            await update.message.reply_text("❌ Не вдалося надіслати запит адміну. Перевір OWNER_CHAT_ID/доступ бота.", reply_markup=courier_menu())
            return

    order = ORDERS_DB.get(str(order_id)) if order_id else None
    if order:
        await update.message.reply_text(
            "✅ Запит надіслано адміну. Оберіть дію нижче.",
            reply_markup=support_share_contact_kb(True)
        )
    else:
        await update.message.reply_text("✅ Запит надіслано адміну. Очікуй дзвінок.", reply_markup=courier_menu())



# =========================
# ===== Courier menu router
# =========================

# =========================
# ===== Return order (no fee) confirmation ====
# =========================
async def return_contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Courier sends phone for feedback to request returning an order back to dispatcher (no fee)."""
    if not is_private_chat(update):
        return
    if not update.message:
        return

    courier_id = update.effective_user.id
    pending = context.application.bot_data.get("return_pending", {})
    order_id = pending.get(courier_id)
    if not order_id:
        return

    phone = "-"
    if update.message.contact and update.message.contact.phone_number:
        phone = update.message.contact.phone_number
    elif update.message.text:
        phone = update.message.text.strip()

    # consume pending
    pending.pop(courier_id, None)
    context.application.bot_data["return_pending"] = pending

    order = ORDERS_DB.get(order_id) or {}
    u = update.effective_user

    if not OWNER_CHAT_ID:
        await update.message.reply_text("❌ Не налаштовано OWNER_CHAT_ID.", reply_markup=courier_menu())
        return

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Повернути в диспетчерську (без комісії)", callback_data=f"return:ok:{order_id}:{courier_id}"),
        ],
        [
            InlineKeyboardButton("❌ Відхилити", callback_data=f"return:no:{order_id}:{courier_id}"),
        ]
    ])

    msg = (
        "🔄 **Запит повернення замовлення в диспетчерську (без комісії)**\n\n"
        f"Замовлення: №{order_id}\n"
        f"Курʼєр: {u.full_name} (@{u.username or '-'})\n"
        f"ID: {courier_id}\n"
        f"Телефон для фідбеку: {phone}\n"
        f"Статус: {order.get('status','-')}\n"
    )


    try:
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=msg, reply_markup=kb)
        await update.message.reply_text("✅ Запит відправлено адміну. Очікуйте рішення.", reply_markup=courier_menu())
    except Exception:
        await update.message.reply_text("❌ Не вдалося надіслати запит адміну. Перевір OWNER_CHAT_ID/доступ бота.", reply_markup=courier_menu())


async def courier_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message or not update.message.text:
        return False
    if not is_private_chat(update):
        return False

    uid = update.effective_user.id
    text = update.message.text.strip()

    # Якщо користувач НЕ кур'єр, але натиснув кнопку з кур'єрського меню —
    # не кидаємо його в клієнтське меню "за замовчуванням".
    if uid not in COURIERS:
        courier_buttons = {
            "📦 Мої активні",
            "💳 Мій баланс",
            "⭐ Мій рейтинг",
            "🆔 Мій ID",
            "✅ Доставлено",
            "📸 Надіслати чек (фото)",
            "🆘 Техпідтримка",
            "⬅️ Курєрське меню",
        }
        if text in courier_buttons:
            await update.message.reply_text("❌ Ви не є кур’єром. Якщо треба — зверніться до адміністратора.")
            return True
        return False

    # ✅ Кур'єрські дії
    if text == "🆔 Мій ID":
        await update.message.reply_text(f"🆔 Ваш ID: `{uid}`", parse_mode="Markdown", reply_markup=courier_menu())
        return True

    if text == "💳 Мій баланс":
        bal = db_get_balance(uid)
        await update.message.reply_text(f"💳 Ваш баланс: **{bal} грн**", parse_mode="Markdown", reply_markup=courier_menu())
        return True

    if text == "⭐ Мій рейтинг":
        await show_my_rating(update, context, uid)
        return True

    if text == "✅ Доставлено":
        order = get_single_active_order_for_courier(uid)
        if not order:
            await update.message.reply_text("Оберіть активне замовлення через «📦 Мої активні».", reply_markup=courier_menu())
            return True
        order_id = str(order.get("order_id"))
        order["status"] = "await_customer"
        try:
            if order.get("admin_chat_id") and order.get("admin_msg_id"):
                await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
        except Exception:
            pass
        customer_id = order.get("customer_id")
        try:
            await context.bot.send_message(
                chat_id=customer_id,
                text=(f"🚚 Ваше замовлення №{order_id} доставлено.\nНатисніть кнопку нижче, якщо ви отримали 👇"),
                reply_markup=kb_customer_done(order_id)
            )
        except Exception:
            pass
        await update.message.reply_text(f"✅ Позначено як доставлено: №{order_id}", reply_markup=courier_active_order_menu(bool(order.get("delivery_mode") == "buy")))
        return True

    if text == "📸 Надіслати чек (фото)":
        order = get_single_active_order_for_courier(uid)
        if not order or order.get("delivery_mode") != "buy":
            await update.message.reply_text("Оберіть активне замовлення через «📦 Мої активні».", reply_markup=courier_menu())
            return True
        context.application.bot_data.setdefault("check_pending", {})
        context.application.bot_data["check_pending"][uid] = str(order.get("order_id"))
        await update.message.reply_text("📎 Надішліть чек або інший медіа-файл одним повідомленням (фото / файл / відео).", reply_markup=courier_active_order_menu(True))
        return True

    if text == "🆘 Техпідтримка":
        # загальна техпідтримка з кур'єр меню (не по конкретному замовленню)
        order = get_single_active_order_for_courier(uid)
        SUPPORT_CONTACT_PENDING[uid] = str(order.get("order_id")) if order else None
        await update.message.reply_text(
            "Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.",
            reply_markup=support_share_contact_kb(bool(order))
        )
        return True

    
    if text == "🔄 Закрити замовлення":
        # Повернення активного замовлення в диспетчерську (без списання комісії) через підтвердження owner
        active_ids = [str(oid) for oid, o in ORDERS_DB.items()
                      if o.get("courier_id") == uid and (o.get("status") in ("accepted", "delivered_wait_done") or o.get("manual_pending"))]

        if not active_ids:
            await update.message.reply_text("📭 У вас немає активних замовлень.", reply_markup=courier_menu())
            return True

        # Якщо декілька — даємо вибір
        kb = []
        for oid in active_ids[:10]:
            kb.append([InlineKeyboardButton(f"🔄 Повернути №{oid}", callback_data=f"return:req:{oid}")])

        await update.message.reply_text(
            "🔄 Оберіть замовлення, яке потрібно повернути в диспетчерську.\n"
            "Після цього бот попросить номер телефону для фідбеку — запит піде в owner-групу на підтвердження.",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return True

    if text == "📦 Мої активні":
        active_ids = [str(oid) for oid, o in ORDERS_DB.items()
                      if o.get("courier_id") == uid and (o.get("status") in ("accepted", "delivered_wait_done") or o.get("manual_pending"))]

        if not active_ids:
            await update.message.reply_text("📭 У вас немає активних замовлень.", reply_markup=courier_menu())
            return True

        await update.message.reply_text("Оберіть активне замовлення 👇", reply_markup=courier_menu())
        await update.message.reply_text("👇 Список активних:", reply_markup=kb_courier_active_list(active_ids))
        return True

    if text == "⬅️ Назад в меню":
        order = get_single_active_order_for_courier(uid)
        if order:
            await update.message.reply_text("Повертаю до меню активного замовлення 👇", reply_markup=courier_active_order_menu(bool(order.get("delivery_mode") == "buy")))
        else:
            await update.message.reply_text("Повертаю в меню кур’єра 👇", reply_markup=courier_menu())
        return True

    if text == "↩️ Повернутись до замовлення":
        order = get_any_active_order_for_courier(uid)
        if order:
            await send_courier_active_order_details(context, uid, order)
        else:
            await update.message.reply_text("📭 Активне замовлення не знайдено.", reply_markup=courier_menu())
        return True

    if text == "⬅️ Курєрське меню":
        await update.message.reply_text("Повертаю в меню кур’єра 👇", reply_markup=courier_menu())
        return True

    return False



async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point /start for both clients and owner."""
    try:
        context.user_data.clear()
    except Exception:
        pass

    if not update.message:
        return ConversationHandler.END

    # Owner menu
    if is_owner_chat(update):
        await update.message.reply_text("Меню овнера 👇", reply_markup=owner_quick_kb())
        await update.message.reply_text("Обери роль для тесту або натисни /panel", reply_markup=role_menu())
        return ROLE_CHOICE

    # Private users (clients/couriers)
    if is_private_chat(update):
        await update.message.reply_text(
            f"ℹ️ Доставки працюють {WORK_HOURS_START}–{WORK_HOURS_END}. 🌙 Нічні замовлення не приймаються"
        )
        await update.message.reply_text("Привіт! Хто ти? Обери 👇", reply_markup=role_menu())
        return ROLE_CHOICE

    return ConversationHandler.END


async def role_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "🙋 Я клієнт":
        await update.message.reply_text("Ок! Меню клієнта 👇", reply_markup=main_menu())
        return CHOICE

    if text == "🚚 Я кур'єр":
        uid = update.effective_user.id
        if uid not in COURIERS:
            await update.message.reply_text("❌ Ви не є кур’єром. Якщо треба — зверніться до адміна.", reply_markup=role_menu())
            return ROLE_CHOICE
        if is_blacklisted_courier(uid):
            await update.message.reply_text("⛔ Ви заблоковані і не можете виконувати замовлення. Зверніться до адміністратора.", reply_markup=role_menu())
            return ROLE_CHOICE
        await update.message.reply_text("✅ Ви кур’єр. Меню кур’єра 👇", reply_markup=courier_menu())
        return CHOICE

    await update.message.reply_text("Обери одну з кнопок 👇", reply_markup=role_menu())
    return ROLE_CHOICE


async def choice_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    handled = await courier_menu_router(update, context)
    if handled:
        return CHOICE

    text = (update.message.text or "").strip()

    # ✅ Якщо у клієнта є активне замовлення — показуємо спеціальне меню
    uid = update.effective_user.id
    active_map = context.application.bot_data.get("customer_active_orders", {})
    active_order_id = active_map.get(int(uid))
    if active_order_id:
        if text == "⬅️ Головне меню":
            await update.message.reply_text("Головне меню 👇", reply_markup=main_menu())
            return CHOICE

        if text == "📞 Зв’язатись з кур’єром":
            order = ORDERS_DB.get(str(active_order_id))
            if not order:
                await update.message.reply_text("ℹ️ Активне замовлення не знайдено.", reply_markup=main_menu())
                active_map.pop(int(uid), None)
                context.application.bot_data["customer_active_orders"] = active_map
                return CHOICE

            cu = (order.get("courier_username") or "-").lstrip("@")
            url = tme_url(cu)
            if not url:
                await update.message.reply_text(
                    "ℹ️ У кур’єра немає username в Telegram. Попросіть кур’єра написати вам першим або зверніться в техпідтримку.",
                    reply_markup=customer_active_menu()
                )
                return CHOICE

            await update.message.reply_text(
                "📞 Натисніть кнопку нижче, щоб відкрити чат з кур’єром 👇",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📞 Написати кур’єру", url=url)]])
            )
            return CHOICE

        if text == "🆘 Техпідтримка (замовлення)":
            order = ORDERS_DB.get(str(active_order_id))
            u = update.effective_user
            msg = (
                "🆘 Техпідтримка (клієнт)\n\n"
                f"Замовлення: №{active_order_id}\n"
                f"Клієнт: {u.full_name} (@{u.username or '-'})\n"
                f"ID: {u.id}\\n"
                f"Телефон: {order.get('phone','-') if order else '-'}\n"
            )
            if OWNER_CHAT_ID:
                try:
                    await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=msg)
                except Exception:
                    pass
            await update.message.reply_text("✅ Запит відправлено в техпідтримку. Очікуйте відповідь.", reply_markup=customer_active_menu())
            return CHOICE


    # ✅ ВИКУП: після підтвердження гарантії овнером клієнт надсилає деталі покупки (1 повідомлення)
    buy_pending = context.application.bot_data.get("buy_details_pending", {})
    uid = update.effective_user.id
    if uid in buy_pending:
        order_id = buy_pending.pop(uid)
        context.application.bot_data["buy_details_pending"] = buy_pending
    
        # зберігаємо деталі та показуємо підсумок + кнопку підтвердження
        context.user_data["buy_details"] = text or "-"
        # важливо: переконаємось, що order_id той самий
        context.user_data["pending_order_id"] = order_id
    
        await update.message.reply_text("✅ Деталі покупки збережено.")
        await show_preconfirm_summary(update, context)
        return CONFIRM_ORDER
    
    
    if text == "🚚 Доставка":
        # work hours only for clients
        if (not is_owner_chat(update)) and is_private_chat(update):
            uid = update.effective_user.id
            if uid not in COURIERS and (not is_within_work_hours()):
                await update.message.reply_text(f"Ми працюємо з {WORK_HOURS_START}–{WORK_HOURS_END}", reply_markup=main_menu())
                return CHOICE

        await update.message.reply_text("Ок! Обери режим:", reply_markup=delivery_mode_menu())
        return DELIVERY_MODE

    if text == "💳 Тариф":
        await update.message.reply_text(tariff_text(), reply_markup=main_menu())
        return CHOICE

    if text == "🛠 Підтримка":
        await update.message.reply_text(support_text(), reply_markup=callme_kb())
        return CALLME_PHONE

    await update.message.reply_text("Оберіть пункт з меню 👇", reply_markup=main_menu())
    return CHOICE


async def callme_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    phone_raw = None
    if update.message.contact:
        phone_raw = update.message.contact.phone_number
    elif update.message.text:
        phone_raw = update.message.text.strip()

    if not phone_raw:
        await update.message.reply_text(
            "Надішли номер кнопкою «📞 Зателефонуйте мені» або напиши номер текстом.",
            reply_markup=callme_kb()
        )
        return CALLME_PHONE

    user = update.effective_user
    msg = (
        "📞 Запит на дзвінок (клієнт)\n\n"
        f"👤 {user.full_name} (@{user.username or '-'})\n"
        f"📞 Номер: {phone_raw}\n"
        f"🆔 user_id: {user.id}"
    )
    if OWNER_CHAT_ID:
        try:
            await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=msg)
        except Exception:
            await update.message.reply_text("❌ Не вдалося надіслати запит в owner-чат. Перевір OWNER_CHAT_ID і права бота.", reply_markup=main_menu())
            return CHOICE

    await update.message.reply_text("🙏 Дякую за звернення! Очікуйте дзвінок від оператора.", reply_markup=main_menu())
    return CHOICE


async def delivery_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    if text not in ("🛒 Купити / викуп кур’єром", "📦 Забрати готове (оплачено)"):
        await update.message.reply_text("Обери кнопку 👇", reply_markup=delivery_mode_menu())
        return DELIVERY_MODE

    if "викуп" in text.lower():
        context.user_data["delivery_mode"] = "buy"
    else:
        context.user_data["delivery_mode"] = "pickup"

    await update.message.reply_text("Ок! Обери тип доставки:", reply_markup=delivery_type_menu())
    return DELIV_TYPE


async def delivery_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    if text not in ["⏰ На певний час", "⚡ Термінова"]:
        await update.message.reply_text("Обери тип доставки кнопкою 👇", reply_markup=delivery_type_menu())
        return DELIV_TYPE

    context.user_data["delivery_type_label"] = text
    context.user_data["delivery_type_key"] = "scheduled" if "певний час" in text.lower() else "urgent"

    if context.user_data["delivery_type_key"] == "scheduled":
        await update.message.reply_text(
            "🕒 На котру годину?\n"
            "Напиши дату і час (наприклад: 27.02 14:30 або 27.02.2026 14:30).",
            reply_markup=back_only_kb()
        )
        return WHEN_INPUT

    await update.message.reply_text(addr_prompt_text("Звідки забрати?"), reply_markup=addr_input_kb())
    return FROM_ADDR


async def when_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    when_txt = (update.message.text or "").strip()
    context.user_data["when_temp"] = when_txt
    await update.message.reply_text(f"Підтверди час:\n\n🕒 {when_txt}", reply_markup=when_confirm_kb())
    return WHEN_CONFIRM


async def when_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    if text == "✏️ Змінити час":
        await update.message.reply_text("Ок, введи дату і час ще раз:", reply_markup=back_only_kb())
        return WHEN_INPUT

    if text == "✅ Підтвердити час":
        context.user_data["scheduled_when"] = context.user_data.get("when_temp", "")
        await update.message.reply_text(addr_prompt_text("Звідки забрати?"), reply_markup=addr_input_kb())
        return FROM_ADDR

    await update.message.reply_text("Обери кнопку 👇", reply_markup=when_confirm_kb())
    return WHEN_CONFIRM


async def from_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    # ✅ Гео-точка з телефону
    if update.message and update.message.location:
        loc = update.message.location
        context.user_data["from_coords_temp"] = (float(loc.latitude), float(loc.longitude))
        await update.message.reply_text(
            "✅ Точку (ЗВІДКИ) отримано. Тепер напиши адресу текстом у форматі нижче 👇",
            reply_markup=addr_input_kb()
        )
        return FROM_ADDR

    addr = ((update.message.text or "") if update.message else "").strip()

    # ✅ Альтернатива для Telegram Desktop: координати або лінк Google Maps
    coords = extract_coords_from_text(addr)
    if coords:
        context.user_data["from_coords_temp"] = coords
        if not has_locality(addr):
            await update.message.reply_text(
                "✅ Точку (ЗВІДКИ) отримано з посилання/координат. Тепер напиши адресу текстом у форматі нижче 👇",
                reply_markup=addr_input_kb()
            )
            return FROM_ADDR

    if not has_locality(addr):
        await update.message.reply_text(locality_hint(), reply_markup=addr_input_kb())
        return FROM_ADDR

    context.user_data["from_addr_temp"] = addr
    await update.message.reply_text(f"Підтверди адресу звідки:\n\n📍 {addr}", reply_markup=addr_confirm_kb())
    return CONFIRM_FROM


async def confirm_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    if text == "✏️ Змінити адресу":
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу звідки ще раз:"), reply_markup=addr_input_kb())
        return FROM_ADDR

    if text == "✅ Підтвердити адресу":
        context.user_data["from_addr"] = context.user_data.get("from_addr_temp", "")
        if context.user_data.get("from_coords_temp"):
            context.user_data["from_coords"] = context.user_data.get("from_coords_temp")

        if context.user_data.get("edit_mode") == "from":
            context.user_data.pop("edit_mode", None)
            await show_preconfirm_summary(update, context)
            return CONFIRM_ORDER

        await update.message.reply_text(addr_prompt_text("Куди доставити?"), reply_markup=addr_input_kb())
        return TO_ADDR

    await update.message.reply_text("Обери кнопку 👇", reply_markup=addr_confirm_kb())
    return CONFIRM_FROM


async def to_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    # ✅ Гео-точка з телефону
    if update.message and update.message.location:
        loc = update.message.location
        context.user_data["to_coords_temp"] = (float(loc.latitude), float(loc.longitude))
        await update.message.reply_text(
            "✅ Точку (КУДИ) отримано. Тепер напиши адресу текстом у форматі нижче 👇",
            reply_markup=addr_input_kb()
        )
        return TO_ADDR

    addr = ((update.message.text or "") if update.message else "").strip()

    # ✅ Альтернатива для Telegram Desktop: координати або лінк Google Maps
    coords = extract_coords_from_text(addr)
    if coords:
        context.user_data["to_coords_temp"] = coords
        if not has_locality(addr):
            await update.message.reply_text(
                "✅ Точку (КУДИ) отримано з посилання/координат. Тепер напиши адресу текстом у форматі нижче 👇",
                reply_markup=addr_input_kb()
            )
            return TO_ADDR

    if not has_locality(addr):
        await update.message.reply_text(locality_hint(), reply_markup=addr_input_kb())
        return TO_ADDR

    context.user_data["to_addr_temp"] = addr
    await update.message.reply_text(f"Підтверди адресу куди:\n\n🎯 {addr}", reply_markup=addr_confirm_kb())
    return CONFIRM_TO


async def confirm_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    if text == "✏️ Змінити адресу":
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу куди ще раз:"), reply_markup=addr_input_kb())
        return TO_ADDR

    if text == "✅ Підтвердити адресу":
        context.user_data["to_addr"] = context.user_data.get("to_addr_temp", "")
        if context.user_data.get("to_coords_temp"):
            context.user_data["to_coords"] = context.user_data.get("to_coords_temp")

        if context.user_data.get("edit_mode") == "to":
            context.user_data.pop("edit_mode", None)
            await show_preconfirm_summary(update, context)
            return CONFIRM_ORDER

        # branching by mode
        if context.user_data.get("delivery_mode") == "buy":
            await update.message.reply_text("🧾 Напишіть список того, що треба купити (1 повідомлення).", reply_markup=back_only_kb())
            return BUY_LIST

        await update.message.reply_text("Що веземо? (коротко: пакунок/їжа/документи)", reply_markup=back_only_kb())
        return ITEM

    await update.message.reply_text("Обери кнопку 👇", reply_markup=addr_confirm_kb())
    return CONFIRM_TO


async def buy_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    context.user_data["buy_list"] = (update.message.text or "").strip()
    await update.message.reply_text("💰 Вкажіть приблизну суму покупок (числом, грн). Напр: 450", reply_markup=back_only_kb())
    return BUY_APPROX_SUM


async def buy_approx_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    approx = _money_int_from_text(update.message.text or "")
    if approx is None or approx <= 0:
        await update.message.reply_text("Вкажіть суму числом (грн). Напр: 450", reply_markup=back_only_kb())
        return BUY_APPROX_SUM

    context.user_data["buy_approx_sum"] = int(approx)
    g = calc_guarantee(int(approx))
    context.user_data["guarantee_amount"] = g

    # create temp order_id now for guarantee reference
    order_id = gen_order_id()
    context.user_data["pending_order_id"] = order_id

    pay_text = (
        f"Для викупу потрібна гарантія: **{g} грн**.\n\n"
        "⚠️ **Обов’язково вкажіть коментар/призначення платежу**:\n"
        f"`ГАРАНТІЯ №{order_id}`\n\n"
        f"💳 Оплата **{PAYMENT_RECEIVER_NAME}**\n"
        f"Картка: `{PAYMENT_CARD}`\n\n"
        "Після оплати натисніть кнопку нижче 👇"
    )
    await update.message.reply_text(pay_text, parse_mode="Markdown", reply_markup=kb_customer_paid(order_id))

    # timer: if not clicked "I paid" -> auto cancel
    schedule_once(context, job_autocancel_if_not_clicked_paid, delay_sec=15 * 60, data={"customer_id": update.effective_user.id, "order_id": order_id},
        name=f"autocancel:{order_id}")
    # reminder 5 min before
    remind_sec = max(0, (15 - REMINDER_BEFORE_CANCEL_MIN) * 60)
    schedule_once(context, 
        job_remind_before_autocancel,
        delay_sec=remind_sec,
        data={"customer_id": update.effective_user.id, "order_id": order_id},
        name=f"remind:{order_id}"
    )

    return BUY_WAIT_PAID_CLICK


async def job_remind_before_autocancel(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    customer_id = data.get("customer_id")
    order_id = data.get("order_id")
    # if still pending and not clicked
    if not customer_id or not order_id:
        return
    # we store flag in application.bot_data
    pending = context.application.bot_data.get("guarantee_pending", {})
    st = pending.get(order_id, {})
    if st.get("status") == "await_paid_click":
        try:
            await context.bot.send_message(
                customer_id,
                f"⏳ Нагадування: якщо ви не натиснете «✅ Я оплатив гарантію» — замовлення №{order_id} буде скасовано автоматично."
            )
        except Exception:
            pass


async def job_autocancel_if_not_clicked_paid(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    customer_id = data.get("customer_id")
    order_id = data.get("order_id")
    if not customer_id or not order_id:
        return

    pending = context.application.bot_data.get("guarantee_pending", {})
    st = pending.get(order_id, {})
    if st.get("status") == "await_paid_click":
        pending.pop(order_id, None)
        context.application.bot_data["guarantee_pending"] = pending
        try:
            await context.bot.send_message(customer_id, f"❌ Замовлення №{order_id} скасовано (не підтверджено оплату гарантії).")
        except Exception:
            pass


async def item(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    context.user_data["item"] = (update.message.text or "").strip()
    await update.message.reply_text("Телефон для звʼязку:", reply_markup=phone_kb())
    return PHONE


async def phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    if update.message.contact:
        context.user_data["phone"] = update.message.contact.phone_number
    else:
        txt = (update.message.text or "").strip()
        context.user_data["phone"] = "" if txt.lower().startswith("пропуст") else txt

    await update.message.reply_text(
        "Коментар (підʼїзд/поверх/номер замовлення/час/оплата). Якщо нема — напиши «-»",
        reply_markup=back_only_kb()
    )
    return COMMENT


async def comment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    context.user_data["comment"] = ((update.message.text or "").strip() or "-")
    await show_preconfirm_summary(update, context)
    return CONFIRM_ORDER


async def show_preconfirm_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data
    from_addr_v = data.get("from_addr", "")
    to_addr_v = data.get("to_addr", "")
    is_urgent = data.get("delivery_type_key") == "urgent"

    session: aiohttp.ClientSession = context.application.bot_data["http"]

    from_coords = data.get("from_coords")
    to_coords = data.get("to_coords")

    # якщо клієнт не ділився точкою — геокодуємо адресу
    if not from_coords:
        from_coords = await geocode_address_google(from_addr_v, session)
    if not to_coords:
        to_coords = await geocode_address_google(to_addr_v, session)
    dist_km = get_distance_km(from_coords, to_coords)

    fee_rate = JETWAY_FEE_RATE_DEFAULT
    total, fee, cut, outside, safe_dist = calculate_finance_auto(from_addr_v, to_addr_v, dist_km, is_urgent, fee_rate)

    data["from_coords"] = from_coords
    data["to_coords"] = to_coords
    data["dist_km"] = dist_km
    data["safe_dist_km"] = safe_dist
    data["total_price"] = total
    data["fee"] = fee
    data["courier_cut"] = cut
    data["is_outside"] = outside
    data["fee_rate"] = fee_rate

    outside_text = "🏘️ Поза містом" if outside else "🏢 По місту"
    when_line = data.get("scheduled_when", "терміново")

    # summary differs for buy
    if data.get("delivery_mode") == "buy":
        summary = (
            "🧾 **Ваша заявка на ВИКУП**\n\n"
            f"🚚 Тип: {data.get('delivery_type_label','-')}\n"
            f"🕒 Час: {when_line}\n"
            f"📍 Звідки: {from_addr_v}\n"
            f"🎯 Куди: {to_addr_v}\n"
            f"🧾 Список: {data.get('buy_list','-')}\n"
            f"💰 Приблизна сума покупок: {data.get('buy_approx_sum','-')} грн\n"
            f"🔒 Гарантія: {data.get('guarantee_amount','-')} грн\n"
            f"📞 Тел: {data.get('phone') or 'не вказано'}\n"
            f"💬 Коментар: {data.get('comment','-')}\n\n"
            "------------------------\n"
            f"{outside_text}\n"
            f"📏 Приблизна відстань: {dist_km:.1f} км\n"
            f"💰 **Приблизна вартість доставки: {total} грн**\n"
            "------------------------\n"
            "ℹ️ Фінальна ціна може трохи відрізнятись залежно від маршруту.\n\n"
            "Якщо все вірно — натисніть «✅ Підтвердити» 👇"
        )
    else:
        summary = (
            "🧾 **Ваша заявка на доставку**\n\n"
            f"🚚 Тип: {data.get('delivery_type_label','-')}\n"
            f"🕒 Час: {when_line}\n"
            f"📍 Звідки: {from_addr_v}\n"
            f"🎯 Куди: {to_addr_v}\n"
            f"📦 Що: {data.get('item','-')}\n"
            f"📞 Тел: {data.get('phone') or 'не вказано'}\n"
            f"💬 Коментар: {data.get('comment','-')}\n\n"
            "------------------------\n"
            f"{outside_text}\n"
            f"📏 Приблизна відстань: {dist_km:.1f} км\n"
            f"💰 **Приблизна вартість доставки: {total} грн**\n"
            "------------------------\n"
            "ℹ️ Фінальна ціна може трохи відрізнятись залежно від маршруту.\n\n"
            "Якщо все вірно — натисніть «✅ Підтвердити» 👇"
        )

    await update.message.reply_text(summary, reply_markup=order_confirm_kb(), parse_mode="Markdown")
    # === PART 2/2 START ===

async def confirm_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text in ("⬅️ Назад в меню", "❌ Скасувати"):
        await update.message.reply_text("Скасовано. Меню 👇", reply_markup=main_menu())
        context.user_data.clear()
        return CHOICE

    if text == "📍 Змінити звідки":
        context.user_data["edit_mode"] = "from"
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу звідки ще раз:"), reply_markup=addr_input_kb())
        return FROM_ADDR

    if text == "🎯 Змінити куди":
        context.user_data["edit_mode"] = "to"
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу куди ще раз:"), reply_markup=addr_input_kb())
        return TO_ADDR

    if text == "💬 Змінити коментар":
        context.user_data["edit_mode"] = "comment"
        await update.message.reply_text("Напиши новий коментар або «-»", reply_markup=back_only_kb())
        return COMMENT

    if text != "✅ Підтвердити":
        await update.message.reply_text("Обери кнопку 👇", reply_markup=order_confirm_kb())
        return CONFIRM_ORDER

    data = context.user_data
    user = update.effective_user
    delivery_mode_v = data.get("delivery_mode", "pickup")  # pickup/buy

    # For BUY we require that guarantee was confirmed by owner before allow submit
    if delivery_mode_v == "buy":
        # customer must have guarantee confirmed in bot_data
        pending = context.application.bot_data.get("guarantee_pending", {})
        oid = data.get("pending_order_id")
        st = pending.get(oid, {})
        if st.get("status") != "confirmed":
            await update.message.reply_text(
                "⏳ Спочатку дочекайтесь підтвердження гарантії від оператора.\n"
                "Якщо ви вже оплатили — натисніть «✅ Я оплатив гарантію» в повідомленні з оплатою.",
                reply_markup=main_menu()
            )
            return CHOICE

    order_id = data.get("pending_order_id") or gen_order_id()

    from_addr_v = data.get("from_addr", "")
    to_addr_v = data.get("to_addr", "")
    from_coords = data.get("from_coords")
    to_coords = data.get("to_coords")
    dist_km = float(data.get("dist_km") or 0.0)

    is_urgent = data.get("delivery_type_key") == "urgent"
    fee_rate = float(data.get("fee_rate") or JETWAY_FEE_RATE_DEFAULT)

    total = int(data.get("total_price") or 0)
    fee = int(data.get("fee") or 0)
    outside = bool(data.get("is_outside"))

    when_line = data.get("scheduled_when", "терміново")
    outside_text = "🏘️ Поза містом" if outside else "🏢 По місту"

    # fill order base
    ORDERS_DB[order_id] = {
        "order_id": order_id,
        "status": "searching",
        "delivery_mode": delivery_mode_v,
        "delivery_label": data.get("delivery_type_label", "-"),
        "delivery_key": data.get("delivery_type_key", "-"),
        "scheduled_when": when_line,

        "customer_id": user.id,
        "customer_name": user.full_name,
        "customer_username": user.username or "-",
        "customer_chat_id": update.effective_chat.id,

        "from_addr": from_addr_v,
        "to_addr": to_addr_v,
        "from_coords": from_coords,
        "to_coords": to_coords,
        "dist_km": dist_km,

        "total": total,
        "fee": fee,
        "fee_rate": fee_rate,

        "outside": outside,

        "phone": data.get("phone") or "не вказано",
        "comment": data.get("comment", "-"),

        # pickup fields
        "item": data.get("item", "-"),

        # buy fields
        "buy_list": data.get("buy_list", "-"),
        "buy_approx_sum": int(data.get("buy_approx_sum") or 0),
        "guarantee_amount": int(data.get("guarantee_amount") or 0),
        "buy_details": data.get("buy_details", "-"),

        "courier_id": None,
        "courier_name": None,
        "admin_chat_id": dispatcher_chat_id(),
        "admin_msg_id": None,
        "is_urgent": is_urgent,

        # ✅ для ручної фіналізації (після вводу км)
        "manual_pending": False,
        "manual_total": None,
        "manual_fee": None,
        "manual_dist": None,

        # ✅ юзернейм кур’єра (заповнюється при прийнятті)
        "courier_username": None,
    }

    # message to dispatcher
    if delivery_mode_v == "buy":
        title = f"🟡 **ВИКУП • ПОТРІБЕН ЧЕК** №{order_id}"
        body = (
            f"{outside_text}\n"
            f"🕒 Час: {when_line}\n"
            f"📍 Звідки: {from_addr_v}\n"
            f"🗺️ Лінк звідки: {fmt_link(from_coords)}\n"
            f"📍 Гео (ЗВІДКИ): {fmt_link(from_coords)}\n"
            f"🎯 Куди: {to_addr_v}\n"
            f"🗺️ Лінк куди: {fmt_link(to_coords)}\n"
            f"📍 Гео (КУДИ): {fmt_link(to_coords)}\n"
            f"🧾 Список: {ORDERS_DB[order_id]['buy_list']}\n"
            f"💰 Орієнт. сума покупок: {ORDERS_DB[order_id]['buy_approx_sum']} грн\n"
            f"🔒 Гарантія: {ORDERS_DB[order_id]['guarantee_amount']} грн\n"
            f"📞 Телефон клієнта: {ORDERS_DB[order_id]['phone']}\n"
            f"💬 Коментар: {ORDERS_DB[order_id]['comment']}\n"
            "------------------------\n"
            f"💰 Доставка (авто): {total} грн\n"
        )
    else:
        title = f"🚚 **НОВЕ ЗАМОВЛЕННЯ №{order_id}**"
        body = (
            f"{outside_text}\n"
            f"🕒 Час: {when_line}\n"
            f"📍 Звідки: {from_addr_v}\n"
            f"🧭 Коорд. звідки: {fmt_coords(from_coords)}\n"
            f"🗺️ Лінк звідки: {fmt_link(from_coords)}\n"
            f"📍 Гео (ЗВІДКИ): {fmt_link(from_coords)}\n"
            f"🎯 Куди: {to_addr_v}\n"
            f"🧭 Коорд. куди: {fmt_coords(to_coords)}\n"
            f"🗺️ Лінк куди: {fmt_link(to_coords)}\n"
            f"📍 Гео (КУДИ): {fmt_link(to_coords)}\n"
            f"📦 Що: {data.get('item','-')}\n"
            f"📞 Тел: {data.get('phone') or 'не вказано'}\n"
            f"💬 Коментар: {data.get('comment','-')}\n"
            "------------------------\n"
            f"💰 До сплати клієнтом: {total} грн\n"
        )

    msg_to_couriers = f"{title}\n{body}"

    dispatcher_chat = ORDERS_DB[order_id]["admin_chat_id"]
    if dispatcher_chat:
        sent = await context.bot.send_message(
            chat_id=dispatcher_chat,
            text=msg_to_couriers,
            reply_markup=kb_accept(order_id),
            parse_mode="Markdown"
        )
        ORDERS_DB[order_id]["admin_msg_id"] = sent.message_id

    if OWNER_CHAT_ID:
        try:
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=(
                    f"📥 Нове замовлення №{order_id}\n"
                    f"Тип: {'🛒 ВИКУП' if delivery_mode_v=='buy' else '📦 ГОТОВЕ'}\n"
                    f"Клієнт: {user.full_name} (@{user.username or '-'})\n"
                    f"Сума (авто): {total} грн | Комісія: {fee} грн"
                ),
                reply_markup=kb_owner_force_close(order_id)
            )
        except Exception:
            pass

    await update.message.reply_text(
        "✅ Ваше замовлення відправлено кур’єрам. Очікуйте підтвердження.",
        reply_markup=main_menu()
    )
    context.user_data.clear()
    return CHOICE



# =========================
# ===== No-lambda handler ==
# =========================
async def buy_wait_paid_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Стан BUY_WAIT_PAID_CLICK: очікуємо тільки натискання inline-кнопок ("Я оплатив"/"Скасувати").
    # Якщо користувач щось пише — просто повертаємо його в меню.
    if update.message:
        try:
            await update.message.reply_text("Ок 👍", reply_markup=main_menu())
        except Exception:
            pass
    return CHOICE

# =========================
# ===== Rating via text ====
# =========================
async def rating_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Accept rating as plain text digit 1..5 after close.
    We'll store in bot_data["rating_pending"][customer_id] = (order_id, courier_id)
    """
    if not update.message or not update.message.text:
        return
    if not is_private_chat(update):
        return

    txt = update.message.text.strip()
    if txt not in ("1", "2", "3", "4", "5"):
        return

    pending = context.application.bot_data.get("rating_pending", {})
    key = update.effective_user.id
    if key not in pending:
        return

    order_id, courier_id = pending.pop(key)
    context.application.bot_data["rating_pending"] = pending

    ok = db_try_add_rating(order_id, int(courier_id), int(key), int(txt))
    if not ok:
        await update.message.reply_text("ℹ️ Оцінка вже була збережена або сталася помилка.")
        return

    avg_all, cnt_all = db_get_courier_rating(int(courier_id), None)
    if OWNER_CHAT_ID:
        try:
            await context.bot.send_message(
                OWNER_CHAT_ID,
                text=(
                    "⭐ Новий відгук\n"
                    f"Замовлення №{order_id}\n"
                    f"Кур’єр: `{courier_id}`\n"
                    f"Оцінка: {txt}\n"
                    f"Рейтинг кур’єра (all): {avg_all:.2f} ({cnt_all})"
                ),
                parse_mode="Markdown"
            )
        except Exception:
            pass
    await update.message.reply_text("✅ Дякуємо за оцінку!")


# =========================
# ===== Callback router ===
# =========================
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = (query.data or "").strip()

    # owner panel
    if data.startswith(("stats:", "export:", "ratings:", "checks:", "closed:", "active:", "owner:", "bl:", "wh:")):
        return await owner_panel_callback(update, context)

    # return order to dispatcher (courier request + owner confirmation)
    if data.startswith("return:req:"):
        await query.answer()
        order_id = data.split(":", 2)[2]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)
        if query.from_user.id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        # mark pending: courier must send phone/contact
        context.application.bot_data.setdefault("return_pending", {})
        context.application.bot_data["return_pending"][query.from_user.id] = order_id

        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="📞 Надішліть номер телефону для фідбеку (кнопкою «Поділитися контактом» або просто текстом).",
            reply_markup=support_share_contact_kb(),
        )
        return

    if data.startswith("return:ok:") or data.startswith("return:no:"):
        await query.answer()
        parts = data.split(":")
        action = parts[1]  # ok/no
        order_id = parts[2] if len(parts) > 2 else ""
        courier_id = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None

        # only owner can confirm
        if query.message.chat.id != OWNER_CHAT_ID:
            return await query.answer("Недостатньо прав.", show_alert=True)

        order = ORDERS_DB.get(order_id)
        if not order:
            await context.bot.send_message(OWNER_CHAT_ID, f"⚠️ Замовлення №{order_id} не знайдено.")
            return

        if action == "no":
            # deny
            if courier_id:
                try:
                    await context.bot.send_message(courier_id, f"❌ Запит на повернення замовлення №{order_id} відхилено адміністратором.")
                except Exception:
                    pass
            await query.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(OWNER_CHAT_ID, f"❌ Відхилено повернення №{order_id}.")
            return

        # approve: unassign courier and re-post to dispatcher
        old_courier_id = order.get("courier_id")
        order["courier_id"] = None
        order["courier_name"] = None
        order["courier_username"] = None
        order["status"] = "searching"
        order["manual_pending"] = False  # якщо було — скидаємо, щоб інший курʼєр брав з нуля

        # notify courier
        if old_courier_id:
            try:
                await context.bot.send_message(old_courier_id, f"✅ Замовлення №{order_id} повернуто в диспетчерську. Комісія не списана.", reply_markup=courier_menu())
            except Exception:
                pass

        # repost to dispatcher chat
        try:
            await repost_order_to_dispatcher(context, order_id)
        except Exception:
            pass

        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(OWNER_CHAT_ID, f"✅ Замовлення №{order_id} повернуто в диспетчерську (без списання комісії).")
        return



    # guarantee payment flow
    if data.startswith("gpaid:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        customer_id = query.from_user.id

        pending = context.application.bot_data.get("guarantee_pending", {})
        st = pending.get(order_id)
        if not st:
            pending[order_id] = {"status": "await_owner_confirm", "customer_id": customer_id}
        else:
            # already clicked; keep status
            st["customer_id"] = customer_id
            if st.get("status") == "confirmed":
                await context.bot.send_message(customer_id, "✅ Оплату вже підтверджено. Можете продовжувати оформлення.")
                return
            st["status"] = "await_owner_confirm"

        context.application.bot_data["guarantee_pending"] = pending

        # notify owner to confirm
        if OWNER_CHAT_ID:
            owner_text = (
                "💳 Клієнт натиснув «Я оплатив гарантію»\\n"
                f"Замовлення №{order_id}\\n"
                f"User: {query.from_user.full_name} (@{query.from_user.username or '-'})\\n"
                f"ID: {customer_id}\\n"
                f"Коментар платежу має бути: ГАРАНТІЯ №{order_id}"
            )
            try:
                await context.bot.send_message(
                    OWNER_CHAT_ID,
                    owner_text,
                    reply_markup=kb_owner_payment_confirm(order_id),
                )
            except Exception:
                # Do not crash the callback on owner send errors
                try:
                    await context.bot.send_message(
                        customer_id,
                        "⚠️ Не вдалося надіслати запит адміну. Перевірте, що бот має доступ до owner-групи, і спробуйте ще раз.",
                    )
                except Exception:
                    pass
                return

        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(customer_id, "⏳ Зачекайте, підтверджуємо оплату гарантії…")
        return

    if data.startswith("gcancel:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        pending = context.application.bot_data.get("guarantee_pending", {})
        pending.pop(order_id, None)
        context.application.bot_data["guarantee_pending"] = pending
        try:
            await query.edit_message_text(f"❌ Замовлення №{order_id} скасовано.")
        except Exception:
            pass
        return

    if data.startswith("gpay_ok:"):
        await query.answer()
        if query.message.chat.id != OWNER_CHAT_ID:
            return await query.answer("Недостатньо прав.", show_alert=True)

        order_id = data.split(":", 1)[1]
        pending = context.application.bot_data.get("guarantee_pending", {})
        st = pending.get(order_id)
        if not st:
            return await query.answer("Немає очікування по цьому замовленню.", show_alert=True)

        st["status"] = "confirmed"
        pending[order_id] = st
        context.application.bot_data["guarantee_pending"] = pending

        customer_id = st.get("customer_id")
        if customer_id:
            try:
                await context.bot.send_message(
                    customer_id,
                    "✅ Оплату гарантії підтверджено.\n"
                    "Тепер заповніть деталі покупки/магазин/альтернативи у коментарі і підтвердіть замовлення (якщо ще не підтвердили)."
                )
            except Exception:
                pass

            # дозволяємо клієнту одним повідомленням надіслати деталі покупки після підтвердження гарантії
            buy_pending = context.application.bot_data.get("buy_details_pending", {})
            buy_pending[int(customer_id)] = str(order_id)
            context.application.bot_data["buy_details_pending"] = buy_pending

        try:
            await query.edit_message_text(f"✅ Гарантію по №{order_id} підтверджено.")
        except Exception:
            pass
        return

    if data.startswith("gpay_no:"):
        await query.answer()
        if query.message.chat.id != OWNER_CHAT_ID:
            return await query.answer("Недостатньо прав.", show_alert=True)

        order_id = data.split(":", 1)[1]
        pending = context.application.bot_data.get("guarantee_pending", {})
        st = pending.pop(order_id, None)
        context.application.bot_data["guarantee_pending"] = pending

        if st and st.get("customer_id"):
            try:
                await context.bot.send_message(st["customer_id"], f"❌ Оплату гарантії по №{order_id} не підтверджено. Замовлення скасовано.")
            except Exception:
                pass

            # дозволяємо клієнту одним повідомленням надіслати деталі покупки після підтвердження гарантії
            buy_pending = context.application.bot_data.get("buy_details_pending", {})
            buy_pending[int(customer_id)] = str(order_id)
            context.application.bot_data["buy_details_pending"] = buy_pending

        try:
            await query.edit_message_text(f"❌ Відхилено. №{order_id} скасовано.")
        except Exception:
            pass
        return

    # rate inline
    if data.startswith("rate:"):
        await query.answer()
        _, order_id, courier_id, r = data.split(":")
        customer_id = query.from_user.id
        if r not in ("1", "2", "3", "4", "5"):
            return

        ok = db_try_add_rating(order_id, int(courier_id), int(customer_id), int(r))
        if ok:
            pending = context.application.bot_data.get("rating_pending", {})
            pending.pop(int(customer_id), None)
            context.application.bot_data["rating_pending"] = pending
        if not ok:
            return await query.answer("Оцінку вже враховано.", show_alert=True)

        avg_all, cnt_all = db_get_courier_rating(int(courier_id), None)
        if OWNER_CHAT_ID:
            await context.bot.send_message(
                OWNER_CHAT_ID,
                f"⭐ Новий відгук\n"
                f"Замовлення №{order_id}\n"
                f"Кур’єр: `{courier_id}`\n"
                f"Оцінка: {r}\n"
                f"Рейтинг кур’єра (all): {avg_all:.2f} ({cnt_all})",
                parse_mode="Markdown"
            )
        try:
            await query.edit_message_text("✅ Дякуємо за оцінку!")
        except Exception:
            pass
        return

    # force close by owner
    if data.startswith("force:"):
        await query.answer()
        if query.message.chat.id != OWNER_CHAT_ID:
            return await query.answer("Недостатньо прав.", show_alert=True)

        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        final_total = int(order.get("total") or 0)
        final_fee = int(order.get("fee") or 0)

        await finalize_and_close_order(context, order_id, final_total, final_fee, manual_dist=None, closed_by="овнер (force)")
        try:
            await query.edit_message_text(f"🛠 Замовлення №{order_id} закрито вручну.")
        except Exception:
            pass
        return

    # support pick
    if data.startswith("support_pick:"):
        await query.answer()
        val = data.split(":", 1)[1]
        if val == "cancel":
            try:
                await query.edit_message_text("Скасовано ✅")
            except Exception as e:
                print(e)
                pass
            return

        order_id = val
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        courier_id = query.from_user.id
        if courier_id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        SUPPORT_CONTACT_PENDING[courier_id] = order_id
        await context.bot.send_message(
            chat_id=courier_id,
            text="Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.",
            reply_markup=support_share_contact_kb(True)
        )
        try:
            await query.edit_message_text(f"🆘 Запит по №{order_id} створено. Надішліть номер в особисті боту.")
        except Exception as e:
            print(e)
            pass
        return


    # open order from courier menu
    if data.startswith("courier_open:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        courier_id = query.from_user.id
        if courier_id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        await send_courier_active_order_details(context, courier_id, order)
        return

    # accept
    if data.startswith("accept:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено або вже закрите.", show_alert=True)

        courier_id = query.from_user.id
        if courier_id not in COURIERS:
            return await query.answer("❌ Ви не додані як кур'єр. Напишіть адміну.", show_alert=True)

        if order.get("courier_id") is not None:
            return await query.answer("Це замовлення вже забрав інший кур'єр.", show_alert=True)

        if ACTIVE_ORDERS_COUNT.get(courier_id, 0) >= 3:
            return await query.answer("❌ У вас вже 3 активних замовлення!", show_alert=True)

        balance_now = db_get_balance(courier_id)
        expected_fee = int(order.get("fee") or 0)
        future_balance = balance_now - expected_fee
        if future_balance < MIN_NEGATIVE_BALANCE:
            return await query.answer(
                f"❌ Після цього замовлення ваш баланс стане {future_balance} грн.\n"
                f"Мінімально дозволено: {MIN_NEGATIVE_BALANCE} грн.\n"
                f"Поповніть баланс.",
                show_alert=True
            )

        order["courier_id"] = courier_id
        order["courier_name"] = query.from_user.full_name
        order["courier_username"] = query.from_user.username or "-"
        order["courier_username"] = query.from_user.username or "-"
        order["status"] = "accepted"

        # ✅ позначаємо активне замовлення у клієнта (щоб показати меню зв'язку/підтримки)
        ca = context.application.bot_data.get("customer_active_orders", {})
        ca[int(order.get("customer_id") or 0)] = str(order_id)
        context.application.bot_data["customer_active_orders"] = ca


        ACTIVE_ORDERS_COUNT[courier_id] = ACTIVE_ORDERS_COUNT.get(courier_id, 0) + 1

        # edit dispatcher msg
        try:
            await context.bot.edit_message_text(
                chat_id=order["admin_chat_id"],
                message_id=order["admin_msg_id"],
                text=(query.message.text + f"\n\n✅ Прийняв кур'єр: {query.from_user.full_name}"),
                parse_mode="Markdown",
                reply_markup=None
            )
        except Exception:
            pass

        # send courier controls
        try:
            if order.get("delivery_mode") == "buy":
                kb = kb_courier_controls_buy(order_id)
                info = (
                    f"✅ Ви взяли замовлення №{order_id}\n"
                    f"🟡 ВИКУП • ПОТРІБЕН ЧЕК\n"
                    f"📍 {order.get('from_addr','-')}\n"
                    f"🎯 {order.get('to_addr','-')}\n"
                    f"🧾 Список: {order.get('buy_list','-')}\n"
                    f"🔒 Гарантія: {order.get('guarantee_amount','-')} грн\n"
                    f"📞 Телефон клієнта: {order.get('phone','-')}\n"
                )
            else:
                kb = kb_courier_controls_pickup(order_id)
                info = (
                    f"✅ Ви взяли замовлення №{order_id}\n\n"
                    "Коли доставите — натисніть «✅ Доставлено».\n"
                    "Якщо проблема — тисніть «🆘 Техпідтримка»."
                )

            await context.bot.send_message(
                chat_id=courier_id,
                text=info,
                reply_markup=courier_active_order_menu(bool(order.get("delivery_mode") == "buy"))
            )

            # ✅ окремим повідомленням даємо кнопку "Написати клієнту"
            try:
                kb_contact = kb_contact_customer(order_id, int(order.get("customer_id") or 0), str(order.get("customer_username") or "-"))
                if kb_contact:
                    await context.bot.send_message(
                        chat_id=courier_id,
                        text="📞 Зв’язок з клієнтом:",
                        reply_markup=kb_contact
                    )
                else:
                    await context.bot.send_message(
                        chat_id=courier_id,
                        text="📞 Зв’язок з клієнтом: у клієнта немає username в Telegram.",
                    )
            except Exception:
                pass
    
    
        except Exception:
            pass


        # ✅ повідомляємо клієнта, що кур'єр взяв замовлення, і показуємо меню клієнта
        customer_id = int(order.get("customer_id") or 0)
        if customer_id:
            try:
                await context.bot.send_message(
                    chat_id=customer_id,
                    text=(
                        f"✅ Кур’єр взяв ваше замовлення №{order_id}.\n"
                        "Тепер ви можете зв’язатись з кур’єром або написати в техпідтримку через меню нижче 👇"
                    ),
                    reply_markup=customer_active_menu()
                )
            except Exception:
                pass

        return

    # send check (buy flow)
    if data.startswith("sendcheck:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)
        if query.from_user.id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        # set pending in user_data for courier
        context.application.bot_data.setdefault("check_pending", {})
        context.application.bot_data["check_pending"][query.from_user.id] = order_id
        await context.bot.send_message(query.from_user.id, "📎 Надішліть чек або інший медіа-файл одним повідомленням (фото / файл / відео).")
        return

    # delivered
    if data.startswith("delivered:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        courier_id = query.from_user.id
        if courier_id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        order["status"] = "await_customer"

        # delete dispatcher msg
        try:
            if order.get("admin_chat_id") and order.get("admin_msg_id"):
                await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
        except Exception:
            pass

        # customer confirm receive
        customer_id = order.get("customer_id")
        try:
            await context.bot.send_message(
                chat_id=customer_id,
                text=(f"🚚 Ваше замовлення №{order_id} доставлено.\nНатисніть кнопку нижче, якщо ви отримали 👇"),
                reply_markup=kb_customer_done(order_id)
            )
        except Exception:
            pass

        try:
            await query.edit_message_text(f"✅ Позначено як доставлено: №{order_id}")
        except Exception:
            pass
        return

    # support
    if data.startswith("support:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        courier_id = query.from_user.id
        if courier_id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        SUPPORT_CONTACT_PENDING[courier_id] = order_id
        await context.bot.send_message(
            chat_id=courier_id,
            text="Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.",
            reply_markup=support_share_contact_kb(True)
        )
        return

    # customer done
    if data.startswith("done:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено або вже закрите.", show_alert=True)

        if query.from_user.id != order.get("customer_id"):
            return await query.answer("❌ Підтвердити може тільки замовник!", show_alert=True)

        if order.get("status") != "await_customer":
            return await query.answer("Ще зарано підтверджувати. Очікуйте доставку 🙂", show_alert=True)

        try:
            await query.edit_message_text("Дякуємо! 😊")
        except Exception:
            pass

        order["status"] = "await_finish"

        courier_id = order.get("courier_id")
        if courier_id:
            await context.bot.send_message(
                chat_id=courier_id,
                text=(f"🏁 Клієнт підтвердив отримання по №{order_id}.\n\nФіналізація:"),
                reply_markup=finalize_kb(order_id)
            )
        return

    # finish auto/manual
    if data.startswith("finish_auto:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        if query.from_user.id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        if order.get("status") != "await_finish":
            return await query.answer("Це замовлення не очікує фіналізації.", show_alert=True)

        final_total = int(order.get("total") or 0)
        final_fee = int(order.get("fee") or 0)

        await finalize_and_close_order(context, order_id, final_total, final_fee, manual_dist=None, closed_by="кур'єр")
        try:
            await query.edit_message_text(f"✅ №{order_id} закрито по авто-розрахунку.\nКомісія: {final_fee} грн")
        except Exception:
            pass
        return

    if data.startswith("finish_manual:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        if query.from_user.id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        if order.get("status") != "await_finish":
            return await query.answer("Це замовлення не очікує фіналізації.", show_alert=True)

        FINAL_KM_PENDING[query.from_user.id] = order_id
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text="✏️ Введіть фінальну дистанцію (км) одним числом, напр: 5.7",
            reply_markup=courier_menu()
        )
        return



    if data.startswith("close_manual:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        if query.from_user.id != order.get("courier_id"):
            return await query.answer("❌ Це не ваше замовлення.", show_alert=True)

        if not order.get("manual_pending"):
            return await query.answer("Немає ручної фіналізації для закриття.", show_alert=True)

        final_total = int(order.get("manual_total") or order.get("total") or 0)
        final_fee = int(order.get("manual_fee") or order.get("fee") or 0)
        manual_dist = order.get("manual_dist")

        await finalize_and_close_order(
            context,
            order_id,
            final_total,
            final_fee,
            manual_dist=float(manual_dist) if manual_dist is not None else None,
            closed_by="кур'єр (вручну)"
        )

        try:
            await query.edit_message_text(
                f"✅ №{order_id} закрито.\n💰 До сплати: {final_total} грн\n📉 Комісія: {final_fee} грн"
            )
        except Exception:
            pass
        return

    await query.answer("Невідома дія", show_alert=True)


# =========================
# ===== Receive check photo
# =========================
async def check_media_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not is_private_chat(update):
        return

    courier_id = update.effective_user.id
    pending = context.application.bot_data.get("check_pending", {})
    order_id = pending.get(courier_id)
    if not order_id:
        return

    media_type = None
    file_id = None
    file_name = ""

    if update.message.photo:
        media_type = "photo"
        file_id = update.message.photo[-1].file_id
    elif update.message.document:
        media_type = "document"
        file_id = update.message.document.file_id
        file_name = update.message.document.file_name or ""
    elif update.message.video:
        media_type = "video"
        file_id = update.message.video.file_id
        file_name = getattr(update.message.video, "file_name", "") or ""
    else:
        await update.message.reply_text("Надішліть фото, файл або відео одним повідомленням.")
        return
    pending.pop(courier_id, None)
    context.application.bot_data["check_pending"] = pending

    order = ORDERS_DB.get(order_id)
    if not order:
        await update.message.reply_text("Замовлення не знайдено або вже закрите.")
        return

    # store in sql archive
    db_insert_purchase_check(order_id, file_id, media_type=media_type, file_name=file_name)

    # forward to customer + owner
    customer_id = order.get("customer_id")
    caption = f"🧾 Медіа по замовленню №{order_id}"

    async def _send_media(chat_id: int):
        if media_type == "photo":
            await context.bot.send_photo(chat_id, photo=file_id, caption=caption)
        elif media_type == "video":
            await context.bot.send_video(chat_id, video=file_id, caption=caption)
        else:
            await context.bot.send_document(chat_id, document=file_id, caption=caption)

    try:
        if customer_id:
            await _send_media(int(customer_id))
    except Exception:
        pass
    try:
        if OWNER_CHAT_ID:
            await _send_media(int(OWNER_CHAT_ID))
    except Exception:
        pass

    await update.message.reply_text("✅ Медіа надіслано клієнту та збережено в архіві овнера.")


# =========================
# ===== Post init/shutdown =
# =========================
async def post_init(app: Application):
    app.bot_data["http"] = aiohttp.ClientSession()
    app.bot_data.setdefault("guarantee_pending", {})
    app.bot_data.setdefault("rating_pending", {})
    app.bot_data.setdefault("check_pending", {})
    app.bot_data.setdefault("buy_details_pending", {})

    if OWNER_CHAT_ID:
        try:
            await app.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text="✅ Бот запущено. Кнопка /panel доступна 👇",
                reply_markup=owner_quick_kb()
            )
        except Exception:
            pass


async def post_shutdown(app: Application):
    s = app.bot_data.get("http")
    if s:
        try:
            await s.close()
        except Exception:
            pass


# =========================
# ===== Build app =========
# =========================
def build_app() -> Application:
    if not TOKEN:
        raise RuntimeError("Не знайдено BOT_TOKEN.")

    db_init()
    db_load_couriers_and_balances()

    app = Application.builder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()

    # join notifications
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_members), group=0)
    app.add_handler(ChatMemberHandler(on_chat_member_update, ChatMemberHandler.CHAT_MEMBER), group=0)

    # callbacks
    app.add_handler(CallbackQueryHandler(callback_router), group=0)

    # owner panel command
    app.add_handler(CommandHandler("panel", panel), group=0)

    # owner text input ONLY in OWNER chat
    if OWNER_CHAT_ID:
        app.add_handler(
            MessageHandler(filters.Chat(chat_id=OWNER_CHAT_ID) & (filters.TEXT & ~filters.COMMAND), owner_text_input),
            group=1
        )

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ROLE_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, role_choice)],

            CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, choice_router)],

            CALLME_PHONE: [
                MessageHandler(filters.CONTACT, callme_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, callme_handler),
            ],

            DELIVERY_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_mode)],

            DELIV_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_type)],

            WHEN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, when_input)],
            WHEN_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, when_confirm)],

            FROM_ADDR: [MessageHandler(filters.LOCATION, from_addr), MessageHandler(filters.TEXT & ~filters.COMMAND, from_addr)],
            CONFIRM_FROM: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_from)],

            TO_ADDR: [MessageHandler(filters.LOCATION, to_addr), MessageHandler(filters.TEXT & ~filters.COMMAND, to_addr)],
            CONFIRM_TO: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_to)],

            BUY_LIST: [MessageHandler(filters.TEXT & ~filters.COMMAND, buy_list)],
            BUY_APPROX_SUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, buy_approx_sum)],
            BUY_WAIT_PAID_CLICK: [MessageHandler(filters.TEXT & ~filters.COMMAND, choice_router)],

            ITEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, item)],

            PHONE: [
                MessageHandler(filters.CONTACT, phone),
                MessageHandler(filters.TEXT & ~filters.COMMAND, phone),
            ],

            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, comment)],
            CONFIRM_ORDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_order)],
        },
        fallbacks=[],
        allow_reentry=True,
    )

    app.add_handler(conv, group=2)

    # after conv: manual km / support contact / rating text / check photo
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT & ~filters.COMMAND), final_km_input_handler), group=3)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT & ~filters.COMMAND), return_contact_handler), group=3)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.CONTACT, return_contact_handler), group=3)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT & ~filters.COMMAND), support_contact_handler), group=3)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.CONTACT, support_contact_handler), group=3)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.TEXT & ~filters.COMMAND), rating_text_handler), group=4)
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.PHOTO | filters.VIDEO | filters.Document.ALL), check_media_handler), group=4)

    return app


# =========================
# ===== Runner (webhook/polling)
# =========================
def main():
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError("Не знайдено GOOGLE_MAPS_API_KEY.")
    if not COURIER_GROUP_ID:
        raise RuntimeError("Не знайдено COURIER_GROUP_ID.")
    if not OWNER_CHAT_ID:
        raise RuntimeError("Не знайдено OWNER_CHAT_ID.")

    application = build_app()

    if WEBHOOK_BASE_URL:
        # webhook
        # You must set Telegram webhook to {WEBHOOK_BASE_URL}/webhook
        # PTB handles it automatically in run_webhook
        path = "/webhook"
        if WEBHOOK_SECRET:
            path = f"/webhook/{WEBHOOK_SECRET}"

        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=path.lstrip("/"),
            webhook_url=f"{WEBHOOK_BASE_URL}{path}",
            allowed_updates=Update.ALL_TYPES,
            close_loop=True,
        )
    else:
        # polling fallback
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            close_loop=True,
        )


if __name__ == "__main__":
    main()

# === PART 2/2 END ===
