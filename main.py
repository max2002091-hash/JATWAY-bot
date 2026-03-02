import os
import re
import json
import math
import asyncio
import time
import aiohttp
import csv
from io import StringIO, BytesIO
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Dict, List

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
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
# ===== ENV / CONFIG ======
# =========================
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

COURIER_GROUP_ID = int(os.getenv("COURIER_GROUP_ID", "0"))   # диспетчерська група
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0"))         # овнер група/чат
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))         # сумісність

# ✅ Фіксовані номери підтримки (клієнтська)
SUPPORT_PHONE_1 = "+380968130807"
SUPPORT_PHONE_2 = "+380687294365"

# ===== Оплата (картка) =====
# Якщо хочеш — винеси в ENV: PAYMENT_CARD="4441 1110 2610 2602"
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "4441 1110 2610 2602").strip()
PAYMENT_RECEIVER_NAME = os.getenv("PAYMENT_RECEIVER_NAME", "").strip()  # опційно (ПІБ)

def payment_purpose(order_id: str) -> str:
    # ✅ Авто-текст призначення платежу
    return f"Оплата доставки №{order_id}"

# ===== Авто-скасування (якщо ніхто не взяв) =====
# Замовлення в статусі "searching" скасується автоматично через N хв
ORDER_AUTO_CANCEL_MIN = int(os.getenv("ORDER_AUTO_CANCEL_MIN", "20"))
# Нагадування клієнту за N хв до скасування
REMINDER_BEFORE_CANCEL_MIN = int(os.getenv("REMINDER_BEFORE_CANCEL_MIN", "5"))

# ===== Tariffs =====
PRICE_SCHEDULED_BASE = 110
PRICE_URGENT_BASE = 170
BASE_KM = 2.0
EXTRA_KM_PRICE = 23.0
EXTRA_OUTSIDE_OBUKHIV_PER_KM = 8.0

JETWAY_FEE_RATE = 0.25           # базова (за замовчуванням)
DIST_SAFETY_MULT = 1.10          # +10% лише для попереднього розрахунку (preview)

MIN_NEGATIVE_BALANCE = -50

# ===== Timezone =====
try:
    TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    TZ = timezone(timedelta(hours=2))

# ===== Data dir (Railway Volume friendly) =====
DATA_DIR = os.getenv("DATA_DIR", ".")
os.makedirs(DATA_DIR, exist_ok=True)

COURIERS_FILE = os.path.join(DATA_DIR, "couriers.json")
BALANCES_FILE = os.path.join(DATA_DIR, "courier_balances.json")
STATS_FILE = os.path.join(DATA_DIR, "daily_stats.json")
ARCHIVE_FILE = os.path.join(DATA_DIR, "daily_archive.json")
USERS_FILE = os.path.join(DATA_DIR, "dispatcher_users.json")

FEEDBACK_FILE = os.path.join(DATA_DIR, "feedback.json")
COURIER_META_FILE = os.path.join(DATA_DIR, "courier_meta.json")

ORDERS_FILE = os.path.join(DATA_DIR, "orders_db.json")

# =========================
# ===== Storage (RAM) =====
# =========================
COURIERS: set[int] = set()
COURIER_BALANCES: Dict[int, int] = {}
ACTIVE_ORDERS_COUNT: Dict[int, int] = {}       # courier_id -> count
ORDERS_DB: Dict[str, dict] = {}                # order_id -> data

OWNER_PENDING: Dict[int, str] = {}             # owner_id -> "add"/"del"/"topup"/"withdraw"/"bal"
SUPPORT_CONTACT_PENDING: Dict[int, str] = {}   # courier_id -> order_id (order support)
COURIER_GENERAL_SUPPORT_PENDING: Dict[int, bool] = {}  # courier_id -> True (general support)
FINAL_KM_PENDING: Dict[int, str] = {}          # courier_id -> order_id

DAILY_STATS = {"date": "", "orders": 0, "revenue": 0, "profit": 0}
DAILY_ARCHIVE: Dict[str, List[dict]] = {}      # date_key -> list[order_snapshot]
DISPATCHER_USERS: Dict[str, dict] = {}         # user_id(str) -> record

FEEDBACK_DB: List[dict] = []                   # list of ratings/complaints
COMPLAINT_PENDING: Dict[int, str] = {}         # customer_id -> order_id

COURIER_META: Dict[str, dict] = {}             # str(courier_id) -> {"since":"YYYY-MM-DD"}

# Job refs (щоб можна було скасувати таймери)
ORDER_JOBS: Dict[str, Dict[str, str]] = {}      # order_id -> {"reminder": job_name, "cancel": job_name}

# ===== States =====
(
    ROLE_CHOICE,
    CHOICE,
    DELIV_TYPE,
    WHEN_INPUT,
    WHEN_CONFIRM,
    FROM_ADDR,
    CONFIRM_FROM,
    TO_ADDR,
    CONFIRM_TO,
    ITEM,
    PHONE,
    COMMENT,
    CONFIRM_ORDER,
    CALLME_PHONE
) = range(14)

# =========================
# ===== File helpers ======
# =========================
def load_couriers():
    global COURIERS
    try:
        with open(COURIERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        COURIERS = set(int(x) for x in data)
    except Exception:
        COURIERS = set()


def save_couriers():
    try:
        with open(COURIERS_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(COURIERS), f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_balances():
    global COURIER_BALANCES
    try:
        with open(BALANCES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            COURIER_BALANCES = {int(k): int(v) for k, v in data.items()}
    except Exception:
        COURIER_BALANCES = {}


def save_balances():
    try:
        with open(BALANCES_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): int(v) for k, v in COURIER_BALANCES.items()}, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _today_key() -> str:
    return datetime.now(TZ).date().isoformat()


def save_daily_stats():
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(DAILY_STATS, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def reset_daily_stats_today():
    DAILY_STATS["date"] = _today_key()
    DAILY_STATS["orders"] = 0
    DAILY_STATS["revenue"] = 0
    DAILY_STATS["profit"] = 0
    save_daily_stats()


def load_daily_stats():
    global DAILY_STATS
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            DAILY_STATS.update(d)
    except Exception:
        pass

    if DAILY_STATS.get("date") != _today_key():
        reset_daily_stats_today()


def add_stats(final_total: int, final_fee: int):
    if DAILY_STATS.get("date") != _today_key():
        reset_daily_stats_today()
    DAILY_STATS["orders"] += 1
    DAILY_STATS["revenue"] += int(final_total)
    DAILY_STATS["profit"] += int(final_fee)
    save_daily_stats()


def load_archive():
    global DAILY_ARCHIVE
    try:
        with open(ARCHIVE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            DAILY_ARCHIVE = {str(k): (v if isinstance(v, list) else []) for k, v in d.items()}
    except Exception:
        DAILY_ARCHIVE = {}


def save_archive():
    try:
        with open(ARCHIVE_FILE, "w", encoding="utf-8") as f:
            json.dump(DAILY_ARCHIVE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def add_to_archive_today(snapshot: dict):
    k = _today_key()
    DAILY_ARCHIVE.setdefault(k, [])
    DAILY_ARCHIVE[k].append(snapshot)
    save_archive()


def load_dispatcher_users():
    global DISPATCHER_USERS
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            DISPATCHER_USERS = d
    except Exception:
        DISPATCHER_USERS = {}


def save_dispatcher_users():
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(DISPATCHER_USERS, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def upsert_dispatcher_user(user) -> dict:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    uid = str(user.id)
    rec = DISPATCHER_USERS.get(uid) or {
        "id": int(user.id),
        "name": user.full_name or "-",
        "username": user.username or "-",
        "first_seen": now,
        "last_seen": now,
    }
    rec["name"] = user.full_name or rec.get("name", "-")
    rec["username"] = user.username or rec.get("username", "-")
    rec["last_seen"] = now
    DISPATCHER_USERS[uid] = rec
    save_dispatcher_users()
    return rec


def load_feedback():
    global FEEDBACK_DB
    try:
        with open(FEEDBACK_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        FEEDBACK_DB = d if isinstance(d, list) else []
    except Exception:
        FEEDBACK_DB = []


def save_feedback():
    try:
        with open(FEEDBACK_FILE, "w", encoding="utf-8") as f:
            json.dump(FEEDBACK_DB, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def feedback_has_rating(order_id: str, customer_id: int) -> bool:
    for r in FEEDBACK_DB:
        if r.get("type") == "rating" and r.get("order_id") == order_id and int(r.get("customer_id") or 0) == int(customer_id):
            return True
    return False


def feedback_has_complaint(order_id: str, customer_id: int) -> bool:
    for r in FEEDBACK_DB:
        if r.get("type") == "complaint" and r.get("order_id") == order_id and int(r.get("customer_id") or 0) == int(customer_id):
            return True
    return False


def load_courier_meta():
    global COURIER_META
    try:
        with open(COURIER_META_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        COURIER_META = d if isinstance(d, dict) else {}
    except Exception:
        COURIER_META = {}


def save_courier_meta():
    # ✅ FIX: прибрали дивний COURIer_META := COURIER_META
    try:
        with open(COURIER_META_FILE, "w", encoding="utf-8") as f:
            json.dump(COURIER_META, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def ensure_courier_since(cid: int):
    k = str(cid)
    if k not in COURIER_META:
        COURIER_META[k] = {"since": datetime.now(TZ).date().isoformat()}
        save_courier_meta()
    elif not COURIER_META[k].get("since"):
        COURIER_META[k]["since"] = datetime.now(TZ).date().isoformat()
        save_courier_meta()


def get_courier_since_date(cid: int):
    k = str(cid)
    s = (COURIER_META.get(k) or {}).get("since")
    try:
        return datetime.fromisoformat(s).date() if s else None
    except Exception:
        return None


# ---- ORDERS_DB persistence ----
def _jsonable_order(o: dict) -> dict:
    d = dict(o or {})
    for k in ("from_coords", "to_coords"):
        v = d.get(k)
        if isinstance(v, tuple):
            d[k] = [float(v[0]), float(v[1])]
    return d


def _restore_order(o: dict) -> dict:
    d = dict(o or {})
    for k in ("from_coords", "to_coords"):
        v = d.get(k)
        if isinstance(v, list) and len(v) == 2:
            try:
                d[k] = (float(v[0]), float(v[1]))
            except Exception:
                pass
    return d


def save_orders_db():
    try:
        payload = {str(oid): _jsonable_order(o) for oid, o in (ORDERS_DB or {}).items()}
        with open(ORDERS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_orders_db():
    global ORDERS_DB, ACTIVE_ORDERS_COUNT
    try:
        with open(ORDERS_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        if isinstance(d, dict):
            ORDERS_DB = {str(k): _restore_order(v if isinstance(v, dict) else {}) for k, v in d.items()}
        else:
            ORDERS_DB = {}
    except Exception:
        ORDERS_DB = {}

    ACTIVE_ORDERS_COUNT = {}
    for oid, o in ORDERS_DB.items():
        if o.get("status") in ("accepted", "await_customer", "await_finish"):
            cid = o.get("courier_id")
            if cid:
                ACTIVE_ORDERS_COUNT[int(cid)] = ACTIVE_ORDERS_COUNT.get(int(cid), 0) + 1


# =========================
# ===== Stats helpers =====
# =========================
def _date_from_key(k: str):
    try:
        return datetime.fromisoformat(k).date()
    except Exception:
        return None


def _sum_closed_in_range(start_date, end_date) -> dict:
    load_archive()
    orders = 0
    revenue = 0
    profit = 0

    for k, items in (DAILY_ARCHIVE or {}).items():
        d = _date_from_key(k)
        if not d:
            continue
        if d < start_date or d > end_date:
            continue

        for it in (items or []):
            if it.get("status") != "closed":
                continue

            t = int(it.get("total") or 0)
            f = it.get("fee")
            if f is None:
                f = int(math.ceil(t * JETWAY_FEE_RATE))  # backward compatible
            f = int(f)

            orders += 1
            revenue += t
            profit += f

    direct_to_courier = revenue - profit
    avg_check = int(round(revenue / orders)) if orders else 0

    return {
        "orders": orders,
        "revenue": revenue,
        "profit": profit,
        "direct": direct_to_courier,
        "avg_check": avg_check,
    }


def _fmt_period_stats(title: str, start_date, end_date) -> str:
    s = _sum_closed_in_range(start_date, end_date)
    period = f"{start_date.isoformat()} → {end_date.isoformat()}"

    return (
        f"📊 **{title}**\n"
        f"🗓 Період: {period}\n\n"
        f"✅ Закритих замовлень: {s['orders']}\n"
        f"🧾 Обіг по замовленнях (не дохід сервісу): **{s['revenue']} грн**\n"
        f"📈 **Дохід сервісу (комісія): {s['profit']} грн**\n"
        f"🚚 Оплата кур’єру напряму (не дохід сервісу): **{s['direct']} грн**\n"
        f"🧾 Середній чек: **{s['avg_check']} грн**"
    )


# =========================
# ===== Rating + fee rules
# =========================
def courier_rating(cid: int, days: Optional[int] = None) -> Tuple[float, int]:
    load_feedback()
    now = datetime.now(TZ).replace(tzinfo=None)

    stars: List[int] = []
    for r in FEEDBACK_DB:
        if r.get("type") != "rating":
            continue
        if int(r.get("courier_id") or 0) != int(cid):
            continue

        if days is not None:
            ts = r.get("ts")
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            except Exception:
                dt = None
            if not dt:
                continue
            if (now - dt).days > int(days):
                continue

        try:
            stars.append(int(r.get("stars") or 0))
        except Exception:
            pass

    if not stars:
        return 5.0, 0
    return (sum(stars) / len(stars)), len(stars)


def courier_fee_rate(cid: int) -> float:
    """
    1-й місяць від 'since' -> 25%
    Після 30 днів: якщо рейтинг >= 4.8 (за останні 30 днів) -> 20%, інакше 25%
    """
    since = get_courier_since_date(cid)
    today = datetime.now(TZ).date()

    if not since:
        return 0.25

    days_active = (today - since).days
    if days_active < 30:
        return 0.25

    avg30, _ = courier_rating(cid, days=30)
    return 0.20 if avg30 >= 4.8 else 0.25


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
        "• вул. Київська 1, Обухів\n"
    )

# =========================
# ===== Menus / Texts =====
# =========================
def role_menu():
    return ReplyKeyboardMarkup([["🙋 Я клієнт", "🚚 Я кур'єр"]], resize_keyboard=True)

def main_menu():
    return ReplyKeyboardMarkup([["🚚 Доставка", "💳 Тариф", "🛠 Підтримка"]], resize_keyboard=True)

def courier_menu():
    return ReplyKeyboardMarkup(
        [
            ["📦 Мої активні", "💳 Мій баланс"],
            ["⭐ Мій рейтинг", "🆘 Техпідтримка"],
            ["⬅️ Клієнтське меню"],
        ],
        resize_keyboard=True
    )

def delivery_type_menu():
    return ReplyKeyboardMarkup(
        [["⏰ На певний час", "⚡ Термінова"], ["⬅️ Назад в меню"]],
        resize_keyboard=True
    )

def back_only_kb():
    return ReplyKeyboardMarkup([["⬅️ Назад в меню"]], resize_keyboard=True)

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
        [["✅ Підтвердити адресу", "✏️ Змінити адресу"], ["⬅️ Назад в меню"]],
        resize_keyboard=True
    )

def when_confirm_kb():
    return ReplyKeyboardMarkup(
        [["✅ Підтвердити час", "✏️ Змінити час"], ["⬅️ Назад в меню"]],
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

def owner_quick_kb():
    return ReplyKeyboardMarkup([["/panel"]], resize_keyboard=True)

def tariff_text() -> str:
    return (
        "💳 Наші тарифи\n\n"
        f"⏰ На певний час: {int(PRICE_SCHEDULED_BASE)} грн до {int(BASE_KM)} км\n"
        f"➕ додаткові км: +{int(EXTRA_KM_PRICE)} грн/км\n\n"
        f"⚡ Термінова: {int(PRICE_URGENT_BASE)} грн до {int(BASE_KM)} км\n"
        f"➕ додаткові км: +{int(EXTRA_KM_PRICE)} грн/км\n\n"
        f"🏙️ Поза містом Обухів: додатково +{int(EXTRA_OUTSIDE_OBUKHIV_PER_KM)} грн/км."
    )

def support_text() -> str:
    return (
        "🛠 Підтримка\n"
        f"📞 Наші номери:\n"
        f"• {SUPPORT_PHONE_1}\n"
        f"• {SUPPORT_PHONE_2}\n\n"
        "Натисни «📞 Зателефонуйте мені» або напиши свій номер — ми передзвонимо."
    )

def owner_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Сьогодні", callback_data="stats:today"),
            InlineKeyboardButton("📊 Вчора", callback_data="stats:yesterday"),
        ],
        [
            InlineKeyboardButton("📊 7 днів", callback_data="stats:week"),
            InlineKeyboardButton("📊 Місяць", callback_data="stats:month"),
        ],
        [InlineKeyboardButton("📤 Експорт CSV за місяць (повний)", callback_data="export:month_full")],
        [InlineKeyboardButton("📤 Експорт CSV за місяць (дохід ФОП)", callback_data="export:month_income")],
        [InlineKeyboardButton("🧹 Очистити статистику за сьогодні", callback_data="stats:reset")],
        [InlineKeyboardButton("🗂 Архів за сьогодні", callback_data="archive:today")],
        [
            InlineKeyboardButton("📝 Зауваження до кур’єрів", callback_data="fbadmin:complaints"),
            InlineKeyboardButton("⭐ Рейтинг кур’єрів", callback_data="fbadmin:ratings"),
        ],
        [
            InlineKeyboardButton("👥 Мої кур’єри", callback_data="couriers:list"),
            InlineKeyboardButton("🧾 Користувачі", callback_data="users:list"),
        ],
        [
            InlineKeyboardButton("➕ Додати кур’єра", callback_data="owner:add"),
            InlineKeyboardButton("➖ Видалити кур’єра", callback_data="owner:del"),
        ],
        [
            InlineKeyboardButton("💳 Поповнити баланс", callback_data="owner:topup"),
            InlineKeyboardButton("➖ Зняти баланс", callback_data="owner:withdraw"),
        ],
        [
            InlineKeyboardButton("🔍 Баланс кур’єра", callback_data="owner:bal"),
        ],
    ])

def kb_make_courier(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Зробити кур’єром", callback_data=f"courier:make:{user_id}")]
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
    wait = 0.12 - (now - _LAST_GOOGLE_TS)
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
    phi1, phi2 = math.radians(c1[0]), math.radians(c2[0])
    dphi = math.radians(c2[0] - c1[0])
    dlambda = math.radians(c2[1] - c1[1])
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def is_outside_obukhiv(from_addr: str, to_addr: str) -> bool:
    return ("обухів" not in (from_addr or "").lower()) or ("обухів" not in (to_addr or "").lower())

def calculate_finance_total(
    dist_km: float,
    is_urgent: bool,
    outside: bool,
    use_safety: bool,
    fee_rate: float = JETWAY_FEE_RATE
) -> Tuple[int, int, int, float]:
    base = float(PRICE_URGENT_BASE if is_urgent else PRICE_SCHEDULED_BASE)
    km_price = float(EXTRA_KM_PRICE + (EXTRA_OUTSIDE_OBUKHIV_PER_KM if outside else 0.0))

    used_dist = dist_km * (DIST_SAFETY_MULT if use_safety else 1.0)
    extra_km = max(0.0, used_dist - BASE_KM)

    total_raw = base + (extra_km * km_price)

    total = int(math.ceil(total_raw))
    fee = int(math.ceil(total * float(fee_rate)))
    cut = int(total - fee)
    return total, fee, cut, used_dist

# =========================
# ===== Order keyboards ===
# =========================
def kb_accept(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚚 Взяти замовлення", callback_data=f"accept:{order_id}")]
    ])

def kb_taken() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ВЗЯТО", callback_data="noop")]
    ])

def kb_courier_controls(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Доставлено", callback_data=f"delivered:{order_id}")],
        [InlineKeyboardButton("🆘 Техпідтримка", callback_data=f"support:{order_id}")]
    ])

def kb_customer_done(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Отримав", callback_data=f"done:{order_id}")]
    ])

def finalize_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Ціна вірна (авто, з +10%)", callback_data=f"finish_auto:{order_id}")],
        [InlineKeyboardButton("✏️ Ввести фінальний км (без +10%)", callback_data=f"finish_manual:{order_id}")]
    ])

def kb_owner_force_close(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛠 Закрити вручну", callback_data=f"force:{order_id}")]
    ])

def support_share_contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Поділитись номером", request_contact=True)],
         ["⬅️ Назад в меню"]],
        resize_keyboard=True
    )

def kb_support_pick(orders: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for oid in orders[:10]:
        rows.append([InlineKeyboardButton(f"🆘 №{oid}", callback_data=f"support_pick:{oid}")])
    rows.append([InlineKeyboardButton("❌ Скасувати", callback_data="support_pick:cancel")])
    return InlineKeyboardMarkup(rows)

# ---- customer feedback kb ----
def kb_customer_feedback(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⭐ Оцінити кур’єра", callback_data=f"fb:rate_menu:{order_id}")],
        [InlineKeyboardButton("😡 Поскаржитись", callback_data=f"fb:complain:{order_id}")],
    ])

def kb_rating_stars(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1", callback_data=f"fb:rate:{order_id}:1"),
            InlineKeyboardButton("2", callback_data=f"fb:rate:{order_id}:2"),
            InlineKeyboardButton("3", callback_data=f"fb:rate:{order_id}:3"),
            InlineKeyboardButton("4", callback_data=f"fb:rate:{order_id}:4"),
            InlineKeyboardButton("5", callback_data=f"fb:rate:{order_id}:5"),
        ],
        [InlineKeyboardButton("❌ Скасувати", callback_data=f"fb:rate_cancel:{order_id}")]
    ])

# =========================
# ===== Utils ============
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

def dispatcher_chat_id() -> int:
    return COURIER_GROUP_ID or ADMIN_CHAT_ID

def is_private_chat(update: Update) -> bool:
    try:
        return update.effective_chat and update.effective_chat.type == "private"
    except Exception:
        return False

def courier_active_order_ids(courier_id: int) -> List[str]:
    ids = []
    for oid, o in ORDERS_DB.items():
        if o.get("courier_id") == courier_id and o.get("status") not in ("closed",):
            if o.get("status") in ("accepted", "await_customer", "await_finish"):
                ids.append(oid)
    return ids

def fmt_payment_block(order_id: str) -> str:
    if not PAYMENT_CARD:
        return ""
    receiver = f"{PAYMENT_RECEIVER_NAME}\n" if PAYMENT_RECEIVER_NAME else ""
    return (
        "\n💳 **Оплата**\n"
        f"{receiver}"
        f"Картка: `{PAYMENT_CARD}`\n"
        f"Призначення: `{payment_purpose(order_id)}`\n"
    )

# =========================
# ===== Auto-cancel jobs ===
# =========================
def _job_name(prefix: str, order_id: str) -> str:
    return f"{prefix}:{order_id}"

def _remove_order_jobs(order_id: str):
    ORDER_JOBS.pop(order_id, None)

def cancel_order_timers(app: Application, order_id: str):
    jq = app.job_queue
    meta = ORDER_JOBS.get(order_id) or {}
    for key in ("reminder", "cancel"):
        name = meta.get(key)
        if name:
            for j in jq.get_jobs_by_name(name):
                try:
                    j.schedule_removal()
                except Exception:
                    pass
    _remove_order_jobs(order_id)

def schedule_order_timers(app: Application, order_id: str):
    """
    Планує:
    - нагадування за REMINDER_BEFORE_CANCEL_MIN хв до авто-скасування
    - авто-скасування через ORDER_AUTO_CANCEL_MIN хв
    Працює тільки для статусу "searching".
    """
    order = ORDERS_DB.get(order_id)
    if not order:
        return
    if order.get("status") != "searching":
        return

    created_ts = float(order.get("created_ts") or time.time())
    now_ts = time.time()
    elapsed = max(0.0, now_ts - created_ts)

    cancel_after = max(0.0, ORDER_AUTO_CANCEL_MIN * 60 - elapsed)
    reminder_after = max(0.0, (ORDER_AUTO_CANCEL_MIN - REMINDER_BEFORE_CANCEL_MIN) * 60 - elapsed)

    # якщо вже прострочено — скасуємо одразу
    if cancel_after <= 0:
        app.job_queue.run_once(job_autocancel, when=0, data={"order_id": order_id}, name=_job_name("cancel", order_id))
        ORDER_JOBS[order_id] = {"cancel": _job_name("cancel", order_id)}
        return

    # reminder тільки якщо є сенс
    jobs_meta: Dict[str, str] = {}
    if reminder_after > 0:
        rname = _job_name("reminder", order_id)
        app.job_queue.run_once(job_reminder, when=reminder_after, data={"order_id": order_id}, name=rname)
        jobs_meta["reminder"] = rname

    cname = _job_name("cancel", order_id)
    app.job_queue.run_once(job_autocancel, when=cancel_after, data={"order_id": order_id}, name=cname)
    jobs_meta["cancel"] = cname

    ORDER_JOBS[order_id] = jobs_meta

async def job_reminder(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    order_id = data.get("order_id")
    if not order_id:
        return
    order = ORDERS_DB.get(order_id)
    if not order:
        return
    if order.get("status") != "searching":
        return

    customer_chat_id = order.get("customer_chat_id")
    if not customer_chat_id:
        return

    mins_left = REMINDER_BEFORE_CANCEL_MIN
    try:
        await context.bot.send_message(
            chat_id=customer_chat_id,
            text=(
                f"⏳ Замовлення №{order_id} ще ніхто не взяв.\n"
                f"Якщо протягом **{mins_left} хв** не знайдеться кур’єр — замовлення буде автоматично скасовано.\n\n"
                "Якщо треба — можете створити нове замовлення знову."
            ),
            parse_mode="Markdown"
        )
    except Exception:
        pass

async def job_autocancel(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    order_id = data.get("order_id")
    if not order_id:
        return

    order = ORDERS_DB.get(order_id)
    if not order:
        return

    # Скасовуємо тільки якщо ще "searching"
    if order.get("status") != "searching":
        return

    # прибираємо з диспетчерської повідомлення
    try:
        if order.get("admin_chat_id") and order.get("admin_msg_id"):
            await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
    except Exception:
        pass

    # snapshot в архів
    snap = {
        "order_id": order_id,
        "time": datetime.now(TZ).strftime("%H:%M:%S"),
        "status": "canceled_auto",
        "courier_id": None,
        "courier_name": "-",
        "total": int(order.get("total") or 0),
    }
    add_to_archive_today(snap)

    # повідомлення клієнту
    customer_chat_id = order.get("customer_chat_id")
    try:
        if customer_chat_id:
            await context.bot.send_message(
                chat_id=customer_chat_id,
                text=(
                    f"❌ Замовлення №{order_id} автоматично скасовано (кур’єр не знайшовся).\n"
                    "Можеш оформити нове замовлення у меню «🚚 Доставка»."
                ),
                reply_markup=main_menu()
            )
    except Exception:
        pass

    # повідомлення овнеру
    try:
        if OWNER_CHAT_ID:
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"❌ Авто-скасування: замовлення №{order_id} (ніхто не взяв за {ORDER_AUTO_CANCEL_MIN} хв)."
            )
    except Exception:
        pass

    # прибираємо з БД
    ORDERS_DB.pop(order_id, None)
    save_orders_db()
    _remove_order_jobs(order_id)

# =========================
# ===== Group join notify =
# =========================
async def notify_owner_new_user(context: ContextTypes.DEFAULT_TYPE, u, source_chat_id: int):
    if not OWNER_CHAT_ID or not COURIER_GROUP_ID:
        return
    if source_chat_id != COURIER_GROUP_ID:
        return

    rec = upsert_dispatcher_user(u)
    t = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

    text = (
        "👤 **Новий користувач у диспетчерській**\n\n"
        f"Імʼя: {rec.get('name','-')}\n"
        f"Username: @{rec.get('username','-')}\n"
        f"ID: `{rec.get('id')}`\n"
        f"Час: {t}\n\n"
        "ℹ️ Телефон бот не бачить автоматично (тільки якщо людина поділиться контактом у приваті)."
    )
    await context.bot.send_message(
        chat_id=OWNER_CHAT_ID,
        text=text,
        parse_mode="Markdown",
        reply_markup=kb_make_courier(int(rec["id"]))
    )

async def on_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if update.effective_chat.id != COURIER_GROUP_ID:
        return

    for m in update.message.new_chat_members:
        if m.is_bot:
            continue
        try:
            await notify_owner_new_user(context, m, source_chat_id=update.effective_chat.id)
        except Exception:
            pass

async def on_chat_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    try:
        await notify_owner_new_user(context, u, source_chat_id=cmu.chat.id)
    except Exception:
        pass

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

    if mode in ("add", "del", "bal"):
        try:
            cid = int(txt)
        except ValueError:
            return await update.message.reply_text("ID має бути числом. Приклад: `123456789`", parse_mode="Markdown")

        if mode == "add":
            COURIERS.add(cid)
            save_couriers()
            ensure_courier_since(cid)
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(f"✅ Додано кур’єра: `{cid}`", parse_mode="Markdown")
            try:
                await context.bot.send_message(cid, "✅ Вас додано як кур’єра. Напишіть /start.")
            except Exception:
                pass
            return

        if mode == "del":
            removed = cid in COURIERS
            COURIERS.discard(cid)
            save_couriers()
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(
                f"✅ Видалено кур’єра: `{cid}`" if removed else f"ℹ️ Кур’єра `{cid}` не було у списку.",
                parse_mode="Markdown"
            )
            return

        if mode == "bal":
            bal = int(COURIER_BALANCES.get(cid, 0))
            is_c = "✅" if cid in COURIERS else "❌"
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(
                f"🔍 Кур’єр: `{cid}`\n"
                f"Статус у списку: {is_c}\n"
                f"Баланс (внутр. рахунок комісії): **{bal} грн**",
                parse_mode="Markdown"
            )
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

        COURIER_BALANCES[cid] = int(COURIER_BALANCES.get(cid, 0)) + amount
        save_balances()
        OWNER_PENDING.pop(owner_id, None)

        await update.message.reply_text(
            f"✅ Баланс `{cid}` поповнено на **{amount} грн**.\n"
            f"Поточний баланс: **{COURIER_BALANCES[cid]} грн**",
            parse_mode="Markdown"
        )

        try:
            await context.bot.send_message(
                cid,
                f"💳 Баланс поповнено на {amount} грн.\nПоточний баланс: {COURIER_BALANCES[cid]} грн."
            )
        except Exception:
            pass
        return

    if mode == "withdraw":
        parts = txt.replace(",", ".").split()
        if len(parts) != 2:
            return await update.message.reply_text(
                "Формат: `ID сума`\nПриклад: `123456789 200`",
                parse_mode="Markdown"
            )
        try:
            cid = int(parts[0])
            amount = int(float(parts[1]))
            if amount <= 0:
                raise ValueError
        except Exception:
            return await update.message.reply_text("Помилка. Приклад: `123456789 200`", parse_mode="Markdown")

        COURIER_BALANCES[cid] = int(COURIER_BALANCES.get(cid, 0)) - amount
        save_balances()
        OWNER_PENDING.pop(owner_id, None)

        await update.message.reply_text(
            f"➖ З балансу `{cid}` знято **{amount} грн**.\n"
            f"Поточний баланс: **{COURIER_BALANCES[cid]} грн**",
            parse_mode="Markdown"
        )

        try:
            await context.bot.send_message(
                cid,
                f"➖ З балансу знято {amount} грн.\nПоточний баланс: {COURIER_BALANCES[cid]} грн."
            )
        except Exception:
            pass
        return

async def _send_csv_to_owner(context: ContextTypes.DEFAULT_TYPE, csv_bytes: bytes, filename: str, caption: str):
    bio = BytesIO(csv_bytes)
    bio.name = filename
    bio.seek(0)
    await context.bot.send_document(
        chat_id=OWNER_CHAT_ID,
        document=bio,
        filename=filename,
        caption=caption
    )

def _month_range_dates(today):
    start = today.replace(day=1)
    end = today
    return start, end

def build_month_csv_full_bytes() -> Tuple[bytes, str]:
    load_archive()
    today = datetime.now(TZ).date()
    start, end = _month_range_dates(today)
    ym = today.strftime("%Y-%m")
    filename = f"jetway_export_full_{ym}.csv"

    out = StringIO()
    w = csv.writer(out, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow([
        "date", "time", "order_id",
        "courier_id", "courier_name",
        "customer_id", "customer_name",
        "order_total_uah", "service_fee_uah", "paid_to_courier_direct_uah",
        "closed_by"
    ])

    for date_key, items in (DAILY_ARCHIVE or {}).items():
        d = _date_from_key(date_key)
        if not d or d < start or d > end:
            continue

        for it in (items or []):
            if it.get("status") != "closed":
                continue

            total = int(it.get("total") or 0)
            fee = it.get("fee")
            if fee is None:
                fee = int(math.ceil(total * JETWAY_FEE_RATE))
            fee = int(fee)
            direct = total - fee

            w.writerow([
                date_key,
                it.get("time") or "-",
                it.get("order_id") or "-",
                it.get("courier_id") or "-",
                it.get("courier_name") or "-",
                it.get("customer_id") or "-",
                it.get("customer_name") or "-",
                total,
                fee,
                direct,
                it.get("closed_by") or "-",
            ])

    s = out.getvalue()
    out.close()
    return ("\ufeff" + s).encode("utf-8"), filename

def build_month_csv_income_bytes() -> Tuple[bytes, str]:
    load_archive()
    today = datetime.now(TZ).date()
    start, end = _month_range_dates(today)
    ym = today.strftime("%Y-%m")
    filename = f"jetway_export_income_{ym}.csv"

    out = StringIO()
    w = csv.writer(out, delimiter=";", quoting=csv.QUOTE_MINIMAL)
    w.writerow([
        "date", "time", "order_id",
        "courier_id", "courier_name",
        "service_fee_uah"
    ])

    for date_key, items in (DAILY_ARCHIVE or {}).items():
        d = _date_from_key(date_key)
        if not d or d < start or d > end:
            continue

        for it in (items or []):
            if it.get("status") != "closed":
                continue

            total = int(it.get("total") or 0)
            fee = it.get("fee")
            if fee is None:
                fee = int(math.ceil(total * JETWAY_FEE_RATE))
            fee = int(fee)

            w.writerow([
                date_key,
                it.get("time") or "-",
                it.get("order_id") or "-",
                it.get("courier_id") or "-",
                it.get("courier_name") or "-",
                fee
            ])

    s = out.getvalue()
    out.close()
    return ("\ufeff" + s).encode("utf-8"), filename

async def owner_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = (query.data or "").strip()
    await query.answer()

    if query.message.chat.id != OWNER_CHAT_ID:
        return await query.answer("Недостатньо прав.", show_alert=True)

    if data == "stats:today":
        today = datetime.now(TZ).date()
        msg = _fmt_period_stats("Статистика за сьогодні", today, today)
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")
        return

    if data == "stats:yesterday":
        today = datetime.now(TZ).date()
        y = today - timedelta(days=1)
        msg = _fmt_period_stats("Статистика за вчора", y, y)
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")
        return

    if data == "stats:week":
        today = datetime.now(TZ).date()
        start = today - timedelta(days=6)
        msg = _fmt_period_stats("Статистика за 7 днів", start, today)
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")
        return

    if data == "stats:month":
        today = datetime.now(TZ).date()
        start = today.replace(day=1)
        msg = _fmt_period_stats(f"Статистика за місяць ({today.strftime('%Y-%m')})", start, today)
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")
        return

    if data == "stats:reset":
        reset_daily_stats_today()
        await context.bot.send_message(OWNER_CHAT_ID, f"🧹 Статистика за сьогодні ({_today_key()}) очищена ✅")
        return

    if data == "archive:today":
        load_archive()
        k = _today_key()
        items = DAILY_ARCHIVE.get(k, [])
        if not items:
            await context.bot.send_message(OWNER_CHAT_ID, f"🗂 Архів за сьогодні ({k}) порожній.")
            return
        lines = []
        for it in items[-200:]:
            fee = it.get("fee")
            fee_s = f"{fee} грн" if fee is not None else "-"
            lines.append(
                f"• №{it.get('order_id')} | {it.get('time','-')} | {it.get('status','-')} | {it.get('courier_name','-')} | total={it.get('total','-')} грн | fee={fee_s}"
            )
        await context.bot.send_message(
            OWNER_CHAT_ID,
            f"🗂 **Архів за сьогодні ({k})**\n\n" + "\n".join(lines),
            parse_mode="Markdown"
        )
        return

    if data == "fbadmin:complaints":
        load_feedback()
        items = [x for x in FEEDBACK_DB if x.get("type") == "complaint"]
        items.sort(key=lambda x: x.get("ts", ""), reverse=True)
        if not items:
            await context.bot.send_message(OWNER_CHAT_ID, "📝 Скарг/зауважень поки немає ✅")
            return

        lines = []
        for r in items[:50]:
            txt = (r.get("text") or "-").strip()
            if len(txt) > 200:
                txt = txt[:200] + "…"
            lines.append(
                f"• {r.get('ts','-')} | №{r.get('order_id')}\n"
                f"  Кур’єр: {r.get('courier_name','-')} (`{r.get('courier_id')}`)\n"
                f"  Клієнт: {r.get('customer_name','-')} (@{r.get('customer_username','-')})\n"
                f"  Текст: {txt}"
            )
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "📝 **Зауваження / скарги (останні 50):**\n\n" + "\n\n".join(lines),
            parse_mode="Markdown"
        )
        return

    if data == "fbadmin:ratings":
        load_feedback()
        load_courier_meta()
        load_dispatcher_users()

        if not COURIERS:
            await context.bot.send_message(OWNER_CHAT_ID, "👥 Кур’єрів ще немає.")
            return

        lines = []
        for cid in sorted(COURIERS):
            ensure_courier_since(cid)
            since = get_courier_since_date(cid)

            avg_all, cnt_all = courier_rating(cid, days=None)
            avg30, cnt30 = courier_rating(cid, days=30)
            rate = courier_fee_rate(cid)

            rec = DISPATCHER_USERS.get(str(cid)) or {}
            name = rec.get("name") or "-"
            uname = rec.get("username") or "-"

            since_s = since.isoformat() if since else "-"
            lines.append(
                f"• {name} (@{uname}) — `{cid}`\n"
                f"  since: {since_s}\n"
                f"  ⭐ 30д: **{avg30:.2f}/5** (оцінок: {cnt30}) | ⭐ all: **{avg_all:.2f}/5** (оцінок: {cnt_all})\n"
                f"  💸 Комісія зараз: **{int(rate*100)}%** (1-й місяць 25%, далі 20% якщо ⭐≥4.8 за 30д)"
            )

        await context.bot.send_message(
            OWNER_CHAT_ID,
            "⭐ **Рейтинг кур’єрів + комісія:**\n\n" + "\n\n".join(lines[:50]),
            parse_mode="Markdown"
        )
        return

    if data == "export:month_full":
        csv_bytes, filename = build_month_csv_full_bytes()
        if not csv_bytes or len(csv_bytes) < 30:
            await context.bot.send_message(OWNER_CHAT_ID, "ℹ️ Немає даних для експорту за цей місяць (закритих замовлень).")
            return
        await _send_csv_to_owner(context, csv_bytes, filename, f"📤 CSV (повний) за місяць: {datetime.now(TZ).strftime('%Y-%m')}")
        return

    if data == "export:month_income":
        csv_bytes, filename = build_month_csv_income_bytes()
        if not csv_bytes or len(csv_bytes) < 30:
            await context.bot.send_message(OWNER_CHAT_ID, "ℹ️ Немає даних для експорту за цей місяць (закритих замовлень).")
            return
        await _send_csv_to_owner(context, csv_bytes, filename, f"📤 CSV (дохід ФОП) за місяць: {datetime.now(TZ).strftime('%Y-%m')}")
        return

    if data == "couriers:list":
        load_dispatcher_users()
        if not COURIERS:
            await context.bot.send_message(OWNER_CHAT_ID, "👥 Кур’єрів ще немає.")
            return
        lines = []
        for cid in sorted(COURIERS):
            rec = DISPATCHER_USERS.get(str(cid)) or {}
            uname = rec.get("username", "-")
            name = rec.get("name", "-")
            lines.append(f"• `{cid}` — {name} (@{uname})")
        await context.bot.send_message(OWNER_CHAT_ID, "👥 **Мої кур’єри:**\n" + "\n".join(lines), parse_mode="Markdown")
        return

    if data == "users:list":
        load_dispatcher_users()
        if not DISPATCHER_USERS:
            await context.bot.send_message(OWNER_CHAT_ID, "🧾 Поки що нема користувачів (бот нікого не бачив у диспетчерській).")
            return
        users = list(DISPATCHER_USERS.values())
        users.sort(key=lambda x: x.get("last_seen", ""), reverse=True)
        lines = []
        for u in users[:200]:
            lines.append(f"• {u.get('name','-')} (@{u.get('username','-')}) — `{u.get('id')}`")
        await context.bot.send_message(
            OWNER_CHAT_ID,
            "🧾 **Користувачі диспетчерської (кого бот бачив):**\n\n" + "\n".join(lines),
            parse_mode="Markdown"
        )
        return

    if data == "owner:add":
        OWNER_PENDING[query.from_user.id] = "add"
        await context.bot.send_message(OWNER_CHAT_ID, "➕ Надішли **ID кур’єра** одним числом.\nНаприклад: `123456789`", parse_mode="Markdown")
        return

    if data == "owner:del":
        OWNER_PENDING[query.from_user.id] = "del"
        await context.bot.send_message(OWNER_CHAT_ID, "➖ Надішли **ID кур’єра**, якого треба видалити.\nНаприклад: `123456789`", parse_mode="Markdown")
        return

    if data == "owner:topup":
        OWNER_PENDING[query.from_user.id] = "topup"
        await context.bot.send_message(OWNER_CHAT_ID, "💳 Введи **ID і суму** через пробіл.\nПриклад: `123456789 500`", parse_mode="Markdown")
        return

    if data == "owner:withdraw":
        OWNER_PENDING[query.from_user.id] = "withdraw"
        await context.bot.send_message(OWNER_CHAT_ID, "➖ Введи **ID і суму** через пробіл.\nПриклад: `123456789 200`", parse_mode="Markdown")
        return

    if data == "owner:bal":
        OWNER_PENDING[query.from_user.id] = "bal"
        await context.bot.send_message(OWNER_CHAT_ID, "🔍 Введи **ID кур’єра**.\nПриклад: `123456789`", parse_mode="Markdown")
        return

# =========================
# ===== Finalize & close ==
# =========================
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
        COURIER_BALANCES[courier_id] = int(COURIER_BALANCES.get(courier_id, 0)) - int(final_fee)
        save_balances()
        new_balance = COURIER_BALANCES.get(courier_id, 0)
        ACTIVE_ORDERS_COUNT[courier_id] = max(0, ACTIVE_ORDERS_COUNT.get(courier_id, 1) - 1)

    add_stats(final_total, final_fee)

    try:
        if order.get("admin_chat_id") and order.get("admin_msg_id"):
            await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
    except Exception:
        pass

    dist_line = (
        f"{manual_dist:.2f} км (фінал без +10%)" if manual_dist is not None
        else f"{float(order.get('safe_dist_km', 0.0)):.2f} км (авто з +10%)"
    )

    snapshot = {
        "order_id": order_id,
        "time": datetime.now(TZ).strftime("%H:%M:%S"),
        "status": "closed",
        "courier_id": courier_id,
        "courier_name": order.get("courier_name", "-"),
        "customer_id": order.get("customer_id"),
        "customer_name": order.get("customer_name", "-"),
        "total": int(final_total),
        "fee": int(final_fee),
        "closed_by": closed_by,
    }
    add_to_archive_today(snapshot)

    if OWNER_CHAT_ID:
        report = (
            f"🏁 **ЗАМОВЛЕННЯ №{order_id} ЗАКРИТО**\n"
            f"👤 Клієнт: {order.get('customer_name','-')} (@{order.get('customer_username','-')})\n"
            f"🚚 Кур'єр: {order.get('courier_name','-')} (ID: `{courier_id}`)\n"
            f"📏 Дистанція: {dist_line}\n"
            f"🧾 Обіг по замовленню (не дохід): {final_total} грн\n"
            f"📈 **Дохід сервісу (комісія): {final_fee} грн**\n"
            + (f"💳 Баланс кур'єра (внутр. рахунок комісії): {new_balance} грн\n" if new_balance is not None else "")
            + f"🔒 Закрив: {closed_by}\n"
            + f"🕒 {datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await context.bot.send_message(OWNER_CHAT_ID, report, parse_mode="Markdown")

    if courier_id:
        try:
            await context.bot.send_message(
                courier_id,
                f"✅ №{order_id} закрито.\n📉 Комісія списана: {final_fee} грн.\n💳 Баланс (комісія): {new_balance} грн",
                reply_markup=courier_menu()
            )
        except Exception:
            pass

    order["status"] = "closed"
    ORDERS_DB.pop(order_id, None)
    save_orders_db()

async def final_km_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
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

    outside = bool(order.get("outside"))
    is_urgent = bool(order.get("is_urgent"))
    fee_rate = float(order.get("fee_rate") or JETWAY_FEE_RATE)

    total, fee, _, _used = calculate_finance_total(dist_real, is_urgent, outside, use_safety=False, fee_rate=fee_rate)

    await finalize_and_close_order(context, order_id, total, fee, manual_dist=dist_real, closed_by="кур'єр (вручну, без +10%)")

    await update.message.reply_text(
        f"✅ Замовлення №{order_id} фіналізовано.\n"
        f"📏 Фінальний км: {dist_real:.2f}\n"
        f"🧾 Обіг по замовленню: {total} грн\n"
        f"📈 Дохід сервісу (комісія): {fee} грн",
        reply_markup=courier_menu()
    )

# =========================
# ===== Complaints text ===
# =========================
async def complaint_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    if not is_private_chat(update):
        return

    uid = update.effective_user.id
    if uid not in COMPLAINT_PENDING:
        return

    order_id = COMPLAINT_PENDING.get(uid)
    text = update.message.text.strip()
    if len(text) < 3:
        return await update.message.reply_text("Опишіть проблему трохи детальніше (мінімум кілька слів).")

    COMPLAINT_PENDING.pop(uid, None)

    order = ORDERS_DB.get(order_id)
    rec = {
        "type": "complaint",
        "ts": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "order_id": order_id,
        "text": text,
        "courier_id": (order or {}).get("courier_id"),
        "courier_name": (order or {}).get("courier_name"),
        "customer_id": uid,
        "customer_name": (order or {}).get("customer_name", update.effective_user.full_name),
        "customer_username": (order or {}).get("customer_username", update.effective_user.username or "-"),
    }
    FEEDBACK_DB.append(rec)
    save_feedback()

    if OWNER_CHAT_ID:
        msg = (
            f"😡 **Скарга на кур’єра**\n\n"
            f"Замовлення: №{order_id}\n"
            f"Клієнт: {rec.get('customer_name','-')} (@{rec.get('customer_username','-')}) ID: `{rec.get('customer_id')}`\n"
            f"Кур’єр: {rec.get('courier_name','-')} ID: `{rec.get('courier_id')}`\n"
            f"Час: {rec.get('ts')}\n\n"
            f"Текст:\n{text}"
        )
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")

    await update.message.reply_text("✅ Дякую. Скаргу відправлено адміну.")

# =========================
# ===== Support contact ===
# =========================
async def support_contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    courier_id = update.effective_user.id

    order_id = None
    mode = None
    if courier_id in SUPPORT_CONTACT_PENDING:
        order_id = SUPPORT_CONTACT_PENDING.pop(courier_id, None)
        mode = "order"
    elif courier_id in COURIER_GENERAL_SUPPORT_PENDING:
        COURIER_GENERAL_SUPPORT_PENDING.pop(courier_id, None)
        mode = "general"
    else:
        return

    phone = update.message.contact.phone_number if update.message.contact else "-"

    u = update.effective_user
    if mode == "order":
        msg = (
            "🆘 **Техпідтримка від кур'єра (по замовленню)**\n\n"
            f"Кур'єр: {u.full_name} (@{u.username or '-'})\n"
            f"ID: `{u.id}`\n"
            f"Телефон: {phone}\n"
            f"Замовлення: №{order_id}\n"
        )
        kb = kb_owner_force_close(order_id)
    else:
        msg = (
            "🆘 **Техпідтримка від кур'єра (загальна)**\n\n"
            f"Кур'єр: {u.full_name} (@{u.username or '-'})\n"
            f"ID: `{u.id}`\n"
            f"Телефон: {phone}\n"
            f"Активних замовлень може не бути."
        )
        kb = None

    if OWNER_CHAT_ID:
        await context.bot.send_message(
            OWNER_CHAT_ID,
            msg,
            parse_mode="Markdown",
            reply_markup=kb
        )

    await update.message.reply_text("✅ Запит надіслано адміну. Очікуй дзвінок.", reply_markup=courier_menu())

# =========================
# ===== Courier menu router
# =========================
async def courier_menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message or not update.message.text:
        return False
    if not is_private_chat(update):
        return False

    uid = update.effective_user.id
    if uid not in COURIERS:
        return False

    text = update.message.text.strip()

    if text == "💳 Мій баланс":
        bal = int(COURIER_BALANCES.get(uid, 0))
        await update.message.reply_text(f"💳 Баланс (комісія): **{bal} грн**", parse_mode="Markdown", reply_markup=courier_menu())
        return True

    if text == "⭐ Мій рейтинг":
        load_courier_meta()
        ensure_courier_since(uid)
        since = get_courier_since_date(uid)

        avg_all, cnt_all = courier_rating(uid, days=None)
        avg30, cnt30 = courier_rating(uid, days=30)
        rate = courier_fee_rate(uid)

        today = datetime.now(TZ).date()
        days_active = (today - since).days if since else 0
        left = max(0, 30 - days_active)

        msg = (
            f"⭐ **Ваш рейтинг**\n\n"
            f"⭐ 30 днів: **{avg30:.2f}/5** (оцінок: {cnt30})\n"
            f"⭐ Всього: **{avg_all:.2f}/5** (оцінок: {cnt_all})\n\n"
            f"📅 Кур’єр з: {since.isoformat() if since else '-'}\n"
            + (f"⏳ До кінця 1-го місяця: {left} днів (комісія 25%)\n\n" if days_active < 30 else "\n")
            + f"💸 Поточна комісія: **{int(rate*100)}%**\n"
            "Умова: після 1-го місяця комісія 20%, якщо ⭐ за 30 днів ≥ 4.8"
        )
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=courier_menu())
        return True

    if text == "📦 Мої активні":
        ids = courier_active_order_ids(uid)
        if not ids:
            await update.message.reply_text("📦 Активних замовлень немає.", reply_markup=courier_menu())
            return True

        lines = []
        for oid in ids[:10]:
            o = ORDERS_DB.get(oid, {})
            lines.append(f"• №{oid} — {o.get('status','-')}\n  {o.get('from_addr','-')} → {o.get('to_addr','-')}")
        await update.message.reply_text("📦 Ваші активні замовлення:\n\n" + "\n\n".join(lines), reply_markup=courier_menu())
        return True

    if text == "🆘 Техпідтримка":
        ids = courier_active_order_ids(uid)
        if ids:
            await update.message.reply_text("🆘 Оберіть замовлення, по якому потрібна допомога:", reply_markup=kb_support_pick(ids))
            return True

        COURIER_GENERAL_SUPPORT_PENDING[uid] = True
        await update.message.reply_text(
            "🆘 Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.\n"
            "Якщо є проблема без замовлення — теж можна звертатися.",
            reply_markup=support_share_contact_kb()
        )
        return True

    if text == "⬅️ Клієнтське меню":
        await update.message.reply_text("Ок, показую клієнтське меню 👇", reply_markup=main_menu())
        return True

    return False

# =========================
# ===== Customer flow =====
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()

    if not is_private_chat(update):
        return ConversationHandler.END

    await update.message.reply_text("Привіт! Обери роль 👇", reply_markup=role_menu())
    return ROLE_CHOICE

async def role_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "🙋 Я клієнт":
        await update.message.reply_text("Ок! Меню клієнта 👇", reply_markup=main_menu())
        return CHOICE

    if text == "🚚 Я кур'єр":
        uid = update.effective_user.id
        if uid not in COURIERS:
            await update.message.reply_text("❌ Ви не є кур’єром. Зверніться до адміна.", reply_markup=role_menu())
            return ROLE_CHOICE
        await update.message.reply_text("✅ Меню кур’єра 👇", reply_markup=courier_menu())
        return CHOICE

    await update.message.reply_text("Обери кнопку 👇", reply_markup=role_menu())
    return ROLE_CHOICE

async def choice_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    handled = await courier_menu_router(update, context)
    if handled:
        return CHOICE

    text = (update.message.text or "").strip()

    if text == "🚚 Доставка":
        await update.message.reply_text("Ок! Обери тип доставки:", reply_markup=delivery_type_menu())
        return DELIV_TYPE

    if text == "💳 Тариф":
        await update.message.reply_text(tariff_text(), reply_markup=main_menu())
        return CHOICE

    if text == "🛠 Підтримка":
        await update.message.reply_text(support_text(), reply_markup=callme_kb())
        return CALLME_PHONE

    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Меню 👇", reply_markup=main_menu())
        return CHOICE

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
        await context.bot.send_message(chat_id=OWNER_CHAT_ID, text=msg)

    await update.message.reply_text("🙏 Дякую! Очікуйте дзвінок від оператора.", reply_markup=main_menu())
    return CHOICE

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
            "🕒 На котру годину?\nНапиши дату і час (наприклад: 27.02 14:30 або 27.02.2026 14:30).",
            reply_markup=back_only_kb()
        )
        return WHEN_INPUT

    await update.message.reply_text(addr_prompt_text("Звідки забрати?"), reply_markup=back_only_kb())
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
        await update.message.reply_text(addr_prompt_text("Звідки забрати?"), reply_markup=back_only_kb())
        return FROM_ADDR

    await update.message.reply_text("Обери кнопку 👇", reply_markup=when_confirm_kb())
    return WHEN_CONFIRM

async def from_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    addr = (update.message.text or "").strip()
    if not has_locality(addr):
        await update.message.reply_text(locality_hint(), reply_markup=back_only_kb())
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
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу звідки ще раз:"), reply_markup=back_only_kb())
        return FROM_ADDR

    if text == "✅ Підтвердити адресу":
        context.user_data["from_addr"] = context.user_data.get("from_addr_temp", "")
        if context.user_data.get("edit_mode") == "from":
            context.user_data.pop("edit_mode", None)
            await show_preconfirm_summary(update, context)
            return CONFIRM_ORDER

        await update.message.reply_text(addr_prompt_text("Куди доставити?"), reply_markup=back_only_kb())
        return TO_ADDR

    await update.message.reply_text("Обери кнопку 👇", reply_markup=addr_confirm_kb())
    return CONFIRM_FROM

async def to_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text("Повертаю в меню 👇", reply_markup=main_menu())
        return CHOICE

    addr = (update.message.text or "").strip()
    if not has_locality(addr):
        await update.message.reply_text(locality_hint(), reply_markup=back_only_kb())
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
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу куди ще раз:"), reply_markup=back_only_kb())
        return TO_ADDR

    if text == "✅ Підтвердити адресу":
        context.user_data["to_addr"] = context.user_data.get("to_addr_temp", "")
        if context.user_data.get("edit_mode") == "to":
            context.user_data.pop("edit_mode", None)
            await show_preconfirm_summary(update, context)
            return CONFIRM_ORDER

        await update.message.reply_text("Що веземо? (коротко: пакунок/їжа/документи)", reply_markup=back_only_kb())
        return ITEM

    await update.message.reply_text("Обери кнопку 👇", reply_markup=addr_confirm_kb())
    return CONFIRM_TO

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

    from_coords = await geocode_address_google(from_addr_v, session)
    to_coords = await geocode_address_google(to_addr_v, session)
    dist_km = get_distance_km(from_coords, to_coords)

    outside = is_outside_obukhiv(from_addr_v, to_addr_v)
    total, fee, cut, used_dist = calculate_finance_total(dist_km, is_urgent, outside, use_safety=True, fee_rate=JETWAY_FEE_RATE)

    data["from_coords"] = from_coords
    data["to_coords"] = to_coords
    data["dist_km"] = dist_km
    data["safe_dist_km"] = used_dist
    data["total_price"] = total
    data["fee"] = fee
    data["courier_cut"] = cut
    data["is_outside"] = outside

    outside_text = "🏘️ Поза містом" if outside else "🏢 По місту"
    when_line = data.get("scheduled_when", "терміново")

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
        f"📏 Відстань (Google): {dist_km:.2f} км\n"
        f"🧾 **Обіг по замовленню: {total} грн**\n"
        "------------------------\n"
        "ℹ️ Попередня сума включає **+10% запас** через неточності Google Maps.\n"
        "Після доставки сума може бути **меншою**, якщо фінальний кілометраж підтвердиться.\n\n"
        "Якщо все вірно — натисніть «✅ Підтвердити» 👇"
    )

    await update.message.reply_text(summary, reply_markup=order_confirm_kb(), parse_mode="Markdown")

async def confirm_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text in ("⬅️ Назад в меню", "❌ Скасувати"):
        await update.message.reply_text("Скасовано. Меню 👇", reply_markup=main_menu())
        context.user_data.clear()
        return CHOICE

    if text == "📍 Змінити звідки":
        context.user_data["edit_mode"] = "from"
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу звідки ще раз:"), reply_markup=back_only_kb())
        return FROM_ADDR

    if text == "🎯 Змінити куди":
        context.user_data["edit_mode"] = "to"
        await update.message.reply_text(addr_prompt_text("Ок, введи адресу куди ще раз:"), reply_markup=back_only_kb())
        return TO_ADDR

    if text == "💬 Змінити коментар":
        await update.message.reply_text("Напиши новий коментар або «-»", reply_markup=back_only_kb())
        return COMMENT

    if text != "✅ Підтвердити":
        await update.message.reply_text("Обери кнопку 👇", reply_markup=order_confirm_kb())
        return CONFIRM_ORDER

    data = context.user_data
    user = update.effective_user
    order_id = gen_order_id()

    from_addr_v = data.get("from_addr", "")
    to_addr_v = data.get("to_addr", "")
    from_coords = data.get("from_coords")
    to_coords = data.get("to_coords")
    dist_km = float(data.get("dist_km") or 0.0)
    safe_dist_km = float(data.get("safe_dist_km") or 0.0)

    is_urgent = data.get("delivery_type_key") == "urgent"
    total = int(data.get("total_price") or 0)
    fee = int(data.get("fee") or 0)
    outside = bool(data.get("is_outside"))

    when_line = data.get("scheduled_when", "терміново")
    outside_text = "🏘️ Поза містом" if outside else "🏢 По місту"

    ORDERS_DB[order_id] = {
        "order_id": order_id,
        "status": "searching",
        "created_ts": float(time.time()),
        "created_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": user.id,
        "customer_name": user.full_name,
        "customer_username": user.username or "-",
        "customer_chat_id": update.effective_chat.id,
        "delivery_label": data.get("delivery_type_label", "-"),
        "delivery_key": data.get("delivery_type_key", "-"),
        "scheduled_when": when_line,
        "from_addr": from_addr_v,
        "to_addr": to_addr_v,
        "from_coords": from_coords,
        "to_coords": to_coords,
        "dist_km": dist_km,
        "safe_dist_km": safe_dist_km,
        "total": total,
        "fee": fee,
        "fee_rate": float(JETWAY_FEE_RATE),  # буде оновлено при accept кур’єром
        "outside": outside,
        "item": data.get("item", "-"),
        "phone": data.get("phone") or "не вказано",
        "comment": data.get("comment", "-"),
        "courier_id": None,
        "courier_name": None,
        "admin_chat_id": dispatcher_chat_id(),
        "admin_msg_id": None,
        "is_urgent": is_urgent,
    }
    save_orders_db()

    msg_to_couriers = (
        f"🚚 **НОВЕ ЗАМОВЛЕННЯ №{order_id}**\n"
        f"{outside_text}\n"
        f"🕒 Час: {when_line}\n"
        f"📍 Звідки: {from_addr_v}\n"
        f"🧭 Коорд. звідки: {fmt_coords(from_coords)}\n"
        f"🗺️ Лінк звідки: {fmt_link(from_coords)}\n"
        f"🎯 Куди: {to_addr_v}\n"
        f"🧭 Коорд. куди: {fmt_coords(to_coords)}\n"
        f"🗺️ Лінк куди: {fmt_link(to_coords)}\n"
        f"📦 Що: {data.get('item','-')}\n"
        f"📞 Тел: {data.get('phone') or 'не вказано'}\n"
        f"💬 Коментар: {data.get('comment','-')}\n"
        "------------------------\n"
        f"📏 Відстань (Google): {dist_km:.2f} км\n"
        f"🧾 Обіг (preview +10%): **{total} грн**\n"
    )

    dispatcher_chat = ORDERS_DB[order_id]["admin_chat_id"]
    if dispatcher_chat:
        sent = await context.bot.send_message(
            chat_id=dispatcher_chat,
            text=msg_to_couriers,
            reply_markup=kb_accept(order_id),
            parse_mode="Markdown"
        )
        ORDERS_DB[order_id]["admin_msg_id"] = sent.message_id
        save_orders_db()

    if OWNER_CHAT_ID:
        await context.bot.send_message(
            chat_id=OWNER_CHAT_ID,
            text=(
                f"📥 Нове замовлення №{order_id}\n"
                f"Клієнт: {user.full_name} (@{user.username or '-'})\n"
                f"Обіг (preview +10%): {total} грн | Комісія (попер.): {fee} грн"
            ),
            reply_markup=kb_owner_force_close(order_id)
        )

    # ✅ Плануємо нагадування + авто-скасування
    try:
        schedule_order_timers(context.application, order_id)
    except Exception:
        pass

    # ✅ Клієнту одразу показуємо платіжні реквізити + призначення
    pay = fmt_payment_block(order_id)
    autocancel_info = f"\n⏳ Якщо кур’єр не знайдеться за **{ORDER_AUTO_CANCEL_MIN} хв** — замовлення скасується автоматично.\n"

    await update.message.reply_text(
        "✅ Ваше замовлення відправлено кур’єрам. Очікуйте підтвердження."
        + (autocancel_info if ORDER_AUTO_CANCEL_MIN > 0 else "")
        + (pay if pay else ""),
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )
    context.user_data.clear()
    return CHOICE

# =========================
# ===== Callback router ===
# =========================
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = (query.data or "").strip()

    if data.startswith(("stats:", "archive:", "owner:", "couriers:", "users:", "export:", "fbadmin:")):
        return await owner_panel_callback(update, context)

    if data.startswith("fb:"):
        await query.answer()
        parts = data.split(":")
        action = parts[1] if len(parts) > 1 else ""

        if action == "rate_menu":
            order_id = parts[2]
            order = ORDERS_DB.get(order_id)
            if not order:
                return await query.answer("Замовлення не знайдено.", show_alert=True)
            if query.from_user.id != order.get("customer_id"):
                return await query.answer("Оцінити може тільки замовник.", show_alert=True)

            load_feedback()
            if feedback_has_rating(order_id, query.from_user.id):
                return await query.answer("Ви вже оцінювали це замовлення ✅", show_alert=True)

            try:
                await query.edit_message_text(
                    f"⭐ Оцініть кур’єра за замовлення №{order_id}:",
                    reply_markup=kb_rating_stars(order_id)
                )
            except Exception:
                await context.bot.send_message(
                    chat_id=query.from_user.id,
                    text=f"⭐ Оцініть кур’єра за замовлення №{order_id}:",
                    reply_markup=kb_rating_stars(order_id)
                )
            return

        if action == "rate":
            order_id = parts[2]
            stars = int(parts[3])

            if stars < 1 or stars > 5:
                return await query.answer("Некоректна оцінка.", show_alert=True)

            order = ORDERS_DB.get(order_id)
            if not order:
                return await query.answer("Замовлення не знайдено.", show_alert=True)
            if query.from_user.id != order.get("customer_id"):
                return await query.answer("Оцінити може тільки замовник.", show_alert=True)

            load_feedback()
            if feedback_has_rating(order_id, query.from_user.id):
                return await query.answer("Ви вже оцінювали ✅", show_alert=True)

            rec = {
                "type": "rating",
                "ts": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "order_id": order_id,
                "stars": stars,
                "courier_id": order.get("courier_id"),
                "courier_name": order.get("courier_name"),
                "customer_id": order.get("customer_id"),
                "customer_name": order.get("customer_name"),
                "customer_username": order.get("customer_username"),
            }
            FEEDBACK_DB.append(rec)
            save_feedback()

            if OWNER_CHAT_ID:
                msg = (
                    f"⭐ **Оцінка кур’єра**\n\n"
                    f"Замовлення: №{order_id}\n"
                    f"Клієнт: {rec.get('customer_name','-')} (@{rec.get('customer_username','-')}) ID: `{rec.get('customer_id')}`\n"
                    f"Кур’єр: {rec.get('courier_name','-')} ID: `{rec.get('courier_id')}`\n"
                    f"Оцінка: **{stars}/5**\n"
                    f"Час: {rec.get('ts')}"
                )
                await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")

            try:
                await query.edit_message_text(f"✅ Дякую! Оцінку {stars}/5 збережено.")
            except Exception:
                pass
            return

        if action == "rate_cancel":
            try:
                await query.edit_message_text("Скасовано ✅")
            except Exception:
                pass
            return

        if action == "complain":
            order_id = parts[2]
            order = ORDERS_DB.get(order_id)
            if not order:
                return await query.answer("Замовлення не знайдено.", show_alert=True)
            if query.from_user.id != order.get("customer_id"):
                return await query.answer("Скаргу може подати тільки замовник.", show_alert=True)

            load_feedback()
            if feedback_has_complaint(order_id, query.from_user.id):
                return await query.answer("Скарга по цьому замовленню вже є ✅", show_alert=True)

            COMPLAINT_PENDING[query.from_user.id] = order_id
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text=(
                    f"😡 Опишіть проблему текстом по замовленню №{order_id}.\n\n"
                    "Після цього повідомлення скарга піде адміну."
                )
            )
            try:
                await query.edit_message_text("✍️ Напишіть скаргу текстом у чаті з ботом.")
            except Exception:
                pass
            return

        return

    if data.startswith("courier:make:"):
        await query.answer()
        if query.message.chat.id != OWNER_CHAT_ID:
            return await query.answer("Недостатньо прав.", show_alert=True)
        uid = int(data.split(":")[-1])
        COURIERS.add(uid)
        save_couriers()
        ensure_courier_since(uid)
        await query.answer("✅ Зроблено кур’єром!", show_alert=False)
        try:
            await context.bot.send_message(uid, "✅ Вас зроблено кур’єром. Напишіть /start.")
        except Exception:
            pass
        return

    if data == "noop":
        await query.answer()
        return

    if data.startswith("force:"):
        await query.answer()
        if query.message.chat.id != OWNER_CHAT_ID:
            return await query.answer("Недостатньо прав.", show_alert=True)

        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено.", show_alert=True)

        # якщо force — прибираємо авто-таймери
        try:
            cancel_order_timers(context.application, order_id)
        except Exception:
            pass

        final_total = int(order.get("total") or 0)
        final_fee = int(order.get("fee") or 0)

        await finalize_and_close_order(context, order_id, final_total, final_fee, manual_dist=None, closed_by="овнер (force)")
        try:
            await query.edit_message_text(f"🛠 Замовлення №{order_id} закрито вручну.")
        except Exception:
            pass
        return

    if data.startswith("support_pick:"):
        await query.answer()
        val = data.split(":", 1)[1]
        if val == "cancel":
            try:
                await query.edit_message_text("Скасовано ✅")
            except Exception:
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
            text="🆘 Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.",
            reply_markup=support_share_contact_kb()
        )
        try:
            await query.edit_message_text(f"🆘 Запит по №{order_id} створено. Надішліть номер в особисті боту.")
        except Exception:
            pass
        return

    if data.startswith("accept:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено або вже закрите.", show_alert=True)

        courier_id = query.from_user.id

        if courier_id not in COURIERS:
            return await query.answer("❌ Ви не додані як кур'єр.", show_alert=True)

        if order.get("courier_id") is not None:
            return await query.answer("Це замовлення вже забрав інший кур'єр.", show_alert=True)

        if ACTIVE_ORDERS_COUNT.get(courier_id, 0) >= 3:
            return await query.answer("❌ У вас вже 3 активних замовлення!", show_alert=True)

        load_courier_meta()
        ensure_courier_since(courier_id)

        fee_rate = courier_fee_rate(courier_id)
        order["fee_rate"] = float(fee_rate)
        order["fee"] = int(math.ceil(int(order.get("total") or 0) * float(fee_rate)))

        balance_now = int(COURIER_BALANCES.get(courier_id, 0))
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
        order["status"] = "accepted"
        ACTIVE_ORDERS_COUNT[courier_id] = ACTIVE_ORDERS_COUNT.get(courier_id, 0) + 1
        save_orders_db()

        # ✅ як тільки взяли — скасовуємо reminder/cancel
        try:
            cancel_order_timers(context.application, order_id)
        except Exception:
            pass

        try:
            await context.bot.edit_message_text(
                chat_id=order["admin_chat_id"],
                message_id=order["admin_msg_id"],
                text=(query.message.text + f"\n\n✅ **ВЗЯВ:** {query.from_user.full_name}\n💸 Комісія цього кур’єра: {int(fee_rate*100)}%"),
                parse_mode="Markdown",
                reply_markup=kb_taken()
            )
        except Exception:
            pass

        try:
            await context.bot.send_message(
                chat_id=courier_id,
                text=(
                    f"🚚 Ви взяли замовлення №{order_id}\n\n"
                    "Коли доставите — натисніть «✅ Доставлено».\n"
                    "Якщо проблема — тисніть «🆘 Техпідтримка»."
                ),
                reply_markup=kb_courier_controls(order_id)
            )
        except Exception:
            pass

        # ✅ клієнту підтвердження + реквізити/призначення
        try:
            await context.bot.send_message(
                chat_id=order.get("customer_chat_id"),
                text=(
                    f"✅ Кур’єр прийняв замовлення №{order_id}.\n"
                    f"🚚 Кур’єр: {order.get('courier_name','-')}\n"
                    + (fmt_payment_block(order_id) if PAYMENT_CARD else "")
                ),
                parse_mode="Markdown"
            )
        except Exception:
            pass

        return

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
        save_orders_db()

        try:
            if order.get("admin_chat_id") and order.get("admin_msg_id"):
                await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
        except Exception:
            pass

        snap = {
            "order_id": order_id,
            "time": datetime.now(TZ).strftime("%H:%M:%S"),
            "status": "delivered",
            "courier_id": courier_id,
            "courier_name": order.get("courier_name", "-"),
            "total": int(order.get("total") or 0),
        }
        add_to_archive_today(snap)

        if OWNER_CHAT_ID:
            await context.bot.send_message(
                OWNER_CHAT_ID,
                (
                    f"📦 **ДОСТАВЛЕНО (очікуємо підтвердження клієнта) №{order_id}**\n"
                    f"🚚 Кур'єр: {order.get('courier_name','-')} (ID: `{courier_id}`)\n"
                    f"📍 {order.get('from_addr','-')} → {order.get('to_addr','-')}\n"
                    f"🧾 Обіг (preview+10%): {order.get('total','-')} грн"
                ),
                parse_mode="Markdown"
            )

        customer_id = order.get("customer_id")
        try:
            await context.bot.send_message(
                chat_id=customer_id,
                text=(
                    f"🚚 Ваше замовлення №{order_id} доставлено кур’єром.\n"
                    "Натисніть кнопку нижче, коли отримаєте замовлення:"
                ),
                reply_markup=kb_customer_done(order_id)
            )
        except Exception:
            pass

        try:
            await query.edit_message_text(f"✅ Позначено як доставлено: №{order_id}")
        except Exception:
            pass
        return

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
            text="🆘 Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.",
            reply_markup=support_share_contact_kb()
        )
        return

    if data.startswith("done:"):
        await query.answer()
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await query.answer("Замовлення не знайдено або вже закрите.", show_alert=True)

        if query.from_user.id != order.get("customer_id"):
            return await query.answer("❌ Підтвердити може тільки замовник!", show_alert=True)

        if order.get("status") != "await_customer":
            return await query.answer("Ще зарано підтверджувати 🙂", show_alert=True)

        try:
            await query.edit_message_text("Дякуємо за замовлення! 😊")
        except Exception:
            pass

        order["status"] = "await_finish"
        save_orders_db()

        courier_id = order.get("courier_id")
        if courier_id:
            await context.bot.send_message(
                chat_id=courier_id,
                text=(
                    f"🏁 Клієнт підтвердив отримання №{order_id}.\n\n"
                    "Підтвердіть фінальну суму:"
                ),
                reply_markup=finalize_kb(order_id)
            )

        try:
            await context.bot.send_message(
                chat_id=query.from_user.id,
                text="🙏 Дякую! Можеш оцінити кур’єра або написати скаргу (якщо є проблема):",
                reply_markup=kb_customer_feedback(order_id)
            )
        except Exception:
            pass

        return

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

        await finalize_and_close_order(context, order_id, final_total, final_fee, manual_dist=None, closed_by="кур'єр (авто, з +10%)")
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
            text="✏️ Введіть фінальний кілометраж одним числом (наприклад: 5.7)",
            reply_markup=courier_menu()
        )
        return

    await query.answer("Невідома дія", show_alert=True)

# =========================
# ===== App lifecycle =====
# =========================
async def post_init(app: Application):
    app.bot_data["http"] = aiohttp.ClientSession()

    # ✅ після рестарту — відновлюємо таймери для "searching"
    try:
        for oid, o in list(ORDERS_DB.items()):
            if (o or {}).get("status") == "searching":
                schedule_order_timers(app, oid)
    except Exception:
        pass

    if OWNER_CHAT_ID:
        try:
            await app.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text="✅ Бот запущено. Натисніть /panel 👇",
                reply_markup=owner_quick_kb()
            )
            await app.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"💾 DATA_DIR: {DATA_DIR}"
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
    app = Application.builder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()

    load_couriers()
    load_balances()
    load_daily_stats()
    load_archive()
    load_dispatcher_users()
    load_feedback()
    load_courier_meta()
    load_orders_db()

    # join notifications (диспетчерська)
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

    # complaint text input (customers) - quick exit if not pending
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, complaint_text_handler), group=2)

    # manual km input (couriers) - quick exit if not pending
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, final_km_input_handler), group=3)

    # courier support phone + general support phone
    app.add_handler(MessageHandler(filters.CONTACT, support_contact_handler), group=0)

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ROLE_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, role_choice)],

            CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, choice_router)],

            CALLME_PHONE: [
                MessageHandler(filters.CONTACT, callme_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, callme_handler),
            ],

            DELIV_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_type)],

            WHEN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, when_input)],
            WHEN_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, when_confirm)],

            FROM_ADDR: [MessageHandler(filters.TEXT & ~filters.COMMAND, from_addr)],
            CONFIRM_FROM: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_from)],

            TO_ADDR: [MessageHandler(filters.TEXT & ~filters.COMMAND, to_addr)],
            CONFIRM_TO: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_to)],

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
    app.add_handler(conv, group=4)

    return app

def main():
    if not TOKEN:
        raise RuntimeError("Не знайдено BOT_TOKEN.")
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError("Не знайдено GOOGLE_MAPS_API_KEY.")
    if not COURIER_GROUP_ID:
        raise RuntimeError("Не знайдено COURIER_GROUP_ID.")
    if not OWNER_CHAT_ID:
        raise RuntimeError("Не знайдено OWNER_CHAT_ID.")

    application = build_app()
    application.run_polling(allowed_updates=Update.ALL_TYPES, close_loop=True)

if __name__ == "__main__":
    main()
