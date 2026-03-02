# =========================
# Jetway Delivery Bot (WORKING)
# python-telegram-bot 21.6+
# =========================
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
# ===== ENV / CONFIG ======
# =========================
TOKEN = os.getenv("BOT_TOKEN")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

COURIER_GROUP_ID = int(os.getenv("COURIER_GROUP_ID", "0"))   # диспетчерська група
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0"))         # овнер група/чат
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))         # сумісність (старе)

SUPPORT_PHONE_1 = "+380968130807"
SUPPORT_PHONE_2 = "+380687294365"

PAYMENT_CARD = os.getenv("PAYMENT_CARD", "4441 1110 2610 2602").strip()
PAYMENT_RECEIVER_NAME = os.getenv("PAYMENT_RECEIVER_NAME", "").strip()

def payment_purpose(order_id: str) -> str:
    return f"Оплата доставки №{order_id}"

ORDER_AUTO_CANCEL_MIN = int(os.getenv("ORDER_AUTO_CANCEL_MIN", "20"))
REMINDER_BEFORE_CANCEL_MIN = int(os.getenv("REMINDER_BEFORE_CANCEL_MIN", "5"))

PRICE_SCHEDULED_BASE = 110
PRICE_URGENT_BASE = 170
BASE_KM = 2.0
EXTRA_KM_PRICE = 23.0
EXTRA_OUTSIDE_OBUKHIV_PER_KM = 8.0

JETWAY_FEE_RATE = 0.25
MIN_RATINGS_30D = 5  # мінімум оцінок за 30 днів, щоб комісія стала 20%
DIST_SAFETY_MULT = 1.10
MIN_NEGATIVE_BALANCE = -50

PURCHASE_DEPOSIT_RATE = 0.25
PURCHASE_DEPOSIT_MIN = 150
PURCHASE_DEPOSIT_MAX = 500

WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "10"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "22"))

def working_hours_notice() -> str:
    return f"ℹ️ Доставки працюють **{WORK_START_HOUR:02d}:00–{WORK_END_HOUR:02d}:00**.\n🌙 Нічні замовлення не приймаються."

try:
    TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    TZ = timezone(timedelta(hours=2))

def _now_local() -> datetime:
    return datetime.now(TZ)

def is_within_working_hours(dt: datetime) -> bool:
    start = dt.replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
    end = dt.replace(hour=WORK_END_HOUR, minute=0, second=0, microsecond=0)
    return start <= dt < end

def parse_scheduled_datetime(text: str) -> Optional[datetime]:
    t = (text or "").strip()
    if not t:
        return None
    t = t.replace("/", ".")
    fmts = ["%d.%m.%Y %H:%M", "%d.%m %H:%M"]
    for f in fmts:
        try:
            dt = datetime.strptime(t, f)
            if f == "%d.%m %H:%M":
                dt = dt.replace(year=_now_local().year)
            return dt.replace(tzinfo=None)
        except Exception:
            continue
    return None

# =========================
# ===== Data dir ==========
# =========================
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
ACTIVE_ORDERS_COUNT: Dict[int, int] = {}
ORDERS_DB: Dict[str, dict] = {}

OWNER_PENDING: Dict[int, str] = {}
SUPPORT_CONTACT_PENDING: Dict[int, str] = {}
COURIER_GENERAL_SUPPORT_PENDING: Dict[int, bool] = {}
FINAL_KM_PENDING: Dict[int, str] = {}

DAILY_STATS = {"date": "", "orders": 0, "revenue": 0, "profit": 0}
DAILY_ARCHIVE: Dict[str, List[dict]] = {}
DISPATCHER_USERS: Dict[str, dict] = {}

FEEDBACK_DB: List[dict] = []
COMPLAINT_PENDING: Dict[int, str] = {}
COURIER_META: Dict[str, dict] = {}

ORDER_JOBS: Dict[str, Dict[str, str]] = {}
RECEIPT_PHOTO_PENDING: Dict[int, str] = {}  # courier_id -> order_id

# =========================
# ===== States ============
# =========================
(
    ROLE_CHOICE,
    CHOICE,
    ORDER_KIND,
    DELIV_TYPE,
    WHEN_INPUT,
    WHEN_CONFIRM,
    FROM_ADDR,
    CONFIRM_FROM,
    TO_ADDR,
    CONFIRM_TO,
    PURCHASE_ITEMS,
    PURCHASE_EST_SUM,
    PURCHASE_DEPOSIT_CONFIRM,
    PICKUP_INFO,
    PHONE,
    COMMENT,
    CONFIRM_ORDER,
    CALLME_PHONE,
) = range(18)

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

def find_order_snapshot(order_id: str) -> Optional[dict]:
    o = ORDERS_DB.get(order_id)
    if o:
        return {
            "order_id": o.get("order_id"),
            "courier_id": o.get("courier_id"),
            "courier_name": o.get("courier_name"),
            "customer_id": o.get("customer_id"),
            "customer_name": o.get("customer_name"),
            "customer_username": o.get("customer_username"),
        }

    load_archive()
    for _, items in (DAILY_ARCHIVE or {}).items():
        for it in (items or []):
            if str(it.get("order_id")) == str(order_id):
                return {
                    "order_id": it.get("order_id"),
                    "courier_id": it.get("courier_id"),
                    "courier_name": it.get("courier_name"),
                    "customer_id": it.get("customer_id"),
                    "customer_name": it.get("customer_name"),
                    "customer_username": "-",
                }
    return None

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
                f = int(math.ceil(t * JETWAY_FEE_RATE))
            f = int(f)
            orders += 1
            revenue += t
            profit += f
    direct_to_courier = revenue - profit
    avg_check = int(round(revenue / orders)) if orders else 0
    return {"orders": orders, "revenue": revenue, "profit": profit, "direct": direct_to_courier, "avg_check": avg_check}

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
    since = get_courier_since_date(cid)
    today = datetime.now(TZ).date()
    if not since:
        return 0.25

    days_active = (today - since).days
    if days_active < 30:
        return 0.25

    avg30, cnt30 = courier_rating(cid, days=30)
    if cnt30 >= MIN_RATINGS_30D and avg30 >= 4.8:
        return 0.20

    return 0.25

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
MAIN_MENU_INFO = "🕘 Доставки виконуються щодня з 10:00 до 22:00\n🌙 Нічні замовлення не приймаються"
MAIN_MENU_TEXT = "Оберіть дію:\n\n" + MAIN_MENU_INFO

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
        resize_keyboard=True,
    )

def order_kind_menu():
    return ReplyKeyboardMarkup(
        [
            ["🟡 Викуп кур’єром (магазин без онлайн-оплати)"],
            ["✅ Забрати готове (вже оплачено)"],
            ["⬅️ Назад в меню"],
        ],
        resize_keyboard=True,
    )

def delivery_type_menu():
    return ReplyKeyboardMarkup([["⏰ На певний час", "⚡ Термінова"], ["⬅️ Назад в меню"]], resize_keyboard=True)

def back_only_kb():
    return ReplyKeyboardMarkup([["⬅️ Назад в меню"]], resize_keyboard=True)

def phone_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📱 Поділитись контактом", request_contact=True)],
            ["Пропустити"],
            ["⬅️ Назад в меню"],
        ],
        resize_keyboard=True,
    )

def callme_kb():
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📞 Зателефонуйте мені", request_contact=True)],
            ["⬅️ Назад в меню"],
        ],
        resize_keyboard=True,
    )

def addr_confirm_kb():
    return ReplyKeyboardMarkup([["✅ Підтвердити адресу", "✏️ Змінити адресу"], ["⬅️ Назад в меню"]], resize_keyboard=True)

def when_confirm_kb():
    return ReplyKeyboardMarkup([["✅ Підтвердити час", "✏️ Змінити час"], ["⬅️ Назад в меню"]], resize_keyboard=True)

def order_confirm_kb():
    return ReplyKeyboardMarkup(
        [
            ["✅ Підтвердити", "❌ Скасувати"],
            ["📍 Змінити звідки", "🎯 Змінити куди"],
            ["💬 Змінити коментар"],
            ["⬅️ Назад в меню"],
        ],
        resize_keyboard=True,
    )

def deposit_confirm_kb():
    return ReplyKeyboardMarkup([["✅ Я оплатив гарантію"], ["✏️ Змінити суму"], ["⬅️ Назад в меню"]], resize_keyboard=True)

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
    return InlineKeyboardMarkup(
        [
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
            [InlineKeyboardButton("🔍 Баланс кур’єра", callback_data="owner:bal")],
        ]
    )

def kb_make_courier(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("➕ Зробити кур’єром", callback_data=f"courier:make:{user_id}")]])

# =========================
# ===== Geocoding =========
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
    fee_rate: float = JETWAY_FEE_RATE,
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
    return InlineKeyboardMarkup([[InlineKeyboardButton("🚚 Взяти замовлення", callback_data=f"accept:{order_id}")]])

def kb_taken() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ ВЗЯТО", callback_data="noop")]])

def kb_courier_controls(order_id: str, order_kind_key: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("✅ Доставлено", callback_data=f"delivered:{order_id}")]]
    if order_kind_key == "purchase":
        rows.append([InlineKeyboardButton("🧾 Надіслати фото чека", callback_data=f"receipt:{order_id}")])
    rows.append([InlineKeyboardButton("🆘 Техпідтримка", callback_data=f"support:{order_id}")])
    return InlineKeyboardMarkup(rows)

def kb_customer_done(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ Отримав", callback_data=f"done:{order_id}")]])

def finalize_kb(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Ціна вірна (авто, з +10%)", callback_data=f"finish_auto:{order_id}")],
            [InlineKeyboardButton("✏️ Ввести фінальний км (без +10%)", callback_data=f"finish_manual:{order_id}")],
        ]
    )

def kb_owner_force_close(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🛠 Закрити вручну", callback_data=f"force:{order_id}")]])

def support_share_contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[KeyboardButton("📱 Поділитись номером", request_contact=True)], ["⬅️ Назад в меню"]], resize_keyboard=True)

def kb_support_pick(orders: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for oid in orders[:10]:
        rows.append([InlineKeyboardButton(f"🆘 №{oid}", callback_data=f"support_pick:{oid}")])
    rows.append([InlineKeyboardButton("❌ Скасувати", callback_data="support_pick:cancel")])
    return InlineKeyboardMarkup(rows)

def kb_customer_feedback(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⭐ Оцінити кур’єра", callback_data=f"fb:rate_menu:{order_id}")],
            [InlineKeyboardButton("😡 Поскаржитись", callback_data=f"fb:complain:{order_id}")],
        ]
    )

def kb_rating_stars(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1", callback_data=f"fb:rate:{order_id}:1"),
                InlineKeyboardButton("2", callback_data=f"fb:rate:{order_id}:2"),
                InlineKeyboardButton("3", callback_data=f"fb:rate:{order_id}:3"),
                InlineKeyboardButton("4", callback_data=f"fb:rate:{order_id}:4"),
                InlineKeyboardButton("5", callback_data=f"fb:rate:{order_id}:5"),
            ],
            [InlineKeyboardButton("❌ Скасувати", callback_data=f"fb:rate_cancel:{order_id}")],
        ]
    )

# =========================
# ===== Utils =============
# =========================
def gen_order_id() -> str:
    return str(int(time.time() * 1000))[-8:]

def fmt_payment_block(order_id: str) -> str:
    if not PAYMENT_CARD:
        return ""
    receiver = f"{PAYMENT_RECEIVER_NAME}\n" if PAYMENT_RECEIVER_NAME else ""
    return (
        "\n💳 **Оплата**\n"
        f"{receiver}"
        f"Картка: {PAYMENT_CARD}\n"
        f"Призначення: {payment_purpose(order_id)}\n"
    )

def calc_purchase_deposit(estimated_sum: int) -> int:
    raw = int(math.ceil(float(estimated_sum) * PURCHASE_DEPOSIT_RATE))
    return int(max(PURCHASE_DEPOSIT_MIN, min(PURCHASE_DEPOSIT_MAX, raw)))

def order_kind_label(kind_key: str) -> str:
    if kind_key == "purchase":
        return "🟡 ВИКУП • ПОТРІБЕН ЧЕК"
    return "✅ ОПЛАЧЕНО • ЗАБРАТИ ГОТОВЕ"

def dispatcher_chat_id() -> int:
    return COURIER_GROUP_ID or ADMIN_CHAT_ID

def is_private_chat(update: Update) -> bool:
    try:
        return update.effective_chat and update.effective_chat.type == ChatType.PRIVATE
    except Exception:
        return False

def courier_active_order_ids(courier_id: int) -> List[str]:
    ids = []
    for oid, o in ORDERS_DB.items():
        if o.get("courier_id") == courier_id and o.get("status") not in ("closed",):
            if o.get("status") in ("accepted", "await_customer", "await_finish"):
                ids.append(oid)
    return ids

def _safe_md(s: str) -> str:
    return (s or "").replace("_", "\\_").replace("*", "\\*").replace("`", "\\`")

def _order_summary_for_customer(d: dict) -> str:
    kind = order_kind_label(d.get("order_kind_key", "pickup_ready"))
    deliv = "⚡ Термінова" if d.get("is_urgent") else "⏰ На певний час"
    when_line = ""
    if not d.get("is_urgent"):
        when_line = f"🕒 Час: {d.get('when_text','-')}\n"
    pickup_info = d.get("pickup_info") or "-"
    items_line = ""
    deposit_line = ""
    if d.get("order_kind_key") == "purchase":
        items_line = f"🧾 Список: {d.get('purchase_items','-')}\n"
        deposit_line = f"🔒 Гарантія: {int(d.get('purchase_deposit') or 0)} грн\n"
    phone_line = f"📞 Телефон: {d.get('phone','-')}\n" if d.get("phone") else ""
    comment_line = f"💬 Коментар: {d.get('comment','-')}\n" if d.get("comment") else ""
    return (
        f"🧾 **Підсумок замовлення**\n\n"
        f"🧩 Тип: {kind}\n"
        f"📦 Доставка: {deliv}\n"
        f"{when_line}"
        f"📍 Звідки: {_safe_md(d.get('from_addr','-'))}\n"
        f"🎯 Куди: {_safe_md(d.get('to_addr','-'))}\n"
        f"ℹ️ Що забрати/деталі: {_safe_md(pickup_info)}\n"
        f"{items_line}"
        f"{deposit_line}"
        f"{phone_line}"
        f"{comment_line}"
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
    order = ORDERS_DB.get(order_id)
    if not order or order.get("status") != "searching":
        return
    created_ts = float(order.get("created_ts") or time.time())
    now_ts = time.time()
    elapsed = max(0.0, now_ts - created_ts)

    cancel_after = max(0.0, ORDER_AUTO_CANCEL_MIN * 60 - elapsed)
    reminder_after = max(0.0, (ORDER_AUTO_CANCEL_MIN - REMINDER_BEFORE_CANCEL_MIN) * 60 - elapsed)

    if cancel_after <= 0:
        app.job_queue.run_once(job_autocancel, when=0, data={"order_id": order_id}, name=_job_name("cancel", order_id))
        ORDER_JOBS[order_id] = {"cancel": _job_name("cancel", order_id)}
        return

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
    if not order or order.get("status") != "searching":
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
            parse_mode="Markdown",
        )
    except Exception:
        pass

async def job_autocancel(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    order_id = data.get("order_id")
    if not order_id:
        return
    order = ORDERS_DB.get(order_id)
    if not order or order.get("status") != "searching":
        return

    try:
        if order.get("admin_chat_id") and order.get("admin_msg_id"):
            await context.bot.delete_message(chat_id=order["admin_chat_id"], message_id=order["admin_msg_id"])
    except Exception:
        pass

    snap = {
        "order_id": order_id,
        "time": datetime.now(TZ).strftime("%H:%M:%S"),
        "status": "canceled_auto",
        "courier_id": None,
        "courier_name": "-",
        "customer_id": order.get("customer_id"),
        "customer_name": order.get("customer_name", "-"),
        "total": int(order.get("total") or 0),
    }
    add_to_archive_today(snap)

    customer_chat_id = order.get("customer_chat_id")
    try:
        if customer_chat_id:
            await context.bot.send_message(
                chat_id=customer_chat_id,
                text=(
                    f"❌ Замовлення №{order_id} автоматично скасовано (кур’єр не знайшовся).\n"
                    "Можеш оформити нове замовлення у меню «🚚 Доставка».\n\n"
                    f"{MAIN_MENU_INFO}"
                ),
                reply_markup=main_menu(),
            )
    except Exception:
        pass

    try:
        if OWNER_CHAT_ID:
            await context.bot.send_message(
                chat_id=OWNER_CHAT_ID,
                text=f"❌ Авто-скасування: замовлення №{order_id} (ніхто не взяв за {ORDER_AUTO_CANCEL_MIN} хв).",
            )
    except Exception:
        pass

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

    uname = rec.get("username") or "-"
    uname_line = f"@{uname}" if uname != "-" else "-"

    text = (
        "👤 **Новий користувач у диспетчерській**\n\n"
        f"Імʼя: {rec.get('name','-')}\n"
        f"Username: {uname_line}\n"
        f"ID: {rec.get('id')}\n"
        f"Час: {t}\n\n"
        "ℹ️ Телефон бот не бачить автоматично (тільки якщо людина поділиться контактом у приваті)."
    )
    await context.bot.send_message(
        chat_id=OWNER_CHAT_ID,
        text=text,
        parse_mode="Markdown",
        reply_markup=kb_make_courier(int(rec["id"])),
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
            return await update.message.reply_text("ID має бути числом. Приклад: 123456789", parse_mode="Markdown")

        if mode == "add":
            COURIERS.add(cid)
            save_couriers()
            load_courier_meta()
            ensure_courier_since(cid)
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(f"✅ Додано кур’єра: {cid}", parse_mode="Markdown")
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
                f"✅ Видалено кур’єра: {cid}" if removed else f"ℹ️ Кур’єра {cid} не було у списку.",
                parse_mode="Markdown",
            )
            return

        if mode == "bal":
            bal = int(COURIER_BALANCES.get(cid, 0))
            is_c = "✅" if cid in COURIERS else "❌"
            OWNER_PENDING.pop(owner_id, None)
            await update.message.reply_text(
                f"🔍 Кур’єр: {cid}\n"
                f"Статус у списку: {is_c}\n"
                f"Баланс (внутр. рахунок комісії): **{bal} грн**",
                parse_mode="Markdown",
            )
            return

    if mode == "topup":
        parts = txt.replace(",", ".").split()
        if len(parts) != 2:
            return await update.message.reply_text("Формат: ID сума\nПриклад: 123456789 500", parse_mode="Markdown")
        try:
            cid = int(parts[0])
            amount = int(float(parts[1]))
        except Exception:
            return await update.message.reply_text("Помилка. Приклад: 123456789 500", parse_mode="Markdown")
        COURIER_BALANCES[cid] = int(COURIER_BALANCES.get(cid, 0)) + amount
        save_balances()
        OWNER_PENDING.pop(owner_id, None)
        await update.message.reply_text(
            f"✅ Баланс {cid} поповнено на **{amount} грн**.\nПоточний баланс: **{COURIER_BALANCES[cid]} грн**",
            parse_mode="Markdown",
        )
        try:
            await context.bot.send_message(cid, f"💳 Баланс поповнено на {amount} грн.\nПоточний баланс: {COURIER_BALANCES[cid]} грн.")
        except Exception:
            pass
        return

    if mode == "withdraw":
        parts = txt.replace(",", ".").split()
        if len(parts) != 2:
            return await update.message.reply_text("Формат: ID сума\nПриклад: 123456789 200", parse_mode="Markdown")
        try:
            cid = int(parts[0])
            amount = int(float(parts[1]))
            if amount <= 0:
                raise ValueError
        except Exception:
            return await update.message.reply_text("Помилка. Приклад: 123456789 200", parse_mode="Markdown")
        COURIER_BALANCES[cid] = int(COURIER_BALANCES.get(cid, 0)) - amount
        save_balances()
        OWNER_PENDING.pop(owner_id, None)
        await update.message.reply_text(
            f"➖ З балансу {cid} знято **{amount} грн**.\nПоточний баланс: **{COURIER_BALANCES[cid]} грн**",
            parse_mode="Markdown",
        )
        try:
            await context.bot.send_message(cid, f"➖ З балансу знято {amount} грн.\nПоточний баланс: {COURIER_BALANCES[cid]} грн.")
        except Exception:
            pass
        return

async def _send_csv_to_owner(context: ContextTypes.DEFAULT_TYPE, csv_bytes: bytes, filename: str, caption: str):
    bio = BytesIO(csv_bytes)
    bio.name = filename
    bio.seek(0)
    await context.bot.send_document(chat_id=OWNER_CHAT_ID, document=bio, filename=filename, caption=caption)

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
    w.writerow(
        [
            "date","time","order_id","courier_id","courier_name","customer_id","customer_name",
            "order_total_uah","service_fee_uah","paid_to_courier_direct_uah","closed_by"
        ]
    )
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
            w.writerow(
                [
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
                ]
            )
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
    w.writerow(["date", "time", "order_id", "courier_id", "courier_name", "service_fee_uah"])
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
            w.writerow([date_key, it.get("time") or "-", it.get("order_id") or "-", it.get("courier_id") or "-", it.get("courier_name") or "-", fee])
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
        await context.bot.send_message(OWNER_CHAT_ID, f"🗂 **Архів за сьогодні ({k})**\n\n" + "\n".join(lines), parse_mode="Markdown")
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
            uname_line = f"@{uname}" if uname != "-" else "-"
            lines.append(f"• {cid} — {name} ({uname_line})")
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
            uname = u.get("username", "-")
            uname_line = f"@{uname}" if uname != "-" else "-"
            lines.append(f"• {u.get('name','-')} ({uname_line}) — {u.get('id')}")
        await context.bot.send_message(OWNER_CHAT_ID, "🧾 **Користувачі диспетчерської (кого бот бачив):**\n\n" + "\n".join(lines), parse_mode="Markdown")
        return

    if data == "owner:add":
        OWNER_PENDING[query.from_user.id] = "add"
        await context.bot.send_message(OWNER_CHAT_ID, "➕ Надішли **ID кур’єра** одним числом.\nНаприклад: 123456789", parse_mode="Markdown")
        return

    if data == "owner:del":
        OWNER_PENDING[query.from_user.id] = "del"
        await context.bot.send_message(OWNER_CHAT_ID, "➖ Надішли **ID кур’єра**, якого треба видалити.\nНаприклад: 123456789", parse_mode="Markdown")
        return

    if data == "owner:topup":
        OWNER_PENDING[query.from_user.id] = "topup"
        await context.bot.send_message(OWNER_CHAT_ID, "💳 Введи **ID і суму** через пробіл.\nПриклад: 123456789 500", parse_mode="Markdown")
        return

    if data == "owner:withdraw":
        OWNER_PENDING[query.from_user.id] = "withdraw"
        await context.bot.send_message(OWNER_CHAT_ID, "➖ Введи **ID і суму** через пробіл.\nПриклад: 123456789 200", parse_mode="Markdown")
        return

    if data == "owner:bal":
        OWNER_PENDING[query.from_user.id] = "bal"
        await context.bot.send_message(OWNER_CHAT_ID, "🔍 Введи **ID кур’єра**.\nПриклад: 123456789", parse_mode="Markdown")
        return

    if data == "fbadmin:ratings":
        load_feedback()
        by: Dict[str, List[int]] = {}
        for r in FEEDBACK_DB:
            if r.get("type") != "rating":
                continue
            cid = str(r.get("courier_id") or "")
            try:
                st = int(r.get("stars") or 0)
            except Exception:
                st = 0
            if not cid or st <= 0:
                continue
            by.setdefault(cid, []).append(st)

        if not by:
            await context.bot.send_message(OWNER_CHAT_ID, "⭐ Поки що немає оцінок.")
            return

        rows = []
        for cid, stars in by.items():
            avg = sum(stars) / len(stars)
            rows.append((avg, len(stars), cid))
        rows.sort(reverse=True)

        lines = []
        load_dispatcher_users()
        for avg, cnt, cid in rows[:200]:
            rec = DISPATCHER_USERS.get(str(cid)) or {}
            name = rec.get("name", "-")
            uname = rec.get("username", "-")
            uname_line = f"@{uname}" if uname != "-" else "-"
            lines.append(f"• {cid} — {name} ({uname_line}) | ⭐ {avg:.2f} (оцінок: {cnt})")

        await context.bot.send_message(OWNER_CHAT_ID, "⭐ **Рейтинг кур’єрів:**\n\n" + "\n".join(lines), parse_mode="Markdown")
        return

    if data == "fbadmin:complaints":
        load_feedback()
        complaints = [r for r in FEEDBACK_DB if r.get("type") == "complaint"]
        if not complaints:
            await context.bot.send_message(OWNER_CHAT_ID, "📝 Скарг немає ✅")
            return
        complaints = complaints[-50:]
        lines = []
        for r in complaints[::-1]:
            lines.append(
                f"• {r.get('ts','-')} | №{r.get('order_id','-')} | {r.get('courier_name','-')} | {r.get('customer_name','-')}\n"
                f"  {r.get('text','-')}"
            )
        await context.bot.send_message(OWNER_CHAT_ID, "📝 **Останні скарги:**\n\n" + "\n\n".join(lines), parse_mode="Markdown")
        return

# =========================
# ===== Finalize + close ===
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
        f"{manual_dist:.2f} км (фінал без +10%)"
        if manual_dist is not None
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
            f"🚚 Кур'єр: {order.get('courier_name','-')} (ID: {courier_id})\n"
            f"🧩 Тип: {order_kind_label(order.get('order_kind_key','pickup_ready'))}\n"
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
                reply_markup=courier_menu(),
            )
        except Exception:
            pass

    customer_chat_id = order.get("customer_chat_id")
    if customer_chat_id:
        try:
            await context.bot.send_message(
                customer_chat_id,
                f"✅ Замовлення №{order_id} закрито.\nДякуємо! Можете оцінити кур’єра 👇",
                reply_markup=kb_customer_feedback(order_id),
            )
        except Exception:
            pass

    ORDERS_DB.pop(order_id, None)
    save_orders_db()

# =========================
# ===== Final km input =====
# =========================
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
        f"✅ Замовлення №{order_id} фіналізовано.\n📏 Фінальний км: {dist_real:.2f}\n🧾 Обіг по замовленню: {total} грн\n📈 Дохід сервісу (комісія): {fee} грн",
        reply_markup=courier_menu(),
    )

# =========================
# ===== Complaints text ====
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
    snap = find_order_snapshot(order_id) or {}

    rec = {
        "type": "complaint",
        "ts": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "order_id": order_id,
        "text": text,
        "courier_id": snap.get("courier_id"),
        "courier_name": snap.get("courier_name") or "-",
        "customer_id": uid,
        "customer_name": snap.get("customer_name") or update.effective_user.full_name,
        "customer_username": snap.get("customer_username") or (update.effective_user.username or "-"),
    }
    FEEDBACK_DB.append(rec)
    save_feedback()

    if OWNER_CHAT_ID:
        msg = (
            f"😡 **Скарга на кур’єра**\n\n"
            f"Замовлення: №{order_id}\n"
            f"Клієнт: {rec.get('customer_name','-')} (@{rec.get('customer_username','-')}) ID: {rec.get('customer_id')}\n"
            f"Кур’єр: {rec.get('courier_name','-')} ID: {rec.get('courier_id')}\n"
            f"Час: {rec.get('ts')}\n\n"
            f"Текст:\n{text}"
        )
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown")

    await update.message.reply_text("✅ Дякую. Скаргу відправлено адміну.")

# =========================
# ===== Support contact ====
# =========================
async def support_contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ВАЖЛИВО: цей handler обробляє ТІЛЬКИ кур'єрські pending-запити.
    # Клієнтські контакти ("Зателефонуйте мені") обробляються в ConversationHandler -> CALLME_PHONE.
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
            f"ID: {u.id}\n"
            f"Телефон: {phone}\n"
            f"Замовлення: №{order_id}\n"
        )
        kb = kb_owner_force_close(order_id)
    else:
        msg = (
            "🆘 **Техпідтримка від кур'єра (загальна)**\n\n"
            f"Кур'єр: {u.full_name} (@{u.username or '-'})\n"
            f"ID: {u.id}\n"
            f"Телефон: {phone}\n"
            f"Активних замовлень може не бути."
        )
        kb = None

    if OWNER_CHAT_ID:
        await context.bot.send_message(OWNER_CHAT_ID, msg, parse_mode="Markdown", reply_markup=kb)
    await update.message.reply_text("✅ Запит надіслано адміну. Очікуй дзвінок.", reply_markup=courier_menu())

# =========================
# ===== Receipt photo ======
# =========================
async def receipt_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.photo:
        return
    if not is_private_chat(update):
        return
    courier_id = update.effective_user.id
    if courier_id not in RECEIPT_PHOTO_PENDING:
        return
    order_id = RECEIPT_PHOTO_PENDING.pop(courier_id, None)
    order = ORDERS_DB.get(order_id)
    if not order:
        return await update.message.reply_text("Замовлення не знайдено або вже закрите.", reply_markup=courier_menu())
    if courier_id != order.get("courier_id"):
        return await update.message.reply_text("❌ Це не ваше замовлення.", reply_markup=courier_menu())

    customer_chat_id = order.get("customer_chat_id")
    if not customer_chat_id:
        return await update.message.reply_text("Не знайдено чат клієнта.", reply_markup=courier_menu())

    photo = update.message.photo[-1]
    caption = f"🧾 Фото чека по замовленню №{order_id}"

    try:
        await context.bot.send_photo(chat_id=customer_chat_id, photo=photo.file_id, caption=caption)
    except Exception:
        pass
    try:
        if OWNER_CHAT_ID:
            await context.bot.send_message(OWNER_CHAT_ID, f"🧾 Кур’єр надіслав чек клієнту по №{order_id}.")
    except Exception:
        pass

    await update.message.reply_text("✅ Фото чека відправлено клієнту.", reply_markup=courier_menu())

# =========================
# ===== Courier router =====
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
            f"Умова: після 1-го місяця комісія 20%, якщо ⭐ за 30 днів ≥ 4.8 і є мін. {MIN_RATINGS_30D} оцінок"
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
            lines.append(
                f"• №{oid} — {o.get('status','-')} | {order_kind_label(o.get('order_kind_key','pickup_ready'))}\n"
                f" {o.get('from_addr','-')} → {o.get('to_addr','-')}"
            )
        await update.message.reply_text("📦 Ваші активні замовлення:\n\n" + "\n\n".join(lines), reply_markup=courier_menu())
        return True

    if text == "🆘 Техпідтримка":
        ids = courier_active_order_ids(uid)
        if ids:
            await update.message.reply_text("🆘 Оберіть замовлення, по якому потрібна допомога:", reply_markup=kb_support_pick(ids))
            return True
        COURIER_GENERAL_SUPPORT_PENDING[uid] = True
        await update.message.reply_text(
            "🆘 Надішліть номер телефону кнопкою нижче — адміну прийде ваш контакт.",
            reply_markup=support_share_contact_kb(),
        )
        return True

    if text == "⬅️ Клієнтське меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
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
        await update.message.reply_text(working_hours_notice(), parse_mode="Markdown")
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
        await update.message.reply_text("Ок! Обери тип замовлення:", reply_markup=order_kind_menu())
        return ORDER_KIND

    if text == "💳 Тариф":
        await update.message.reply_text(tariff_text(), reply_markup=main_menu())
        await update.message.reply_text(working_hours_notice(), parse_mode="Markdown")
        return CHOICE

    if text == "🛠 Підтримка":
        await update.message.reply_text(support_text(), reply_markup=callme_kb())
        await update.message.reply_text(working_hours_notice(), parse_mode="Markdown")
        return CALLME_PHONE

    if text == "⬅️ Назад в меню":
        await update.message.reply_text("Меню 👇", reply_markup=main_menu())
        await update.message.reply_text(working_hours_notice(), parse_mode="Markdown")
        return CHOICE

    await update.message.reply_text("Оберіть пункт з меню 👇", reply_markup=main_menu())
    await update.message.reply_text(working_hours_notice(), parse_mode="Markdown")
    return CHOICE

# --- CALLME (клієнтська підтримка) ---
async def callme_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    phone_raw = None
    if update.message and update.message.contact:
        phone_raw = update.message.contact.phone_number
    elif update.message and update.message.text:
        phone_raw = update.message.text.strip()

    if not phone_raw:
        await update.message.reply_text(
            "Надішли номер кнопкою «📞 Зателефонуйте мені» або напиши номер текстом.",
            reply_markup=callme_kb(),
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

# --- ORDER: kind ---
async def order_kind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    if t.startswith("🟡"):
        context.user_data["order_kind_key"] = "purchase"
    elif t.startswith("✅"):
        context.user_data["order_kind_key"] = "pickup_ready"
    else:
        await update.message.reply_text("Оберіть кнопку 👇", reply_markup=order_kind_menu())
        return ORDER_KIND

    await update.message.reply_text("Ок. Обери тип доставки:", reply_markup=delivery_type_menu())
    return DELIV_TYPE

# --- ORDER: delivery type ---
async def delivery_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    if t == "⚡ Термінова":
        context.user_data["is_urgent"] = True
        await update.message.reply_text(addr_prompt_text("📍 Звідки забрати?"), reply_markup=back_only_kb())
        return FROM_ADDR

    if t == "⏰ На певний час":
        context.user_data["is_urgent"] = False
        await update.message.reply_text(
            "🕒 Введіть час доставки у форматі:\n"
            "• 05.03 18:30\n"
            "або\n"
            "• 05.03.2026 18:30\n\n"
            f"{working_hours_notice()}",
            parse_mode="Markdown",
            reply_markup=back_only_kb(),
        )
        return WHEN_INPUT

    await update.message.reply_text("Оберіть кнопку 👇", reply_markup=delivery_type_menu())
    return DELIV_TYPE

async def when_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    dt = parse_scheduled_datetime(t)
    if not dt:
        await update.message.reply_text("❌ Не зрозумів формат. Приклад: 05.03 18:30", reply_markup=back_only_kb())
        return WHEN_INPUT

    # dt naive -> interpret local today/year
    local_dt = dt.replace(tzinfo=TZ)
    if not is_within_working_hours(local_dt):
        await update.message.reply_text("🌙 Нічні замовлення не приймаються.\nВкажіть час у межах роботи.", reply_markup=back_only_kb())
        return WHEN_INPUT

    context.user_data["when_text"] = t
    await update.message.reply_text(f"Підтвердити час: **{t}** ?", parse_mode="Markdown", reply_markup=when_confirm_kb())
    return WHEN_CONFIRM

async def when_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    if t == "✏️ Змінити час":
        await update.message.reply_text("Ок, введіть час ще раз (05.03 18:30):", reply_markup=back_only_kb())
        return WHEN_INPUT
    if t != "✅ Підтвердити час":
        await update.message.reply_text("Оберіть кнопку 👇", reply_markup=when_confirm_kb())
        return WHEN_CONFIRM

    await update.message.reply_text(addr_prompt_text("📍 Звідки забрати?"), reply_markup=back_only_kb())
    return FROM_ADDR

# --- ORDER: from address ---
async def from_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    if not has_locality(t):
        await update.message.reply_text(locality_hint(), reply_markup=back_only_kb())
        return FROM_ADDR

    context.user_data["from_addr"] = t
    await update.message.reply_text(f"Підтвердіть адресу:\n\n**{_safe_md(t)}**", parse_mode="Markdown", reply_markup=addr_confirm_kb())
    return CONFIRM_FROM

async def confirm_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    if t == "✏️ Змінити адресу":
        await update.message.reply_text(addr_prompt_text("📍 Звідки забрати?"), reply_markup=back_only_kb())
        return FROM_ADDR
    if t != "✅ Підтвердити адресу":
        await update.message.reply_text("Оберіть кнопку 👇", reply_markup=addr_confirm_kb())
        return CONFIRM_FROM

    await update.message.reply_text(addr_prompt_text("🎯 Куди доставити?"), reply_markup=back_only_kb())
    return TO_ADDR

# --- ORDER: to address ---
async def to_addr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    if not has_locality(t):
        await update.message.reply_text(locality_hint(), reply_markup=back_only_kb())
        return TO_ADDR

    context.user_data["to_addr"] = t
    await update.message.reply_text(f"Підтвердіть адресу:\n\n**{_safe_md(t)}**", parse_mode="Markdown", reply_markup=addr_confirm_kb())
    return CONFIRM_TO

async def confirm_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    if t == "✏️ Змінити адресу":
        await update.message.reply_text(addr_prompt_text("🎯 Куди доставити?"), reply_markup=back_only_kb())
        return TO_ADDR
    if t != "✅ Підтвердити адресу":
        await update.message.reply_text("Оберіть кнопку 👇", reply_markup=addr_confirm_kb())
        return CONFIRM_TO

    if context.user_data.get("order_kind_key") == "purchase":
        await update.message.reply_text(
            "🧾 Напишіть список того, що треба купити (1 повідомлення).",
            reply_markup=back_only_kb(),
        )
        return PURCHASE_ITEMS

    await update.message.reply_text(
        "ℹ️ Опишіть що саме забрати/передати (підʼїзд, поверх, код домофону тощо).",
        reply_markup=back_only_kb(),
    )
    return PICKUP_INFO

# --- PURCHASE: items ---
async def purchase_items(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    if len(t) < 2:
        await update.message.reply_text("Напишіть список трохи детальніше 🙏", reply_markup=back_only_kb())
        return PURCHASE_ITEMS
    context.user_data["purchase_items"] = t
    await update.message.reply_text(
        "💰 Вкажіть приблизну суму покупок (числом, грн). Напр: 450",
        reply_markup=back_only_kb(),
    )
    return PURCHASE_EST_SUM

async def purchase_est_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    try:
        s = int(float(t.replace(",", ".")))
        if s <= 0 or s > 20000:
            raise ValueError
    except Exception:
        await update.message.reply_text("Введіть тільки число (грн). Напр: 450", reply_markup=back_only_kb())
        return PURCHASE_EST_SUM

    context.user_data["purchase_est_sum"] = s
    dep = calc_purchase_deposit(s)
    context.user_data["purchase_deposit"] = dep
    await update.message.reply_text(
        f"🔒 Для викупу потрібна **гарантія**: **{dep} грн**.\n"
        f"Після оплати натисніть кнопку нижче.\n"
        + (fmt_payment_block("ГАРАНТІЯ") if PAYMENT_CARD else ""),
        parse_mode="Markdown",
        reply_markup=deposit_confirm_kb(),
    )
    return PURCHASE_DEPOSIT_CONFIRM

async def purchase_deposit_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    if t == "✏️ Змінити суму":
        await update.message.reply_text("Ок, введіть приблизну суму покупок ще раз (грн):", reply_markup=back_only_kb())
        return PURCHASE_EST_SUM
    if t != "✅ Я оплатив гарантію":
        await update.message.reply_text("Оберіть кнопку 👇", reply_markup=deposit_confirm_kb())
        return PURCHASE_DEPOSIT_CONFIRM

    await update.message.reply_text(
        "ℹ️ Опишіть деталі: що купувати/який магазин/бренд/розмір/альтернатива і т.д.",
        reply_markup=back_only_kb(),
    )
    return PICKUP_INFO

# --- PICKUP INFO ---
async def pickup_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    context.user_data["pickup_info"] = t
    await update.message.reply_text("📞 Залиште номер телефону (кнопкою або текстом):", reply_markup=phone_kb())
    return PHONE

async def phone_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.text and update.message.text.strip() == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    if update.message and update.message.text and update.message.text.strip() == "Пропустити":
        context.user_data["phone"] = ""
        await update.message.reply_text("💬 Додайте коментар (або напишіть «-»):", reply_markup=back_only_kb())
        return COMMENT

    phone_raw = None
    if update.message and update.message.contact:
        phone_raw = update.message.contact.phone_number
    elif update.message and update.message.text:
        phone_raw = update.message.text.strip()

    if not phone_raw:
        await update.message.reply_text("Надішліть контакт кнопкою або напишіть номер текстом.", reply_markup=phone_kb())
        return PHONE

    context.user_data["phone"] = phone_raw
    await update.message.reply_text("💬 Додайте коментар (або напишіть «-»):", reply_markup=back_only_kb())
    return COMMENT

async def comment_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE
    if t == "-" or t.lower() == "нема":
        context.user_data["comment"] = ""
    else:
        context.user_data["comment"] = t

    await update.message.reply_text(_order_summary_for_customer(context.user_data) + "\nНатисніть **✅ Підтвердити** щоб створити замовлення.",
                                  parse_mode="Markdown", reply_markup=order_confirm_kb())
    return CONFIRM_ORDER

# --- CONFIRM ORDER ---
async def confirm_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = (update.message.text or "").strip()
    if t == "⬅️ Назад в меню":
        await update.message.reply_text(MAIN_MENU_TEXT, reply_markup=main_menu())
        return CHOICE

    if t == "📍 Змінити звідки":
        await update.message.reply_text(addr_prompt_text("📍 Звідки забрати?"), reply_markup=back_only_kb())
        return FROM_ADDR
    if t == "🎯 Змінити куди":
        await update.message.reply_text(addr_prompt_text("🎯 Куди доставити?"), reply_markup=back_only_kb())
        return TO_ADDR
    if t == "💬 Змінити коментар":
        await update.message.reply_text("Введіть коментар (або «-»):", reply_markup=back_only_kb())
        return COMMENT

    if t == "❌ Скасувати":
        await update.message.reply_text("❌ Скасовано. Меню 👇", reply_markup=main_menu())
        return CHOICE

    if t != "✅ Підтвердити":
        await update.message.reply_text("Оберіть кнопку 👇", reply_markup=order_confirm_kb())
        return CONFIRM_ORDER

    # створення замовлення
    http: aiohttp.ClientSession = context.application.bot_data.get("http")  # type: ignore
    if not http:
        http = aiohttp.ClientSession()
        context.application.bot_data["http"] = http

    from_a = context.user_data.get("from_addr", "")
    to_a = context.user_data.get("to_addr", "")
    await update.message.reply_text("⏳ Рахую маршрут...")

    from_coords = await geocode_address_google(from_a, http)
    to_coords = await geocode_address_google(to_a, http)
    if not from_coords or not to_coords:
        await update.message.reply_text(
            "❌ Не вдалося знайти координати однієї з адрес.\n"
            "Спробуйте уточнити адресу (додайте населений пункт/вулицю/номер).",
            reply_markup=main_menu(),
        )
        return CHOICE

    dist_km = get_distance_km(from_coords, to_coords)
    outside = is_outside_obukhiv(from_a, to_a)
    is_urgent = bool(context.user_data.get("is_urgent", False))

    # До прийняття кур'єром — показуємо суму клієнту (без прив’язки до комісії кур’єра)
    total, fee_est, _, safe_dist = calculate_finance_total(dist_km, is_urgent, outside, use_safety=True, fee_rate=JETWAY_FEE_RATE)

    order_id = gen_order_id()
    u = update.effective_user

    order = {
        "order_id": order_id,
        "status": "searching",
        "created_ts": time.time(),
        "customer_chat_id": update.effective_chat.id,
        "customer_id": u.id,
        "customer_name": u.full_name,
        "customer_username": u.username or "-",
        "order_kind_key": context.user_data.get("order_kind_key", "pickup_ready"),
        "is_urgent": is_urgent,
        "when_text": context.user_data.get("when_text", "") if not is_urgent else "",
        "from_addr": from_a,
        "to_addr": to_a,
        "from_coords": from_coords,
        "to_coords": to_coords,
        "dist_km": float(dist_km),
        "safe_dist_km": float(safe_dist),
        "outside": bool(outside),
        "pickup_info": context.user_data.get("pickup_info", ""),
        "phone": context.user_data.get("phone", ""),
        "comment": context.user_data.get("comment", ""),
        "purchase_items": context.user_data.get("purchase_items", ""),
        "purchase_est_sum": int(context.user_data.get("purchase_est_sum") or 0),
        "purchase_deposit": int(context.user_data.get("purchase_deposit") or 0),
        "total": int(total),
        "fee_rate": float(JETWAY_FEE_RATE),  # буде перераховано на accept під кур’єра
        "fee": int(fee_est),
        "courier_id": None,
        "courier_name": None,
        "admin_chat_id": dispatcher_chat_id(),
        "admin_msg_id": None,
    }

    ORDERS_DB[order_id] = order
    save_orders_db()

    # пост у диспетчерську
    admin_text = (
        f"🆕 **ЗАМОВЛЕННЯ №{order_id}**\n"
        f"{order_kind_label(order['order_kind_key'])}\n\n"
        f"📍 {_safe_md(from_a)}\n"
        f"🎯 {_safe_md(to_a)}\n"
        f"📏 ~{safe_dist:.2f} км (авто +10%)\n"
        f"💰 **{total} грн**\n"
        + (f"🕒 {order.get('when_text','')}\n" if not is_urgent else "")
        + (f"🧾 Список: {_safe_md(order.get('purchase_items',''))}\n" if order["order_kind_key"] == "purchase" else "")
        + (f"🔒 Гарантія: {order.get('purchase_deposit')} грн\n" if order["order_kind_key"] == "purchase" else "")
        + (f"ℹ️ {_safe_md(order.get('pickup_info',''))}\n" if order.get("pickup_info") else "")
    )
    try:
        msg = await context.bot.send_message(
            chat_id=order["admin_chat_id"],
            text=admin_text,
            parse_mode="Markdown",
            reply_markup=kb_accept(order_id),
        )
        order["admin_msg_id"] = msg.message_id
        save_orders_db()
    except Exception:
        pass

    schedule_order_timers(context.application, order_id)

    await update.message.reply_text(
        f"✅ Замовлення №{order_id} створено.\n"
        f"Сума: **{total} грн**\n"
        "Шукаємо кур’єра…",
        parse_mode="Markdown",
        reply_markup=main_menu(),
    )
    return CHOICE

# =========================
# ===== Callback router ====
# =========================
# ✅ Ліміт активних замовлень на кур’єра
MAX_ACTIVE_ORDERS = 3

def courier_can_take_order(cid: int) -> bool:
    return ACTIVE_ORDERS_COUNT.get(cid, 0) < MAX_ACTIVE_ORDERS


# =========================
# ===== Callback router ====
# =========================
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = (q.data or "").strip()
    await q.answer()

    if data == "noop":
        return

    # швидке додавання кур'єра з кнопки
    if data.startswith("courier:make:"):
        if not OWNER_CHAT_ID or q.message.chat.id != OWNER_CHAT_ID:
            return await q.answer("Недостатньо прав.", show_alert=True)
        try:
            uid = int(data.split(":")[-1])
        except Exception:
            return
        COURIERS.add(uid)
        save_couriers()
        load_courier_meta()
        ensure_courier_since(uid)
        await context.bot.send_message(OWNER_CHAT_ID, f"✅ Додано кур’єра: {uid}")
        try:
            await context.bot.send_message(uid, "✅ Вас додано як кур’єра. Напишіть /start.")
        except Exception:
            pass
        return

    # owner panel
    if (
        data.startswith("stats:")
        or data.startswith("export:")
        or data.startswith("couriers:")
        or data.startswith("users:")
        or data.startswith("owner:")
        or data.startswith("archive:")
        or data.startswith("fbadmin:")
    ):
        return await owner_panel_callback(update, context)

    # =========================
    # ===== accept order =======
    # =========================
    if data.startswith("accept:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order or order.get("status") != "searching":
            return await q.answer("Замовлення вже неактивне.", show_alert=True)

        courier_id = q.from_user.id
        if courier_id not in COURIERS:
            return await q.answer("Ви не кур’єр.", show_alert=True)

        # ✅ ЛІМІТ 3 АКТИВНИХ ЗАМОВЛЕННЯ
        if not courier_can_take_order(courier_id):
            return await q.answer(f"❌ У вас вже {MAX_ACTIVE_ORDERS} активних замовлення.", show_alert=True)

        # простий захист по балансу
        bal = int(COURIER_BALANCES.get(courier_id, 0))
        if bal < MIN_NEGATIVE_BALANCE:
            return await q.answer("Баланс занадто мінусовий. Зверніться до адміна.", show_alert=True)

        # ✅ зупиняємо авто-таймери скасування, бо замовлення вже взяли
        try:
            cancel_order_timers(context.application, order_id)
        except Exception:
            pass

        load_courier_meta()
        ensure_courier_since(courier_id)
        rate = float(courier_fee_rate(courier_id))

        order["courier_id"] = courier_id
        order["courier_name"] = q.from_user.full_name
        order["status"] = "accepted"
        order["fee_rate"] = rate

        # Перерахунок суми/комісії під кур’єра (для внутрішніх звітів)
        total, fee, _, safe_dist = calculate_finance_total(
            float(order.get("dist_km") or 0.0),
            bool(order.get("is_urgent")),
            bool(order.get("outside")),
            use_safety=True,
            fee_rate=rate,
        )
        order["safe_dist_km"] = float(safe_dist)
        order["total"] = int(total)
        order["fee"] = int(fee)
        save_orders_db()

        ACTIVE_ORDERS_COUNT[courier_id] = ACTIVE_ORDERS_COUNT.get(courier_id, 0) + 1

        # оновити пост у диспетчерській (мінімально)
        try:
            await q.edit_message_reply_markup(reply_markup=kb_taken())
        except Exception:
            pass

        # кур'єру - деталі
        courier_text = (
            f"✅ Ви взяли замовлення №{order_id}\n\n"
            f"{order_kind_label(order.get('order_kind_key','pickup_ready'))}\n"
            f"📍 {order.get('from_addr','-')}\n"
            f"🎯 {order.get('to_addr','-')}\n"
            f"ℹ️ {order.get('pickup_info','-')}\n"
            + (f"🧾 Список: {order.get('purchase_items','-')}\n" if order.get("order_kind_key") == "purchase" else "")
            + (f"🔒 Гарантія: {order.get('purchase_deposit')} грн\n" if order.get("order_kind_key") == "purchase" else "")
            + (f"📞 Телефон клієнта: {order.get('phone','-')}\n" if order.get("phone") else "")
        )
        try:
            await context.bot.send_message(
                chat_id=courier_id,
                text=courier_text,
                reply_markup=kb_courier_controls(order_id, order.get("order_kind_key", "pickup_ready")),
            )
        except Exception:
            pass

        # клієнту - що кур'єр знайдений + оплата
        try:
            await context.bot.send_message(
                chat_id=order["customer_chat_id"],
                text=(
                    f"🚚 Кур’єр знайдений для №{order_id}!\n"
                    f"Кур’єр: {order.get('courier_name','-')}\n\n"
                    f"💰 Сума: **{order.get('total')} грн**\n"
                    + fmt_payment_block(order_id)
                    + "\nПісля отримання натисніть «✅ Отримав» (коли кур’єр позначить «Доставлено»)."
                ),
                parse_mode="Markdown",
                reply_markup=main_menu(),
            )
        except Exception:
            pass
        return

    # courier delivered
    if data.startswith("delivered:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order or order.get("status") != "accepted":
            return await q.answer("Стан недоступний.", show_alert=True)
        if q.from_user.id != int(order.get("courier_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)

        order["status"] = "await_customer"
        save_orders_db()

        try:
            await context.bot.send_message(
                chat_id=order["customer_chat_id"],
                text=f"📦 Замовлення №{order_id} доставлено.\nНатисніть кнопку нижче, якщо ви отримали 👇",
                reply_markup=kb_customer_done(order_id),
            )
        except Exception:
            pass
        return await q.answer("Ок ✅")

    # customer done
    if data.startswith("done:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order or order.get("status") != "await_customer":
            return await q.answer("Стан недоступний.", show_alert=True)
        if q.from_user.id != int(order.get("customer_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)

        order["status"] = "await_finish"
        save_orders_db()

        try:
            await context.bot.send_message(
                chat_id=order["courier_id"],
                text=f"✅ Клієнт підтвердив отримання по №{order_id}.\nФіналізація:",
                reply_markup=finalize_kb(order_id),
            )
        except Exception:
            pass
        return await q.answer("Дякуємо ✅")

    # finish auto
    if data.startswith("finish_auto:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order or order.get("status") != "await_finish":
            return await q.answer("Стан недоступний.", show_alert=True)
        if q.from_user.id != int(order.get("courier_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)

        total = int(order.get("total") or 0)
        fee = int(
            order.get("fee")
            or int(math.ceil(total * float(order.get("fee_rate") or JETWAY_FEE_RATE)))
        )
        await finalize_and_close_order(context, order_id, total, fee, manual_dist=None, closed_by="кур'єр (авто)")
        return

    # finish manual -> просимо км
    if data.startswith("finish_manual:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order or order.get("status") != "await_finish":
            return await q.answer("Стан недоступний.", show_alert=True)
        if q.from_user.id != int(order.get("courier_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)
        FINAL_KM_PENDING[q.from_user.id] = order_id
        try:
            await context.bot.send_message(
                chat_id=q.from_user.id,
                text="✏️ Введіть фінальну дистанцію (км) одним числом, напр: 5.7",
                reply_markup=courier_menu(),
            )
        except Exception:
            pass
        return

    # support per order
    if data.startswith("support:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await q.answer("Замовлення не знайдено.", show_alert=True)
        if q.from_user.id != int(order.get("courier_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)
        SUPPORT_CONTACT_PENDING[q.from_user.id] = order_id
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text="🆘 Надішліть номер телефону кнопкою (контакт) — адміну прийде ваш номер.",
            reply_markup=support_share_contact_kb(),
        )
        return

    # support pick (from courier menu)
    if data.startswith("support_pick:"):
        pick = data.split(":", 1)[1]
        if pick == "cancel":
            return
        order_id = pick
        order = ORDERS_DB.get(order_id)
        if not order:
            return await q.answer("Замовлення не знайдено.", show_alert=True)
        if q.from_user.id != int(order.get("courier_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)
        SUPPORT_CONTACT_PENDING[q.from_user.id] = order_id
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text="🆘 Надішліть номер телефону кнопкою (контакт) — адміну прийде ваш номер.",
            reply_markup=support_share_contact_kb(),
        )
        return

    # receipt photo
    if data.startswith("receipt:"):
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await q.answer("Замовлення не знайдено.", show_alert=True)
        if q.from_user.id != int(order.get("courier_id") or 0):
            return await q.answer("Недостатньо прав.", show_alert=True)
        RECEIPT_PHOTO_PENDING[q.from_user.id] = order_id
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text=f"🧾 Надішліть **фото чека** одним фото (по №{order_id}).",
            parse_mode="Markdown",
            reply_markup=courier_menu(),
        )
        return

    # owner force close
    if data.startswith("force:"):
        if not OWNER_CHAT_ID or q.message.chat.id != OWNER_CHAT_ID:
            return await q.answer("Недостатньо прав.", show_alert=True)
        order_id = data.split(":", 1)[1]
        order = ORDERS_DB.get(order_id)
        if not order:
            return await q.answer("Замовлення не знайдено.", show_alert=True)
        if order.get("status") == "closed":
            return await q.answer("Вже закрите.", show_alert=True)

        total = int(order.get("total") or 0)
        fee = int(
            order.get("fee")
            or int(math.ceil(total * float(order.get("fee_rate") or JETWAY_FEE_RATE)))
        )
        await finalize_and_close_order(context, order_id, total, fee, manual_dist=None, closed_by="owner (force)")
        return

    # feedback block
    if data.startswith("fb:"):
        parts = data.split(":")

        if len(parts) >= 3 and parts[1] == "rate_menu":
            order_id = parts[2]
            snap = find_order_snapshot(order_id)
            if not snap:
                return await q.answer("Замовлення не знайдено.", show_alert=True)
            if q.from_user.id != int(snap.get("customer_id") or 0):
                return await q.answer("Недостатньо прав.", show_alert=True)
            if feedback_has_rating(order_id, q.from_user.id):
                return await q.answer("Ви вже ставили оцінку ✅", show_alert=True)
            try:
                await q.message.reply_text("Оцініть кур’єра:", reply_markup=kb_rating_stars(order_id))
            except Exception:
                pass
            return

        if len(parts) >= 4 and parts[1] == "rate":
            order_id = parts[2]
            try:
                stars = int(parts[3])
            except Exception:
                stars = 0
            if stars < 1 or stars > 5:
                return

            snap = find_order_snapshot(order_id)
            if not snap:
                return await q.answer("Замовлення не знайдено.", show_alert=True)
            if q.from_user.id != int(snap.get("customer_id") or 0):
                return await q.answer("Недостатньо прав.", show_alert=True)
            if feedback_has_rating(order_id, q.from_user.id):
                return await q.answer("Ви вже ставили оцінку ✅", show_alert=True)

            rec = {
                "type": "rating",
                "ts": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "order_id": order_id,
                "stars": stars,
                "courier_id": snap.get("courier_id"),
                "courier_name": snap.get("courier_name"),
                "customer_id": q.from_user.id,
                "customer_name": q.from_user.full_name,
                "customer_username": q.from_user.username or "-",
            }
            FEEDBACK_DB.append(rec)
            save_feedback()

            try:
                await q.message.reply_text("✅ Дякую за оцінку!")
            except Exception:
                pass

            if OWNER_CHAT_ID:
                try:
                    await context.bot.send_message(
                        OWNER_CHAT_ID,
                        f"⭐ Оцінка кур’єру: **{stars}/5**\n"
                        f"№{order_id}\n"
                        f"Кур’єр: {snap.get('courier_name','-')} (ID: {snap.get('courier_id')})\n"
                        f"Клієнт: {q.from_user.full_name} (@{q.from_user.username or '-'})",
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
            return

        if len(parts) >= 3 and parts[1] == "rate_cancel":
            try:
                await q.message.reply_text("Ок ✅")
            except Exception:
                pass
            return

        if len(parts) >= 3 and parts[1] == "complain":
            order_id = parts[2]
            snap = find_order_snapshot(order_id)
            if not snap:
                return await q.answer("Замовлення не знайдено.", show_alert=True)
            if q.from_user.id != int(snap.get("customer_id") or 0):
                return await q.answer("Недостатньо прав.", show_alert=True)
            if feedback_has_complaint(order_id, q.from_user.id):
                return await q.answer("Скарга вже надіслана ✅", show_alert=True)

            COMPLAINT_PENDING[q.from_user.id] = order_id
            try:
                await q.message.reply_text("😡 Опишіть проблему одним повідомленням (текстом).")
            except Exception:
                pass
            return

    return


# =========================
# ===== App lifecycle =====
# =========================
async def post_init(app: Application):
    app.bot_data["http"] = aiohttp.ClientSession()
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
                reply_markup=owner_quick_kb(),
            )
            await app.bot.send_message(chat_id=OWNER_CHAT_ID, text=f"💾 DATA_DIR: {DATA_DIR}")
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

    # --- ConversationHandler (головний, щоб /start + меню + замовлення працювали) ---
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            ROLE_CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, role_choice)],
            CHOICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, choice_router)],
            CALLME_PHONE: [
                MessageHandler(filters.CONTACT, callme_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, callme_handler),
            ],
            ORDER_KIND: [MessageHandler(filters.TEXT & ~filters.COMMAND, order_kind)],
            DELIV_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, delivery_type)],
            WHEN_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, when_input)],
            WHEN_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, when_confirm)],
            FROM_ADDR: [MessageHandler(filters.TEXT & ~filters.COMMAND, from_addr)],
            CONFIRM_FROM: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_from)],
            TO_ADDR: [MessageHandler(filters.TEXT & ~filters.COMMAND, to_addr)],
            CONFIRM_TO: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_to)],
            PURCHASE_ITEMS: [MessageHandler(filters.TEXT & ~filters.COMMAND, purchase_items)],
            PURCHASE_EST_SUM: [MessageHandler(filters.TEXT & ~filters.COMMAND, purchase_est_sum)],
            PURCHASE_DEPOSIT_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, purchase_deposit_confirm)],
            PICKUP_INFO: [MessageHandler(filters.TEXT & ~filters.COMMAND, pickup_info)],
            PHONE: [
                MessageHandler(filters.CONTACT, phone_input),
                MessageHandler(filters.TEXT & ~filters.COMMAND, phone_input),
            ],
            COMMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, comment_input)],
            CONFIRM_ORDER: [MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_order)],
        },
        fallbacks=[CommandHandler("start", start)],
        name="jetway_conv",
        persistent=False,
    )
    app.add_handler(conv, group=0)

    # --- Group join notify ---
    app.add_handler(MessageHandler(filters.StatusUpdate.NEW_CHAT_MEMBERS, on_new_members), group=1)
    app.add_handler(ChatMemberHandler(on_chat_member_update, ChatMemberHandler.CHAT_MEMBER), group=1)

    # --- Callbacks ---
    app.add_handler(CallbackQueryHandler(callback_router), group=1)

    # --- Owner panel ---
    app.add_handler(CommandHandler("panel", panel), group=1)
    if OWNER_CHAT_ID:
        app.add_handler(
            MessageHandler(filters.Chat(chat_id=[OWNER_CHAT_ID]) & (filters.TEXT & ~filters.COMMAND), owner_text_input),
            group=2,
        )

    # --- Extra handlers (працюють поза conv) ---
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, complaint_text_handler), group=3)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, final_km_input_handler), group=3)
    app.add_handler(MessageHandler(filters.PHOTO, receipt_photo_handler), group=3)

    # Кур’єрські контакти для саппорту (тільки pending)
    app.add_handler(MessageHandler(filters.CONTACT, support_contact_handler), group=3)

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
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
