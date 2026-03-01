import os
import re
import asyncio
import time
import aiohttp
from typing import Optional, Tuple

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
    filters,
)

# ===== ENV =====
TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# ✅ Фіксовані номери підтримки
SUPPORT_PHONE_1 = "+380968130807"
SUPPORT_PHONE_2 = "+380687294365"

# ✅ Google Geocoding API Key
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()

# ===== Tariffs =====
PRICE_SCHEDULED_BASE = 110
PRICE_URGENT_BASE = 150
BASE_KM = 2
EXTRA_KM_PRICE = 23
EXTRA_OUTSIDE_OBUKHIV_PER_KM = 5

# ===== States =====
(
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
) = range(13)


# ===== Helpers: locality required (STRICT) =====
def has_locality(addr: str) -> bool:
    """
    Строга вимога: адреса має містити населений пункт.
    Приймаємо лише якщо:
    1) є явний маркер: м./с./смт/селище/місто/село (на початку або після коми)
       напр: "м. Обухів, вул. Київська 10"
    АБО
    2) є кома, і ПІСЛЯ коми є схоже на назву населеного пункту (мін 3 літери),
       напр: "вул. Київська 10, Обухів"
    """
    a = (addr or "").strip()
    if not a:
        return False

    low = a.lower()

    # 1) явні маркери нас. пункту
    marker_pattern = r"(^|,|\s)(м\.|с\.|смт|селище|місто|село)\s*[A-Za-zА-Яа-яІіЇїЄєҐґ'\-]{3,}"
    if re.search(marker_pattern, low):
        return True

    # 2) формат "вул..., Місто" (обов'язково кома)
    parts = [p.strip() for p in a.split(",") if p.strip()]
    if len(parts) >= 2:
        locality_part = parts[-1]  # остання частина після коми
        # має бути мін 3 літери і без цифр
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


# ===== Texts =====
def tariff_text() -> str:
    return (
        "💳 Наші тарифи\n\n"
        f"⏰ На певний час: {PRICE_SCHEDULED_BASE} грн до {BASE_KM} км\n"
        f"➕ кожен додатковий 1 км: +{EXTRA_KM_PRICE} грн\n\n"
        f"⚡ Термінова: {PRICE_URGENT_BASE} грн до {BASE_KM} км\n"
        f"➕ кожен додатковий 1 км: +{EXTRA_KM_PRICE} грн\n\n"
        f"🏙️ Поза містом Обухів: додаткова плата за км +{EXTRA_OUTSIDE_OBUKHIV_PER_KM} грн."
    )


def support_text() -> str:
    return (
        "🛠 Підтримка\n"
        f"📞 Наші номери:\n"
        f"• {SUPPORT_PHONE_1}\n"
        f"• {SUPPORT_PHONE_2}\n\n"
        "Натисни «📞 Зателефонуйте мені» або напиши свій номер — ми передзвонимо."
    )


# ===== Keyboards =====
def main_menu():
    return ReplyKeyboardMarkup(
        [["🚚 Доставка", "💳 Тариф", "🛠 Підтримка"]],
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
    return ReplyKeyboardMarkup([["✅ Підтвердити", "❌ Скасувати"]], resize_keyboard=True)


# ===== Geocoding (Google, Ukraine-only) =====
_GEOCODE_CACHE: dict[str, Tuple[float, float]] = {}
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


async def geocode_address_google(address: str) -> Optional[Tuple[float, float]]:
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
        async with aiohttp.ClientSession() as session:
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


# ===== Handlers =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Привіт! Обери, що тобі потрібно 👇", reply_markup=main_menu())
    return CHOICE


async def choice_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    await update.message.reply_text("Оберіть пункт з меню 👇", reply_markup=main_menu())
    return CHOICE


# ✅ Підтримка: “Зателефонуйте мені”
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

    phone_clean = phone_raw.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone_clean.startswith("00"):
        phone_clean = "+" + phone_clean[2:]

    user = update.effective_user
    urgent_msg = (
        "🚨 ТЕРМІНОВИЙ ДЗВІНОК!\n"
        "Потрібно передзвонити клієнту.\n\n"
        f"👤 {user.full_name} (@{user.username or '-'})\n"
        f"📞 Номер: {phone_raw}\n"
        f"🆔 user_id: {user.id}"
    )

    inline_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📞 Передзвонити", url=f"tel:{phone_clean}")]
    ])

    if ADMIN_CHAT_ID != 0:
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=urgent_msg)

    # ✅ НОВИЙ текст як ти просив
    await update.message.reply_text(
        "🙏 Дякую за звернення! Очікуйте дзвінок від оператора.",
        reply_markup=main_menu()
    )
    return CHOICE


# --- Delivery flow ---
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

    context.user_data["comment"] = (update.message.text or "").strip()

    data = context.user_data
    when_line = data.get("scheduled_when", "терміново")

    summary = (
        "🧾 Заявка на доставку\n"
        f"🚚 Тип: {data.get('delivery_type_label','-')}\n"
        f"🕒 Час: {when_line}\n"
        f"📍 Звідки: {data.get('from_addr','-')}\n"
        f"🎯 Куди: {data.get('to_addr','-')}\n"
        f"📦 Що: {data.get('item','-')}\n"
        f"📞 Тел: {data.get('phone') or 'не вказано'}\n"
        f"💬 Коментар: {data.get('comment','-')}\n\n"
        "Підтвердити?"
    )

    await update.message.reply_text(summary, reply_markup=order_confirm_kb())
    return CONFIRM_ORDER


async def confirm_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if "Підтверд" not in text:
        await update.message.reply_text("Скасовано. Меню 👇", reply_markup=main_menu())
        context.user_data.clear()
        return CHOICE

    data = context.user_data
    user = update.effective_user
    when_line = data.get("scheduled_when", "терміново")

    tariff_line = (
        f"⏰ {PRICE_SCHEDULED_BASE} грн до {BASE_KM}км (+{EXTRA_KM_PRICE}/км)"
        if data.get("delivery_type_key") == "scheduled"
        else f"⚡ {PRICE_URGENT_BASE} грн до {BASE_KM}км (+{EXTRA_KM_PRICE}/км)"
    )

    from_addr = data.get("from_addr", "")
    to_addr = data.get("to_addr", "")

    from_coords = await geocode_address_google(from_addr)
    to_coords = await geocode_address_google(to_addr)

    def fmt_coords(coords: Optional[Tuple[float, float]]) -> str:
        if not coords:
            return "не знайдено"
        return f"{coords[0]:.6f}, {coords[1]:.6f}"

    def fmt_link(coords: Optional[Tuple[float, float]]) -> str:
        if not coords:
            return "-"
        return gmaps_link_from_coords(coords[0], coords[1])

    msg = (
        "🚚 Нове замовлення\n"
        f"👤 Клієнт: {user.full_name} (@{user.username or '-'})\n"
        f"🚚 Тип: {data.get('delivery_type_label','-')}\n"
        f"🕒 Час: {when_line}\n"
        f"💳 Тариф: {tariff_line}\n"
        f"📍 Звідки: {from_addr}\n"
        f"🧭 Коорд. звідки: {fmt_coords(from_coords)}\n"
        f"🗺️ Лінк звідки: {fmt_link(from_coords)}\n"
        f"🎯 Куди: {to_addr}\n"
        f"🧭 Коорд. куди: {fmt_coords(to_coords)}\n"
        f"🗺️ Лінк куди: {fmt_link(to_coords)}\n"
        f"📦 Що: {data.get('item','-')}\n"
        f"📞 Тел: {data.get('phone') or 'не вказано'}\n"
        f"💬 Коментар: {data.get('comment','-')}\n"
    )

    if ADMIN_CHAT_ID != 0:
        await context.bot.send_message(ADMIN_CHAT_ID, msg)

    await update.message.reply_text(
        "✅ Ваше замовлення в обробці. Очікуйте дзвінок від оператора.",
        reply_markup=main_menu()
    )
    context.user_data.clear()
    return CHOICE


def build_app() -> Application:
    app = Application.builder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
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

    app.add_handler(conv)
    return app


# ===== Runner for Python 3.14 =====
def run_polling_py314(app: Application):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def runner():
        await app.initialize()
        await app.start()
        if app.updater is None:
            raise RuntimeError("Updater is None. Перевір python-telegram-bot і токен.")
        await app.updater.start_polling()
        await asyncio.Event().wait()

    try:
        loop.run_until_complete(runner())
    except KeyboardInterrupt:
        pass
    finally:
        try:
            loop.run_until_complete(app.stop())
            loop.run_until_complete(app.shutdown())
        except Exception:
            pass
        loop.close()


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Не знайдено BOT_TOKEN. Задай: setx BOT_TOKEN \"...\" або set BOT_TOKEN=... перед запуском.")
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError("Не знайдено GOOGLE_MAPS_API_KEY. Задай: setx GOOGLE_MAPS_API_KEY \"...\"")

    application = build_app()
    run_polling_py314(application)
