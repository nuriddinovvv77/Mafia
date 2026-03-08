# mafia_bot.py
# -*- coding: utf-8 -*-
"""
Mafia bot with admin functions:
- /broadcast <message> — send message to all players
- /addadmin <user_id> and /removeadmin <user_id> — manage admins
- /listusers — view all users and balances
- After order, bot requests payment proof, forwards to admins
- Admins can confirm/cancel orders with /confirm <user_id> or /cancel <user_id>
- Safe API calls, JSON serialization (set -> list), timers, keyboard cleanup
- DAY_TIMEOUT = 30 (strict), emoji-enhanced messages
- Dead players can't vote, night silence
"""

import os
import json
import random
import logging
import threading
import time
from collections import Counter
from typing import Dict, Any, List, Optional

from telebot import TeleBot, types

# ============================ CONFIG ============================
TOKEN = os.getenv("MAFIA_BOT_TOKEN", "8711388131:AAF6CIqHWTdftPMn83e5SlwgGYdPDN27m9M")
ADMIN_IDS = {int(os.getenv("MAFIA_ADMIN_ID", "7340561719"))}  # Admin set
BOT_USERNAME = os.getenv("MAFIA_BOT_USERNAME", "nuriddinov_mafiabot")

DATA_DIR = "data"
PROFILES_FILE = os.path.join(DATA_DIR, "profiles.json")
GAMES_FILE = os.path.join(DATA_DIR, "games.json")
HISTORY_FILE = os.path.join(DATA_DIR, "history.json")
ADMINS_FILE = os.path.join(DATA_DIR, "admins.json")

REGISTRATION_TIMEOUT = 60
MIN_PLAYERS = 3
DAY_TIMEOUT = 30
NIGHT_TIMEOUT = 60

PRICE_PER_DIAMOND = 3000
DEFAULT_PACKS = [1, 5, 10, 50, 100]

# ============================ LOGGING ============================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("mafia_bot")

# ============================ BOT ============================
bot = TeleBot(TOKEN, parse_mode="HTML")

# ============================ GLOBAL STATE ============================
LOCK = threading.RLock()

profiles: Dict[str, Dict[str, Any]] = {}
games: Dict[str, Dict[str, Any]] = {}
history: List[Dict[str, Any]] = []
diamond_orders: Dict[str, Dict[str, Any]] = {}
waiting_for_custom_amount: set[int] = set()
waiting_for_check: Dict[int, str] = {}  # user_id -> order_id
timers: Dict[str, Dict[str, Optional[threading.Timer]]] = {}

# ============================ PERSISTENCE ============================
def ensure_data_dir() -> None:
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def default_serializer(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def save_json(path: str, data: Any) -> None:
    try:
        ensure_data_dir()
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=default_serializer)
        os.replace(tmp, path)
    except Exception as e:
        logger.exception("save_json %s error: %s", path, e)

def load_json(path: str):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.exception("load_json %s error: %s", path, e)
    return None

def persist_profiles() -> None:
    with LOCK:
        save_json(PROFILES_FILE, profiles)

def persist_games() -> None:
    safe_games = {}
    with LOCK:
        for k, g in games.items():
            copyg = {}
            for kk, vv in g.items():
                if isinstance(vv, set):
                    copyg[kk] = list(vv)
                else:
                    copyg[kk] = vv
            safe_games[k] = copyg
        save_json(GAMES_FILE, safe_games)

def persist_history() -> None:
    with LOCK:
        save_json(HISTORY_FILE, history)

def persist_admins() -> None:
    with LOCK:
        save_json(ADMINS_FILE, list(ADMIN_IDS))

def persist_all() -> None:
    persist_profiles()
    persist_games()
    persist_history()
    persist_admins()

# initial load
with LOCK:
    profiles.update(load_json(PROFILES_FILE) or {})
    games.update(load_json(GAMES_FILE) or {})
    history = load_json(HISTORY_FILE) or []
    loaded_admins = load_json(ADMINS_FILE)
    if loaded_admins:
        ADMIN_IDS.update(set(loaded_admins))

# ============================ HELPERS ============================
def uid_str(uid: int) -> str:
    return str(int(uid))

def cid_str(cid: int) -> str:
    return str(int(cid))

def get_username_obj(user) -> str:
    try:
        if getattr(user, "username", None):
            return f"@{user.username}"
        if getattr(user, "first_name", None):
            return user.first_name
        return str(user.id)
    except Exception:
        return str(getattr(user, "id", "unknown"))

def get_username_id(uid: int) -> str:
    try:
        ch = bot.get_chat(uid)
        if getattr(ch, "username", None):
            return f"@{ch.username}"
        if getattr(ch, "first_name", None):
            return ch.first_name
        return str(uid)
    except Exception:
        return str(uid)

def ensure_profile(uid: int, name: str = "") -> Dict[str, Any]:
    key = uid_str(uid)
    with LOCK:
        if key not in profiles:
            profiles[key] = {
                "name": name or str(uid),
                "money": 0,
                "diamonds": 0,
                "doctor_save_used": False,
                "guaranteed_active_role": False,
                "protection_active": False,
            }
            persist_profiles()
        else:
            prof = profiles[key]
            prof.setdefault("money", 0)
            prof.setdefault("diamonds", 0)
            prof.setdefault("doctor_save_used", False)
            prof.setdefault("guaranteed_active_role", False)
            prof.setdefault("protection_active", False)
    return profiles[key]

def safe_api(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.debug("API call failed: %s %s %s", fn.__name__, args, kwargs)
        logger.exception(e)
        return None

def safe_answer_callback(cb_query, text=None, show_alert=False):
    try:
        if text:
            safe_api(bot.answer_callback_query, cb_query.id, text, show_alert=show_alert)
        else:
            safe_api(bot.answer_callback_query, cb_query.id)
    except Exception:
        logger.debug("Ignoring failed answer_callback_query (likely too old)")

# ============================ MARKUPS ============================
def main_reply_markup() -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    m.add("🎮 Играть", "👤 Профиль")
    m.add("ℹ Помощь")
    return m

def profile_reply_markup(uid: Optional[int] = None) -> types.ReplyKeyboardMarkup:
    m = types.ReplyKeyboardMarkup(resize_keyboard=True)
    prof = ensure_profile(uid) if uid else None
    if prof and prof["diamonds"] > 0:
        m.add("💠 Использовать алмаз")
    if prof and prof["money"] >= 100 and not prof["protection_active"]:
        m.add("🛡 Использовать деньги (100)")
    m.add("💎 Купить алмазы")
    m.add("🔙 Главное меню")
    return m

def join_markup() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🕹️ Присоединиться к игре", callback_data="join_game"))
    return kb

# ============================ COMMANDS ============================
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    try:
        ensure_profile(msg.from_user.id, get_username_obj(msg.from_user))
        prof = profiles[uid_str(msg.from_user.id)]
        text = (
            "👋 <b>Добро пожаловать, {name}!</b>\n"
            "Я — <b>True Mafia</b> бот! 🕵️‍♂️\n"
            "Добавь меня в группу и используй /startgame, чтобы начать эпичную игру в мафию! 🎲"
        ).format(name=prof["name"])
        if msg.chat.type == "private":
            safe_api(bot.send_message, msg.from_user.id, text, reply_markup=main_reply_markup())
            add_url = f"https://t.me/{BOT_USERNAME}?startgroup=true"
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("➕ Добавить в свой чат", url=add_url))
            safe_api(bot.send_message, msg.from_user.id, "Или добавь меня в группу:", reply_markup=kb)
        else:
            safe_api(bot.reply_to, msg, (
                "🎮 Я — бот для игры в мафию в группах!\n"
                "Команды: /startgame, /begin, /endgame\n"
                "Игроки должны написать мне в ЛС (/start)"
            ))
    except Exception:
        logger.exception("start failed")

@bot.message_handler(commands=["help"])
def cmd_help(msg):
    help_text = (
        "ℹ️ <b>Помощь — True Mafia</b>\n\n"
        "• <b>/startgame</b> — начать регистрацию (авто-старт через 60 сек при ≥ 3 игроках)\n"
        "• <b>/begin</b> — начать игру вручную\n"
        "• <b>Голосование днём</b> — через кнопки (30 сек)\n"
        "• <b>/profile</b> — профиль, алмазы, деньги и защита\n"
        "• <b>Защита</b> — стоит 100 сум, разовая, спасает от убийства мафии в одну ночь\n"
    )
    try:
        if msg.chat.type == "private":
            safe_api(bot.send_message, msg.from_user.id, help_text)
        else:
            safe_api(bot.reply_to, msg, "📜 Инструкцию отправил в ЛС.")
            safe_api(bot.send_message, msg.from_user.id, help_text)
    except Exception:
        logger.exception("help failed")

# ============================ PROFILE / SHOP ============================
@bot.message_handler(commands=["profile"])
def cmd_profile(msg):
    if msg.chat.type != "private":
        safe_api(bot.reply_to, msg, "⚠️ /profile доступна только в ЛС.")
        return
    uid = msg.from_user.id
    prof = ensure_profile(uid, get_username_obj(msg.from_user))
    status = "активна 🛡" if prof["protection_active"] else "не активна ❌"
    safe_api(bot.send_message, uid,
        ("👤 <b>Профиль: {name}</b>\n"
         "💵 <b>Баланс:</b> {money} сум\n"
         "🛡 <b>Защита:</b> {prot}\n"
         "💎 <b>Алмазы:</b> {d}\n\n"
         "Выберите действие:")
        .format(name=prof["name"], money=prof["money"], prot=status, d=prof["diamonds"]),
        reply_markup=profile_reply_markup(uid=uid))

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "💠 Использовать алмаз")
def use_diamond(msg):
    uid = msg.from_user.id
    prof = ensure_profile(uid, get_username_obj(msg.from_user))
    with LOCK:
        if prof["diamonds"] <= 0:
            safe_api(bot.send_message, uid, "⚠️ У вас нет алмазов.")
            return
        prof["diamonds"] -= 1
        prof["guaranteed_active_role"] = True
        persist_profiles()
    safe_api(bot.send_message, uid, "💠 <b>Алмаз использован!</b> В следующей игре вам гарантирована активная роль.", reply_markup=profile_reply_markup(uid=uid))

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🛡 Использовать деньги (100)")
def use_money_for_protection(msg):
    uid = msg.from_user.id
    prof = ensure_profile(uid, get_username_obj(msg.from_user))
    with LOCK:
        if prof["protection_active"]:
            safe_api(bot.send_message, uid, "🛡 <b>У вас уже активна защита.</b>")
            return
        if prof["money"] < 100:
            safe_api(bot.send_message, uid, "❗ <b>Недостаточно средств.</b> Нужна сумма 100.")
            return
        prof["money"] -= 100
        prof["protection_active"] = True
        persist_profiles()
    safe_api(bot.send_message, uid, "🛡 <b>Защита активирована!</b> Разовая защита от убийства.", reply_markup=profile_reply_markup(uid=uid))

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "💎 Купить алмазы")
def diamonds_menu(msg):
    uid = msg.from_user.id
    ensure_profile(uid, get_username_obj(msg.from_user))
    text = "💎 <b>Выберите пакет алмазов или введите своё количество:</b>"
    kb = types.InlineKeyboardMarkup(row_width=2)
    for cnt in DEFAULT_PACKS:
        price = cnt * PRICE_PER_DIAMOND
        kb.add(types.InlineKeyboardButton(f"{cnt} 💎 — {price} сум", callback_data=f"buy_{cnt}"))
    kb.add(types.InlineKeyboardButton("✏️ Ввести своё", callback_data="buy_custom"))
    safe_api(bot.send_message, uid, text, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data and (c.data.startswith("buy_") or c.data in ["buy_custom", "confirm_order", "cancel_order"]))
def buy_callback(call):
    uid = call.from_user.id
    data = call.data
    if data == "buy_custom":
        waiting_for_custom_amount.add(uid)
        safe_api(bot.send_message, uid, "✏️ <b>Введите количество алмазов (целое число):</b>")
        safe_answer_callback(call, "Введите количество в ЛС")
        return

    if data.startswith("buy_"):
        cnt = int(data.split("_", 1)[1])
        with LOCK:
            order_id = f"{uid}_{int(time.time())}"
            diamond_orders[order_id] = {"user_id": uid, "count": cnt, "price": cnt * PRICE_PER_DIAMOND, "status": "new"}
            waiting_for_check[uid] = order_id
        safe_api(bot.send_message, uid, "📸 <b>Пожалуйста, пришлите чек об оплате (фото или файл).</b>")
        show_order_confirmation(uid, order_id)
        safe_answer_callback(call, "Отправьте чек")
        return

    if data == "confirm_order":
        with LOCK:
            order_id = waiting_for_check.get(uid)
            order = diamond_orders.get(order_id)
            if order and order["status"] == "new":
                order["status"] = "pending"
        if order:
            for admin_id in ADMIN_IDS:
                try:
                    safe_api(bot.send_message, admin_id,
                        f"🛒 <b>Новый заказ алмазов:</b>\nПользователь: {get_username_id(uid)}\nID: {uid}\nКол-во: {order['count']} 💎\nСумма: {order['price']} сум\nОжидает чека.")
                except Exception:
                    logger.exception("notify admin %s failed", admin_id)
        safe_api(bot.send_message, uid, "✅ <b>Заказ отправлен на проверку.</b> Пришлите чек об оплате.")
        safe_answer_callback(call, "Заказ отправлен")
        return

    if data == "cancel_order":
        with LOCK:
            order_id = waiting_for_check.pop(uid, None)
            if order_id:
                diamond_orders.pop(order_id, None)
        safe_api(bot.send_message, uid, "❌ <b>Заказ отменён.</b> Возвращаемся в профиль.", reply_markup=profile_reply_markup(uid=uid))
        safe_answer_callback(call, "Отменено")
        return

def show_order_confirmation(uid: int, order_id: str) -> None:
    with LOCK:
        order = diamond_orders.get(order_id)
    if not order:
        safe_api(bot.send_message, uid, "⚠️ <b>Заказ не найден.</b>")
        return
    text = (
        f"💎 <b>Вы собираетесь купить {order['count']} алмазов за {order['price']} сум.</b>\n\n"
        "💳 <b>Оплатите на карту:</b>\n"
        "<code>9860 3501 4899 3578</code>\n"
        "<i>Otabek Nuriddinov</i>\n\n"
        "После оплаты пришлите чек и нажмите «Подтвердить»."
    )
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("✅ Подтвердить", callback_data="confirm_order"))
    kb.add(types.InlineKeyboardButton("❌ Отменить", callback_data="cancel_order"))
    safe_api(bot.send_message, uid, text, reply_markup=kb)

@bot.message_handler(content_types=["photo", "document"], func=lambda m: m.chat.type == "private" and m.from_user.id in waiting_for_check)
def handle_check(msg):
    uid = msg.from_user.id
    with LOCK:
        order_id = waiting_for_check.get(uid)
        order = diamond_orders.get(order_id)
    if not order or order["status"] != "pending":
        safe_api(bot.send_message, uid, "⚠️ Заказ не найден или уже обработан.")
        return

    check_content = msg.photo[-1] if msg.photo else msg.document
    check_id = check_content.file_id
    caption = (
        f"🛒 <b>Чек для заказа:</b>\n"
        f"Пользователь: {get_username_id(uid)}\n"
        f"ID: {uid}\n"
        f"Кол-во: {order['count']} 💎\n"
        f"Сумма: {order['price']} сум\n"
        f"Подтвердите /confirm {uid} или /cancel {uid}"
    )

    for admin_id in ADMIN_IDS:
        try:
            if msg.photo:
                safe_api(bot.send_photo, admin_id, check_id, caption=caption)
            else:
                safe_api(bot.send_document, admin_id, check_id, caption=caption)
        except Exception:
            logger.exception("Failed to forward check to admin %s", admin_id)

    safe_api(bot.send_message, uid, "📸 <b>Чек отправлен администраторам на проверку.</b> Ожидайте подтверждения.")
    with LOCK:
        order["check_id"] = check_id
        order["check_type"] = "photo" if msg.photo else "document"

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.from_user.id in waiting_for_custom_amount)
def handle_custom_amount(message):
    user_id = message.from_user.id
    if user_id not in waiting_for_custom_amount:
        return
    waiting_for_custom_amount.discard(user_id)
    text = message.text.strip()
    try:
        count = int(text)
        if count <= 0:
            safe_api(bot.send_message, user_id, "❗ <b>Введите положительное число.</b>")
            return
        with LOCK:
            order_id = f"{user_id}_{int(time.time())}"
            diamond_orders[order_id] = {"user_id": user_id, "count": count, "price": count * PRICE_PER_DIAMOND, "status": "new"}
            waiting_for_check[user_id] = order_id
        show_order_confirmation(user_id, order_id)
        safe_api(bot.send_message, user_id, "📸 <b>Пожалуйста, пришлите чек об оплате (фото или файл).</b>")
    except ValueError:
        safe_api(bot.send_message, user_id, "❗ <b>Пожалуйста, введите корректное число.</b>")

@bot.message_handler(commands=["confirm"])
def admin_confirm_order(message):
    if message.from_user.id not in ADMIN_IDS:
        safe_api(bot.reply_to, message, "🚫 Только для админа.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        safe_api(bot.reply_to, message, "Используйте: /confirm <user_id>")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        safe_api(bot.reply_to, message, "Неверный ID пользователя.")
        return
    with LOCK:
        order_id = next((oid for oid, o in diamond_orders.items() if o["user_id"] == user_id and o["status"] == "pending"), None)
        order = diamond_orders.get(order_id)
        if not order:
            safe_api(bot.reply_to, message, "Заказ не найден или уже обработан.")
            return
        prof = ensure_profile(user_id, get_username_id(user_id))
        prof["diamonds"] += order["count"]
        waiting_for_check.pop(user_id, None)
        diamond_orders.pop(order_id, None)
        persist_profiles()
    safe_api(bot.reply_to, message, f"✅ Заказ пользователя {get_username_id(user_id)} подтверждён, алмазы начислены.")
    safe_api(bot.send_message, user_id, f"🎉 <b>Ваш заказ на {order['count']} алмазов подтверждён и зачислен!</b>", reply_markup=profile_reply_markup(uid=user_id))

@bot.message_handler(commands=["cancel"])
def admin_cancel_order(message):
    if message.from_user.id not in ADMIN_IDS:
        safe_api(bot.reply_to, message, "🚫 Только для админа.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        safe_api(bot.reply_to, message, "Используйте: /cancel <user_id>")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        safe_api(bot.reply_to, message, "Неверный ID пользователя.")
        return
    with LOCK:
        order_id = next((oid for oid, o in diamond_orders.items() if o["user_id"] == user_id and o["status"] == "pending"), None)
        if order_id:
            diamond_orders.pop(order_id, None)
            waiting_for_check.pop(user_id, None)
            safe_api(bot.reply_to, message, f"❌ Заказ пользователя {get_username_id(user_id)} отменён.")
            safe_api(bot.send_message, user_id, "❌ <b>Ваш заказ был отменён администратором.</b>", reply_markup=profile_reply_markup(uid=user_id))
        else:
            safe_api(bot.reply_to, message, "Заказ не найден.")

# ============================ ADMIN COMMANDS ============================
@bot.message_handler(commands=["addadmin"])
def add_admin_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        safe_api(bot.reply_to, message, "🚫 Только для админа.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        safe_api(bot.reply_to, message, "Используйте: /addadmin <user_id>")
        return
    try:
        user_id = int(parts[1])
        with LOCK:
            if user_id in ADMIN_IDS:
                safe_api(bot.reply_to, message, f"⚠️ Пользователь {get_username_id(user_id)} уже админ.")
                return
            ADMIN_IDS.add(user_id)
            persist_admins()
        safe_api(bot.reply_to, message, f"✅ {get_username_id(user_id)} добавлен в админы.")
        safe_api(bot.send_message, user_id, "🎉 Вы назначены администратором бота!")
    except ValueError:
        safe_api(bot.reply_to, message, "⚠️ Неверный ID пользователя.")

@bot.message_handler(commands=["removeadmin"])
def remove_admin_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        safe_api(bot.reply_to, message, "🚫 Только для админа.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        safe_api(bot.reply_to, message, "Используйте: /removeadmin <user_id>")
        return
    try:
        user_id = int(parts[1])
        with LOCK:
            if user_id not in ADMIN_IDS:
                safe_api(bot.reply_to, message, f"⚠️ Пользователь {get_username_id(user_id)} не админ.")
                return
            if user_id == message.from_user.id:
                safe_api(bot.reply_to, message, "⚠️ Нельзя удалить себя из админов.")
                return
            ADMIN_IDS.discard(user_id)
            persist_admins()
        safe_api(bot.reply_to, message, f"✅ {get_username_id(user_id)} удалён из админов.")
        safe_api(bot.send_message, user_id, "❌ Вы больше не администратор бота.")
    except ValueError:
        safe_api(bot.reply_to, message, "⚠️ Неверный ID пользователя.")

@bot.message_handler(commands=["broadcast"])
def broadcast_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        safe_api(bot.reply_to, message, "🚫 Только для админа.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_api(bot.reply_to, message, "Используйте: /broadcast <сообщение>")
        return
    broadcast_msg = parts[1]
    with LOCK:
        user_ids = list(profiles.keys())
    for uid in user_ids:
        try:
            safe_api(bot.send_message, int(uid), f"📢 <b>Объявление от админа:</b>\n{broadcast_msg}")
        except Exception:
            logger.debug("Failed to send broadcast to %s", uid)
    safe_api(bot.reply_to, message, f"✅ Сообщение отправлено {len(user_ids)} пользователям.")

@bot.message_handler(commands=["listusers"])
def list_users_cmd(message):
    if message.from_user.id not in ADMIN_IDS:
        safe_api(bot.reply_to, message, "🚫 Только для админа.")
        return
    with LOCK:
        if not profiles:
            safe_api(bot.reply_to, message, "⚠️ Нет зарегистрированных пользователей.")
            return
        lines = ["<b>Список пользователей:</b>"]
        for uid, prof in profiles.items():
            name = prof.get("name", str(uid))
            money = prof.get("money", 0)
            diamonds = prof.get("diamonds", 0)
            lines.append(f"ID: {uid}, Имя: {name}, Баланс: {money} сум, Алмазы: {diamonds} 💎")
        text = "\n".join(lines)
    safe_api(bot.reply_to, message, text)

# ============================ GAME CORE ============================
def new_game_struct() -> Dict[str, Any]:
    return {
        "state": None,
        "players": [],
        "roles": {},
        "alive": [],
        "phase": None,
        "votes": {},
        "night_kill": None,
        "doctor_save": None,
        "join_msg_id": None,
        "vote_msg_id": None,
        "kill_count": {},
        "started_at": None,
        "current_night_msgs": [],
        "phase_start_time": None,
    }

def start_registration_timer(chat_id: int) -> None:
    key = cid_str(chat_id)
    cancel_registration_timer(chat_id)
    t = threading.Timer(REGISTRATION_TIMEOUT, registration_timeout_handler, args=(chat_id,))
    with LOCK:
        timers.setdefault(key, {})["registration"] = t
    t.daemon = True
    t.start()

def cancel_registration_timer(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        t = timers.get(key, {}).get("registration")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
            timers[key]["registration"] = None

def registration_timeout_handler(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game or game.get("state") != "waiting":
            return
        players = game.get("players", [])
    if len(players) >= MIN_PLAYERS:
        safe_api(bot.send_message, chat_id, f"⏱ <b>Авто-старт: найдено {len(players)} игроков — начинаем игру!</b>")
        begin_game_by_chat(chat_id, auto=True)
    else:
        safe_api(bot.send_message, chat_id, f"⏱ <b>Время регистрации истекло.</b> Присоединилось {len(players)} — недостаточно (нужно {MIN_PLAYERS}).")

def update_registration_message(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        players = list(game.get("players", []))
        msg_id = game.get("join_msg_id")
    if not players:
        txt = "🎲 <b>Регистрация началась!</b> Нажмите кнопку, чтобы присоединиться:"
    else:
        lines = [f"🎲 <b>Регистрация! Присоединились {len(players)} игрока(ов):</b>"]
        for i, uid in enumerate(players, 1):
            lines.append(f"{i}. {get_username_id(uid)}")
        lines.append("\nНажмите кнопку, чтобы присоединиться:")
        txt = "\n".join(lines)
    try:
        if msg_id:
            safe_api(bot.edit_message_text, txt, chat_id, msg_id, reply_markup=join_markup())
        else:
            sent = safe_api(bot.send_message, chat_id, txt, reply_markup=join_markup())
            if sent:
                with LOCK:
                    games[key]["join_msg_id"] = sent.message_id
                    persist_games()
    except Exception:
        logger.exception("update_registration_message failed")

@bot.message_handler(commands=["startgame"])
def startgame_cmd(message):
    if message.chat.type not in ("group", "supergroup"):
        safe_api(bot.reply_to, message, "⚠️ /startgame только в группе.")
        return
    chat_id = message.chat.id
    key = cid_str(chat_id)
    with LOCK:
        if key in games and games[key].get("state") == "started":
            safe_api(bot.reply_to, message, "⚠️ <b>Игра уже идёт!</b>")
            return
        games[key] = new_game_struct()
        games[key]["state"] = "waiting"
        persist_games()
    sent = safe_api(bot.send_message, chat_id, "🎲 <b>Регистрация началась!</b> Нажмите кнопку, чтобы присоединиться:", reply_markup=join_markup())
    if sent:
        with LOCK:
            games[key]["join_msg_id"] = sent.message_id
            persist_games()
    start_registration_timer(chat_id)

@bot.callback_query_handler(func=lambda c: c.data == "join_game")
def join_game_callback(call):
    try:
        chat_id = call.message.chat.id
        uid = call.from_user.id
        key = cid_str(chat_id)
        with LOCK:
            if key not in games or games[key]["state"] != "waiting":
                safe_answer_callback(call, "❌ Регистрация закрыта.")
                return
            game = games[key]
            if uid in game["players"]:
                safe_answer_callback(call, "✅ Вы уже в игре.")
                return
            game["players"].append(uid)
            game["kill_count"][uid_str(uid)] = 0
            ensure_profile(uid, get_username_obj(call.from_user))
            persist_games()
        safe_answer_callback(call, f"🎉 {get_username_obj(call.from_user)}, вы присоединились!")
        try:
            safe_api(bot.send_message, uid, f"✅ <b>Вы присоединились к игре в группе '{call.message.chat.title}'!</b>")
        except Exception:
            pass
        update_registration_message(chat_id)
    except Exception:
        logger.exception("join_game_callback failed")

def pretty_group_start(chat_id: int, players: List[int]) -> None:
    text = (
        "🤵🏻 <b>True Mafia</b>:\n"
        "Игра начинается! 🎉\n\n"
        "В течение нескольких секунд бот пришлёт вам личное сообщение с ролью.\n\n"
        "🌃 <b>Наступает ночь...</b>\n\n"
        "<b>Живые игроки:</b>\n"
    )
    for i, p in enumerate(players, 1):
        text += f"{i}. {get_username_id(p)}\n"
    safe_api(bot.send_message, chat_id, text)

def begin_game_by_chat(chat_id: int, auto: bool = False) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game or game.get("state") != "waiting":
            safe_api(bot.send_message, chat_id, "⚠️ <b>Нет регистрации.</b> Запустите /startgame.")
            return
        if len(game["players"]) < MIN_PLAYERS and not auto:
            safe_api(bot.send_message, chat_id, f"❌ <b>Для начала нужно минимум {MIN_PLAYERS} игрока(ов).</b>")
            return

        cancel_registration_timer(chat_id)
        players = list(game["players"])
        game["state"] = "started"
        game["alive"] = players.copy()
        game["phase"] = "night_mafia"
        game["votes"] = {}
        game["night_kill"] = None
        game["doctor_save"] = None
        game["kill_count"] = {uid_str(p): 0 for p in players}
        game["started_at"] = int(time.time())
        game["phase_start_time"] = int(time.time())
        game["current_night_msgs"] = []

        num_special = min(3, len(players))
        roles_list = ["🤵🏻 Дон", "💉 Доктор", "🕵️ Комиссар"][:num_special]

        guaranteed = [p for p in players if profiles.get(uid_str(p), {}).get("guaranteed_active_role")]
        random.shuffle(guaranteed)
        assigned: Dict[int, str] = {}
        available_special = roles_list.copy()

        for p in guaranteed:
            if not available_special:
                break
            assigned[p] = available_special.pop(0)
            profiles[uid_str(p)]["guaranteed_active_role"] = False

        remaining = [p for p in players if p not in assigned]
        random.shuffle(remaining)
        remaining_roles = available_special + ["👨🏼 Мирный житель"] * (len(remaining) - len(available_special))
        random.shuffle(remaining_roles)
        for p, r in zip(remaining, remaining_roles):
            assigned[p] = r
        game["roles"] = {uid_str(p): assigned[p] for p in assigned}

        for p in players:
            role = assigned.get(p, "👨🏼 Мирный житель")
            try:
                safe_api(bot.send_message, p, f"🎭 <b>Ваша роль: {role}</b>")
                ensure_profile(p, get_username_id(p))
                profiles[uid_str(p)]["doctor_save_used"] = False
            except Exception:
                logger.exception("failed send role to %s", p)
                safe_api(bot.send_message, chat_id, f"⚠️ Не удалось отправить роль {get_username_id(p)}. Попросите написать боту в ЛС.")

        persist_profiles()
        persist_games()

    pretty_group_start(chat_id, players)
    send_mafia_vote(chat_id)

@bot.message_handler(commands=["begin"])
def begin_cmd(message):
    if message.chat.type not in ("group", "supergroup"):
        safe_api(bot.reply_to, message, "⚠️ /begin только в группе.")
        return
    begin_game_by_chat(message.chat.id)

def start_phase_timer(chat_id: int, timeout: int, handler) -> None:
    key = cid_str(chat_id)
    cancel_phase_timer(chat_id)
    t = threading.Timer(timeout, handler, args=(chat_id,))
    t.daemon = True
    with LOCK:
        timers.setdefault(key, {})["phase"] = t
        games[key]["phase_start_time"] = int(time.time())
    t.start()

def cancel_phase_timer(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        t = timers.get(key, {}).get("phase")
        if t:
            try:
                t.cancel()
            except Exception:
                pass
            timers[key]["phase"] = None
            if key in games:
                games[key]["phase_start_time"] = None

# ============================ NIGHT: MAFIA ============================
def send_mafia_vote(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        roles = game.get("roles", {})
        alive = list(game.get("alive", []))
        mafia = [int(uid) for uid, r in roles.items() if "Дон" in r and int(uid) in alive]

    safe_api(bot.send_message, chat_id, "🌙 <b>Ночь. Город засыпает...</b>")

    if not mafia:
        with LOCK:
            games[key]["phase"] = "night_doctor"
            games[key]["phase_start_time"] = int(time.time())
            persist_games()
        send_doctor_save(chat_id)
        return

    msgs = []
    for m in mafia:
        kb = types.InlineKeyboardMarkup(row_width=1)
        for t in alive:
            if t == m:
                continue
            kb.add(types.InlineKeyboardButton(get_username_id(t), callback_data=f"mafia_kill:{t}"))
        sent = safe_api(bot.send_message, m, "💀 <b>Выберите жертву для убийства:</b>", reply_markup=kb)
        if sent:
            msgs.append((m, sent.message_id))

    with LOCK:
        games[key]["current_night_msgs"] = msgs
        games[key]["phase"] = "night_mafia"
        games[key]["phase_start_time"] = int(time.time())
        persist_games()
    start_phase_timer(chat_id, NIGHT_TIMEOUT, night_timeout)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("mafia_kill:"))
def mafia_kill_handler(call):
    try:
        target_id = int(call.data.split(":", 1)[1])
        user_id = call.from_user.id

        chat_id = None
        with LOCK:
            for cid, g in games.items():
                if user_id in g.get("players", []) and g.get("state") == "started":
                    chat_id = int(cid)
                    break
        if not chat_id:
            safe_answer_callback(call, "❌ Вы не участвуете.")
            return

        key = cid_str(chat_id)
        with LOCK:
            game = games.get(key)
            if not game or game.get("phase") != "night_mafia":
                safe_answer_callback(call, "⏳ Время для выбора мафии истекло.")
                return
            if "Дон" not in game["roles"].get(uid_str(user_id), ""):
                safe_answer_callback(call, "🚫 Вы не мафия.")
                return
            if target_id not in game["alive"] or target_id == user_id:
                safe_answer_callback(call, "⚠️ Неверный выбор.")
                return
            game["night_kill"] = target_id
            game["current_night_msgs"] = []
            persist_games()

        target_name = get_username_id(target_id)
        safe_answer_callback(call)
        safe_api(bot.send_message, user_id, f"💀 <b>Вы выбрали убить этого человека: {target_name}</b>")
        safe_api(bot.edit_message_reply_markup, user_id, call.message.id, reply_markup=None)

        cancel_phase_timer(chat_id)
        with LOCK:
            games[key]["phase"] = "night_doctor"
            persist_games()
        send_doctor_save(chat_id)
    except Exception:
        logger.exception("mafia_kill_handler failed")

# ============================ NIGHT: DOCTOR ============================
def send_doctor_save(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        doctors = [int(pid) for pid, role in game["roles"].items() if "Доктор" in role and int(pid) in game["alive"]]

    if not doctors:
        with LOCK:
            games[key]["phase"] = "night_commissar"
            games[key]["phase_start_time"] = int(time.time())
            persist_games()
        send_commissar_check(chat_id)
        return

    msgs = []
    for d in doctors:
        prof = ensure_profile(d, get_username_id(d))
        if prof.get("doctor_save_used"):
            continue
        kb = types.InlineKeyboardMarkup(row_width=1)
        with LOCK:
            alive = list(games[key]["alive"])
        for t in alive:
            kb.add(types.InlineKeyboardButton(get_username_id(t), callback_data=f"doctor_save:{t}"))
        sent = safe_api(bot.send_message, d, "💉 <b>Выберите игрока, которого хотите спасти сегодня ночью:</b>", reply_markup=kb)
        if sent:
            msgs.append((d, sent.message_id))

    with LOCK:
        games[key]["current_night_msgs"] = msgs
        games[key]["phase"] = "night_doctor"
        games[key]["phase_start_time"] = int(time.time())
        persist_games()
    start_phase_timer(chat_id, NIGHT_TIMEOUT, night_timeout)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("doctor_save:"))
def doctor_save_handler(call):
    try:
        user_id = call.from_user.id
        target_id = int(call.data.split(":", 1)[1])

        chat_id = None
        with LOCK:
            for cid, g in games.items():
                if user_id in g.get("players", []) and g.get("state") == "started":
                    chat_id = int(cid)
                    break
        if not chat_id:
            safe_answer_callback(call, "❌ Вы не участвуете.")
            return

        key = cid_str(chat_id)
        with LOCK:
            game = games.get(key)
            prof = ensure_profile(user_id, get_username_id(user_id))
            if not game or game.get("phase") != "night_doctor":
                safe_answer_callback(call, "⏳ Время для выбора доктора истекло.")
                return
            if prof.get("doctor_save_used"):
                safe_answer_callback(call, "⚠️ Вы уже использовали спасение доктора.")
                return
            if target_id not in game["alive"]:
                safe_answer_callback(call, "⚠️ Неверный выбор.")
                return
            game["doctor_save"] = target_id
            prof["doctor_save_used"] = True
            game["current_night_msgs"] = []
            persist_profiles()
            persist_games()

        target_name = get_username_id(target_id)
        safe_answer_callback(call)
        safe_api(bot.send_message, user_id, f"💉 <b>Вы выбрали лечить этого человека: {target_name}</b>")
        safe_api(bot.edit_message_reply_markup, user_id, call.message.id, reply_markup=None)

        cancel_phase_timer(chat_id)
        with LOCK:
            games[key]["phase"] = "night_commissar"
            persist_games()
        send_commissar_check(chat_id)
    except Exception:
        logger.exception("doctor_save_handler failed")

# ============================ NIGHT: COMMISSAR ============================
def send_commissar_check(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        commissars = [int(pid) for pid, role in game["roles"].items() if "Комиссар" in role and int(pid) in game["alive"]]

    if not commissars:
        start_day(chat_id)
        return

    with LOCK:
        alive = list(games[key]["alive"])

    msgs = []
    for c in commissars:
        kb = types.InlineKeyboardMarkup(row_width=1)
        for t in alive:
            kb.add(types.InlineKeyboardButton(get_username_id(t), callback_data=f"commissar_check:{t}"))
        sent = safe_api(bot.send_message, c, "🕵️ <b>Выберите игрока для проверки:</b>", reply_markup=kb)
        if sent:
            msgs.append((c, sent.message_id))

    with LOCK:
        games[key]["current_night_msgs"] = msgs
        games[key]["phase"] = "night_commissar"
        games[key]["phase_start_time"] = int(time.time())
        persist_games()
    start_phase_timer(chat_id, NIGHT_TIMEOUT, night_timeout)

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("commissar_check:"))
def commissar_check_handler(call):
    try:
        user_id = call.from_user.id
        target_id = int(call.data.split(":", 1)[1])

        chat_id = None
        with LOCK:
            for cid, g in games.items():
                if user_id in g.get("players", []) and g.get("state") == "started":
                    chat_id = int(cid)
                    break
        if not chat_id:
            safe_answer_callback(call, "❌ Вы не участвуете.")
            return

        key = cid_str(chat_id)
        with LOCK:
            game = games.get(key)
            if not game or game.get("phase") != "night_commissar":
                safe_answer_callback(call, "⏳ Время для проверки комиссара истекло.")
                return
            if target_id not in game["alive"]:
                safe_answer_callback(call, "⚠️ Неверный выбор.")
                return
            role = game["roles"].get(uid_str(target_id), "Неизвестно")
            is_mafia = "Дон" in role
            game["current_night_msgs"] = []
            persist_games()

        target_name = get_username_id(target_id)
        result = "мафия" if is_mafia else "не мафия"
        safe_answer_callback(call)
        safe_api(bot.send_message, user_id, f"🕵️ <b>Этот человек {target_name} — {result}</b>")
        safe_api(bot.edit_message_reply_markup, user_id, call.message.id, reply_markup=None)

        cancel_phase_timer(chat_id)
        start_day(chat_id)
    except Exception:
        logger.exception("commissar_check_handler failed")

def night_timeout(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game or game.get("state") != "started":
            return
        phase = game.get("phase")
        msgs = game.get("current_night_msgs", [])
        game["current_night_msgs"] = []
        persist_games()

    for uid, mid in msgs:
        safe_api(bot.edit_message_reply_markup, uid, mid, reply_markup=None)

    if phase == "night_mafia":
        with LOCK:
            games[key]["night_kill"] = None
            games[key]["phase"] = "night_doctor"
            games[key]["phase_start_time"] = int(time.time())
            persist_games()
        send_doctor_save(chat_id)
    elif phase == "night_doctor":
        with LOCK:
            games[key]["doctor_save"] = None
            games[key]["phase"] = "night_commissar"
            games[key]["phase_start_time"] = int(time.time())
            persist_games()
        send_commissar_check(chat_id)
    elif phase == "night_commissar":
        start_day(chat_id)

# ============================ DAY / VOTING ============================
def start_day(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        victim = game.get("night_kill")
        saved = game.get("doctor_save")

        prevented_by_protection = False
        if victim is not None:
            vic_prof = ensure_profile(victim, get_username_id(victim))
            if vic_prof.get("protection_active"):
                prevented_by_protection = True
                vic_prof["protection_active"] = False
                persist_profiles()

        if victim is not None and victim != saved and not prevented_by_protection:
            if victim in game["alive"]:
                game["alive"].remove(victim)
                safe_api(bot.send_message, chat_id, f"☠️ <b>Ночью погиб {get_username_id(victim)}.</b>")
        else:
            msg = "🌙 <b>Никто не погиб этой ночью.</b>"
            if prevented_by_protection:
                msg += " <i>Цель была под защитой.</i>"
            safe_api(bot.send_message, chat_id, msg)

        game["night_kill"] = None
        game["doctor_save"] = None
        game["phase"] = "day"
        game["votes"] = {}
        game["phase_start_time"] = int(time.time())
        alive_now = list(game.get("alive", []))
        persist_games()

    alive_list = "\n".join([f"{i}. {get_username_id(p)}" for i, p in enumerate(alive_now, 1)]) or "—"
    safe_api(bot.send_message, chat_id, f"🏙 <b>День.</b>\n<b>Живые игроки:</b>\n{alive_list}\n\nОбсуждаем и голосуем. Время: {DAY_TIMEOUT} сек.")
    send_day_vote_buttons(chat_id)
    start_phase_timer(chat_id, DAY_TIMEOUT, day_timeout)

def send_day_vote_buttons(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        alive = list(game.get("alive", []))
    if not alive:
        return
    kb = types.InlineKeyboardMarkup(row_width=2)
    for p in alive:
        kb.add(types.InlineKeyboardButton(get_username_id(p), callback_data=f"vote:{p}"))
    sent = safe_api(bot.send_message, chat_id, "<b>Нажмите на игрока, за которого голосуете:</b>", reply_markup=kb)
    if sent:
        with LOCK:
            games[key]["vote_msg_id"] = sent.message_id
            persist_games()

@bot.callback_query_handler(func=lambda c: c.data and c.data.startswith("vote:"))
def vote_handler(call):
    try:
        voter = call.from_user.id
        target = int(call.data.split(":", 1)[1])
        chat_id = call.message.chat.id
        key = cid_str(chat_id)

        with LOCK:
            game = games.get(key)
            if not game or game.get("state") != "started":
                safe_answer_callback(call, "⚠️ Игра не активна.")
                return
            if game.get("phase") != "day":
                safe_answer_callback(call, "⚠️ Сейчас не день для голосования.")
                return
            if voter not in game["alive"]:
                safe_answer_callback(call, "⚠️ Вы мертвы и не можете голосовать.")
                return
            if target not in game.get("alive", []):
                safe_answer_callback(call, "⚠️ Этот игрок уже мёртв или не в игре.")
                return
            game["votes"][uid_str(voter)] = target
            persist_games()
        safe_answer_callback(call, f"✅ Ваш голос за {get_username_id(target)} принят.")
        safe_api(bot.send_message, chat_id, f"🗳 <b>{get_username_id(voter)} проголосовал за {get_username_id(target)}.</b>")
    except Exception:
        logger.exception("vote_handler failed")

def day_timeout(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game or game.get("phase") != "day":
            return
        votes_map = dict(game.get("votes", {}))

    if not votes_map:
        safe_api(bot.send_message, chat_id, "⏱ <b>Время голосования истекло — голосов нет. Переход в ночь.</b>")
        with LOCK:
            game["votes"] = {}
            game["phase"] = "night_mafia"
            game["phase_start_time"] = int(time.time())
            persist_games()
        send_mafia_vote(chat_id)
        return

    cnt = Counter(votes_map.values())
    most_voted, most_count = cnt.most_common(1)[0]
    total_votes = sum(cnt.values())
    yes = most_count
    no = total_votes - most_count

    safe_api(bot.send_message, chat_id, f"<b>Голоса: всего {total_votes}. За казнь {get_username_id(most_voted)}: {yes} | Против: {no}</b>")

    with LOCK:
        if yes > no and most_voted in game.get("alive", []):
            game["alive"].remove(most_voted)
            role = game.get("roles", {}).get(uid_str(most_voted), "Неизвестно")
            safe_api(bot.send_message, chat_id, f"⚖️ <b>Казнён {get_username_id(most_voted)} ({role}).</b>")
        else:
            safe_api(bot.send_message, chat_id, "<b>Никто не был казнён.</b>")
        game["votes"] = {}
        persist_games()

    check_game_end(chat_id)

# ============================ END & REWARDS ============================
def reward_players(winner_side: str, game: Dict[str, Any]) -> None:
    roles = game.get("roles", {})
    players = game.get("players", [])
    with LOCK:
        for p in players:
            pkey = uid_str(p)
            prof = ensure_profile(p, get_username_id(p))
            role = roles.get(pkey, "")
            if winner_side == "Мирные жители":
                if "Дон" not in role:
                    prof["money"] = prof.get("money", 0) + 20
            elif winner_side == "Мафия":
                if "Дон" in role:
                    prof["money"] = prof.get("money", 0) + 10
        persist_profiles()

def check_game_end(chat_id: int) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        alive_roles = [game.get("roles", {}).get(uid_str(p), "") for p in game.get("alive", [])]
        mafia_alive = [r for r in alive_roles if "Дон" in r]
        citizens_alive = [r for r in alive_roles if "Дон" not in r]

    if not mafia_alive:
        safe_api(bot.send_message, chat_id, "🎉 <b>Граждане победили!</b>")
        with LOCK:
            reward_players("Мирные жители", game)
        send_final_stats_and_cleanup(chat_id, "Мирные жители")
        return

    if len(mafia_alive) >= len(citizens_alive):
        safe_api(bot.send_message, chat_id, "💀 <b>Мафия победила!</b>")
        with LOCK:
            reward_players("Мафия", game)
        send_final_stats_and_cleanup(chat_id, "Мафия")
        return

    safe_api(bot.send_message, chat_id, "🕹️ <b>Игра продолжается. Ночь.</b>")
    with LOCK:
        game["phase"] = "night_mafia"
        game["phase_start_time"] = int(time.time())
        persist_games()
    send_mafia_vote(chat_id)

def send_final_stats_and_cleanup(chat_id: int, winner: str) -> None:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if not game:
            return
        players = list(game.get("players", []))
        roles = dict(game.get("roles", {}))
        alive_now = set(game.get("alive", []))
        started = game.get("started_at") or int(time.time())

    lines = []
    lines.append("<b>Игра окончена!</b> 🎲")
    lines.append(f"<b>Победители:</b> {winner}\n")
    lines.append("<b>Роли игроков:</b>")
    for p in players:
        role = roles.get(uid_str(p), "Неизвестно")
        mark = " (жив)" if p in alive_now else ""
        lines.append(f"• {get_username_id(p)} — {role}{mark}")
    took = int(time.time()) - started
    mins = took // 60
    secs = took % 60
    lines.append(f"\nИгра длилась: {mins} мин. {secs} сек.")
    lines.append("\n💵 <b>Награды выданы автоматически</b> (см. /profile).")

    safe_api(bot.send_message, chat_id, "\n".join(lines))

    with LOCK:
        history.append({
            "chat_id": chat_id,
            "finished_at": int(time.time()),
            "winner": winner,
            "players": players,
            "roles": roles,
        })
        persist_history()
        games.pop(key, None)
        persist_games()
    cancel_phase_timer(chat_id)
    cancel_registration_timer(chat_id)

@bot.message_handler(commands=["endgame"])
def endgame_handler(msg):
    if msg.chat.type not in ("group", "supergroup"):
        safe_api(bot.reply_to, msg, "⚠️ /endgame только в группе.")
        return
    chat_id = msg.chat.id
    with LOCK:
        exists = cid_str(chat_id) in games
    if exists:
        send_final_stats_and_cleanup(chat_id, "Прервано админом")
        safe_api(bot.reply_to, msg, "<b>Игра остановлена и данные очищены.</b>")
    else:
        safe_api(bot.reply_to, msg, "<b>Игра не запущена.</b>")

# ============================ MISC ============================
@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🎮 Играть")
def private_play(msg):
    safe_api(bot.send_message, msg.from_user.id, "Добавь меня в группу и используй /startgame.", reply_markup=main_reply_markup())

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "👤 Профиль")
def private_profile(msg):
    cmd_profile(msg)

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "ℹ Помощь")
def private_help_button(msg):
    txt = (
        "📜 <b>Помощь — инструкции:</b>\n"
        "• Добавьте бота в группу и используйте /startgame\n"
        "• /begin — начать игру (организатор)\n"
        "• Днём голосуем кнопками (30 сек)\n"
        "• В профиле: алмазы, баланс и покупка защиты за 100"
    )
    safe_api(bot.send_message, msg.from_user.id, txt)

@bot.message_handler(func=lambda m: m.chat.type == "private" and m.text == "🔙 Главное меню")
def back_to_main(msg):
    safe_api(bot.send_message, msg.from_user.id, "<b>Главное меню:</b>", reply_markup=main_reply_markup())

# ============================ NIGHT SILENCE ENFORCER ============================
def is_night_phase(chat_id: int) -> bool:
    key = cid_str(chat_id)
    with LOCK:
        game = games.get(key)
        if game and game.get("state") == "started":
            phase = game.get("phase")
            return phase and phase.startswith("night")
    return False

@bot.message_handler(func=lambda m: m.chat.type in ("group", "supergroup") and is_night_phase(m.chat.id))
def enforce_night_silence(msg):
    safe_api(bot.delete_message, msg.chat.id, msg.message_id)

# ============================ STARTUP RESTORE ============================
def startup_restore() -> None:
    ensure_data_dir()
    changed = False
    with LOCK:
        for cid, g in list(games.items()):
            if g.get("state") == "started":
                try:
                    safe_api(bot.send_message, int(cid), "⚠️ <b>Бот перезапущен.</b> Текущая игра остановлена. Запустите /startgame заново.")
                except Exception:
                    pass
                g["state"] = "waiting"
                g["phase"] = None
                g["phase_start_time"] = None
                changed = True
        if changed:
            persist_games()

startup_restore()

# ============================ RUN ============================
if __name__ == "__main__":
    print("Mafia bot running...")
    if TOKEN == "REPLACE_ME" or not BOT_USERNAME:
        logger.error("Установите переменные окружения MAFIA_BOT_TOKEN и MAFIA_BOT_USERNAME.")
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=60)
    except KeyboardInterrupt:
        print("Stopped by user")
    except Exception as e:
        logger.exception("Polling stopped: %s", e)
