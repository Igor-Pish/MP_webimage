import os
import logging
import aiohttp
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import CommandStart, Command

# работаем напрямую с БД (как просил)
from db import list_admin_chat_ids, upsert_admin, delete_admin

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
FLASK_API_BASE = os.getenv("FLASK_API_BASE", "http://localhost:8000/api")

SUPERADMIN_CHAT_ID = os.getenv("TELEGRAM_SUPERADMIN_CHAT_ID")
SUPERADMIN_CHAT_ID = int(SUPERADMIN_CHAT_ID) if SUPERADMIN_CHAT_ID else None

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

def is_admin(user_id: int) -> bool:
    try:
        if SUPERADMIN_CHAT_ID and int(user_id) == SUPERADMIN_CHAT_ID:
            return True
        return int(user_id) in set(list_admin_chat_ids())
    except Exception:
        return False


@dp.message_handler(CommandStart())
async def on_start(m: types.Message):
    await m.answer(
        "Привет! Я бот мониторинга цен.\n"
        "Когда появятся нарушения — пришлю уведомление.\n\n"
        "Команды:\n"
        "/myid — показать ваш chat_id\n"
        "/admins — список админов (только для админов)\n"
        "/addadmin chat_id — добавить админа (только для админов)\n"
        "/deladmin chat_id — удалить админа (только для админов)"
    )


@dp.message_handler(Command("myid"))
async def cmd_myid(m: types.Message):
    await m.answer(f"Ваш chat_id: <code>{m.chat.id}</code>")


@dp.message_handler(Command("admins"))
async def cmd_admins(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    try:
        ids_ = list_admin_chat_ids()
    except Exception as e:
        await m.answer(f"Ошибка получения админов: {e}")
        return

    if not ids_:
        await m.answer("Админов нет.")
        return
    ids_str = ", ".join([str(x) for x in ids_])
    await m.answer(f"Админы: {ids_str}")


@dp.message_handler(Command("addadmin"))
async def cmd_addadmin(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Использование: /addadmin chat_id")
        return
    try:
        new_id = int(parts[1])
    except Exception:
        await m.answer("chat_id должен быть числом")
        return

    username = None
    # можно добавить ник из ответа на сообщение
    if m.reply_to_message and m.reply_to_message.from_user:
        username = m.reply_to_message.from_user.username

    try:
        upsert_admin(new_id, username)
        await m.answer(f"Добавлен админ: {new_id}")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")


@dp.message_handler(Command("deladmin"))
async def cmd_deladmin(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Использование: /deladmin chat_id")
        return
    if del_id == SUPERADMIN_CHAT_ID:
        await m.answer("Суперадмина удалить нельзя ❌")
        return
    try:
        del_id = int(parts[1])
    except Exception:
        await m.answer("chat_id должен быть числом")
        return
    try:
        delete_admin(del_id)
        await m.answer(f"Удалён админ: {del_id}")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")


@dp.callback_query_handler(lambda c: c.data and c.data.startswith("send_articles:"))
async def on_send_articles(cb: types.CallbackQuery):
    if not is_admin(cb.from_user.id):
        await cb.answer("У вас нет прав для этого действия", show_alert=True)
        return
    try:
        seller_id = int(cb.data.split(":", 1)[1])
    except Exception:
        await cb.answer("Некорректные данные", show_alert=True)
        return

    await cb.answer("Собираю артикулы…")
    url = f"{FLASK_API_BASE}/sellers/{seller_id}/violations"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as r:
                if r.status != 200:
                    txt = await r.text()
                    await cb.message.answer(f"Не удалось получить данные: {r.status}\n{txt}")
                    return
                data = await r.json()
    except Exception as e:
        await cb.message.answer(f"Ошибка запроса к API: {e}")
        return

    items = data.get("items", []) or []
    if not items:
        await cb.message.answer("На данный момент нарушений для этого селлера нет ✅")
        return

    nm_ids = [str(it.get("nm_id")) for it in items if it.get("nm_id")]
    chunk = 70
    for i in range(0, len(nm_ids), chunk):
        part = ", ".join(nm_ids[i:i + chunk])
        await cb.message.answer("Артикулы:\n" + part)

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")
    executor.start_polling(dp, skip_updates=True)

if __name__ == "__main__":
    main()