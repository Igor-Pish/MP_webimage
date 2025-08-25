import os
import logging
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import CommandStart

from dotenv import load_dotenv
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
FLASK_API_BASE = os.getenv("FLASK_API_BASE", "http://localhost:8000/api")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

@dp.message_handler(CommandStart())
async def on_start(m: types.Message):
    await m.answer(
        "Привет! Я бот мониторинга цен.\n"
        "Когда появятся нарушения — пришлю уведомление.\n"
        "В уведомлении будет кнопка «Отправить артикулы»."
    )

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("send_articles:"))
async def on_send_articles(cb: types.CallbackQuery):
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

    items = data.get("items", [])
    if not items:
        await cb.message.answer("На данный момент нарушений для этого селлера нет ✅")
        return

    nm_ids = [str(it.get("nm_id")) for it in items if it.get("nm_id")]
    chunk = 70
    for i in range(0, len(nm_ids), chunk):
        part = ", ".join(nm_ids[i:i+chunk])
        await cb.message.answer("Артикулы:\n" + part)

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")
    executor.start_polling(dp, skip_updates=True)

if __name__ == "__main__":
    main()