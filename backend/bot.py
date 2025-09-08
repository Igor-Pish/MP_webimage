import os
import logging
import asyncio
import aiohttp
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.dispatcher.filters import CommandStart, Command

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import (
    EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_EXECUTED
)
from pytz import timezone

from telegram_client import send_daily_summary
from db import (
    list_admin_chat_ids, upsert_admin, delete_admin,
    get_seller_name, list_sellers_with_violations
)

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
FLASK_API_BASE = os.getenv("FLASK_API_BASE", "http://localhost:8000/api")
UI_URL = os.getenv("UI_URL", "https://mp.web-image.su")

FLASK_API_BASE = os.getenv("FLASK_API_BASE", "https://mp.web-image.su/api")

SUPERADMIN_CHAT_ID = os.getenv("TELEGRAM_SUPERADMIN_CHAT_ID")
SUPERADMIN_CHAT_ID = int(SUPERADMIN_CHAT_ID) if SUPERADMIN_CHAT_ID else None

SCHED_TZ = os.getenv("SCHED_TZ", "Europe/Moscow")

# Логирование
logging.basicConfig(level=logging.INFO)
logging.getLogger("apscheduler").setLevel(logging.INFO)

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# Холдер планировщика: создаём в on_startup, чтобы привязать к актуальному loop
scheduler: AsyncIOScheduler = None

# ===== Utils =====
def is_admin(user_id: int) -> bool:
    try:
        if SUPERADMIN_CHAT_ID and int(user_id) == SUPERADMIN_CHAT_ID:
            return True
        return int(user_id) in set(list_admin_chat_ids())
    except Exception:
        return False

async def daily_summary_job():
    """Собрать нарушителей и разослать сводку всем админам."""
    try:
        violators = list_sellers_with_violations()
        send_daily_summary(violators, link_to_ui=UI_URL)
        logging.info("[scheduler] summary sent: %d violators", len(violators or []))
    except Exception as e:
        logging.exception("[scheduler] daily_summary_job error: %s", e)

def _job_listener(event):
    if event.code == EVENT_JOB_EXECUTED:
        logging.info("[APS] job executed: %s", event.job_id)
    elif event.code == EVENT_JOB_MISSED:
        logging.warning("[APS] job MISSED: %s", event.job_id)
    elif event.code == EVENT_JOB_ERROR:
        logging.exception("[APS] job ERROR: %s", event.job_id)

async def daily_silent_refresh():
    url = f"{FLASK_API_BASE}/products/refresh-batch?full=1&silent=1"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, timeout=600) as r:
                txt = await r.text()
                print("[daily_silent_refresh]", r.status, txt[:200])
    except Exception as e:
        print("[daily_silent_refresh] error:", e)

# ===== Handlers =====
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
        await m.answer(f"Ошибка получения админов: {e}", parse_mode=None)
        return

    if not ids_:
        await m.answer("Админов нет.", parse_mode=None)
        return
    ids_str = ", ".join([str(x) for x in ids_])
    await m.answer(f"Админы: {ids_str}", parse_mode=None)

@dp.message_handler(Command("addadmin"))
async def cmd_addadmin(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Использование: /addadmin chat_id", parse_mode=None)
        return
    try:
        new_id = int(parts[1])
    except Exception:
        await m.answer("chat_id должен быть числом", parse_mode=None)
        return

    username = None
    if m.reply_to_message and m.reply_to_message.from_user:
        username = m.reply_to_message.from_user.username

    try:
        upsert_admin(new_id, username)
        await m.answer(f"Добавлен админ: {new_id}", parse_mode=None)
    except Exception as e:
        await m.answer(f"Ошибка: {e}", parse_mode=None)

@dp.message_handler(Command("deladmin"))
async def cmd_deladmin(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    parts = (m.text or "").split()
    if len(parts) < 2:
        await m.answer("Использование: /deladmin chat_id", parse_mode=None)
        return
    try:
        del_id = int(parts[1])
    except Exception:
        await m.answer("chat_id должен быть числом", parse_mode=None)
        return

    if SUPERADMIN_CHAT_ID and del_id == SUPERADMIN_CHAT_ID:
        await m.answer("Суперадмина удалить нельзя ❌", parse_mode=None)
        return

    try:
        delete_admin(del_id)
        await m.answer(f"Удалён админ: {del_id}", parse_mode=None)
    except Exception as e:
        await m.answer(f"Ошибка: {e}", parse_mode=None)

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
                    await cb.message.answer(f"Не удалось получить данные: {r.status}\n{txt}", parse_mode=None)
                    return
                data = await r.json()
    except Exception as e:
        await cb.message.answer(f"Ошибка запроса к API: {e}", parse_mode=None)
        return

    items = data.get("items", []) or []
    if not items:
        await cb.message.answer("На данный момент нарушений для этого селлера нет ✅", parse_mode=None)
        return

    nm_ids = [str(it.get("nm_id")) for it in items if it.get("nm_id")]
    seller_link = f"https://www.wildberries.ru/seller/{seller_id}"
    seller_name = get_seller_name(seller_id)
    title = seller_name or (f"ID {seller_id}")

    # Шлём «шапку» + чанками артикулы
    await cb.message.answer(f'Артикулы продавца <a href="{seller_link}">{title}</a>:\n')

    chunk = 70
    for i in range(0, len(nm_ids), chunk):
        part = ", ".join(nm_ids[i:i + chunk])
        await cb.message.answer(part, parse_mode=None)

@dp.message_handler(Command("summary_now"))
async def cmd_summary_now(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    await m.answer("Отправляю сводку…", parse_mode=None)
    try:
        violators = list_sellers_with_violations()
        res = send_daily_summary(violators, link_to_ui=UI_URL)
        ok = res.get("ok", 0) if isinstance(res, dict) else "?"
        fail = res.get("fail", 0) if isinstance(res, dict) else "?"
        await m.answer(f"Готово. Отправлено: {ok}, ошибок: {fail}", parse_mode=None)
    except Exception as e:
        await m.answer(f"Ошибка: {e}", parse_mode=None)

@dp.message_handler(Command("time"))
async def cmd_time_now(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    try:
        from datetime import datetime
        now = datetime.now(timezone(SCHED_TZ))
        # без HTML-парсинга, чтобы не падало на спецсимволах
        await m.answer(
            "Серверное время: {}\nЧасовой пояс: {}\n".format(
                now.strftime('%Y-%m-%d %H:%M:%S'), SCHED_TZ
            ),
            parse_mode=None
        )
    except Exception as e:
        await m.answer(f"Ошибка: {e}", parse_mode=None)

@dp.message_handler(Command("jobs"))
async def cmd_jobs(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    jobs = scheduler.get_jobs() if scheduler else []
    lines = [f"- {j.id} → next={j.next_run_time}" for j in jobs]
    await m.answer("Задания:\n" + ("\n".join(lines) if lines else "нет"), parse_mode=None)

@dp.message_handler(Command("test_in_30s"))
async def cmd_test_in_30s(m: types.Message):
    if not is_admin(m.from_user.id):
        return
    from datetime import datetime, timedelta
    when = datetime.now(timezone(SCHED_TZ)) + timedelta(seconds=30)
    scheduler.add_job(daily_summary_job, "date", run_date=when, id="once_30s", replace_existing=True)
    await m.answer(f"Ок, сводка будет отправлена в {when.strftime('%H:%M:%S')}", parse_mode=None)

# ===== Scheduler lifecycle =====
async def on_startup(dp_: Dispatcher):
    global scheduler
    loop = asyncio.get_event_loop()

    # Настройки ежедневной сводки из ENV (по умолчанию 10:00 по будням)
    SUM_HOUR = int(os.getenv("SUMMARY_HOUR", "10"))
    SUM_MIN = int(os.getenv("SUMMARY_MINUTE", "0"))
    SUM_DOW = os.getenv("SUMMARY_DAYS", "mon-fri")  # "mon-fri" или "mon,wed,fri"

    scheduler = AsyncIOScheduler(
        event_loop=loop,
        timezone=timezone(SCHED_TZ),
        job_defaults=dict(
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,  # выполнить, если проспали, в течение часа
        ),
    )
    scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_MISSED | EVENT_JOB_ERROR)

    # Ежедневная сводка нарушителей
    scheduler.add_job(
        daily_summary_job,
        trigger="cron",
        day_of_week=SUM_DOW,
        hour=SUM_HOUR,
        minute=SUM_MIN,
        id="daily_summary",
        replace_existing=True,
    )
    # Ежедневное обновление всех товаров в «тихом» режиме (без пушей)
    scheduler.add_job(
        daily_silent_refresh,
        trigger="interval",
        hours=6,
        id="silent_refresh",
        replace_existing=True,
    )

    scheduler.start()
    logging.info("[APS] started. Jobs: %s", [j.id for j in scheduler.get_jobs()])

async def on_shutdown(dp_: Dispatcher):
    global scheduler
    if scheduler:
        scheduler.shutdown(wait=False)
        logging.info("[APS] stopped")

def main():
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

    executor.start_polling(
        dp,
        skip_updates=True,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
    )


if __name__ == "__main__":
    main()