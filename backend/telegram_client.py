import os
import requests

from db import list_admin_chat_ids  # <-- берём список админов из БД

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

API = f"https://api.telegram.org/bot{BOT_TOKEN}"

def _send_to(chat_id, text, keyboard=None):
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if keyboard:
        payload["reply_markup"] = keyboard
    try:
        r = requests.post(f"{API}/sendMessage", json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"[tg] send error to {chat_id}: {e}")

def send_violation_alert(seller_id, seller_name=None):
    """
    Отправить всем админам уведомление о нарушителе с кнопкой «Отправить артикулы».
    """
    if not BOT_TOKEN:
        return

    admins = list_admin_chat_ids()
    if not admins:
        # если админов в таблице нет — тихо выходим (ничего не шлём)
        return

    seller_link = f"https://www.wildberries.ru/seller/{seller_id}"
    title = seller_name or ("ID %s" % seller_id)

    text = (
        "<b>Нарушение цен</b>\n"
        "Селлер: <a href=\"%s\">%s</a>\n"
        "Нажмите кнопку ниже, чтобы получить артикулы."
    ) % (seller_link, title)

    keyboard = {
        "inline_keyboard": [[
            {"text": "Отправить артикулы", "callback_data": "send_articles:%s" % seller_id}
        ]]
    }

    for chat_id in admins:
        _send_to(chat_id, text, keyboard)
