import os
import requests
from typing import Optional

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID", "")

API = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_violation_alert(seller_id: int, seller_name: Optional[str]) -> None:
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        return

    seller_link = f"https://www.wildberries.ru/seller/{seller_id}"
    title = seller_name or f"ID {seller_id}"

    text = (
        f"<b>Нарушение цен</b>\n"
        f"Селлер: <a href=\"{seller_link}\">{title}</a>\n"
        f"Нажмите кнопку ниже, чтобы получить артикулы."
    )

    keyboard = {
        "inline_keyboard": [
            [
                {
                    "text": "Отправить артикулы",
                    "callback_data": f"send_articles:{seller_id}"
                }
            ]
        ]
    }

    payload = {
        "chat_id": ADMIN_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "reply_markup": keyboard,
    }
    try:
        r = requests.post(f"{API}/sendMessage", json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        print(f"[tg] send_violation_alert error: {e}")