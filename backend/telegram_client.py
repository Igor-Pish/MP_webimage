import os
import requests

from db import list_admin_chat_ids, sales_last_24h

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

def _send_text(chat_id: int, text: str, reply_markup=None):
    if not BOT_TOKEN:
        return False
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    r = requests.post(f"{API}/sendMessage", json=payload, timeout=15)
    r.raise_for_status()
    return True

def send_daily_summary(violators, link_to_ui="https://mp.web-image.su"):
    """
    violators: список словарей вида:
      [{"seller_id": 123, "seller_name": "Название", "violations": 7}, ...]
    Рассылаем всем админам из БД (таблица tg_admins).
    """
    # ленивый импорт, чтобы не городить циклические зависимости
    try:
        from db import list_admin_chat_ids
        admins = list_admin_chat_ids()
    except Exception:
        admins = []

    if not admins:
        return {"ok": 0, "fail": 0}

    if not violators:
        text = "На сейчас нарушителей нет ✅"
    else:
        lines = ["<b>Ежедневная сводка по нарушителям</b>", ""]
        sales = sales_last_24h()
        for v in violators:
            v["sold_last_24h"] = sales.get(v["seller_id"], 0)
            sid = v.get("seller_id")
            name = v.get("seller_name") or f"ID {sid}"
            cnt = v.get("violations", 0)
            link = f"https://www.wildberries.ru/seller/{sid}"
            lines.append(f"— <a href=\"{link}\">{name}</a>: {cnt}")
            lines.append(f"  Продано за сутки: {v['sold_last_24h']}")
        if link_to_ui:
            lines += ["", f"<a href=\"{link_to_ui}\">Открыть панель</a>"]
        text = "\n".join(lines)

    ok = fail = 0
    for chat_id in admins:
        try:
            _send_text(chat_id, text)
            ok += 1
        except Exception:
            fail += 1
    return {"ok": ok, "fail": fail}
