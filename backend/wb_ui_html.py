import re
import time
import html
from typing import Optional, Dict

import requests
from bs4 import BeautifulSoup

SESSION = requests.Session()
SESSION.headers.update({
    # реалистичный браузер
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.wildberries.ru/",
})

_PRICE_DIGITS = re.compile(r"[^\d]")

def _to_float_rub(txt: Optional[str]) -> Optional[float]:
    """ '1 480 ₽' -> 1480.0 """
    if not txt:
        return None
    digits = _PRICE_DIGITS.sub("", txt)
    if not digits:
        return None
    try:
        return float(digits)
    except ValueError:
        return None

def _text(soup: BeautifulSoup, css: str) -> Optional[str]:
    el = soup.select_one(css)
    if not el:
        return None
    return html.unescape(el.get_text(strip=True))

def fetch_ui_prices_from_html(nm_id: int, retries: int = 2) -> Dict:
    """
    Возвращает ТОЛЬКО UI-цены, как в карточке:
      - current_price_ui (фиолетовая, итоговая)
      - price_before_discount_ui (зачёркнутая "до скидок")
      - wallet_price_ui (если WB показывает цену с кошельком)
    Ничего не пишет в БД.
    """
    url = f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"
    last_exc = None

    for attempt in range(retries + 1):
        try:
            r = SESSION.get(url, timeout=15)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}")

            soup = BeautifulSoup(r.text, "html.parser")

            # основные селекторы WB
            wallet_txt  = _text(soup, "span.price-block__wallet-price")
            current_txt = _text(soup, "ins.price-block__final-price")
            old_txt     = _text(soup, "del.price-block__old-price span")

            # запасные варианты (иногда верстка другая)
            if current_txt is None:
                current_txt = _text(soup, ".price-block__price-new") or _text(soup, ".price__lower-price")
            if old_txt is None:
                old_txt = _text(soup, ".price-block__price-old del span") or _text(soup, ".price__old-price")

            return {
                "nm_id": nm_id,
                "url": url,
                "current_price_ui": _to_float_rub(current_txt),
                "price_before_discount_ui": _to_float_rub(old_txt),
                "wallet_price_ui": _to_float_rub(wallet_txt),
                "source": "html",
            }
        except Exception as e:
            last_exc = e
            time.sleep(0.5 * (2 ** attempt))  # 0.5s, 1s, 2s

    raise RuntimeError(f"WB HTML parse failed for nm_id={nm_id}: {last_exc}")