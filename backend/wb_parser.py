import requests

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru,en;q=0.9",
    "Connection": "keep-alive",
})

def fetch_wb_price(nm_id: int) -> dict:
    """
    Возвращаем:
      - price_before_discount  ← price.basic / 100
      - price_after_seller_discount ← price.product / 100
    Если в карточке НЕТ ни одного размера с валидной ценой — возвращаем price_after_seller_discount = -1
    (сентинел «нет в продаже/цена недоступна»).
    """
    url = f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&lang=ru&nm={nm_id}"

    r = SESSION.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    try:
        product = data["data"]["products"][0]
    except (KeyError, IndexError):
        # реально нет карточки
        raise ValueError(f"Товар с nm_id={nm_id} не найден")

    # селлер
    seller_id = product.get("supplierId")
    seller_name = product.get("supplier")

    # пробегаемся по всем размерам и берём первый валидный прайс
    price_before_discount = None
    price_after_seller_discount = None

    for s in product.get("sizes", []) or []:
        pr = (s or {}).get("price") or {}
        basic = pr.get("basic")
        prod = pr.get("product")

        # оба числа существуют и > 0
        if isinstance(basic, (int, float)) and isinstance(prod, (int, float)) and basic > 0 and prod > 0:
            price_before_discount = basic / 100.0
            price_after_seller_discount = prod / 100.0
            break

    # если ни в одном размере нет цены — помечаем как «нет в продаже»
    if price_after_seller_discount is None:
        price_before_discount = 0.0
        price_after_seller_discount = -1.0

    return {
        "nm_id": nm_id,
        "brand": product.get("brand", ""),
        "title": product.get("name", ""),
        "price_before_discount": price_before_discount,
        "price_after_seller_discount": price_after_seller_discount,
        "seller_id": seller_id,
        "seller_name": seller_name,
    }