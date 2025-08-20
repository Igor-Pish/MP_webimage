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
    Возвращаем цены из sizes[].price:
      - price_before_discount  ← price.basic / 100   (до скидки)
      - price_after_seller_discount ← price.product / 100 (итоговая)
    Если price в первом размере пустой — ищем в следующем.
    """
    url = (f"https://card.wb.ru/cards/v2/detail?appType=1&curr=rub&dest=-1257786&lang=ru&nm={nm_id}")

    r = SESSION.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    try:
        product = data["data"]["products"][0]
        price_before_discount = product["sizes"][0]["price"]["basic"] / 100
        price_after_seller_discount = product["sizes"][0]["price"]["product"] / 100

        # НОВОЕ: селлер
        seller_id = product.get("supplierId")
        seller_name = product.get("supplier")
    except (KeyError, IndexError):
        raise ValueError(f"Товар с nm_id={nm_id} не найден")

    return {
        "nm_id": nm_id,
        "brand": product.get("brand", ""),
        "title": product.get("name", ""),
        "price_before_discount": price_before_discount,
        "price_after_seller_discount": price_after_seller_discount,
        "seller_id": seller_id,
        "seller_name": seller_name,
    }
