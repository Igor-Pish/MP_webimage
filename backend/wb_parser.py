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
    url = (f"https://card.wb.ru/cards/v2/detail"
           f"?appType=1&curr=rub&dest=-1257786&lang=ru&nm={nm_id}")

    r = SESSION.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()

    products = (data.get("data") or {}).get("products") or []
    if not products:
        raise ValueError(f"Товар с nm_id={nm_id} не найден")

    p = products[0]

    basic_u = 0
    product_u = 0
    for s in p.get("sizes", []):
        pr = (s or {}).get("price") or {}
        # в новом формате поля называются basic/product/total/...
        if not basic_u:
            basic_u = int(pr.get("basic") or 0)
        if not product_u:
            product_u = int(pr.get("product") or 0)
        if basic_u and product_u:
            break

    price_before_discount = basic_u / 100.0 if basic_u else 0.0
    price_after_seller_discount = product_u / 100.0 if product_u else 0.0

    return {
        "nm_id": nm_id,
        "brand": p.get("brand", ""),
        "title": p.get("name", ""),
        "price_before_discount": price_before_discount,          # ← basic/100
        "price_after_seller_discount": price_after_seller_discount,  # ← product/100
    }
