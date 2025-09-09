import os
from typing import Optional, List, Dict, Tuple
from dotenv import load_dotenv

load_dotenv()

import mysql.connector
from mysql.connector import pooling
from mysql.connector import Error as MySQLError

DB_CFG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "database": os.getenv("DB_NAME", ""),
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PASSWORD", ""),
    "charset": "utf8mb4",
    "use_unicode": True,
    "autocommit": False,
}
TABLE = os.getenv("DB_TABLE", "products")

# 0 = обновляем только пустые (price_after_seller_discount IS NULL/0)
STALE_HOURS = int(os.getenv("STALE_HOURS", "0"))

_pool: Optional[pooling.MySQLConnectionPool] = None

def _ensure_pool() -> None:
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(pool_name="mp_pool", pool_size=5, **DB_CFG)

def get_conn():
    _ensure_pool()
    return _pool.get_connection()

def _effective_table(table: Optional[str]) -> str:
    """Если table не передан — используем TABLE из .env."""
    return table if table else TABLE

def _row_to_dict(row) -> Dict:
    return {
        "nm_id": row[0],
        "brand": row[1],
        "title": row[2],
        "seller_id": row[3],
        "seller_name": row[4],
        "price_before_discount": float(row[5]) if row[5] is not None else 0.0,
        "price_after_seller_discount": float(row[6]) if row[6] is not None else 0.0,
        "ui_price": int(row[7]) if row[7] is not None else None,
        "rrc": float(row[8]) if row[8] is not None else None,
        "updated_at": row[9].isoformat() if row[9] else None,
        "sales_24h": int(row[10]) if row[10] is not None else None,
    }

def list_products(table: Optional[str] = None) -> List[Dict]:
    t = _effective_table(table)
    sql = f"""
        SELECT
            nm_id, brand, title, seller_id, seller_name,
            price_before_discount, price_after_seller_discount,
            ui_price, rrc, updated_at, sales_24h
        FROM {t}
        ORDER BY nm_id
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_products error: {e}")

def upsert_product(
    nm_id: int,
    brand: str,
    title: str,
    price_before: float,
    price_after: float,
    seller_id: Optional[int] = None,
    seller_name: Optional[str] = None,
    ui_price: Optional[int] = None,
    table: Optional[str] = None,
    sales_24h: Optional[int] = None,
) -> None:
    t = _effective_table(table)
    sql = f"""
        INSERT INTO {t}
            (nm_id, brand, title, seller_id, seller_name,
             price_before_discount, price_after_seller_discount, ui_price, updated_at, sales_24h)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        ON DUPLICATE KEY UPDATE
            brand = VALUES(brand),
            title = VALUES(title),
            seller_id = VALUES(seller_id),
            seller_name = VALUES(seller_name),
            price_before_discount = VALUES(price_before_discount),
            price_after_seller_discount = VALUES(price_after_seller_discount),
            ui_price = VALUES(ui_price),
            updated_at = NOW(),
            sales_24h = COALESCE(VALUES(sales_24h), sales_24h)
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (nm_id, brand, title, seller_id, seller_name, price_before, price_after, ui_price, sales_24h))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL upsert_product error: {e}")

def set_rrc(nm_id: int, rrc: Optional[float], table: Optional[str] = None) -> None:
    t = _effective_table(table)
    sql = f"UPDATE {t} SET rrc=%s, updated_at=NOW() WHERE nm_id=%s"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (rrc, nm_id))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL set_rrc error: {e}")

def delete_product(nm_id: int, table: Optional[str] = None) -> None:
    t = _effective_table(table)
    sql = f"DELETE FROM {t} WHERE nm_id=%s"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (nm_id,))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL delete_product error: {e}")

def delete_all_products(table: Optional[str] = None) -> int:
    """Удаляет все товары из таблицы."""
    t = _effective_table(table)
    sql = f"DELETE FROM {t}"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                affected = cur.rowcount if cur.rowcount is not None else 0
            conn.commit()
            return int(affected or 0)
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL delete_all_products error: {e}")

# ====== История остатков на складе (отдельная таблица) ======
def insert_stock_snapshot(nm_id: int, seller_id: int, stock_total: int) -> None:
    sql = "INSERT INTO wb_stock_history (nm_id, seller_id, stock_total) VALUES (%s, %s, %s)"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (nm_id, seller_id, stock_total))
        conn.commit()
    finally:
        conn.close()

# ====== Продажи по артикулу за 24 часа ======
def sales_24h_for_nm_list(nm_ids: List[int]) -> Dict[int, int]:
    """
    Возвращает {nm_id: sold_last_24h} только для переданных nm_id.
    Продажи считаем как суммарные положительные падения stock_total за последние 24 часа.
    """
    if not nm_ids:
        return {}
    # безопасно готовим плейсхолдеры
    placeholders = ",".join(["%s"] * len(nm_ids))
    sql = f"""
        SELECT nm_id, stock_total, snapshot_ts
        FROM wb_stock_history
        WHERE nm_id IN ({placeholders})
          AND snapshot_ts >= (NOW() - INTERVAL 24 HOUR)
        ORDER BY nm_id, snapshot_ts
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(nm_ids))
                rows = cur.fetchall()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL sales_24h_for_nm_list error: {e}")

    # rows: (nm_id, stock_total, snapshot_ts)
    by_nm: Dict[int, List[int]] = {}
    for nm_id, stock_total, _ts in rows:
        by_nm.setdefault(int(nm_id), []).append(int(stock_total))

    result: Dict[int, int] = {}
    for nm_id, series in by_nm.items():
        prev = None
        sold = 0
        for cur in series:
            if prev is not None and cur < prev:
                sold += (prev - cur)
            prev = cur
        result[nm_id] = sold
    return result

# ====== Продажи по селлеру за 24 часа ======
def sales_last_24h() -> dict[int, int]:
    """
    Возвращает словарь {seller_id: продано за 24 часа}.
    """
    sql = """
        SELECT seller_id, nm_id, stock_total, snapshot_ts
        FROM wb_stock_history
        WHERE snapshot_ts >= (NOW() - INTERVAL 24 HOUR)
        ORDER BY nm_id, snapshot_ts
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    finally:
        conn.close()

    by_nm = {}
    for seller_id, nm_id, stock_total, ts in rows:
        by_nm.setdefault((seller_id, nm_id), []).append(stock_total)

    sales = {}
    for (seller_id, nm_id), stocks in by_nm.items():
        prev = None
        sold = 0
        for cur in stocks:
            if prev is not None and cur < prev:
                sold += prev - cur
            prev = cur
        if sold > 0:
            sales[seller_id] = sales.get(seller_id, 0) + sold
    return sales

# ====== Поддержка батч-обновления ======
def _where_need_refresh() -> str:
    where = "(price_after_seller_discount IS NULL OR price_after_seller_discount = 0)"
    if STALE_HOURS > 0:
        where = f"({where} OR updated_at IS NULL OR updated_at < (NOW() - INTERVAL {STALE_HOURS} HOUR))"
    return where

def list_nm_ids_for_refresh(limit: int, table: Optional[str] = None) -> List[int]:
    t = _effective_table(table)
    where = _where_need_refresh()
    sql = f"""
        SELECT nm_id
        FROM {t}
        WHERE {where}
        ORDER BY (updated_at IS NULL) DESC, updated_at ASC, nm_id ASC
        LIMIT %s
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
                return [int(r[0]) for r in rows]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_nm_ids_for_refresh error: {e}")

def count_needing_refresh(table: Optional[str] = None) -> int:
    t = _effective_table(table)
    where = _where_need_refresh()
    sql = f"SELECT COUNT(*) FROM {t} WHERE {where}"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                (cnt,) = cur.fetchone()
                return int(cnt or 0)
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL count_needing_refresh error: {e}")

def list_nm_ids_any(limit: int, offset: int = 0, table: Optional[str] = None) -> List[int]:
    """Любые товары, стабильный порядок, с LIMIT/OFFSET (на случай полного прохода)."""
    t = _effective_table(table)
    sql = f"""
        SELECT nm_id
        FROM {t}
        ORDER BY nm_id ASC
        LIMIT %s OFFSET %s
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (limit, offset))
                return [int(r[0]) for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_nm_ids_any error: {e}")

def count_all_rows(table: Optional[str] = None) -> int:
    t = _effective_table(table)
    sql = f"SELECT COUNT(*) FROM {t}"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                (cnt,) = cur.fetchone()
                return int(cnt or 0)
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL count_all_rows error: {e}")

# ====== Для алармов ======
def list_violations_for_seller(seller_id: int, table: Optional[str] = None) -> List[Dict]:
    """Нарушения у селлера: ui_price < rrc."""
    t = _effective_table(table)
    sql = f"""
        SELECT nm_id
        FROM {t}
        WHERE seller_id = %s
          AND ui_price IS NOT NULL AND ui_price > 0
          AND rrc IS NOT NULL AND rrc > 0
          AND ui_price < rrc
        ORDER BY nm_id
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (seller_id,))
                return [{"nm_id": int(r[0])} for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_violations_for_seller error: {e}")

def list_sellers_with_violations(table: Optional[str] = None) -> List[Dict]:
    """Селлеры, у которых есть хотя бы одно нарушение."""
    t = _effective_table(table)
    sql = f"""
        SELECT seller_id, MAX(seller_name) as seller_name, COUNT(*) as cnt
        FROM {t}
        WHERE ui_price IS NOT NULL AND ui_price > 0
          AND rrc IS NOT NULL AND rrc > 0
          AND ui_price < rrc
        GROUP BY seller_id
        ORDER BY cnt DESC
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                return [
                    {"seller_id": int(r[0]), "seller_name": r[1], "violations": int(r[2])}
                    for r in rows
                ]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_sellers_with_violations error: {e}")

def get_seller_name(seller_id: int, table: Optional[str] = None) -> Optional[str]:
    t = _effective_table(table)
    sql = f"SELECT MAX(seller_name) FROM {t} WHERE seller_id=%s"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (seller_id,))
                (name,) = cur.fetchone()
                return name
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL get_seller_name error: {e}")

# ====== Telegram admins (отдельная таблица, без параметра table) ======
def list_admin_chat_ids():
    sql = "SELECT chat_id FROM tg_admins"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [int(r[0]) for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_admin_chat_ids error: {e}")

def upsert_admin(chat_id, username=None):
    sql = "INSERT INTO tg_admins (chat_id, username) VALUES (%s, %s) ON DUPLICATE KEY UPDATE username = VALUES(username)"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (int(chat_id), username))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL upsert_admin error: {e}")

def delete_admin(chat_id):
    sql = "DELETE FROM tg_admins WHERE chat_id=%s"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (int(chat_id),))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL delete_admin error: {e}")

# ====== Пагинация для фронтенда ======
def _sanitize_order(order_by: str, order_dir: str) -> Tuple[str, str]:
    allowed = {
        "nm_id": "nm_id",
        "seller_name": "seller_name",
        "price_after_seller_discount": "price_after_seller_discount",
        "updated_at": "updated_at",
        "rrc": "rrc",
        "sales_24h": "sales_24h",
    }
    col = allowed.get(order_by, "nm_id")
    dir_ = "DESC" if (order_dir or "").lower() == "desc" else "ASC"
    return col, dir_

def list_products_page(limit: int, offset: int, order_by: str = "nm_id", order_dir: str = "asc", table: Optional[str] = None) -> List[Dict]:
    t = _effective_table(table)
    col, dir_ = _sanitize_order(order_by, order_dir)
    sql = f"""
        SELECT
            nm_id, brand, title, seller_id, seller_name,
            price_before_discount, price_after_seller_discount,
            ui_price, rrc, updated_at, sales_24h
        FROM {t}
        ORDER BY {col} {dir_}, nm_id ASC
        LIMIT %s OFFSET %s
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (limit, offset))
                return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_products_page error: {e}")

def list_products_page_violations(limit: int, offset: int, order_by: str = "nm_id", order_dir: str = "asc", table: Optional[str] = None) -> List[Dict]:
    t = _effective_table(table)
    col, dir_ = _sanitize_order(order_by, order_dir)
    sql = f"""
        SELECT
            nm_id, brand, title, seller_id, seller_name,
            price_before_discount, price_after_seller_discount,
            ui_price, rrc, updated_at, sales_24h
        FROM {t}
        WHERE rrc IS NOT NULL AND rrc > 0
          AND ((ui_price IS NOT NULL AND ui_price > 0 AND ui_price < rrc)
          OR (price_after_seller_discount IS NOT NULL AND price_after_seller_discount > 0 AND price_after_seller_discount < rrc))
        ORDER BY {col} {dir_}, nm_id ASC
        LIMIT %s OFFSET %s
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (limit, offset))
                return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_products_page_violations error: {e}")

def count_violations(table: Optional[str] = None) -> int:
    t = _effective_table(table)
    sql = f"""
        SELECT COUNT(*)
        FROM {t}
        WHERE rrc IS NOT NULL AND rrc > 0
          AND ((ui_price IS NOT NULL AND ui_price > 0 AND ui_price < rrc)
          OR (price_after_seller_discount IS NOT NULL AND price_after_seller_discount > 0 AND price_after_seller_discount < rrc))
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                (cnt,) = cur.fetchone()
                return int(cnt or 0)
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL count_violations error: {e}")

# --- Advisory lock (имя задаётся снаружи, завязано на table) ---
def acquire_advisory_lock(name: str, timeout: int = 0) -> bool:
    sql = "SELECT GET_LOCK(%s, %s)"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (name, timeout))
                (ok,) = cur.fetchone()
                return bool(ok)
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL acquire_advisory_lock error: {e}")

def release_advisory_lock(name: str) -> None:
    sql = "SELECT RELEASE_LOCK(%s)"
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (name,))
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL release_advisory_lock error: {e}")