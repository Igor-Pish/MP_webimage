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
    }

def list_products() -> List[Dict]:
    sql = f"""
        SELECT
            nm_id,
            brand,
            title,
            seller_id,
            seller_name,
            price_before_discount,
            price_after_seller_discount,
            ui_price,
            rrc,
            updated_at
        FROM {TABLE}
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
) -> None:
    sql = f"""
        INSERT INTO {TABLE}
            (nm_id, brand, title, seller_id, seller_name,
             price_before_discount, price_after_seller_discount, ui_price, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            brand = VALUES(brand),
            title = VALUES(title),
            seller_id = VALUES(seller_id),
            seller_name = VALUES(seller_name),
            price_before_discount = VALUES(price_before_discount),
            price_after_seller_discount = VALUES(price_after_seller_discount),
            ui_price = VALUES(ui_price),
            updated_at = NOW()
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (nm_id, brand, title, seller_id, seller_name, price_before, price_after, ui_price))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL upsert_product error: {e}")

def set_rrc(nm_id: int, rrc: Optional[float]) -> None:
    sql = f"UPDATE {TABLE} SET rrc=%s, updated_at=NOW() WHERE nm_id=%s"
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

def delete_product(nm_id: int) -> None:
    sql = f"DELETE FROM {TABLE} WHERE nm_id=%s"
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

def delete_all_products() -> int:
    """
    Удаляет все товары из таблицы.
    Возвращает количество удалённых строк (может быть недоступно для некоторых драйверов).
    """
    sql = f"DELETE FROM {TABLE}"
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

# ====== Поддержка батч-обновления ======
def _where_need_refresh() -> str:
    where = "(price_after_seller_discount IS NULL OR price_after_seller_discount = 0)"
    if STALE_HOURS > 0:
        where = f"({where} OR updated_at IS NULL OR updated_at < (NOW() - INTERVAL {STALE_HOURS} HOUR))"
    return where

def list_nm_ids_for_refresh(limit: int) -> List[int]:
    where = _where_need_refresh()
    sql = f"""
        SELECT nm_id
        FROM {TABLE}
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

def count_needing_refresh() -> int:
    where = _where_need_refresh()
    sql = f"SELECT COUNT(*) FROM {TABLE} WHERE {where}"
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

def list_nm_ids_any(limit: int, offset: int = 0) -> List[int]:
    """
    Любые товары, стабильный порядок nm_id ASC, с LIMIT/OFFSET.
    Используется для принудительного полного прохода по таблице.
    """
    sql = f"""
        SELECT nm_id
        FROM {TABLE}
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

def count_all_rows() -> int:
    sql = f"SELECT COUNT(*) FROM {TABLE}"
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

# ====== Для алармов: выборка нарушителей и их артикулов ======
def list_violations_for_seller(seller_id: int) -> List[Dict]:
    """
    Нарушения у селлера: ui_price < rrc и оба заданы.
    Возвращает [{'nm_id': ...}, ...]
    """
    sql = f"""
        SELECT nm_id
        FROM {TABLE}
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

def list_sellers_with_violations() -> List[Dict]:
    """
    Селлеры, у которых есть хотя бы одно нарушение.
    Возвращает [{'seller_id': ..., 'seller_name': ..., 'violations': N}, ...]
    """
    sql = f"""
        SELECT seller_id, MAX(seller_name) as seller_name, COUNT(*) as cnt
        FROM {TABLE}
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

def get_seller_name(seller_id: int) -> Optional[str]:
    sql = f"SELECT MAX(seller_name) FROM {TABLE} WHERE seller_id=%s"
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
    
# ====== Telegram admins ======
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
def list_products_page_violations(per_page: int, offset: int) -> List[Dict]:
    sql = f"""
        SELECT
            nm_id,
            brand,
            title,
            seller_id,
            seller_name,
            price_before_discount,
            price_after_seller_discount,
            ui_price,
            rrc,
            updated_at
        FROM {TABLE}
        WHERE ui_price IS NOT NULL AND ui_price > 0
          AND rrc IS NOT NULL AND rrc > 0
          AND ui_price < rrc
        ORDER BY nm_id
        LIMIT %s OFFSET %s
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (per_page, offset))
                return [_row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL list_products_page_violations error: {e}")
    
def _sanitize_order(order_by: str, order_dir: str) -> Tuple[str, str]:
    # Белый список колонок для сортировки
    allowed = {
        "nm_id": "nm_id",
        "seller_name": "seller_name",
        "price_after_seller_discount": "price_after_seller_discount",
        "updated_at": "updated_at",
        "rrc": "rrc",
    }
    col = allowed.get(order_by, "nm_id")
    dir_ = "DESC" if (order_dir or "").lower() == "desc" else "ASC"
    return col, dir_

def list_products_page(limit: int, offset: int, order_by: str = "nm_id", order_dir: str = "asc") -> List[Dict]:
    col, dir_ = _sanitize_order(order_by, order_dir)
    sql = f"""
        SELECT
            nm_id, brand, title, seller_id, seller_name,
            price_before_discount, price_after_seller_discount,
            ui_price, rrc, updated_at
        FROM {TABLE}
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

def list_products_page_violations(limit: int, offset: int, order_by: str = "nm_id", order_dir: str = "asc") -> List[Dict]:
    col, dir_ = _sanitize_order(order_by, order_dir)
    sql = f"""
        SELECT
            nm_id, brand, title, seller_id, seller_name,
            price_before_discount, price_after_seller_discount,
            ui_price, rrc, updated_at
        FROM {TABLE}
        WHERE ui_price IS NOT NULL AND ui_price > 0
          AND rrc IS NOT NULL AND rrc > 0
          AND ui_price < rrc
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

def count_violations() -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {TABLE}
        WHERE ui_price IS NOT NULL AND ui_price > 0
          AND rrc IS NOT NULL AND rrc > 0
          AND ui_price < rrc
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