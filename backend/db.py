import os
from typing import Optional, List, Dict

from dotenv import load_dotenv

# Подтягиваем .env РАНЬШЕ импортов mysql, чтобы переменные сразу были в os.environ
load_dotenv()

import mysql.connector
from mysql.connector import pooling
from mysql.connector import Error as MySQLError


# Конфигурация БД из .env
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

# Пул соединений создаём лениво, чтобы импорт модуля не падал при временной недоступности MySQL
_pool: Optional[pooling.MySQLConnectionPool] = None


def _ensure_pool() -> None:
    """Создаёт пул подключений при первом обращении."""
    global _pool
    if _pool is None:
        # pool_size можно увеличить, если у вас много одновременных запросов
        _pool = pooling.MySQLConnectionPool(pool_name="mp_pool", pool_size=5, **DB_CFG)


def get_conn():
    """Берёт соединение из пула."""
    _ensure_pool()
    return _pool.get_connection()


def _row_to_dict(row) -> Dict:
    """
    Маппинг строки -> dict.
    ПОРЯДОК ДОЛЖЕН СОВПАДАТЬ с SELECT в list_products().
    """
    return {
        "nm_id": row[0],
        "brand": row[1],
        "title": row[2],
        "price_before_discount": float(row[3]) if row[3] is not None else 0.0,
        "price_after_seller_discount": float(row[4]) if row[4] is not None else 0.0,
        "rrc": float(row[5]) if row[5] is not None else None,
        "updated_at": row[6].isoformat() if row[6] else None,
    }


def list_products() -> List[Dict]:
    """
    Возвращает все товары из таблицы, отсортированные по nm_id.
    """
    sql = f"""
        SELECT
            nm_id,
            brand,
            title,
            price_before_discount,
            price_after_seller_discount,
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
        # Прокидываем читаемую ошибку наверх — Flask вернёт 500 с текстом
        raise RuntimeError(f"MySQL list_products error: {e}")


def upsert_product(
    nm_id: int,
    brand: str,
    title: str,
    price_before: float,
    price_after: float,
) -> None:
    """
    Вставляет/обновляет товар по nm_id.
    ВНИМАНИЕ: rrc НЕ трогаем (чтобы не затирать вручную введённое значение).
    """
    sql = f"""
        INSERT INTO {TABLE}
            (nm_id, brand, title, price_before_discount, price_after_seller_discount, updated_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE
            brand = VALUES(brand),
            title = VALUES(title),
            price_before_discount = VALUES(price_before_discount),
            price_after_seller_discount = VALUES(price_after_seller_discount),
            updated_at = NOW()
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (nm_id, brand, title, price_before, price_after))
            conn.commit()
        finally:
            conn.close()
    except MySQLError as e:
        raise RuntimeError(f"MySQL upsert_product error: {e}")


def set_rrc(nm_id: int, rrc: Optional[float]) -> None:
    """
    Ставит/очищает РРЦ.
    rrc = None -> NULL в БД.
    """
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
    """
    Удаляет товар по nm_id. Идемпотентно: если нет — просто 0 затронутых строк.
    """
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