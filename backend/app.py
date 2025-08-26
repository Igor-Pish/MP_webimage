from flask import Flask, request, jsonify
from flask_cors import CORS
from wb_parser import fetch_wb_price
from db import (
    list_products as db_list,
    upsert_product,
    delete_product as db_delete,
    set_rrc,
    list_nm_ids_for_refresh,
    count_needing_refresh,
    list_sellers_with_violations,
    delete_all_products,
)

from utils import calc_rrc_from_title, _parse_nm_id, _parse_price_like, _detect_columns

from telegram_client import send_violation_alert

import os
from math import floor
from typing import Optional
from dotenv import load_dotenv
from openpyxl import load_workbook

load_dotenv()  # загрузим .env заранее

# ====== Параметры для расчёта фиолетовой цены ======
UI_WALLET_PERCENT = float(os.getenv("UI_WALLET_PERCENT", "0"))        # 0.02 => 2%
UI_ROUND_THRESHOLD = float(os.getenv("UI_ROUND_THRESHOLD", "0"))      # порог округления (0 = выкл)
UI_ROUND_STEP = float(os.getenv("UI_ROUND_STEP", "1"))                # шаг округления (>=1)

# ====== Параметры пакетного обновления ======
BATCH_REFRESH_LIMIT = int(os.getenv("BATCH_REFRESH_LIMIT", "20"))

# ====== Загрузка файлов (xlsx) ======
UPLOAD_ALLOWED_EXT = {".xlsx"}


def calc_ui_price_from_product(base_price: Optional[float]) -> Optional[int]:
    """
    Фиолетовая цена (UI) от price_after_seller_discount (в рублях, целое):
      raw = base * (1 - p)
      если base >= threshold: ui = floor(raw / step) * step
      иначе ui = raw
    Возвращаем целое (рубли).
    """
    if base_price is None:
        return None
    try:
        base = float(base_price)
        p = UI_WALLET_PERCENT
        thr = UI_ROUND_THRESHOLD
        step = UI_ROUND_STEP if UI_ROUND_STEP > 0 else 1.0

        raw = base * (1.0 - p) if p > 0 else base
        if thr > 0 and base >= thr:
            ui = floor(raw / step) * step
        else:
            ui = raw
        return int(floor(ui))
    except Exception:
        return None

application = Flask(__name__)
CORS(
    application,
    resources={r"/api/*": {"origins": "*"}},
    methods=["GET", "POST", "DELETE", "PATCH", "OPTIONS"],
)

@application.get("/api/health")
def health():
    return jsonify({"ok": True})

@application.get("/api/products")
def list_products():
    try:
        # Возвращаем как есть из БД (ui_price хранится в таблице)
        items = db_list()
        return jsonify({"items": items})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.post("/api/products")
def add_product():
    body = request.get_json(silent=True) or {}
    nm_id = body.get("nm_id")
    try:
        nm_id = int(nm_id)
    except Exception:
        return jsonify({"ok": False, "error": "nm_id должен быть числом"}), 400
    try:
        data = fetch_wb_price(nm_id)
        rrc_auto = calc_rrc_from_title(data.get("title"))
        ui_price = calc_ui_price_from_product(
            float(data.get("price_after_seller_discount") or 0)
        )
        upsert_product(
            nm_id=data["nm_id"],
            brand=data.get("brand", "") or "",
            title=data.get("title", "") or "",
            price_before=float(data.get("price_before_discount") or 0),
            price_after=float(data.get("price_after_seller_discount") or 0),
            seller_id=data.get("seller_id"),
            seller_name=data.get("seller_name") or None,
            ui_price=int(ui_price) if ui_price is not None else None,
        )

        if rrc_auto is not None:
            set_rrc(nm_id, rrc_auto)

        return jsonify({"ok": True, "item": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.post("/api/products/<int:nm_id>/refresh")
def refresh_product(nm_id: int):
    try:
        data = fetch_wb_price(nm_id)
        rrc_auto = calc_rrc_from_title(data.get("title"))
        ui_price = calc_ui_price_from_product(
            float(data.get("price_after_seller_discount") or 0)
        )
        upsert_product(
            nm_id=data["nm_id"],
            brand=data.get("brand", "") or "",
            title=data.get("title", "") or "",
            price_before=float(data.get("price_before_discount") or 0),
            price_after=float(data.get("price_after_seller_discount") or 0),
            seller_id=data.get("seller_id"),
            seller_name=data.get("seller_name") or None,
            ui_price=int(ui_price) if ui_price is not None else None,
        )

        if rrc_auto is not None:
            set_rrc(nm_id, rrc_auto)
        
        return jsonify({"ok": True, "item": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.post("/api/products/<int:nm_id>/rrc")
def post_rrc(nm_id: int):
    body = request.get_json(silent=True) or {}
    rrc = body.get("rrc", None)

    if rrc in ("", None):
        val = None
    else:
        try:
            val = float(rrc)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "rrc должно быть числом"}), 400

    try:
        set_rrc(nm_id, val)
        return jsonify({"ok": True, "nm_id": nm_id, "rrc": val})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.route("/api/products/<int:nm_id>/rrc", methods=["PATCH"])
def patch_rrc(nm_id: int):
    body = request.get_json(silent=True) or {}
    rrc = body.get("rrc", None)

    if rrc in ("", None):
        val = None
    else:
        try:
            val = float(rrc)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "rrc должно быть числом"}), 400

    try:
        set_rrc(nm_id, val)
        return jsonify({"ok": True, "nm_id": nm_id, "rrc": val})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.delete("/api/products/<int:nm_id>")
def delete_product(nm_id: int):
    try:
        db_delete(nm_id)
        return jsonify({"ok": True, "nm_id": nm_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.post("/api/products/refresh-batch")
def refresh_batch():
    """
    Обновляет цены у N товаров (лимит из .env или ?limit=).
    Берём кандидатов среди тех, кому «нужно обновление».
    Возвращаем remaining и done, чтобы фронт мог крутить цикл до конца.
    """
    try:
        limit = request.args.get("limit", type=int) or BATCH_REFRESH_LIMIT
        nm_ids = list_nm_ids_for_refresh(limit)

        updated = []
        errors = []

        for nm_id in nm_ids:
            try:
                data = fetch_wb_price(nm_id)
                rrc_auto = calc_rrc_from_title(data.get("title"))
                ui_price = calc_ui_price_from_product(
                    float(data.get("price_after_seller_discount") or 0)
                )
                upsert_product(
                    nm_id=data["nm_id"],
                    brand=data.get("brand", "") or "",
                    title=data.get("title", "") or "",
                    price_before=float(data.get("price_before_discount") or 0),
                    price_after=float(data.get("price_after_seller_discount") or 0),
                    seller_id=data.get("seller_id"),
                    seller_name=data.get("seller_name") or None,
                    ui_price=int(ui_price) if ui_price is not None else None,
                )

                if rrc_auto is not None:
                    set_rrc(nm_id, rrc_auto)

                updated.append(nm_id)
            except Exception as e:
                errors.append({"nm_id": nm_id, "error": str(e)})

        # после обновления цен — проверка нарушителей и уведомление
        remaining = count_needing_refresh()
        try:
            violators = list_sellers_with_violations()
            for v in violators:
                send_violation_alert(v["seller_id"], v.get("seller_name"))
        except Exception as e:
            # не роняем запрос из-за алертов
            print(f"[alerts] send error: {e}")

        return jsonify({
            "ok": True,
            "requested": limit,
            "selected": len(nm_ids),
            "updated_count": len(updated),
            "updated": updated,
            "errors_count": len(errors),
            "errors": errors,
            "remaining": remaining,
            "done": remaining == 0,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    
@application.delete("/api/products")
def delete_all():
    """
    Удаляет все товары из БД.
    """
    try:
        removed = delete_all_products()
        return jsonify({"ok": True, "removed": removed})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.post("/api/upload-xlsx")
def upload_xlsx():
    """
    Импорт XLSX c «умным» поиском колонок.
    - Сначала ищем по заголовку: Артикул / РРЦ.
    - Если не нашли, определяем эвристически:
        * Артикул: колонка с макс. числом «похожих на nm_id»
        * РРЦ: колонка, где большинство значений ∈ {1300, 1500} (или просто числовая колонка)
    - Если РРЦ не нашли — импортируем только nm_id (РРЦ остаётся как есть).
    """
    try:
        file = request.files.get("file")
        if not file:
            return jsonify({"ok": False, "error": "Файл не передан"}), 400

        wb = load_workbook(file, read_only=True, data_only=True)
        ws = wb.active

        nm_col_arg = request.args.get("nm_col", type=int)
        rrc_col_arg = request.args.get("rrc_col", type=int)

        # Автоопределение колонок
        if nm_col_arg is not None or rrc_col_arg is not None:
            idx_nm, idx_rrc = nm_col_arg, rrc_col_arg
        else:
            idx_nm, idx_rrc = _detect_columns(ws)

        total = 0
        affected = 0
        skipped = 0
        errors = 0

        # Перебираем строки
        for row in ws.iter_rows(min_row=2, values_only=True):
            total += 1

            nm_val = None
            if idx_nm is not None and idx_nm < len(row):
                nm_val = _parse_nm_id(row[idx_nm])

            if not nm_val:
                skipped += 1
                continue

            rrc_val = None
            if idx_rrc is not None and idx_rrc < len(row):
                rrc_parsed = _parse_price_like(row[idx_rrc])
                if rrc_parsed is not None:
                    rrc_val = float(int(round(rrc_parsed)))  # до целых рублей

            try:
                # создаём/обновляем строку с нулевыми ценами (ui_price=None),
                # реальную цену подтянет батч-обновление
                upsert_product(
                    nm_id=nm_val,
                    brand="",
                    title="",
                    price_before=0.0,
                    price_after=0.0,
                    seller_id=None,
                    seller_name=None,
                    ui_price=None,
                )
                if rrc_val is not None:
                    set_rrc(nm_val, rrc_val)
                affected += 1
            except Exception:
                errors += 1

        return jsonify({
            "ok": True,
            "total": total,
            "affected": affected,
            "skipped": skipped,
            "errors_count": errors,
            "columns": {
                "nm_idx": idx_nm,
                "rrc_idx": idx_rrc,
                "header_nm": (ws.cell(row=1, column=(idx_nm+1)).value if idx_nm is not None else None),
                "header_rrc": (ws.cell(row=1, column=(idx_rrc+1)).value if idx_rrc is not None else None),
            }
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@application.get("/api/sellers/<int:seller_id>/violations")
def seller_violations(seller_id: int):
    """
    Возвращает список артикулов с нарушением для селлера.
    Критерий: ui_price < rrc (оба не NULL/0).
    Формат: { items: [ { nm_id }, ... ] }
    """
    try:
        from db import list_violations_for_seller
        rows = list_violations_for_seller(seller_id)
        return jsonify({"items": rows})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.get("/")
def index_root():
    return "API OK with DB. try /api/health"