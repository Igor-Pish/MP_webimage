from flask import Flask, request, jsonify
from flask_cors import CORS
from wb_parser import fetch_wb_price
from db import (
    list_products as db_list,
    upsert_product,
    delete_product as db_delete,
    set_rrc,
    list_nm_ids_for_refresh,
    count_needing_refresh,  # NEW
)

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
        # Округлим вниз до целого рубля
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
        items = db_list()
        # Добавим вычисленное поле ui_price на лету
        enriched = []
        for it in items:
            it = dict(it)  # не портим исходник
            base = it.get("price_after_seller_discount")
            it["ui_price"] = calc_ui_price_from_product(base)
            enriched.append(it)
        return jsonify({"items": enriched})
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
        upsert_product(
            nm_id=data["nm_id"],
            brand=data.get("brand", "") or "",
            title=data.get("title", "") or "",
            price_before=float(data.get("price_before_discount") or 0),
            price_after=float(data.get("price_after_seller_discount") or 0),
            seller_id=data.get("seller_id"),
            seller_name=data.get("seller_name") or None,
        )
        return jsonify({"ok": True, "item": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.post("/api/products/<int:nm_id>/refresh")
def refresh_product(nm_id: int):
    try:
        data = fetch_wb_price(nm_id)
        upsert_product(
            nm_id=data["nm_id"],
            brand=data.get("brand", "") or "",
            title=data.get("title", "") or "",
            price_before=float(data.get("price_before_discount") or 0),
            price_after=float(data.get("price_after_seller_discount") or 0),
            seller_id=data.get("seller_id"),
            seller_name=data.get("seller_name") or None,
        )
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
    Берём кандидатов среди тех, кому «нужно обновление» (см. db.count_needing_refresh()).
    Возвращаем также remaining и done, чтобы фронт мог крутить цикл до конца.
    """
    try:
        limit = request.args.get("limit", type=int) or BATCH_REFRESH_LIMIT
        nm_ids = list_nm_ids_for_refresh(limit)

        updated = []
        errors = []

        for nm_id in nm_ids:
            try:
                data = fetch_wb_price(nm_id)
                upsert_product(
                    nm_id=data["nm_id"],
                    brand=data.get("brand", "") or "",
                    title=data.get("title", "") or "",
                    price_before=float(data.get("price_before_discount") or 0),
                    price_after=float(data.get("price_after_seller_discount") or 0),
                    seller_id=data.get("seller_id"),
                    seller_name=data.get("seller_name") or None,
                )
                updated.append(nm_id)
            except Exception as e:
                errors.append({"nm_id": nm_id, "error": str(e)})

        remaining = count_needing_refresh()
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

@application.post("/api/upload-xlsx")
def upload_xlsx():
    """
    Принимаем .xlsx, берём РРЦ из 3-го столбца (C), артикул из 4-го (D).
    Вставляем (или обновляем) строки с нулевыми ценами + выставляем rrc.
    Никаких цен сейчас не тянем — их подтянет пакетное обновление.
    """
    try:
        file = request.files.get("file")
        if not file:
            return jsonify({"ok": False, "error": "Файл не передан"}), 400

        # Читаем сразу из потока
        wb = load_workbook(file, read_only=True, data_only=True)
        ws = wb.active

        total = 0
        affected = 0
        skipped = 0
        errors = 0

        for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            total += 1
            rrc_raw = row[2] if len(row) > 2 else None   # C
            nm_raw  = row[3] if len(row) > 3 else None   # D

            if nm_raw is None:
                skipped += 1
                continue

            try:
                nm_id = int(str(nm_raw).strip())
            except Exception:
                skipped += 1
                continue

            try:
                rrc_val = None
                if rrc_raw not in (None, ""):
                    rrc_val = float(str(rrc_raw).replace(",", ".").strip())

                # создаём/обновляем строку с нулевыми ценами,
                # чтобы потом батчем подтянуть реальные цены
                upsert_product(
                    nm_id=nm_id,
                    brand="",
                    title="",
                    price_before=0.0,
                    price_after=0.0,
                    seller_id=None,
                    seller_name=None,
                )
                set_rrc(nm_id, rrc_val)
                affected += 1
            except Exception:
                errors += 1

        return jsonify({
            "ok": True,
            "total": total,
            "affected": affected,
            "skipped": skipped,
            "errors_count": errors,
        })

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.get("/")
def index_root():
    return "API OK with DB. try /api/health"