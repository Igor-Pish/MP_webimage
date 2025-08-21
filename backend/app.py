from flask import Flask, request, jsonify
from flask_cors import CORS
from wb_parser import fetch_wb_price
from db import list_products as db_list, upsert_product, delete_product as db_delete
from db import set_rrc

import os
from math import floor
from dotenv import load_dotenv

load_dotenv()  # загрузим .env заранее

# Константы для расчёта фиолетовой цены (в рублях)
UI_WALLET_PERCENT = float(os.getenv("UI_WALLET_PERCENT", "0"))        # например 0.02 = 2%
UI_ROUND_THRESHOLD = float(os.getenv("UI_ROUND_THRESHOLD", "0"))      # порог для округления, руб (0 = не применять)
UI_ROUND_STEP = float(os.getenv("UI_ROUND_STEP", "1"))                # кратность округления, руб (минимум 1)

def calc_ui_price_from_product(base_price: float | None) -> float | None:
    """
    Фиолетовая цена (UI) от price_after_seller_discount:
      raw = base * (1 - p)
      если base >= threshold: ui = floor(raw / step) * step
      иначе ui = raw
    Все величины — в рублях. Возвращает число с 2 знаками после запятой.
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
        return float(f"{ui:.2f}")
    except Exception:
        return None

application = Flask(__name__)
CORS(application, resources={r"/api/*": {"origins": "*"}},
     methods=["GET", "POST", "DELETE", "PATCH", "OPTIONS"])

@application.get("/api/health")
def health():
    return jsonify({"ok": True})

@application.get("/api/products")
def list_products():
    try:
        items = db_list()  # [{'nm_id', 'brand', 'title', 'price_before_discount', 'price_after_seller_discount', 'rrc', ...}, ...]
        # NEW: добавим вычисленное поле ui_price на лету
        enriched = []
        for it in items:
            it = dict(it)  # скопируем, чтобы не портить исходник
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

@application.delete("/api/products/<int:nm_id>")
def delete_product(nm_id: int):
    try:
        db_delete(nm_id)
        return jsonify({"ok": True, "nm_id": nm_id})
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

@application.post("/api/products/<int:nm_id>/delete")
def delete_product_post(nm_id: int):
    try:
        db_delete(nm_id)
        return jsonify({"ok": True, "nm_id": nm_id})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@application.get("/")
def index_root():
    return "API OK with DB. try /api/health"