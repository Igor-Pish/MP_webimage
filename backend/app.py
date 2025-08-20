from flask import Flask, request, jsonify
from flask_cors import CORS
from wb_parser import fetch_wb_price
from db import list_products as db_list, upsert_product, delete_product as db_delete
from db import set_rrc
from wb_ui_html import fetch_ui_prices_from_html

application = Flask(__name__)
CORS(application, resources={r"/api/*": {"origins": "*"}},
     methods=["GET", "POST", "DELETE", "PATCH", "OPTIONS"])

@application.get("/api/health")
def health():
    return jsonify({"ok": True})

@application.get("/api/products")
def list_products():
    try:
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

@application.route("/api/products/<int:nm_id>/ui-price", methods=["GET", "POST"])
def ui_price(nm_id: int):
    """
    Возвращает ТОЛЬКО UI-цены (ничего не пишет в БД).
    Делаем удобный формат для фронта:
      {
        ok: true,
        current_price_ui: ...,
        price_before_discount_ui: ...,
        wallet_price_ui: ...,
        ui: { ... те же поля + url, nm_id, source ... }
      }
    """
    try:
        ui = fetch_ui_prices_from_html(nm_id)  # {nm_id, url, current_price_ui, price_before_discount_ui, wallet_price_ui, source}
        return jsonify({
            "ok": True,
            "current_price_ui": ui.get("current_price_ui"),
            "price_before_discount_ui": ui.get("price_before_discount_ui"),
            "wallet_price_ui": ui.get("wallet_price_ui"),
            "ui": ui
        })
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
    # тот же код, что в patch_rrc — переиспользуем логику
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

    # верификация входа
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