"""
fetch.py
Descarga y filtra productos de múltiples categorías de thorprovider.com
y guarda cada una en su propio archivo JSON.
Pagina automáticamente para obtener TODOS los productos, no solo los primeros 60.
"""

import json
import time
import requests
from pathlib import Path

BASE_DIR     = Path(__file__).parent
API_ENDPOINT = "https://elastic-thorprovider.com:9200/products/_search"

HEADERS = {
    "User-Agent":    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) Gecko/20100101 Firefox/148.0",
    "Content-Type":  "application/json",
    "Accept":        "*/*",
    "Referer":       "https://thorprovider.com/",
    "Origin":        "https://thorprovider.com",
    "Authorization": "Apikey SWlwRnRwRUI0SWZOTlJtcGs3emo6WUFZamhvRDlRcE9qRFZXRm44c19TUQ==",
}

CATEGORIAS = [
    ("energ.json",  "PJQXoHsSgJPi9xfYQ5SH"),
    ("cel.json",    "844"),
    ("herra.json",  "401"),
    ("acce.json",   "232"),
    ("ofic.json",   "owGL7EJLTvtlVzyBmcRk"),
    ("comp.json",   "c19iTWch7lN9EFsHIRLJ"),
    ("hog.json",    "cMxTKjltE5VHAnK11AOH"),
    ("aud.json",    "sZvEN1dvy5VJid6WvgGX"),
    ("accem.json",  "yUtS1QI3YLpCMOxtdXci"),
]

EXCLUDED_WAREHOUSE_ID = "4028"
PAGE_SIZE = 100   # productos por petición


def build_payload(cat_id: str, size: int = PAGE_SIZE, from_: int = 0) -> dict:
    cat_filter = {"bool": {"should": [{"term": {"category.keyword": cat_id}}]}}
    return {
        "aggs": {
            "attributes_buckets": {
                "aggs": {
                    "attributes": {
                        "terms": {
                            "field": "attribute.data.name.keyword",
                            "size": 10000,
                            "order": {"_count": "desc"}
                        }
                    }
                },
                "filter": {"bool": {"must": [cat_filter]}}
            },
            "tags_buckets": {
                "aggs": {
                    "tags": {
                        "terms": {
                            "field": "tag.id.keyword",
                            "size": 10000,
                            "order": {"_count": "desc"}
                        }
                    }
                },
                "filter": {"bool": {"must": [cat_filter]}}
            },
            "max_price": {"max": {"field": "sales-price"}}
        },
        "size": size,
        "from": from_,
        "sort": [{"orderDate": {"order": "desc"}}],
        "query": {
            "bool": {
                "filter": [
                    {"bool": {"filter": [{"term": {"inactive": "false"}}]}},
                    {"bool": {"filter": [{"term": {"discontinued": "false"}}]}},
                    {"bool": {"filter": [{"term": {"category.keyword": cat_id}}]}}
                ]
            }
        }
    }


# ─── Filtros ──────────────────────────────────────────────────────────────────

def all_warehouses_false(src: dict) -> bool:
    warehouses = src.get("product-warehouses", [])
    if warehouses:
        relevant = [w for w in warehouses if str(w.get("id", "")) != EXCLUDED_WAREHOUSE_ID]
        if not relevant:
            return True
        for w in relevant:
            stock = w.get("stock", 0)
            if stock is True or stock == "true":
                return False
            if isinstance(stock, (int, float)) and stock > 0:
                return False
            qty = w.get("quantity", 0)
            if isinstance(qty, (int, float)) and qty > 0:
                return False
        return True

    warehouses_in_stock = src.get("warehousesInStock", [])
    if not warehouses_in_stock:
        return False
    for entry in warehouses_in_stock:
        s   = str(entry)
        wid = s.split("-")[0]
        if wid == EXCLUDED_WAREHOUSE_ID:
            continue
        if s.endswith("-true"):
            return False
    return True


def get_sales_price(src: dict) -> float:
    try:
        return float(src.get("sales-price", 0))
    except (TypeError, ValueError):
        return 0.0


def get_ag_ventas_price(src: dict):
    for rate in src.get("item-rates", []):
        if rate.get("name") == "AG.VENTAS":
            try:
                return float(rate["price"])
            except (TypeError, ValueError, KeyError):
                return None
    return None


def should_keep(hit: dict) -> tuple[bool, str]:
    src = hit.get("_source")
    if not src:
        return True, ""
    if all_warehouses_false(src):
        return False, "sin_stock"
    sales_price     = get_sales_price(src)
    ag_ventas_price = get_ag_ventas_price(src)
    if ag_ventas_price is None or ag_ventas_price >= sales_price:
        return False, "ag_ventas_invalido"
    return True, ""


def filter_hits(hits: list) -> tuple[list, int, int]:
    kept = []
    removed_no_stock  = 0
    removed_ag_ventas = 0
    for hit in hits:
        keep, reason = should_keep(hit)
        if keep:
            kept.append(hit)
        elif reason == "sin_stock":
            removed_no_stock += 1
        else:
            removed_ag_ventas += 1
    return kept, removed_no_stock, removed_ag_ventas


# ─── Descarga paginada ────────────────────────────────────────────────────────

def fetch_all_hits(cat_id: str) -> list:
    """Descarga todos los productos de una categoría paginando automáticamente."""

    # Primera página → también nos dice el total real
    payload  = build_payload(cat_id, size=PAGE_SIZE, from_=0)
    resp     = requests.post(API_ENDPOINT, headers=HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    data     = resp.json()

    total_info = data.get("hits", {}).get("total", {})
    total_real = total_info.get("value", 0) if isinstance(total_info, dict) else int(total_info or 0)
    print(f"  🔍 Total en servidor : {total_real}")

    all_hits = list(data.get("hits", {}).get("hits", []))
    del data  # liberar memoria

    # Páginas siguientes si hacen falta
    while len(all_hits) < total_real:
        from_ = len(all_hits)
        print(f"  ⬇️  Descargando desde {from_} hasta {min(from_ + PAGE_SIZE, total_real)}...")

        page_payload = build_payload(cat_id, size=PAGE_SIZE, from_=from_)
        page_resp    = requests.post(API_ENDPOINT, headers=HEADERS, json=page_payload, timeout=30)
        page_resp.raise_for_status()
        page_data    = page_resp.json()

        page_hits = page_data.get("hits", {}).get("hits", [])
        del page_data

        if not page_hits:
            break

        all_hits.extend(page_hits)
        time.sleep(0.5)  # pausa breve entre páginas

    return all_hits


# ─── Procesamiento de una categoría ──────────────────────────────────────────

def process_category(filename: str, cat_id: str):
    output_file = BASE_DIR / filename
    print(f"\n{'─'*50}")
    print(f"📂 {filename}  (cat: {cat_id})")

    hits         = fetch_all_hits(cat_id)
    total_before = len(hits)

    kept, removed_no_stock, removed_ag_ventas = filter_hits(hits)
    del hits  # liberar memoria

    print(f"  📦 Total descargado  : {total_before}")
    print(f"  ❌ Sin stock          : {removed_no_stock}")
    print(f"  ❌ AG.VENTAS inválido : {removed_ag_ventas}")
    print(f"  ✅ Resultado final    : {len(kept)}")

    output = {"hits": kept}
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  💾 Guardado en: {output_file}")
    del kept


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"🚀 Iniciando descarga de {len(CATEGORIAS)} categorías...\n")

    for i, (filename, cat_id) in enumerate(CATEGORIAS, 1):
        print(f"[{i}/{len(CATEGORIAS)}] Procesando {filename}...")
        try:
            process_category(filename, cat_id)
        except Exception as e:
            print(f"  ⚠️  Error en {filename}: {e}")

        if i < len(CATEGORIAS):
            time.sleep(1)

    print(f"\n{'─'*50}")
    print("🏁 Todos los archivos procesados.")


if __name__ == "__main__":
    main()
