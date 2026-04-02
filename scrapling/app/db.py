import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_url = os.environ.get("DATABASE_URL")

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION POOLING — reuse a single connection instead of open/close per query
# ─────────────────────────────────────────────────────────────────────────────
_conn_cache = None

def get_db_connection():
    global _conn_cache
    if not db_url or "YOUR-PASSWORD" in db_url:
        return None
    try:
        # Return cached connection if it's still alive
        if _conn_cache is not None:
            try:
                _conn_cache.cursor().execute("SELECT 1")
                return _conn_cache
            except Exception:
                # Connection went stale, reconnect
                try:
                    _conn_cache.close()
                except Exception:
                    pass
                _conn_cache = None

        conn = psycopg2.connect(db_url)
        conn.autocommit = True  # Required for Supabase Transaction Pooler (port 6543)
        _conn_cache = conn
        return conn
    except Exception as e:
        print(f"[Supabase DB] Error connecting: {e}")
        return None

def is_sku_scraped(sku: str) -> bool:
    """Checks if a SKU exists in the Products Data Warehouse."""
    conn = get_db_connection()
    if not conn or not sku:
        return False
        
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM products WHERE sku = %s LIMIT 1", (sku,))
            return cur.fetchone() is not None
    except Exception as e:
        print(f"[Supabase DB] Error checking SKU '{sku}': {e}")
        return False

def is_product_name_scraped(name: str) -> bool:
    """Fallback dedup: checks if a product name already exists in the Data Warehouse."""
    conn = get_db_connection()
    if not conn or not name:
        return False
        
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM products WHERE product_name ILIKE %s LIMIT 1", (name.strip(),))
            return cur.fetchone() is not None
    except Exception as e:
        print(f"[Supabase DB] Error checking product name: {e}")
        return False

def save_product_to_db(prod: dict):
    """Inserts a fully formatted scraped product into the Supabase Data Warehouse."""
    conn = get_db_connection()
    sku = prod.get("SKU")
    if not conn or not sku:
        print(f"[Supabase DB] Skipping DB insert — no connection or no SKU.")
        return False
        
    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO products (
                    sku, product_name, category, brand, manufacturer,
                    sale_price, discount_base_price, stock, volume, weight,
                    main_image, product_url, search_keywords, detailed_description
                ) VALUES (
                    %(sku)s, %(name)s, %(category)s, %(brand)s, %(manufacturer)s,
                    %(sale)s, %(mrp)s, %(stock)s, %(volume)s, %(weight)s,
                    %(img)s, %(url)s, %(keywords)s, %(desc)s
                ) ON CONFLICT (sku) DO NOTHING
            """
            
            payload = {
                "sku": sku,
                "name": prod.get("Product Name"),
                "category": prod.get("Category"),
                "brand": prod.get("Brand"),
                "manufacturer": prod.get("Manufacturer"),
                "sale": prod.get("Sale Price"),
                "mrp": prod.get("Discount Base Price"),
                "stock": prod.get("Stock", 2),
                "volume": prod.get("Volume"),
                "weight": prod.get("Weight"),
                "img": prod.get("Main Image"),
                "url": prod.get("Product URL"),
                "keywords": prod.get("Search Keywords"),
                "desc": prod.get("Detailed Description"),
            }
            
            cur.execute(sql, payload)
            print(f"[Supabase DB] ✅ Stored product: {sku}")
        return True
    except Exception as e:
        print(f"[Supabase DB] ❌ Error inserting SKU '{sku}': {e}")
        return False
