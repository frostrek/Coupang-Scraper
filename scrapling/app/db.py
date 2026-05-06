import os
import psycopg2
import threading
from dotenv import load_dotenv

load_dotenv()
db_url = os.environ.get("DATABASE_URL")


def is_db_available() -> bool:
    """Quick check if DB is configured and reachable. Never crashes."""
    try:
        conn = get_db_connection()
        return conn is not None
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────────
# THREAD-SAFE CONNECTION POOLING — one connection per thread, auto-reconnect
# ─────────────────────────────────────────────────────────────────────────────
_local = threading.local()

def get_db_connection():
    """Returns a cached DB connection for the current thread. Auto-reconnects if stale."""
    if not db_url or "YOUR-PASSWORD" in db_url:
        return None
    try:
        conn = getattr(_local, 'conn', None)
        
        # Test if existing connection is still alive
        if conn is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                _local.conn = None

        # Create new connection
        conn = psycopg2.connect(db_url, connect_timeout=10, sslmode='require')
        conn.autocommit = True  # Required for Supabase Transaction Pooler (port 6543)
        _local.conn = conn
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
        # Connection may be broken, reset it
        _local.conn = None
        return False

def get_scraped_skus(sku_list: list) -> set:
    """Bulk checks a list of SKUs in one highly efficient query to reduce DB load."""
    conn = get_db_connection()
    if not conn or not sku_list:
        return set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sku FROM products WHERE sku = ANY(%s)", (sku_list,))
            return {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"[Supabase DB] Error bulk checking SKUs: {e}")
        _local.conn = None
        return set()

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
        _local.conn = None
        return False

def get_scraped_names(name_list: list) -> set:
    """Bulk checks a list of product names in one query using ILIKE ANY."""
    conn = get_db_connection()
    if not conn or not name_list:
        return set()
    try:
        with conn.cursor() as cur:
            # Using Postgres ILIKE ANY for bulk case-insensitive matching
            names = [n.strip() for n in name_list if n.strip()]
            cur.execute("SELECT product_name FROM products WHERE product_name ILIKE ANY(%s)", (names,))
            return {row[0].lower() for row in cur.fetchall() if row[0]}
    except Exception as e:
        print(f"[Supabase DB] Error bulk checking names: {e}")
        _local.conn = None
        return set()

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
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None
        return False

def save_products_bulk(products: list):
    """Inserts multiple fully formatted scraped products into the Supabase Data Warehouse at once."""
    conn = get_db_connection()
    if not conn or not products:
        print("[Supabase DB] Skipping bulk DB insert — no connection or no products.")
        return False
        
    inserted_count = 0
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
            
            for prod in products:
                sku = prod.get("SKU")
                if not sku:
                    continue
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
                inserted_count += 1
            print(f"[Supabase DB] Stored {inserted_count} products in bulk.")
        return True
    except Exception as e:
        print(f"[Supabase DB] Error in bulk insert: {e}")
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None
        return False


def delete_products_by_skus(sku_list: list) -> tuple:
    """Delete products from the DB by a list of SKUs.

    Returns (deleted_count: int, error: str | None).
    deleted_count is the number of rows actually removed.
    error is None on success, or an error message string on failure.
    """
    conn = get_db_connection()
    if not conn:
        return 0, "Database not configured or unreachable"
    if not sku_list:
        return 0, None

    valid_skus = [s for s in sku_list if s and isinstance(s, str)]
    if not valid_skus:
        return 0, None

    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM products WHERE sku = ANY(%s)",
                (valid_skus,)
            )
            deleted = cur.rowcount
        print(f"[Supabase DB] Deleted {deleted} products by SKU list.")
        return deleted, None
    except Exception as e:
        print(f"[Supabase DB] Error deleting products: {e}")
        try:
            conn.close()
        except Exception:
            pass
        _local.conn = None
        return 0, str(e)

