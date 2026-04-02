import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_url = os.environ.get("DATABASE_URL")

def get_db_connection():
    if not db_url or "YOUR-PASSWORD" in db_url:
        return None
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True  # Required for Supabase Transaction Pooler (port 6543)
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
            cur.execute("SELECT sku FROM products WHERE sku = %s", (sku,))
            result = cur.fetchone()
            return result is not None
    except Exception as e:
        print(f"[Supabase DB] Error checking SKU '{sku}': {e}")
        return False
    finally:
        if conn:
            conn.close()

def is_product_name_scraped(name: str) -> bool:
    """Fallback dedup: checks if a product name already exists in the Data Warehouse."""
    conn = get_db_connection()
    if not conn or not name:
        return False
        
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_name FROM products WHERE product_name ILIKE %s LIMIT 1", (name.strip(),))
            result = cur.fetchone()
            return result is not None
    except Exception as e:
        print(f"[Supabase DB] Error checking product name: {e}")
        return False
    finally:
        if conn:
            conn.close()

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
    finally:
        if conn:
            conn.close()
