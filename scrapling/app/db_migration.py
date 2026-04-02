import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
db_url = os.environ.get("DATABASE_URL")

def run_migration():
    if not db_url or "YOUR-PASSWORD" in db_url:
        print("[Migration Error] DATABASE_URL is missing or lacks a real password.")
        return

    print("Connecting to Supabase PostgreSQL...")
    conn = psycopg2.connect(db_url)
    
    try:
        with conn.cursor() as cur:
            # 1. Drop old duplicate-only table if it exists
            cur.execute("DROP TABLE IF EXISTS scraped_products;")
            
            # 2. Create full scalable Data Warehouse table
            print("Creating scalable 'products' table...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    sku TEXT UNIQUE NOT NULL,
                    product_name TEXT,
                    category TEXT,
                    brand TEXT,
                    manufacturer TEXT,
                    sale_price TEXT,
                    discount_base_price TEXT,
                    stock INTEGER,
                    volume TEXT,
                    weight TEXT,
                    main_image TEXT,
                    product_url TEXT,
                    search_keywords TEXT,
                    detailed_description TEXT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # 3. Enable Row Level Security (RLS) for JWT Auth
            print("Enabling Row Level Security (RLS)...")
            cur.execute("ALTER TABLE products ENABLE ROW LEVEL SECURITY;")
            
            # 4. Clean up any existing policies from previous runs
            try:
                cur.execute("DROP POLICY IF EXISTS \"Allow authenticated read only\" ON products;")
                cur.execute("DROP POLICY IF EXISTS \"Allow service role all\" ON products;")
            except Exception:
                pass # Safe to ignore if they don't exist yet
                
            # 5. Create new JWT Auth Policies
            # Frontend users / App clients (with valid Supabase JWT) can SELECT products
            print("Enforcing JWT strictly authenticated SELECT rules for Frontend users...")
            cur.execute("""
                CREATE POLICY "Allow authenticated read only" 
                ON products FOR SELECT 
                TO authenticated 
                USING (true);
            """)

            # 6. Service role / PostgreSQL backend has permission to insert scraped products
            print("Allowing Python Scraper backend full privileges...")
            cur.execute("""
                CREATE POLICY "Allow service role all" 
                ON products FOR ALL 
                TO service_role 
                USING (true) 
                WITH CHECK (true);
            """)

            conn.commit()
            print("✅ Migration successful! Your Supabase database is now a secure API warehouse.")

    except Exception as e:
        print(f"❌ [Migration Failed] {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    run_migration()
