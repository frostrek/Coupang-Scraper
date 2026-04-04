# High-Performance E-commerce Data Miner

A production-grade, highly concurrent e-commerce scraping system designed to extract, normalize, and enrich product data using **Playwright/Scrapling** and **Google Gemini AI**.
Specifically designed to match the precise data-ingestion constraints of modern platforms with absolute data accuracy.

## 🌟 Core Capabilities
- **Advanced Stealth Scraping**: Utilizes `Scrapling`, `Playwright`, and `curl_cffi` to evade bot protections, CAPTCHAs, and headless browser blocking.
- **Multithreaded Execution**: Operates at 10x concurrency (`ThreadPoolExecutor`) for massively parallel product detail extraction without Out-Of-Memory (OOM) crashes.
- **AI Data Normalization**: Integrates Gemini 2.5 Flash to sanitize HTML dump formats, standardize bullet-points, strictly cap descriptions to 70-80 words, and evaluate accurate "Adult Only" tags.
- **Dynamic Bulk Database Prevention**: Hooks directly into a Supabase PostgreSQL instance via connection pooling. Operates bulk queries (`ILIKE ANY`) to drop read-loads by 99% and strictly prevent crawling duplicates.
- **Micro-Precision Financials**: Enforces structured Amazon extraction logic to ensure "Sale Price" represents the crossed-out MRP, and "Discount Base Price" strictly represents the actual paid amount—ignoring all unit-price noise.

## 📦 Required Dependencies
The application relies on several advanced scraping and parsing libraries.
These are managed in `requirements.txt`:
```txt
flask>=3.0.0
beautifulsoup4>=4.12.0
openpyxl>=3.1.0
scrapling>=0.4.3
playwright>=1.49.1
curl_cffi>=0.6.0
browserforge>=1.2.0
google-generativeai>=0.8.0
psycopg2-binary>=2.9.0
python-dotenv>=1.0.0
```

## 🛠 Installation & Setup

1. **Clone the repository and enter the directory**:
   ```bash
   cd Coupang-Scraper/scrapling
   ```

2. **Create and Activate a Virtual Environment**:
   ```bash
   python -m venv venv
   # Windows
   .\venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```

3. **Install Core Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Install Headless Browser Binaries (CRITICAL)**:
   This ensures `playwright` has the hidden chrome binaries required for Scrapling.
   ```bash
   playwright install chromium
   ```

## ⚙️ Configuration & Environment Variables

Create a file named `.env` in the root of the `scrapling` directory.
You must provide the following variables for the system to boot successfully:

```env
# Google Gemini 2.5 API Key (Used for LLM Data Formatting & Sanitization)
GEMINI_API_KEY="AIzaSyXXXXXXXXXXXXXXXXXXXXXXXXX"

# Supabase Postgres Connection String (Used for Duplicate Detection)
# Recommended to use the transaction pooler port (6543)
DATABASE_URL="postgresql://postgres.[ID]:[PASSWORD]@aws-0-eu-central-1.pooler.supabase.com:6543/postgres"
```

## 🗄️ Database Setup (Supabase)
The application expects a `products` table to exist in your PostgreSQL database to track duplicate logic. 
Run this SQL in your Supabase SQL Editor if you are setting up fresh:

```sql
CREATE TABLE IF NOT EXISTS products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category TEXT,
    product_name TEXT,
    brand TEXT,
    manufacturer TEXT,
    sale_price TEXT,
    discount_base_price TEXT,
    stock INTEGER,
    lead_time INTEGER,
    detailed_description TEXT,
    main_image TEXT,
    search_keywords TEXT,
    quantity INTEGER,
    volume TEXT,
    weight TEXT,
    adult_only VARCHAR(10),
    taxable VARCHAR(10),
    parallel_import VARCHAR(10),
    overseas_purchase VARCHAR(10),
    sku VARCHAR(100) UNIQUE,
    model_number VARCHAR(100),
    barcode VARCHAR(100),
    additional_image_1 TEXT,
    additional_image_2 TEXT,
    additional_image_3 TEXT,
    additional_image_4 TEXT,
    product_url TEXT,
    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Optimize duplicate checking queries
CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name);
```

## 🚀 Running the Application

1. Make sure your virtual environment is activated.
2. Start the web server:
   ```bash
   python run.py
   ```
3. Open your browser and navigate to:
   `http://localhost:5000`
4. Enter your Search Target URL e.g. `https://www.amazon.in/s?k=protein+powder` and specify the maximum output limit.

## 📊 Export Format
The scraper generates standard `.xlsx` (Excel) files containing strict validations:
- **Sale Price**: Captures the high M.R.P. (Crossed out price).
- **Discount Base Price**: Captures the lowered selling price.
- **Weight/Volume**: Intelligently parses specifications (`net quantity`) bridging into standard metric `g` or `ml`.
- **Model Number**: Extracted from ASIN and rigidly suffixed with `-1`.
- **Additional Images**: Captures high-resolution lifestyle/banner variants ignoring web-optimized thumbnails.
