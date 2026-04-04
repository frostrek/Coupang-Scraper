"""Scraper module for extracting product data from e-commerce websites."""
import json
import os
import re
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from browserforge.headers import HeaderGenerator

from .helpers import clean_text, extract_price, build_search_url
from .excel_utils import build_excel
from .llm_processor import sanitize_product_data
from . import db

# ─────────────────────────────────────────────────────────────────────────────
# CONCURRENCY CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
MAX_CONCURRENT_PRODUCTS = 10  # Process 10 products in parallel (speed boost)

# Initialize header generator for extreme stealth
header_gen = HeaderGenerator()

# ─────────────────────────────────────────────────────────────────────────────
# FAST FETCH (HTTP Only - 10x Faster than Headless Browser)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_pdp_fast(url):
    """Lightning-fast HTTP-only fetcher using curl_cffi to bypass TLS fingerprinting
    without launching a slow headless browser. Takes ~300ms instead of 3000ms."""
    try:
        headers = header_gen.generate(browser={'name': 'chrome'})
        # Convert dictionary to plain dict if needed, but browserforge gives a dict
        
        response = cffi_requests.get(
            url, 
            headers=headers, 
            # Randomize browser TLS signatures to prevent IP flagging
            impersonate=random.choice(["chrome116", "chrome120", "chrome124", "edge116"]),
            timeout=15
        )
        if response.status_code == 200:
            return response.text
        return None
    except Exception as e:
        print(f"[Fast Fetcher] Bypass Failed: {e}")
        return None

# ─────────────────────────────────────────────────────────────────────────────
# SCrapling FETCH (Stealthy, handles JS + bot-checks)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_with_scrapling(url, wait_sec=3, fetcher=None):
    try:
        from scrapling import StealthyFetcher
        log_msg = f"Fetching with Scrapling: {url}"
        print(log_msg)
        
        # Initialize the fetcher with stealth settings if one is not provided
        if fetcher is None:
            fetcher = StealthyFetcher()
        
        # Scrapling handles viewport, UA automatically
        # extra_flags are critical for Playwright to run correctly inside a Docker container
        response = fetcher.fetch(
            url,
            timeout=45000,  # 45s is plenty — 90s was overkill and delays failures
            network_idle=False,  # Don't wait for every single background tracking script
            disable_resources=True,  # Don't load images/CSS/fonts to save time/memory
            extra_flags=[
                "--no-sandbox", 
                "--disable-dev-shm-usage", 
                "--disable-setuid-sandbox", 
                "--disable-gpu"
            ]
        )
        
        if response.status != 200:
            print(f"[Scrapling] Non-200 status code: {response.status}")
        
        return response.body
    except Exception as e:
        err_msg = str(e)
        print(f"[Scrapling] Error: {err_msg}")
        # Return the error message string so the background job can log it
        return f"ERROR: {err_msg}"

# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT CONTAINER SELECTORS (Organized by platform for scalability)
# ─────────────────────────────────────────────────────────────────────────────
PLATFORM_SELECTORS = {
    'amazon': [
        'div[data-component-type="s-search-result"]',
        'div[data-asin]',
    ],
}

GENERIC_SELECTORS = [
    '[class*="product-card"]',
    'li[class*="product"]',
]


def get_selectors_for_url(url):
    """Get selectors specifically for Amazon or fallback."""
    if 'amazon' in url.lower():
        return PLATFORM_SELECTORS['amazon'] + GENERIC_SELECTORS
    return GENERIC_SELECTORS

def extract_products_from_soup(soup, base_url):
    """Extract product containers from page HTML."""
    containers = []
    selectors = get_selectors_for_url(base_url)
    
    for sel in selectors:
        found = [f for f in soup.select(sel) if len(f.get_text(strip=True)) > 20]
        if len(found) >= 2:
            containers = found
            break

    # Fallback: frequency-based detection
    if not containers:
        price_re = re.compile(r'[\$₹€£¥]\s*\d+|\bprice\b', re.I)
        freq = {}
        for tag in soup.find_all(['div', 'li', 'article'], class_=True):
            txt = tag.get_text()
            if price_re.search(txt) and 30 < len(txt.strip()) < 1200:
                key = tuple(sorted(tag.get('class', [])))
                freq.setdefault(key, []).append(tag)
        if freq:
            best = max(freq.values(), key=len)
            if len(best) >= 2:
                containers = best[:100]

    products = []
    for c in containers[:100]:
        p = extract_single_product(c, base_url)
        if p and p.get('Product Name'):
            products.append(p)
    return products

def _pick(container, selectors, transform=None):
    """Pick first matching selector value from container."""
    for sel in selectors:
        el = container.select_one(sel)
        if el:
            val = clean_text(el.get_text()) if transform is None else transform(el)
            if val:
                return val
    return ''

def extract_single_product(c, base_url):
    """Extract product data from a single product container.
    
    PRICE FIELD SEMANTICS (Coupang Upload Standard):
        Sale Price         = MRP / Original price BEFORE discount (e.g. ₹275, crossed out)
        Discount Base Price = Current price AFTER discount (e.g. ₹220, what buyer pays)
    """
    # Skip sponsored/ad products
    sponsored_selectors = [
        '.s-sponsored-label-text',
        '.s-sponsored-info-icon',
        '.puis-sponsored-label-text',
        '.ad-label',
        'span:-soup-contains("Sponsored")',
    ]
    for sel in sponsored_selectors:
        if c.select_one(sel):
            return None

    # Generic text check for sponsored badges
    badges = c.select('.a-badge-text, [class*="badge"], [class*="label"], [class*="tag"], span')
    for badge in badges:
        btxt = badge.get_text().strip()
        if btxt.lower() in ('sponsored', 'ad') or 'sponsored' in btxt.lower():
            if len(btxt) < 20:
                return None

    # Initialize product with exact CSV template schema
    p = {
        'Category': '',
        'Product Name': '',
        'Brand': '',
        'Manufacturer': '',
        'Sale Price': '',          # MRP / Original (higher, crossed-out)
        'Discount Base Price': '', # Discounted / Current (lower, what buyer pays)
        'Stock': 2,
        'Lead Time': 12,
        'Detailed Description': '',
        'Main Image': '',
        'Search Keywords': '',
        'Quantity': 1,
        'Volume': '',
        'Weight': '',
        'Adult Only': 'N',
        'Taxable': 'N',
        'Parallel Import': 'N',
        'Overseas Purchase': 'Y',
        'SKU': '',
        'Model Number': '',
        'Barcode': '',
        'Additional Image 1': '',
        'Additional Image 2': '',
        'Additional Image 3': '',
        'Additional Image 4': '',
        '_product_url': '',  # Internal use only, not exported
    }

    # Extract SKU early from Amazon's data-asin attribute (most reliable source)
    data_asin = c.get('data-asin', '').strip()
    if data_asin and len(data_asin) == 10:
        p['SKU'] = data_asin
        p['Model Number'] = data_asin

    # Extract Product Name
    name = _pick(c, [
        'span.a-text-normal', 'h2 a span', 'h2 a', 'h3 a', 'h4 a',
        '[class*="product-title"]', '[class*="ProductName"]', '[class*="product-name"]',
        '[class*="product_name"]', '[class*="title"]', '._4rR01T', '._2Tpdn3',
        '[data-testid*="title"]', '[data-testid*="name"]', 'h2', 'h3', 'h4',
    ])
    if not name:
        a = c.find('a', title=True)
        if a:
            name = clean_text(a['title'])
    if name:
        p['Product Name'] = name[:220]

    # ─────────────────────────────────────────────────────────────────────
    #  SALE PRICE = MRP / Original price (crossed-out, before discount)
    # ─────────────────────────────────────────────────────────────────────
    mrp = _pick(c, [
        # Amazon: strikethrough / original price selectors
        'span.a-price.a-text-price span.a-offscreen',
        '.a-text-price span.a-offscreen',
        # Generic: original / old / MRP selectors
        '._3I9_wc',
        '[class*="original-price"]', '[class*="old-price"]', '[class*="mrp"]',
        '[class*="was-price"]', '[class*="compare-price"]', '[class*="list-price"]',
        'del', 's', 'strike',
    ], lambda el: extract_price(el.get_text()))
    if mrp and re.search(r'\d', mrp):
        p['Sale Price'] = mrp

    # ─────────────────────────────────────────────────────────────────────
    #  DISCOUNT BASE PRICE = Current discounted price (what buyer pays)
    # ─────────────────────────────────────────────────────────────────────
    disc_price = None
    for sel in [
        # Amazon: main displayed price (the big bold number)
        'span.a-price[data-a-size="xl"] span.a-offscreen',
        'span.a-price[data-a-size="l"] span.a-offscreen',
        'span.a-price[data-a-size="b"] span.a-offscreen',
        'span.priceToPay span.a-offscreen',
        'span.a-price-whole',
        # Indian e-commerce
        '._30jeq3', '._1_WHN1', '._16Jk6d',
    ]:
        el = c.select_one(sel)
        if el:
            val = extract_price(el.get_text())
            if val and re.search(r'\d', val):
                disc_price = val
                break
                
    if disc_price and re.search(r'\d', disc_price):
        p['Discount Base Price'] = disc_price

    # ─────────────────────────────────────────────────────────────────────
    #  PRICE VALIDATION: Discount must be ≤ Sale Price (MRP)
    #  If discounted is somehow larger, they were scraped from wrong selectors → swap
    # ─────────────────────────────────────────────────────────────────────
    if p.get('Sale Price') and p.get('Discount Base Price'):
        try:
            sale_val = float(re.sub(r'[^\d.]', '', p['Sale Price']))
            disc_val = float(re.sub(r'[^\d.]', '', p['Discount Base Price']))
            if disc_val > sale_val:
                # Discounted should never exceed MRP — selectors grabbed them backwards
                p['Sale Price'], p['Discount Base Price'] = p['Discount Base Price'], p['Sale Price']
        except ValueError:
            pass

    # If only one price is found (no discount), set both fields to exactly the same price
    if not p.get('Sale Price') and p.get('Discount Base Price'):
        p['Sale Price'] = p['Discount Base Price']
    if not p.get('Discount Base Price') and p.get('Sale Price'):
        p['Discount Base Price'] = p['Sale Price']

    # Extract Brand
    brand = _pick(c, [
        '#bylineInfo', 'a#bylineInfo', '._2Wk9S9',
        '[class*="brand"]', '[class*="Brand"]', '[data-testid*="brand"]',
    ])
    if not brand and name:
        brand = name.split(' ')[0]
    if brand and len(brand) < 80:
        p['Brand'] = brand

    # Extract Product URL (internal use for PDP scraping)
    a = c.find('a', href=True)
    if a:
        href = a['href']
        p['_product_url'] = href if href.startswith('http') else urljoin(base_url, href)

    # Extract Main Image
    for sel in ['img.s-image', 'img[class*="product"]', 'img[class*="Product"]', 'img']:
        el = c.select_one(sel)
        if el:
            src = el.get('data-a-dynamic-image') or el.get('srcset') or el.get('src') or ''
            if src.startswith('{'):
                try:
                    urls = json.loads(src)
                    src = max(urls.items(), key=lambda x: x[1][0])[0] if urls else ''
                except (json.JSONDecodeError, ValueError):
                    pass
            if ',' in src:
                src = src.split(',')[-1].strip().split(' ')[0]
            if src and 'http' in src:
                if src.startswith('//'):
                    src = 'https:' + src
                p['Main Image'] = src
                break

    return p

def fetch_product_details(url, existing_p, fetcher=None):
    """Visits the Product Detail Page (PDP) to extract deep information.
    
    Includes dedicated price extraction from PDP for higher accuracy than SERP.
    """
    # Try the 10x faster HTTP-only fetcher first to avoid IP blocks and headless overhead
    html = fetch_pdp_fast(url)
    
    # Check if Amazon blocked the fast fetcher with a CAPTCHA or if it failed
    if not html or "captchacharacters" in html.lower() or "type the characters" in html.lower():
        # Fall back to the heavy, JavaScript-enabled Playwright browser instance
        html = fetch_with_scrapling(url, wait_sec=0, fetcher=fetcher)

    if not html or isinstance(html, str) and html.startswith("ERROR:"):
        return existing_p

    soup = BeautifulSoup(html, 'lxml')
    p = existing_p.copy()

    # ─────────────────────────────────────────────────────────────────────
    # PDP PRICE EXTRACTION (More accurate than SERP)
    # ─────────────────────────────────────────────────────────────────────
    # Sale Price = MRP (crossed out, original, before discount)
    pdp_mrp = None
    for sel in [
        'span.priceBlockStrikePriceString',
        '#listPrice',
        'span.basisPrice span.a-offscreen',
        'span[data-a-strike="true"] span.a-offscreen',
        'span.a-text-strike',
        'del span.a-offscreen',
    ]:
        el = soup.select_one(sel)
        if el:
            val = extract_price(el.get_text())
            if val and re.search(r'\d', val):
                pdp_mrp = val
                break

    # Fallback: look for "M.R.P.:" text pattern specifically (Amazon India)
    if not pdp_mrp:
        mrp_label = soup.find(string=re.compile(r'M\.?R\.?P\.?\s*:?', re.I))
        if mrp_label:
            parent = mrp_label.find_parent()
            if parent:
                price_el = parent.find_next('span', class_=re.compile(r'a-offscreen|a-price'))
                if price_el:
                    val = extract_price(price_el.get_text())
                    if val and re.search(r'\d', val):
                        pdp_mrp = val

    # Discount Base Price = Current discounted price (what buyer pays)
    pdp_disc = None
    for sel in [
        'span.priceToPay span.a-offscreen',
        '#priceblock_dealprice',
        '#priceblock_ourprice',
        '.a-price[data-a-size="xl"] span.a-offscreen',
        '.a-price[data-a-size="l"] span.a-offscreen',
        '.a-price[data-a-size="b"] span.a-offscreen',
        '#corePrice_feature_div span.a-offscreen',
        '#corePriceDisplay_desktop_feature_div span.a-offscreen',
    ]:
        el = soup.select_one(sel)
        if el:
            val = extract_price(el.get_text())
            if val and re.search(r'\d', val):
                pdp_disc = val
                break

    # Apply PDP prices (override SERP prices only if we found better data)
    if pdp_mrp:
        p['Sale Price'] = pdp_mrp
    if pdp_disc:
        p['Discount Base Price'] = pdp_disc

    # Re-validate: Discount must be ≤ Sale Price
    if p.get('Sale Price') and p.get('Discount Base Price'):
        try:
            sale_val = float(re.sub(r'[^\d.]', '', p['Sale Price']))
            disc_val = float(re.sub(r'[^\d.]', '', p['Discount Base Price']))
            if disc_val > sale_val:
                p['Sale Price'], p['Discount Base Price'] = p['Discount Base Price'], p['Sale Price']
        except ValueError:
            pass

    # If only one price found (no discount), set both fields to the same value
    if not p.get('Sale Price') and p.get('Discount Base Price'):
        p['Sale Price'] = p['Discount Base Price']
    if not p.get('Discount Base Price') and p.get('Sale Price'):
        p['Discount Base Price'] = p['Sale Price']

    # 1. Extract Detailed Description (Prioritize "About this item" / feature bullets)
    about_item = soup.select_one('#feature-bullets')
    if about_item:
        p['Detailed Description'] = clean_text(about_item.get_text())[:2000]
    else:
        desc_el = soup.select_one(
            '#productDescription, [class*="description"], [class*="Description"]'
        )
        if desc_el:
            p['Detailed Description'] = clean_text(desc_el.get_text())[:2000]

    # 2. Extract Technical Specs / Item Details (Brand, Manufacturer, ASIN)
    spec_data = {}
    
    # Try multiple common table/list structures for product details
    potential_containers = [
        '#productDetails_db_sections',
        '#productDetails_techSpec_section_1',
        'table[id*="productDetails"]',
        '#detailBullets_feature_div',
        '.a-expander-content',
        '#itemDetails',
        '.prodDetSectionEntry'
    ]
    
    for container_sel in potential_containers:
        container = soup.select_one(container_sel)
        if not container:
            continue
            
        # Case A: Table rows
        for row in container.select('tr'):
            th = row.select_one('th, td.label, .a-color-secondary')
            td = row.select_one('td:not(.label), td.value')
            # Fallbacks if strict structural tags are missing
            if not td and th:
                td = th.find_next_sibling('td') or row.select_one('.a-size-base:not(.a-color-secondary)')
                
            if th and td and th != td:
                key = clean_text(th.get_text()).strip(': ').lower()
                val = clean_text(td.get_text(separator=' ')).strip()
                if key and val:
                    spec_data[key] = val
                    
        # Case B: List items (bullets)
        for li in container.select('li, .a-list-item'):
            # Some Amazon pages have labels in bold spans
            bold_span = li.select_one('span.a-text-bold')
            if bold_span:
                key_text = bold_span.get_text(separator=' ')
                key = clean_text(key_text).strip(': ').lower()
                # Use separator here too
                val = clean_text(li.get_text(separator=' ').replace(key_text, '', 1)).strip(': ')
                if key and val and key.lower() != val.lower():
                    spec_data[key] = val
            else:
                text = clean_text(li.get_text(separator=' '))
                if ':' in text:
                    parts = text.split(':', 1)
                    if len(parts) == 2:
                        key = parts[0].strip().lower()
                        val = parts[1].strip()
                        if key and val and key.lower() != val.lower():
                            spec_data[key] = val
                    
        # Case C: Generic rows (divs)
        for row in container.select('.a-row'):
            text = clean_text(row.get_text())
            if ':' in text:
                parts = text.split(':', 1)
                key = parts[0].strip().lower()
                val = parts[1].strip()
                if key and val and key.lower() != val.lower():
                    spec_data[key] = val

    # Map discovered specs to our fields
    key_map = {
        'brand': 'Brand',
        'manufacturer': 'Manufacturer',
        'asin': 'SKU',
        'item model number': 'Model Number',
        'manufacturer part number': 'Model Number',
        'model number': 'Model Number',
    }
    
    for k, field in key_map.items():
        # 1. Try exact match first
        if k in spec_data and spec_data[k]:
            p[field] = spec_data[k]
        else:
            # 2. Fallback to partial match
            for spec_key, spec_val in spec_data.items():
                if k in spec_key and spec_val:
                    p[field] = spec_val
                    break
    
    # Validation: If SKU was extracted as the literal string "ASIN", drop it so the URL fallback can rescue it
    if p.get('SKU') and p['SKU'].strip().upper() == 'ASIN':
        p['SKU'] = ''

    # Ensure SKU and Model Number are aligned (User Requirement: Model starts with SKU and ends in -1)
    if p.get('SKU'):
        p['Model Number'] = p['SKU'] + "-1"
    elif p.get('Model Number'):
        p['SKU'] = p['Model Number']
        p['Model Number'] = p['Model Number'] + "-1"

    # 3. Extract Additional Images (Bulletproof Amazon strategy)
    add_images = []
    
    # Strategy A: Extract from Amazon's inline JSON (100% reliable for all hi-res thumbnails)
    try:
        json_matches = re.findall(r'"hiRes":"(https://[^"]+)"', html)
        if not json_matches:
            json_matches = re.findall(r'"large":"(https://[^"]+)"', html)
        
        for src in json_matches:
            if 'http' in src and 'GIF' not in src.upper():
                if src not in add_images and src != p.get('Main Image'):
                    add_images.append(src)
    except Exception:
        pass

    # Strategy B: Fallback to DOM parsing for other sites or if JSON fails
    if not add_images:
        for img in soup.select('#altImages img, .imageThumbnail img, [class*="thumbnail"] img'):
            src = img.get('src') or img.get('data-src') or ''
            if src and 'http' in src and 'GIF' not in src.upper():
                hi_res = re.sub(r'\._AC_.*_\.', '.', src)
                if hi_res not in add_images and hi_res != p.get('Main Image'):
                    add_images.append(hi_res)

    if add_images:
        p['Additional Image 1'] = add_images[0]
    if len(add_images) > 1:
        p['Additional Image 2'] = add_images[1]
    if len(add_images) > 2:
        p['Additional Image 3'] = add_images[2]
    if len(add_images) > 3:
        p['Additional Image 4'] = add_images[3]

    # Extract SKU / Model Number (Amazon ASIN) from URL if not found in specs
    if not p.get('SKU'):
        asin_m = re.search(r'/dp/([A-Z0-9]{10})', url)
        if asin_m:
            p['SKU'] = asin_m.group(1)
            p['Model Number'] = p['SKU']

    # ─────────────────────────────────────────────────────────────────────
    #  VOLUME / WEIGHT EXTRACTION (Strict Priority: Specs > Title > Body)
    # ─────────────────────────────────────────────────────────────────────
    def _parse_and_assign_metric(text_chunk):
        """Attempts to parse metrics and returns True if successful."""
        m = re.search(r'(\d+(?:\.\d+)?)\s*(kg|g|gm|ml|l|litre|liters|oz|lb)\b', text_chunk, re.I)
        if not m: return False
        
        amount, unit = float(m.group(1)), m.group(2).lower()
        if unit == 'kg': amount, unit = amount * 1000, 'g'
        elif unit in ('l', 'litre', 'liters'): amount, unit = amount * 1000, 'ml'
        elif unit == 'gm': unit = 'g'
            
        amount = round(amount, 3)
        val = f"{int(amount) if amount.is_integer() else amount} {unit}"
        
        if unit in ('ml', 'l'): p['Volume'] = val
        else: p['Weight'] = val
        return True

    # Priority 1: Check actual DB Spec tables first (Highest accuracy)
    mapped_specs = False
    for spec_key, spec_val in spec_data.items():
        if any(x in spec_key for x in ['net quantity', 'net weight', 'item weight', 'volume', 'item volume']):
            if _parse_and_assign_metric(spec_val):
                mapped_specs = True
                break
                
    # Priority 2: Check the Title
    if not mapped_specs:
        mapped_specs = _parse_and_assign_metric(p.get('Product Name', ''))
        
    # Priority 3: Check entire page dump
    if not mapped_specs:
        specs_text = p.get('Product Name', '') + " " + " ".join(spec_data.values()) + " " + p.get('Detailed Description', '')
        _parse_and_assign_metric(specs_text)
        
    p['_raw_specs'] = p.get('Product Name', '') + " " + " ".join(spec_data.values())

    # Generate Search Keywords from product name
    if p.get('Product Name'):
        words = p['Product Name'].lower().replace(',', '').split()
        unique_words = []
        for w in words:
            if w not in unique_words and len(w) > 2:
                unique_words.append(w)
        p['Search Keywords'] = ', '.join(unique_words[:15])

    return p


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE PRODUCT PROCESSOR (used by ThreadPool)
# ─────────────────────────────────────────────────────────────────────────────
def _process_single_product(prod, job_fetcher, log_fn):
    """Process a single product: PDP fetch → LLM sanitize. Thread-safe.
    
    Returns the enriched product dict or None on critical failure.
    """
    pname = prod.get('Product Name', '')
    product_url = prod.get('_product_url')

    try:
        if product_url:
            log_fn(f"🔎 Deep scraping: {pname[:40]}...")
            prod = fetch_product_details(product_url, prod, fetcher=job_fetcher)
            
            # Gemini LLM Sanitization and Precision Extractor
            log_fn(f"✨ Perfecting with Gemini: {pname[:30]}...")
            prod = sanitize_product_data(prod)
            
            time.sleep(random.uniform(0.1, 0.3))  # Reduced from 0.3-0.8
    except Exception as pdp_err:
        log_fn(f"⚠️ Error enriching {pname[:30]}: {pdp_err}. Using basic data.", 'warn')

    return prod


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND JOB
# ─────────────────────────────────────────────────────────────────────────────
def scrape_job(job_id, jobs, base_url, keyword, max_products, outputs_dir):
    job = jobs[job_id]
    job['status'] = 'running'

    def log(msg, level='info'):
        job['log'].append({'msg': msg, 'level': level})
        job['last_message'] = msg
        print(f"[{job_id}] {msg}")

    try:
        all_products, page = [], 1

        log(f"🌐 Site   : {base_url}")
        log(f"🔑 Keyword: '{keyword}'  |  Max: {max_products}")
        log(f"🚀 Launching Scrapling fetcher (Shared Instance, {MAX_CONCURRENT_PRODUCTS}x concurrency)...")

        # Create ONE shared browser instance to prevent devastating OOM crashes
        from scrapling import StealthyFetcher
        job_fetcher = StealthyFetcher()

        while len(all_products) < max_products:
            url = build_search_url(base_url, keyword, page)
            log(f"📄 Fetching page {page} …")

            html = fetch_with_scrapling(url, wait_sec=5, fetcher=job_fetcher)

            # Check for failure (None) or our custom string error ("ERROR: ...")
            if not html or (isinstance(html, str) and html.startswith("ERROR:")):
                # Extract specific error text if it failed
                specific_err = html if html else "Unknown error occurred"
                msg = (f"Scrape Failed.\nDetailed Error: {specific_err}\n"
                       "This usually means the site is blocking access from this IP or Playwright crashed.")
                log(f"❌ {msg}", 'error')
                job['status'] = 'error'; job['error'] = msg; return

            soup = BeautifulSoup(html, 'lxml')
            for tag in soup(['script','style','noscript','iframe']): tag.decompose()

            products = extract_products_from_soup(soup, base_url)

            if not products:
                text_low = soup.get_text()[:600].lower()
                if any(w in text_low for w in ['captcha','robot','verify','are you human']):
                    msg = "Site is showing a CAPTCHA. Try again later or from a different network."
                elif any(w in text_low for w in ['sign in','log in','login']):
                    msg = "Site requires you to log in before showing products."
                elif page == 1:
                    msg = ("No products detected on page 1.\n"
                           "Possible reasons: keyword has no results, site structure changed,\n"
                           "or the site needs a different URL format.")
                else:
                    log("ℹ️ No more products. Stopping.", 'warn'); break

                if page == 1:
                    log(f"⚠️ {msg}", 'warn')
                    job['status'] = 'error'; job['error'] = msg; return
                break

            # ── Filter duplicates BEFORE expensive PDP/LLM processing ──
            candidates = []
            skipped = 0
            
            # ── Supabase Bulk Optimization (Reads page once instead of row-by-row) ──
            page_skus = []
            page_names = []
            for prod in products:
                sku = prod.get('SKU')
                if not sku and prod.get('_product_url'):
                    asin_m = re.search(r'/dp/([A-Z0-9]{10})', prod.get('_product_url'))
                    if asin_m:
                        sku = asin_m.group(1)
                        prod['SKU'] = sku
                if sku:
                    page_skus.append(sku)
                if prod.get('Product Name'):
                    page_names.append(prod.get('Product Name'))

            scraped_skus_bulk = db.get_scraped_skus(page_skus)
            scraped_names_bulk = db.get_scraped_names(page_names)

            for prod in products:
                if len(all_products) + len(candidates) >= max_products:
                    break
                
                sku = prod.get('SKU')
                pname = prod.get('Product Name', '')

                # ── Duplicate Prevention (SKU-based, then product-name fallback) ──
                if sku and sku in scraped_skus_bulk:
                    log(f"⏭️ Skipping bulk duplicate (SKU: {sku})")
                    skipped += 1
                    continue
                
                # Fallback: check by product name for non-Amazon sites without SKUs
                if not sku and pname and pname.lower().strip() in scraped_names_bulk:
                    log(f"⏭️ Skipping bulk duplicate: {pname[:40]}...")
                    skipped += 1
                    continue

                candidates.append(prod)

            # ── CONCURRENT PRODUCT PROCESSING ──────────────────────────
            added = 0
            if candidates:
                log(f"⚡ Processing {len(candidates)} products ({MAX_CONCURRENT_PRODUCTS}x parallel)...")
                
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_PRODUCTS) as pool:
                    futures = {
                        pool.submit(_process_single_product, prod, job_fetcher, log): prod
                        for prod in candidates
                    }
                    
                    for future in as_completed(futures):
                        try:
                            enriched = future.result()
                            if enriched:
                                # Preserve Product URL for Excel export and UI
                                enriched['Product URL'] = enriched.pop('_product_url', None)
                                enriched.pop('_raw_specs', None)
                                all_products.append(enriched)
                                
                                # Insert fully sanitized product into Postgres Warehouse
                                try:
                                    sku = enriched.get('SKU')
                                    if sku:
                                        db.save_product_to_db(enriched)
                                except Exception as db_err:
                                    pname = enriched.get('Product Name', '')[:30]
                                    log(f"⚠️ DB save failed for {pname}: {db_err}", 'warn')
                                    
                                added += 1
                        except Exception as fut_err:
                            log(f"⚠️ Product processing failed: {fut_err}", 'warn')

            log(f"✅ Page {page}: +{added} new, ⏭️{skipped} skipped  (total {len(all_products)}/{max_products})", 'success')
            job['progress'] = int(min(len(all_products) / max_products * 85, 85))
            job['found']    = len(all_products)

            # Stop ONLY if the page was truly empty (no products found at all)
            # Do NOT stop if products were found but all were duplicates — move to next page!
            if added == 0 and skipped == 0:
                break
            page += 1
            time.sleep(random.uniform(0.8, 1.5))

        if not all_products:
            job['status'] = 'error'; job['error'] = "No products were scraped."; return

        log(f"📊 Building Excel for {len(all_products)} products …")
        job['progress'] = 90
        fp = build_excel(all_products, keyword, base_url, outputs_dir)
        log(f"✅ Excel saved → {os.path.basename(fp)}", 'success')

        job.update({'status':'done','progress':100,'filepath':fp,'total':len(all_products),'products':all_products})

    except Exception as e:
        import traceback
        job['status'] = 'error'; job['error'] = str(e)
        log(f"💥 {e}", 'error')
        print(traceback.format_exc())
    finally:
        # Prevent Memory Leaks! Explicitly close browser
        if 'job_fetcher' in locals() and job_fetcher:
            try:
                log("🧹 Cleaning up browser instance...")
                if hasattr(job_fetcher, 'stop'):
                    job_fetcher.stop()
            except Exception as clean_err:
                print(f"[Cleanup] Failed to stop fetcher: {clean_err}")
