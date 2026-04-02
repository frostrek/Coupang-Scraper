"""Scraper module for extracting product data from e-commerce websites."""
import json
import os
import re
import random
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .helpers import clean_text, extract_price, build_search_url
from .excel_utils import build_excel
from .llm_processor import sanitize_product_data
from . import db

# ─────────────────────────────────────────────────────────────────────────────
# SCrapling FETCH (Stealthy, handles JS + bot-checks)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_with_scrapling(url, wait_sec=3):
    try:
        from scrapling import StealthyFetcher
        log_msg = f"Fetching with Scrapling: {url}"
        print(log_msg)
        
        # Initialize the fetcher with stealth settings
        fetcher = StealthyFetcher()
        # Scrapling handles viewport, UA automatically
        # extra_flags are critical for Playwright to run correctly inside a Docker container
        response = fetcher.fetch(
            url,
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
    """Extract product data from a single product container."""
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
        'Sale Price': '',
        'Discount Base Price': '',
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

    # Extract Sale Price
    price = _pick(c, [
        'span.a-price[data-a-size="xl"] span.a-offscreen', 'span.a-price-whole',
        '._30jeq3', '._1_WHN1', '._16Jk6d',
        '[class*="selling-price"]', '[class*="sale-price"]', '[class*="current-price"]',
        '[class*="offer-price"]', '[class*="discounted"]',
        '[class*="price"]', '[class*="Price"]', '[data-testid*="price"]',
    ], lambda el: extract_price(el.get_text()))
    if price and re.search(r'\d', price):
        p['Sale Price'] = price

    # Extract Discount Base Price (MRP)
    mrp = _pick(c, [
        'span.a-price.a-text-price span.a-offscreen', '._3I9_wc',
        '[class*="original-price"]', '[class*="old-price"]', '[class*="mrp"]',
        '[class*="was-price"]', '[class*="compare-price"]', 'del', 's', 'strike',
    ], lambda el: extract_price(el.get_text()))
    if mrp and re.search(r'\d', mrp):
        p['Discount Base Price'] = mrp

    # Mathematical Price Validation (Amazon occasionally hides Unit pricing under MRP span)
    if p.get('Sale Price') and p.get('Discount Base Price'):
        try:
            sp_val = float(re.sub(r'[^\d.]', '', p['Sale Price']))
            mrp_val = float(re.sub(r'[^\d.]', '', p['Discount Base Price']))
            # If the base price is cheaper than sale, they were scraped backward. Swap them.
            if mrp_val < sp_val:
                p['Sale Price'], p['Discount Base Price'] = p['Discount Base Price'], p['Sale Price']
        except ValueError:
            pass

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

def fetch_product_details(url, existing_p):
    """Visits the Product Detail Page (PDP) to extract deep information."""
    html = fetch_with_scrapling(url, wait_sec=2)
    if not html:
        return existing_p

    soup = BeautifulSoup(html, 'lxml')
    p = existing_p.copy()

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
            th = row.select_one('th, td.label, .a-color-secondary, span.a-text-bold')
            td = row.select_one('td, td.value, .a-size-base, span:not(.a-text-bold)')
            if th and td:
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
                if key and val:
                    spec_data[key] = val
            else:
                text = clean_text(li.get_text(separator=' '))
                if ':' in text:
                    parts = text.split(':', 1)
                    if len(parts) == 2:
                        key = parts[0].strip().lower()
                        val = parts[1].strip()
                        if key and val:
                            spec_data[key] = val
                    
        # Case C: Generic rows (divs)
        for row in container.select('.a-row'):
            text = clean_text(row.get_text())
            if ':' in text:
                parts = text.split(':', 1)
                key = parts[0].strip().lower()
                val = parts[1].strip()
                if key and val:
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
    
    # Ensure SKU and Model Number are identical (User Requirement)
    if p.get('SKU'):
        p['Model Number'] = p['SKU']
    elif p.get('Model Number'):
        p['SKU'] = p['Model Number']

    # 3. Extract Additional Images
    add_images = []
    for img in soup.select('#altImages img, .imageThumbnail img, [class*="thumbnail"] img'):
        src = img.get('src') or img.get('data-src') or ''
        if src and 'http' in src and 'GIF' not in src.upper():
            # Get high-res by removing resizing suffix (Amazon specific _AC_...)
            hi_res = re.sub(r'\._AC_.*_\.', '.', src)
            if hi_res not in add_images and hi_res != p.get('Main Image'):
                add_images.append(hi_res)

    if add_images:
        p['Additional Image 1'] = add_images[0]
    if len(add_images) > 1:
        p['Additional Image 2'] = add_images[1]

    # Extract SKU / Model Number (Amazon ASIN) from URL if not found in specs
    if not p.get('SKU'):
        asin_m = re.search(r'/dp/([A-Z0-9]{10})', url)
        if asin_m:
            p['SKU'] = asin_m.group(1)
            p['Model Number'] = p['SKU']

    # Extract Volume / Weight from specs or feature bullets ONLY
    # This specifically targets actual product specs and prevents the scraper from picking up "39L" sizing choices globally
    specs_text = " ".join(spec_data.values()) + " " + p.get('Detailed Description', '')
    p['_raw_specs'] = specs_text  # Save the raw unstructured dump for Gemini absolute precision
    weight_m = re.search(r'(\d+(?:\.\d+)?\s*(?:kg|g|gm|ml|l|oz|lb))\b', specs_text, re.I)
    if weight_m:
        val = weight_m.group(1)
        # Assign to Weight or Volume based on unit
        if re.search(r'(ml|l)$', val, re.I):
            p['Volume'] = val
        else:
            p['Weight'] = val

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
        log("🚀 Launching Scrapling fetcher...")

        while len(all_products) < max_products:
            url = build_search_url(base_url, keyword, page)
            log(f"📄 Fetching page {page} …")

            html = fetch_with_scrapling(url, wait_sec=5)

            if not html or html.startswith("ERROR:"):
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

            added = 0
            skipped = 0
            for prod in products:
                if len(all_products) >= max_products:
                    break

                product_url = prod.get('_product_url')

                # Pre-extract SKU from URL to check Supabase BEFORE visiting the product page
                sku = prod.get('SKU')
                if not sku and product_url:
                    asin_m = re.search(r'/dp/([A-Z0-9]{10})', product_url)
                    if asin_m:
                        sku = asin_m.group(1)
                        prod['SKU'] = sku
                
                # ── Duplicate Prevention (SKU-based, then product-name fallback) ──
                if sku and db.is_sku_scraped(sku):
                    log(f"⏭️ Skipping duplicate (SKU: {sku})")
                    skipped += 1
                    continue
                
                # Fallback: check by product name for non-Amazon sites without SKUs
                pname = prod.get('Product Name', '')
                if not sku and pname and db.is_product_name_scraped(pname):
                    log(f"⏭️ Skipping duplicate: {pname[:40]}...")
                    skipped += 1
                    continue

                # Enrich with PDP data
                if product_url:
                    log(f"🔎 Deep scraping: {pname[:40]}...")
                    prod = fetch_product_details(product_url, prod)
                    
                    # Gemini LLM Sanitization and Precision Extractor
                    log(f"✨ Perfecting properties with Gemini: {pname[:30]}...")
                    prod = sanitize_product_data(prod)
                    
                    time.sleep(random.uniform(1.2, 2.5))

                # Preserve Product URL for Excel export and UI
                prod['Product URL'] = prod.pop('_product_url', None)
                prod.pop('_raw_specs', None)
                all_products.append(prod)
                
                # Insert fully sanitized product dictionary into Postgres Warehouse
                sku = prod.get('SKU')  # Re-read — PDP scraping may have found the SKU
                if sku:
                    db.save_product_to_db(prod)
                    
                added += 1

            log(f"✅ Page {page}: +{added} new, ⏭️{skipped} skipped  (total {len(all_products)}/{max_products})", 'success')
            job['progress'] = int(min(len(all_products) / max_products * 85, 85))
            job['found']    = len(all_products)

            # Stop ONLY if the page was truly empty (no products found at all)
            # Do NOT stop if products were found but all were duplicates — move to next page!
            if added == 0 and skipped == 0:
                break
            page += 1
            time.sleep(random.uniform(2.0, 3.5))

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
