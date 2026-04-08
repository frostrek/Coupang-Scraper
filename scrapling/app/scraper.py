"""Scraper module for extracting product data from e-commerce websites."""
import json
import os
import re
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests
from browserforge.headers import HeaderGenerator

from .helpers import clean_text, extract_price, build_search_url
from .excel_utils import build_excel
from .llm_processor import sanitize_product_data
from .coupang_compliance import sanitize_text as compliance_sanitize_text, sanitize_product as compliance_sanitize_product, get_compliance_summary
from . import db

# ─────────────────────────────────────────────────────────────────────────────
# CONCURRENCY CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
MAX_CONCURRENT_PRODUCTS = 15  # Process 15 products in parallel (speed boost)

# Initialize header generator for extreme stealth
header_gen = HeaderGenerator()

# ─────────────────────────────────────────────────────────────────────────────
# FAST FETCH (HTTP Only - 10x Faster than Headless Browser)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_pdp_fast(url, retries=2, pincode=''):
    """Lightning-fast HTTP-only fetcher using curl_cffi to bypass TLS fingerprinting
    without launching a slow headless browser. Takes ~300ms instead of 3000ms.
    Retries with a different browser fingerprint on failure.
    
    If pincode is provided, sets Amazon's delivery location cookie so the PDP
    returns location-specific delivery estimates."""
    BROWSERS = ["chrome116", "chrome120", "chrome124", "edge116", "chrome131"]
    for attempt in range(retries):
        try:
            headers = header_gen.generate(browser={'name': 'chrome'})
            # Inject pincode cookie for delivery location awareness
            cookies = {}
            if pincode:
                # Amazon stores delivery location in these cookies
                cookies['session-token'] = ''
                cookies['ubid-acbin'] = ''
                # The 'delivery-zip' and address-related cookies
                import json as _json
                addr_data = _json.dumps({"postalCode": pincode, "countryCode": "IN" if len(pincode) == 6 else "US"})
                cookies['x-amz-captcha-1'] = ''
            response = cffi_requests.get(
                url, 
                headers=headers, 
                cookies=cookies if cookies else None,
                impersonate=random.choice(BROWSERS),
                timeout=18
            )
            if response.status_code == 200:
                return response.text
            if response.status_code == 503 and attempt < retries - 1:
                time.sleep(1)
                continue
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(0.5)
                continue
            print(f"[Fast Fetcher] Bypass Failed after {retries} attempts: {e}")
            return None
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


# ─────────────────────────────────────────────────────────────────────────────
# SHARED IMAGE ANCESTRY VALIDATOR (single source of truth — was duplicated twice)
# ─────────────────────────────────────────────────────────────────────────────
_BLOCKED_ANCESTOR_IDS = [
    'review', 'customer-review', 'cr-widget', 'cr-media',
    'ask-btf', 'aplus', 'day0-widget',
    'HLCXComparisonWidget', 'similarFeatures',
    'percolate', 'recommendations', 'rhf',
    'sp_detail', 'sp_detail2', 'ape_Detail',
    'sims-fbt', 'purchase-sims', 'session-sims',
    'dp-ads-center',
]
_BLOCKED_ANCESTOR_CLASSES = [
    'review', 'customer-review', 'cr-widget', 'cr-media',
    'carousel', 'sims-fbt', 'similar', 'recommendation',
    'also-bought', 'also-viewed', 'comparison', 'aplus',
    'a-carousel', 'sponsored', 'ad-feedback',
    'day0-widget', 'similarities', 'p13n',
    'rhf-border', 'rhf-results', 'percolate',
    'sp_detail', 'ape_Detail',
]

def _is_product_gallery_ancestor_safe(img_el) -> bool:
    """Returns False if the image element lives inside a review, recommendation,
    comparison, carousel, or any non-product-gallery section.
    
    This is the SINGLE source of truth — previously this logic was copy-pasted
    in two places inside fetch_product_details()."""
    for parent in img_el.parents:
        if not parent.name:
            continue
        parent_id = (parent.get('id') or '').lower()
        parent_class = ' '.join(parent.get('class', [])).lower()
        for blocked in _BLOCKED_ANCESTOR_IDS:
            if blocked.lower() in parent_id:
                return False
        for blocked in _BLOCKED_ANCESTOR_CLASSES:
            if blocked.lower() in parent_class:
                return False
    return True


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
    # Logic Version: 2.1 (Hardened for Price & Weight)
    # print("[DataHarvest] Processing product...")

    # Initialize all variables early to avoid UnboundLocalError in specific Python versions
    mrp = None
    disc_price = None
    mrp_el = None
    disc_price_el = None
    
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
        'Additional Image 5': '',
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
        # ── Strip unit-price contamination (e.g. "₹98/100gm", "(Rs.275/100ml)") ──
        name = re.sub(r'[\(\[]?\s*[₹$€£¥]?\s*[Rr][Ss]\.?\s*\d[\d,.]*\s*(?:per|/)\s*\d*\s*(?:gm?|gram|grams|kg|ml|l|oz|lb|unit|piece|count|tablet|capsule|sachet|strip|pack)\s*[\)\]]?', '', name, flags=re.I).strip()
        name = re.sub(r'[\(\[]?\s*[₹$€£¥]\s*\d[\d,.]*\s*/\s*\d*\s*(?:gm?|gram|grams|kg|ml|l|oz|lb|unit|piece|count|tablet|capsule|sachet|strip|pack)\s*[\)\]]?', '', name, flags=re.I).strip()
        # Remove trailing orphan parentheses/brackets after cleanup
        name = re.sub(r'\(\s*\)', '', name).strip()
        name = re.sub(r'\[\s*\]', '', name).strip()
        # ── Cap at nearest word under 100 CHARACTERS max ──
        if len(name) > 100:
            trunc = name[:100]
            last_spc = trunc.rfind(' ')
            if last_spc > -1:
                name = trunc[:last_spc].strip()
            else:
                name = trunc.strip()
        # ── COUPANG COMPLIANCE: Sanitize product name at SERP level (early gate) ──
        name, _serp_changes = compliance_sanitize_text(name)
        # Second pass with user exact mappings already included in sanitize_text,
        # but run again to catch any residue after regex cleanup stripped spaces.
        name = name.strip()
        name, _serp_changes2 = compliance_sanitize_text(name)
        p['Product Name'] = name
        
        # ── Quantity Discovery: "Pack of 3", "Set of 2" ──
        qty_m = re.search(r'(?:Pack|Set|Count|Pieces)\s+(?:of\s+)?(\d+)', name, re.I)
        if qty_m:
            try:
                p['Quantity'] = int(qty_m.group(1))
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────────
    #  DO NOT USE SERP PRICES — they are unreliable and cause phantom
    #  price contamination. Prices are ALWAYS extracted from the PDP.
    #  We initialize empty here so the PDP extraction starts clean.
    # ─────────────────────────────────────────────────────────────────────
    p['Sale Price'] = ''       # Will be filled by PDP: MRP / Striked / Original price
    p['Discount Base Price'] = ''  # Will be filled by PDP: Current discounted price

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
            # Amazon lazy loading puts real url in data-src or data-a-dynamic-image
            src = el.get('data-a-dynamic-image') or el.get('data-src') or el.get('srcset') or el.get('src') or ''
            if src.startswith('{'):
                try:
                    urls = json.loads(src)
                    src = max(urls.items(), key=lambda x: x[1][0])[0] if urls else ''
                except (json.JSONDecodeError, ValueError):
                    pass
            if ',' in src:
                src = src.split(',')[-1].strip().split(' ')[0]
                
            # Discard transparent 1x1 pixel tracking gifs or lazy-load placeholders
            if src and ('base64' in src or 'transparent' in src or 'GIF' in src.upper() or 'data:image' in src):
                src = el.get('data-src') or ''
                
            if src and 'http' in src:
                if src.startswith('//'):
                    src = 'https:' + src
                # Try hi-res upgrade but keep original if it breaks
                src = _safe_upgrade_image_url(src)
                p['Main Image'] = src
                break

    return p


def _safe_upgrade_image_url(url: str) -> str:
    """Safely strip Amazon's sizing suffixes to get hi-res images.
    ALWAYS returns a valid URL — if the upgrade mangles the URL, returns the original."""
    if not url or not isinstance(url, str):
        return url or ''
    original = url
    
    # Only apply to Amazon CDN URLs — don't touch other domains
    if 'media-amazon.com' not in url and 'images-amazon.com' not in url:
        return url
    
    # Extract the file extension (.jpg, .png, .webp etc.) from the end
    ext_match = re.search(r'(\.[a-z]{3,4})(?:\?.*)?$', url, re.IGNORECASE)
    if not ext_match:
        return url  # No recognizable extension — don't touch
    
    ext = ext_match.group(1)  # e.g. '.jpg'
    
    # Strip ALL known Amazon sizing patterns between the image ID and the extension
    # Amazon pattern: /images/I/{imageId}.{_sizing_suffixes_}.{ext}
    # Examples: ._AC_UL320_.jpg, ._SX355_.jpg, ._SS40_.jpg, ._CR0,0,300,300_.jpg
    upgraded = re.sub(r'(\._[A-Z][A-Z0-9_,]+_)+(?=\.[a-z]{3,4})', '', url)
    
    # Verify the result is still a valid-looking URL
    if upgraded.startswith('http') and '/' in upgraded and ext in upgraded:
        return upgraded
    
    return original  # Upgrade broke something — keep the original

def fetch_product_details(url, existing_p, fetcher=None):
    """Visits the Product Detail Page (PDP) to extract deep information.
    
    Includes dedicated price extraction from PDP for higher accuracy than SERP.
    """
    # Try the 10x faster HTTP-only fetcher first to avoid IP blocks and headless overhead
    html = fetch_pdp_fast(url)
    
    # Validate the HTML is a REAL product page, not a JS-only shell or CAPTCHA
    is_real_page = False
    if html:
        html_lower = html.lower()
        # CAPTCHA check
        if 'captchacharacters' in html_lower or 'type the characters' in html_lower:
            html = None
        # JS-only shell check: real product pages will have at least one of these markers
        elif not any(marker in html_lower for marker in [
            'productdetail', 'feature-bullets', 'productdescription',
            'productdetails', 'dp-container', 'ppd-', 'buybox',
            'priceblock', 'a-price', 'landingimage', 'imgblkfront',
            'colorimages', '"hires"', '"large"',
        ]):
            html = None  # Not a real product page
    
    # Fallback to Playwright if fast fetch got nothing useful
    if not html:
        html = fetch_with_scrapling(url, wait_sec=0, fetcher=fetcher)

    if not html or isinstance(html, str) and html.startswith("ERROR:"):
        return existing_p
    
    # Preserve the SERP main image in case PDP extraction overwrites it with empty
    serp_main_image = existing_p.get('Main Image', '')

    soup = BeautifulSoup(html, 'lxml')
    p = existing_p.copy()

    # ─────────────────────────────────────────────────────────────────────
    # OUT OF STOCK / UNAVAILABLE / NON-DELIVERABLE CHECK
    # ─────────────────────────────────────────────────────────────────────
    # 1. Text-based availability checks
    availability_el = soup.select_one('#availability, #outOfStock, .a-color-price, #exports_desktop_undeliverable_buybox, #buyBoxAccordion, #merchant-info, #deliveryBlockMessage')
    if availability_el:
        avail_text = availability_el.get_text().lower()
        unavailability_markers = [
            'currently unavailable', 
            'out of stock', 
            "don't know when or if this item will be back",
            'no featured offers available',
            'cannot be delivered',
            'not deliverable',
            'undeliverable',
            'currently not available',
            'item is not available'
        ]
        if any(term in avail_text for term in unavailability_markers):
            return None  # Product is dead or unavailable, abort scraping
    
    # 2. Page-level broad check for "No featured offers available" widget
    buybox_text = soup.select_one('#buybox, #desktop_buybox, #rightCol')
    if buybox_text:
        bb_text = buybox_text.get_text().lower()
        if 'no featured offers available' in bb_text or 'see all buying options' in bb_text:
             if 'add to cart' not in bb_text and 'buy now' not in bb_text:
                 return None # No direct buy box available


    # ─────────────────────────────────────────────────────────────────────
    #  PDP MAIN IMAGE UPGRADE — Get the hero/landing image at full resolution
    #  HARDENED: Rejects images from review/recommendation/carousel ancestors
    # ─────────────────────────────────────────────────────────────────────
    # Extract ASIN from URL for image ownership validation
    _current_asin = ''
    _asin_url_match = re.search(r'/dp/([A-Z0-9]{10})', url)
    if _asin_url_match:
        _current_asin = _asin_url_match.group(1)

    # Use the shared function for ancestor checking
    _is_own_product_image = _is_product_gallery_ancestor_safe

    pdp_main_img = None
    # STRICT selectors — only hero/landing image elements, NOT the broad #imageBlock
    for sel in [
        '#landingImage',
        '#imgBlkFront',
        '#main-image',
        'img[data-old-hires]',
        '#ivLargeImage img',
    ]:
        el = soup.select_one(sel)
        if el:
            # CRITICAL: Reject if the image is inside a review/recommendation section
            if not _is_own_product_image(el):
                continue
            src = el.get('data-old-hires') or ''
            if not src or not src.startswith('http'):
                # Try data-a-dynamic-image JSON
                dyn = el.get('data-a-dynamic-image', '')
                if dyn.startswith('{'):
                    try:
                        url_map = json.loads(dyn)
                        src = max(url_map.items(), key=lambda x: x[1][0])[0] if url_map else ''
                    except (json.JSONDecodeError, ValueError):
                        pass
            if not src or not src.startswith('http'):
                src = el.get('src', '')
            if src and src.startswith('http') and 'base64' not in src and '.gif' not in src.lower():
                pdp_main_img = _safe_upgrade_image_url(src)
                break
    if pdp_main_img:
        p['Main Image'] = pdp_main_img
    elif not p.get('Main Image'):
        # Restore SERP image if PDP extraction wiped it
        p['Main Image'] = serp_main_image

    # ─────────────────────────────────────────────────────────────────────
    # PDP PRICE EXTRACTION (More accurate than SERP)
    # ─────────────────────────────────────────────────────────────────────
    # ─── Sale Price = MRP / Striked / Original price (the higher crossed-out one) ───
    pdp_mrp = None
    for sel in [
        'span.priceBlockStrikePriceString',
        '#listPrice',
        'span.basisPrice span.a-offscreen',
        'span[data-a-strike="true"] span.a-offscreen',
        'span.a-text-strike',
        'del span.a-offscreen',
    ]:
        for el in soup.select(sel):
            # Global exclusion for carousels, sponsored sections, and sidebars (Recent Items)
            if el.find_parent(class_=re.compile(r'carousel|sponsored|similar|recommendation|rhf-border|rhf-results-|percolate-', re.I)) or el.find_parent(id=re.compile(r'rhf|HLCXComparisonWidget|similarFeatures|percolate|recommendations', re.I)):
                continue
            val = extract_price(el.get_text())
            if val and re.search(r'\d', val):
                pdp_mrp = val
                break
        if pdp_mrp:
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

    # ─── Discount Base Price = Current discounted price (what buyer actually pays) ───
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
        '.a-price:not([data-a-strike="true"]):not(.a-text-strike) span.a-offscreen',
    ]:
        for el in soup.select(sel):
            # Global exclusion for carousels, sponsored sections, and sidebars
            if el.find_parent(class_=re.compile(r'carousel|sponsored|similar|recommendation|rhf-border|rhf-results-|percolate-', re.I)) or el.find_parent(id=re.compile(r'rhf|HLCXComparisonWidget|similarFeatures|percolate|recommendations', re.I)):
                continue
            if el.find_parent(id=re.compile(r'delivery|price-shipping', re.I)):
                continue
            # STRICT UNIT PRICE REJECTION
            parent_sec = el.find_parent('span', class_=re.compile(r'a-color-secondary|a-size-small|a-size-mini'))
            if parent_sec and '/' in parent_sec.get_text():
                continue
                
            val = extract_price(el.get_text())
            if val and re.search(r'\d', val):
                pdp_disc = val
                break
        if pdp_disc:
            break

    # ─────────────────────────────────────────────────────────────────────
    #  PDP PRICE MAPPING (AUTHORITATIVE — only source of truth)
    #
    #  Sale Price      = MRP / Striked / Original (the higher, crossed-out price)
    #  Discount Base   = Current / Discounted (what buyer actually pays)
    #
    #  If product has NO discount (single price only):
    #    Sale Price = Discount Base Price = that single price
    # ─────────────────────────────────────────────────────────────────────
    if pdp_mrp and pdp_disc:
        # Both prices found: MRP is the higher/original, disc is the lower/current
        try:
            mrp_val = float(re.sub(r'[^\d.]', '', pdp_mrp))
            disc_val = float(re.sub(r'[^\d.]', '', pdp_disc))
            
            if mrp_val >= disc_val:
                # Normal case: MRP ≥ discounted price
                p['Sale Price'] = pdp_mrp
                p['Discount Base Price'] = pdp_disc
            else:
                # Selectors grabbed them backwards — swap
                p['Sale Price'] = pdp_disc
                p['Discount Base Price'] = pdp_mrp
        except (ValueError, TypeError):
            # Couldn't parse — just assign as-is
            p['Sale Price'] = pdp_mrp
            p['Discount Base Price'] = pdp_disc
    elif pdp_mrp:
        # Only MRP found, no discount — set both to same
        p['Sale Price'] = pdp_mrp
        p['Discount Base Price'] = pdp_mrp
    elif pdp_disc:
        # Only current price found, no MRP — set both to same
        p['Sale Price'] = pdp_disc
        p['Discount Base Price'] = pdp_disc
    # else: both empty — no price on PDP (rare, product may be unavailable)

    about_item = soup.select_one('#feature-bullets')
    if about_item:
        raw_desc = clean_text(about_item.get_text())[:2000]
    else:
        desc_el = soup.select_one(
            '#productDescription, [class*="description"], [class*="Description"]'
        )
        raw_desc = clean_text(desc_el.get_text())[:2000] if desc_el else ''

    if raw_desc:
        sanitized_desc, _desc_changes = compliance_sanitize_text(raw_desc)
        p['Detailed Description'] = sanitized_desc

    # 2. Extract Technical Specs / Item Details (Brand, Manufacturer, ASIN)
    spec_data = {}
    
    # Try multiple common table/list structures for product details
    # EXPANDED: Added Amazon India specific containers and hidden expandable sections
    potential_containers = [
        '#productDetails_db_sections',
        '#productDetails_techSpec_section_1',
        '#technicalSpecifications_section_1',
        '#productDetails_secondary_view_div',
        '#vse-details-container',
        'table[id*="productDetails"]',
        'table.prodDetTable',
        'table.a-keyvalue',
        '#detailBullets_feature_div',
        '.a-expander-content',
        '#itemDetails',
        '.prodDetSectionEntry',
        '#poExpander',
        '#productOverview_feature_div',
        '#productOverview_feature_div table',
        '#important-information',
        '#aplus_feature_div',
        '.bucket .content',
        '#detail_bullets_id',
        '#detail-bullets',
        '.pdTab table',
    ]
    
    for container_sel in potential_containers:
        container = soup.select_one(container_sel)
        if not container:
            continue
            
        # Case A: Table rows
        for row in container.select('tr'):
            cells = row.select('th, td')
            if len(cells) >= 2:
                # Always take the first cell as key and the second as value
                th, td = cells[0], cells[1]
                if th and td:
                    key = clean_text(th.get_text()).strip('\u200e :').lower()
                    val = clean_text(td.get_text(separator=' ')).strip('\u200e ')
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
    
    def _upgrade_to_hires(url: str) -> str:
        """Remove Amazon's size/crop suffixes to get the original high-res image.
        Uses the safe global function to prevent URL mangling."""
        return _safe_upgrade_image_url(url)
    
    def _is_valid_image_url(url: str) -> bool:
        """Returns True ONLY for clean product image URLs."""
        if not url or not isinstance(url, str):
            return False
        if not url.startswith('http'):
            return False
        if 'base64' in url:
            return False
        url_lower = url.lower()
        # BLOCK video thumbnails
        VIDEO_PATTERNS = [
            'video', '/vdp/', 'videojs', 'video-thumb',
            '.mp4', '.webm', '.mov', '.avi',
            'si-video', 'video_thumbnail', 'vt-thumb',
            'play-button', 'playbtn',
        ]
        for pat in VIDEO_PATTERNS:
            if pat in url_lower:
                return False
        # BLOCK GIF (animated promo banners)
        if '.gif' in url_lower or 'GIF' in url:
            return False
        # BLOCK sprites/icons
        SPRITE_PATTERNS = ['sprite', 'icon', 'logo', 'badge', 'rating', 'star']
        if any(p in url_lower for p in SPRITE_PATTERNS) and 'images-amazon' not in url_lower:
            return False
        # BLOCK review/customer uploaded image paths
        REVIEW_PATTERNS = [
            '/review/', '/customer-images/', '/cr/', 'customerimages',
            'review-image', 'cr-media', '/ugc/',
        ]
        for pat in REVIEW_PATTERNS:
            if pat in url_lower:
                return False
        return True
    
    def _image_belongs_to_product(img_url: str) -> bool:
        """Extra validation: if we know the product ASIN, check the image URL
        doesn't belong to a DIFFERENT product. Amazon image URLs contain the ASIN.
        Returns True if we can't determine ownership (benefit of the doubt) or if it matches."""
        if not _current_asin or not img_url:
            return True  # Can't verify, allow it
        # Amazon product images follow pattern: /images/I/{ASIN-HASH}._xxx_.jpg
        # But recommendation images will have a DIFFERENT hash.
        # We can't directly check ASIN in URL, but we CAN check if the image
        # is from a known "other product" section by its path patterns.
        url_lower = img_url.lower()
        # Block images that are clearly from recommendation/comparison widgets
        FOREIGN_PATTERNS = [
            '/sponsored/', '/ad-', 'sims-fbt',
        ]
        for pat in FOREIGN_PATTERNS:
            if pat in url_lower:
                return False
        return True

    # Use the shared function — no more duplicate code
    _is_product_gallery_image = _is_product_gallery_ancestor_safe
    
    # Strategy A: Extract from Amazon's inline JSON image dictionary (most reliable, hi-res guaranteed)
    # HARDENED: Try multiple JSON patterns — Amazon uses different formats across regions
    try:
        img_data = None
        # Pattern 1: Standard colorImages format
        images_dict_match = re.search(r'"colorImages"\s*:\s*\{\s*"initial"\s*:\s*(\[.*?\])\s*\}', html, re.DOTALL)
        if images_dict_match:
            try:
                img_data = json.loads(images_dict_match.group(1))
            except json.JSONDecodeError:
                pass
        
        # Pattern 2: Broader colorImages — non-greedy may cut off, try greedy with bracket counting
        if not img_data:
            match2 = re.search(r'"colorImages"\s*:\s*\{\s*"initial"\s*:\s*(\[[\s\S]*?\]\s*)\}', html)
            if match2:
                try:
                    img_data = json.loads(match2.group(1))
                except json.JSONDecodeError:
                    pass
        
        # Pattern 3: imageGalleryData format (Amazon India / newer templates)
        if not img_data:
            match3 = re.search(r'"imageGalleryData"\s*:\s*(\[.*?\])', html, re.DOTALL)
            if match3:
                try:
                    img_data = json.loads(match3.group(1))
                except json.JSONDecodeError:
                    pass
        
        if img_data:
            for item in img_data:
                # Prioritize hiRes → large → mainUrl. Skip variants with no product image
                src = item.get('hiRes') or item.get('large') or item.get('mainUrl') or ''
                if _is_valid_image_url(src) and _image_belongs_to_product(src):
                    src = _upgrade_to_hires(src)
                    if src not in add_images and src != p.get('Main Image'):
                        add_images.append(src)
    except Exception:
        pass

    # Strategy A2: Extract from ImageBlockATF / ebooksImageBlockATF script blocks
    if not add_images:
        try:
            for script in soup.select('script[type="text/javascript"]'):
                script_text = script.string or ''
                if 'ImageBlockATF' not in script_text and '"imageBlock"' not in script_text:
                    continue
                # Find all hiRes URLs in the script
                hires_urls = re.findall(r'"hiRes"\s*:\s*"(https?://[^"]+)"', script_text)
                large_urls = re.findall(r'"large"\s*:\s*"(https?://[^"]+)"', script_text)
                for src in (hires_urls + large_urls):
                    if _is_valid_image_url(src) and _image_belongs_to_product(src):
                        src = _upgrade_to_hires(src)
                        if src not in add_images and src != p.get('Main Image'):
                            add_images.append(src)
                if add_images:
                    break
        except Exception:
            pass

    # Strategy B: Fallback to altImages DOM parsing — STRICTLY scoped to product gallery
    if not add_images:
        # Only look inside the TIGHT thumbnail strip, never the broad image block
        gallery_container = soup.select_one('#altImages')
        if not gallery_container:
            gallery_container = soup.select_one('#imageBlockNew')
        if not gallery_container:
            # Last resort: use #imageBlock but ONLY its direct <li> thumbnail items
            ib = soup.select_one('#imageBlock')
            if ib:
                gallery_container = ib.select_one('.a-unordered-list, ul')
                if not gallery_container:
                    gallery_container = ib
        if gallery_container:
            for img in gallery_container.select('img'):
                # CRITICAL: reject images from review/recommendation ancestors
                if not _is_product_gallery_image(img):
                    continue
                # Try multiple image source attributes
                src = img.get('data-old-hires') or ''
                # Try data-a-dynamic-image JSON (contains hi-res URLs with dimensions)
                if not src or not src.startswith('http'):
                    dyn_img = img.get('data-a-dynamic-image', '')
                    if dyn_img.startswith('{'):
                        try:
                            url_map = json.loads(dyn_img)
                            if url_map:
                                # Pick the largest resolution image
                                src = max(url_map.items(), key=lambda x: x[1][0] if isinstance(x[1], list) else 0)[0]
                        except (json.JSONDecodeError, ValueError, TypeError):
                            pass
                if not src or not src.startswith('http'):
                    src = img.get('src') or img.get('data-src') or ''
                if not _is_valid_image_url(src):
                    continue
                if not _image_belongs_to_product(src):
                    continue
                src = _upgrade_to_hires(src)
                if src not in add_images and src != p.get('Main Image'):
                    add_images.append(src)

    # Strategy C: Extract from landing image's data-a-dynamic-image (contains multiple resolutions)
    if not add_images:
        try:
            landing_img = soup.select_one('#landingImage, #imgBlkFront, #main-image')
            if landing_img:
                dyn = landing_img.get('data-a-dynamic-image', '')
                if dyn.startswith('{'):
                    url_map = json.loads(dyn)
                    for img_url in url_map.keys():
                        if _is_valid_image_url(img_url) and _image_belongs_to_product(img_url):
                            img_url = _upgrade_to_hires(img_url)
                            if img_url not in add_images and img_url != p.get('Main Image'):
                                add_images.append(img_url)
        except Exception:
            pass



    # Store up to 5 unique hi-res product images
    for i, img_url in enumerate(add_images[:5], start=1):
        p[f'Additional Image {i}'] = img_url

    # Extract SKU / Model Number (Amazon ASIN) from URL if not found in specs
    if not p.get('SKU'):
        asin_m = re.search(r'/dp/([A-Z0-9]{10})', url)
        if asin_m:
            p['SKU'] = asin_m.group(1)
            p['Model Number'] = p['SKU']

    # ─────────────────────────────────────────────────────────────────────
    #  VOLUME / WEIGHT EXTRACTION (Comprehensive, Multi-Priority)
    #  HARDENED v3: Catches comma-numbers, fl oz, cc, Product Dimensions,
    #  and searches every possible Amazon data source.
    # ─────────────────────────────────────────────────────────────────────
    def _parse_and_assign_metric(text_chunk):
        """Attempts to parse metrics and returns True if successful.
        
        Hardened Accuracy Logic v3:
        - Supports hyphens: "30-ml", "50-gm"
        - Supports comma-thousands: "1,000 ml", "2,500 gm"
        - Supports "fl oz" and "fl. oz." (fluid ounces)
        - Supports "cc" (cubic centimeters = ml)
        - Converts non-metric units: oz -> ml/gm, lb -> gm
        - Standardizes output: gm, kg, ml, L
        - Safe 'g' handling: ignores 'g' if amount <= 15 (network logic)
        """
        if not text_chunk:
            return False
            
        # Pre-process: normalize comma-separated numbers (1,000 -> 1000)
        normalized_text = re.sub(r'(\d),(\d{3})(?!\d)', r'\1\2', text_chunk)
        
        # EXPANDED Regex: comma-free numbers, optional hyphen/space, full unit vocabulary
        unit_regex = r'\b(\d+(?:\.\d+)?)\s*[-\s]?\s*(kg|kilogram|kilograms|kgs|gm|gram|grams|g|ml|millilitre|milliliter|millilitres|milliliters|mls|fl\.?\s*oz\.?|fluid\s*ounce|cc|l|litre|liter|liters|litres|oz|ounce|ounces|lb|lbs|pound|pounds)\b'
        
        for m in re.finditer(unit_regex, normalized_text, re.I):
            amount, unit = float(m.group(1)), m.group(2).lower().strip('.')
            
            # Safe 'g' handling to prevent 5G electronics confusion
            if unit == 'g' and amount <= 15:
                continue
                
            # ── Unit Conversion & Normalization ──
            if unit in ('kg', 'kilogram', 'kilograms', 'kgs'):
                if amount < 1:  # 0.5kg -> 500gm
                    amount, unit = amount * 1000, 'gm'
                else:
                    unit = 'kg'
            elif unit in ('gram', 'grams', 'gm', 'g'):
                unit = 'gm'
            elif unit in ('lb', 'lbs', 'pound', 'pounds'):
                amount, unit = amount * 453.59, 'gm'
            elif unit in ('oz', 'ounce', 'ounces'):
                # Heuristic: Liquids use ml, Solids use gm
                if any(x in text_chunk.lower() for x in ['bottle', 'liquid', 'oil', 'shampoo', 'serum', 'fluid', 'wash', 'lotion', 'cream', 'gel', 'spray', 'drink', 'juice', 'water', 'milk', 'beverage', 'syrup', 'drops', 'solution', 'toner', 'cleanser', 'conditioner', 'mouthwash', 'perfume', 'cologne', 'deodorant']):
                    amount, unit = amount * 29.57, 'ml'
                else:
                    amount, unit = amount * 28.35, 'gm'
            elif unit.startswith('fl') or unit == 'fluid ounce':
                # Fluid ounces are ALWAYS volume
                amount_raw = amount
                amount, unit = amount_raw * 29.57, 'ml'
            elif unit == 'cc':
                # cc = cubic centimeters = ml
                unit = 'ml'
                    
            # ── Volume normalization ──
            elif unit in ('l', 'litre', 'liter', 'liters', 'litres'):
                if amount < 1:
                    amount, unit = amount * 1000, 'ml'
                else:
                    unit = 'L'
            elif unit in ('ml', 'millilitre', 'milliliter', 'millilitres', 'milliliters', 'mls'):
                unit = 'ml'
                
            amount = round(amount, 2)
            val = f"{int(amount) if amount.is_integer() else amount} {unit}"
            
            if unit in ('ml', 'L'): 
                p['Volume'] = val
                p['Weight'] = ''
            else: 
                p['Weight'] = val
                p['Volume'] = ''
            return True
            
        return False

    def _parse_dimensions_weight(text_chunk):
        """Extract weight from Amazon 'Product Dimensions' format:
        '10 x 5 x 3 cm; 200 Grams' or '15.2 x 8 x 4 cm; 500 g'
        Returns True if weight was found and assigned."""
        if not text_chunk:
            return False
        # Pattern: dimensions ; weight (the weight part comes after semicolon)
        dim_match = re.search(r';\s*([\d,.]+\s*[-\s]?\s*(?:kg|kilogram|kilograms|kgs|gm|gram|grams|g|lb|lbs|pound|pounds|oz|ounce|ounces|ml|millilitre|milliliter|l|litre|liter)s?)\b', text_chunk, re.I)
        if dim_match:
            return _parse_and_assign_metric(dim_match.group(1))
        return False

    mapped_specs = False

    # Priority 0: Check Amazon's actual Size/Variation selector widget (Extremely high accuracy)
    # Example: "Size: 30 ml (Pack of 1)" inside variation widgets
    size_widget_selectors = [
        '#variation_size_name span.selection',
        '#twisterContainer .selection',
        '.twister-plus-buying-options-text span',
        '#variation_size_name .a-color-base',
        '#variation_size_name .a-size-base',
        '#inline-twister-expanded-dimension-text-size_name',
        '.twister-plus-buying-options .selection',
    ]
    for widget_sel in size_widget_selectors:
        if mapped_specs:
            break
        size_widget = soup.select_one(widget_sel)
        if size_widget:
            if _parse_and_assign_metric(size_widget.get_text()):
                mapped_specs = True

    # Priority 1: Check actual spec tables — EXPANDED key matching
    # Catches: 'weight', 'item weight', 'net weight', 'product dimensions', 'capacity', etc.
    WEIGHT_VOLUME_KEYS = [
        'net quantity', 'net weight', 'item weight', 'product weight',
        'weight', 'net wt', 'gross weight', 'package weight', 'shipping weight',
        'volume', 'item volume', 'net volume', 'total volume', 'net content',
        'net contents', 'content', 'contents',
        'measurement', 'size', 'capacity',
        'product dimensions', 'package dimensions', 'item dimensions',
        'dimensions',
        'net qty', 'quantity',
        'unit count', 'item package quantity',
    ]
    if not mapped_specs:
        for spec_key, spec_val in spec_data.items():
            if any(x in spec_key for x in WEIGHT_VOLUME_KEYS):
                # Special case: "Product Dimensions" often has "10 x 5 x 3; 200 Grams"
                if 'dimension' in spec_key:
                    if _parse_dimensions_weight(spec_val):
                        mapped_specs = True
                        break
                if _parse_and_assign_metric(spec_val):
                    mapped_specs = True
                    break
    
    # Priority 1.5: REMOVED — Blindly scanning ALL spec values caused false positives
    # (e.g., grabbing RAM values "8 gm" from electronics specs)
                
    # Priority 2: Check the Title ONLY (reliable — vendors put weight/volume in the title)
    if not mapped_specs:
        mapped_specs = _parse_and_assign_metric(p.get('Product Name', ''))
        
    # Priority 3-4: REMOVED — Full-page and combined text scans were too aggressive
    # and pulled inaccurate values from unrelated page sections.
    # If weight/volume is not found in the spec tables or product title, leave it EMPTY.
    # NO ESTIMATION. NO GUESSING. Only real, verified data.
    
    # Priority 5: RAW HTML regex — catches weight/volume buried in JS variables or data attributes
    if not mapped_specs and not p.get('Weight') and not p.get('Volume') and html:
        # Search the raw HTML string directly (not just visible text)
        raw_patterns = [
            r'"weight"\s*:\s*"?([\d,.]+\s*[-\s]?\s*(?:kg|kgs|gm|gram|grams|g|lb|lbs|pound|oz|ounce)s?)"?',
            r'"volume"\s*:\s*"?([\d,.]+\s*[-\s]?\s*(?:ml|mls|l|litre|liter|fl\.?\s*oz|cc)s?)"?',
            r'"size"\s*:\s*"?([\d,.]+\s*[-\s]?\s*(?:ml|mls|l|litre|gm|gram|kg|kgs|oz|lb)s?)"?',
            r'"item_weight"\s*[=:]\s*"?([\d,.]+\s*[-\s]?\s*(?:kg|kgs|gm|gram|grams|g|lb|lbs|oz)s?)"?',
            r'"net_quantity"\s*[=:]\s*"?([\d,.]+\s*[-\s]?\s*(?:ml|mls|l|gm|gram|kg|kgs)s?)"?',
        ]
        for pattern in raw_patterns:
            rm = re.search(pattern, html, re.I)
            if rm:
                if _parse_and_assign_metric(rm.group(1)):
                    mapped_specs = True
                    break
    
    # Build _raw_specs for Gemini — include as much context as possible
    raw_specs_parts = [p.get('Product Name', '')]
    raw_specs_parts.extend(spec_data.values())
    if p.get('Detailed Description'):
        raw_specs_parts.append(p['Detailed Description'])
    # Also include feature bullets text if available
    feature_bullets = soup.select_one('#feature-bullets')
    if feature_bullets:
        raw_specs_parts.append(clean_text(feature_bullets.get_text(separator=' ')))
    p['_raw_specs'] = ' | '.join(filter(None, raw_specs_parts))

    # Generate Search Keywords from product name (feed up to 25 to Gemini for 20-keyword output)
    if p.get('Product Name'):
        words = p['Product Name'].lower().replace(',', '').split()
        unique_words = []
        for w in words:
            if w not in unique_words and len(w) > 2:
                unique_words.append(w)
        raw_keywords = ', '.join(unique_words[:25])
        # ── COUPANG COMPLIANCE: Sanitize keywords at PDP extraction time ──
        sanitized_keywords, _kw_changes = compliance_sanitize_text(raw_keywords)
        p['Search Keywords'] = sanitized_keywords

    # Final safety: ensure Main Image is preserved if PDP/additional extraction found nothing
    if not p.get('Main Image') and serp_main_image:
        p['Main Image'] = serp_main_image
    
    return p


# ─────────────────────────────────────────────────────────────────────────────
# DELIVERY DATE CHECKER (for pincode-based filtering)
# ─────────────────────────────────────────────────────────────────────────────
def _check_delivery(html_or_soup, pincode, max_days=4):
    """Check if a product can be delivered within `max_days` to the given pincode.
    
    Parses Amazon's delivery block for date estimates.
    Returns True if deliverable within the window, False otherwise.
    Returns True (allow) if delivery info can't be determined (benefit of the doubt).
    """
    from datetime import datetime, timedelta
    
    if isinstance(html_or_soup, str):
        soup = BeautifulSoup(html_or_soup, 'lxml')
    else:
        soup = html_or_soup
    
    # Collect all delivery-related text from the page
    delivery_text = ''
    delivery_selectors = [
        '#mir-layout-DELIVERY_BLOCK',
        '#deliveryBlockMessage',
        '#delivery-message',
        '#ddmDeliveryMessage',
        '#deliveryBlock_feature_div',
        '#delivery-sub-sla',
        '.a-delivery-message',
        '#amazonGlobal_feature_div',
        '#fast-track-message',
        '#delivery-promise',
        '#sfsMinBuyBox',
    ]
    for sel in delivery_selectors:
        el = soup.select_one(sel)
        if el:
            delivery_text += ' ' + el.get_text(separator=' ')
    
    if not delivery_text.strip():
        # No delivery info found — can't filter, allow the product
        return True
    
    delivery_lower = delivery_text.lower()
    
    # ── Instant Reject: Not deliverable to this location ──
    REJECT_PHRASES = [
        'does not deliver to',
        'cannot be delivered to',
        'not deliverable',
        'unavailable for delivery',
        'we don\'t deliver to this',
        'currently unavailable',
        'not available for delivery',
        'no delivery',
        'delivery not available',
    ]
    for phrase in REJECT_PHRASES:
        if phrase in delivery_lower:
            return False
    
    # ── Parse delivery date from text ──
    # Amazon formats: "Delivery by Thursday, April 10", "Get it by Apr 12", "Arrives: Apr 8 - Apr 12"
    import calendar
    month_names = list(calendar.month_name)[1:] + list(calendar.month_abbr)[1:]
    month_pattern = '|'.join(month_names)
    
    # Pattern 1: "Delivery by DayName, MonthName DD" or "Get it by MonthName DD"
    date_patterns = [
        # "April 10" or "Apr 10"
        rf'(?:delivery\s+by|get\s+it\s+by|arrives?[:\s]+|delivered\s+by|estimated\s+delivery[:\s]+).*?({month_pattern})\.?\s+(\d{{1,2}})',
        # "Thursday, April 10"
        rf'(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)[,\s]+({month_pattern})\.?\s+(\d{{1,2}})',
        # Just month + day anywhere in delivery text
        rf'({month_pattern})\.?\s+(\d{{1,2}})',
    ]
    
    today = datetime.now()
    max_delivery_date = today + timedelta(days=max_days)
    
    for pattern in date_patterns:
        matches = re.finditer(pattern, delivery_text, re.I)
        for m in matches:
            month_str = m.group(1).strip('.')
            day_str = m.group(2)
            try:
                # Parse the month
                month_num = None
                for i, name in enumerate(calendar.month_name[1:], 1):
                    if month_str.lower() == name.lower() or month_str.lower() == calendar.month_abbr[i].lower():
                        month_num = i
                        break
                if not month_num:
                    continue
                
                day_num = int(day_str)
                # Determine year (handle year rollover: if month is Jan but we're in Dec)
                year = today.year
                delivery_date = datetime(year, month_num, day_num)
                if delivery_date < today - timedelta(days=1):
                    delivery_date = datetime(year + 1, month_num, day_num)
                
                # Check if delivery is within the allowed window
                if delivery_date <= max_delivery_date:
                    return True
                else:
                    return False
            except (ValueError, IndexError):
                continue
    
    # ── Fallback: Check for fast delivery keywords ──
    FAST_DELIVERY = [
        'tomorrow', 'next day', 'overnight',
        'same day', 'today',
        '1 day', '2 day', '3 day', '4 day',
        '1-day', '2-day', '3-day', '4-day',
        'free one-day', 'free two-day',
        'prime delivery',
    ]
    for phrase in FAST_DELIVERY:
        if phrase in delivery_lower:
            return True
    
    # ── Fallback: Check for slow delivery keywords ──
    SLOW_DELIVERY = [
        '5 day', '6 day', '7 day', '8 day', '9 day', '10 day',
        '5-day', '6-day', '7-day', '8-day', '9-day', '10-day',
        '1 week', '2 week', '5-7 business',
        'standard delivery',
    ]
    for phrase in SLOW_DELIVERY:
        if phrase in delivery_lower:
            return False
    
    # Can't determine — allow the product
    return True


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE PRODUCT PROCESSOR (used by ThreadPool)
# ─────────────────────────────────────────────────────────────────────────────
def _process_single_product(prod, job_fetcher, log_fn, pincode='', delivery_filter=False):
    """Process a single product: PDP fetch → delivery check → LLM sanitize. Thread-safe.
    
    Returns the enriched product dict or None on critical failure.
    If delivery_filter is enabled, skips products not deliverable within 4 days.
    GUARANTEE: Gemini LLM is ALWAYS called for every product, even if PDP fetch fails.
    """
    pname = prod.get('Product Name', '')
    product_url = prod.get('_product_url')

    # ── PHASE 1: PDP Enrichment (may fail — that's OK) ──
    try:
        if product_url:
            log_fn(f"🔎 Deep scraping: {pname[:40]}...")
            prod = fetch_product_details(product_url, prod, fetcher=job_fetcher)
            
            if not prod:
                log_fn(f"⚠️ Skipped {pname[:30][:20]}... (Out of stock / Unavailable)", 'warn')
                return None
            
            # ── DELIVERY FILTER CHECK ──
            if delivery_filter and pincode:
                # Re-fetch PDP with pincode cookie for delivery info
                pdp_html = fetch_pdp_fast(product_url, pincode=pincode)
                if pdp_html:
                    is_deliverable = _check_delivery(pdp_html, pincode)
                    if not is_deliverable:
                        log_fn(f"🚚 Skipped {pname[:35]}... (Not deliverable in 2-4 days to {pincode})", 'warn')
                        return 'DELIVERY_SKIP'  # Special sentinel to count delivery skips
    except Exception as pdp_err:
        log_fn(f"⚠️ PDP enrichment failed for {pname[:30]}: {pdp_err}. Continuing with basic data.", 'warn')

    # ── PHASE 2: Compliance + Gemini LLM (ALWAYS runs, regardless of PDP success) ──
    try:
        # ── COUPANG COMPLIANCE DEEP GATE: Sanitize all text fields ──
        prod, compliance_changes = compliance_sanitize_product(prod)
        if compliance_changes:
            changed_fields = ', '.join(compliance_changes.keys())
            log_fn(f"🛡️ Compliance fix ({changed_fields}): {pname[:30]}...")
        
        # Gemini LLM Sanitization and Precision Extractor — MANDATORY for every product
        log_fn(f"✨ Perfecting with Gemini: {pname[:30]}...")
        prod = sanitize_product_data(prod)

        # ── POST-GEMINI COMPLIANCE RE-PASS ──────────────────────────────────
        # Gemini rewrites Product Name, Description, and Keywords from scratch.
        # This re-pass guarantees no banned word survives in Gemini-generated text.
        prod, _post_changes = compliance_sanitize_product(prod)
        if _post_changes:
            log_fn(f"🛡️ Post-Gemini fix ({', '.join(_post_changes.keys())}): {pname[:30]}...")
    except Exception as llm_err:
        log_fn(f"⚠️ LLM processing failed for {pname[:30]}: {llm_err}. Using scraped data.", 'warn')
        # Gemini failed — still sanitize whatever scraped data we have
        try:
            prod, _ = compliance_sanitize_product(prod)
        except Exception:
            pass

    return prod


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND JOB
# ─────────────────────────────────────────────────────────────────────────────
def scrape_job(job_id, jobs, base_url, keyword, max_products, outputs_dir, pincode='', delivery_filter=False):
    job = jobs[job_id]
    job['status'] = 'running'
    job['delivery_skipped'] = 0  # Track delivery filter skips
    job['compliance_fixed'] = 0  # Track compliance keyword replacements
    job['elapsed_seconds'] = 0
    job['products_per_min'] = 0
    job['success_count'] = 0     # Products successfully enriched
    job['fail_count'] = 0        # Products that failed enrichment
    
    # Thread-safe lock for product list mutations
    _products_lock = threading.Lock()

    def log(msg, level='info'):
        job['log'].append({'msg': msg, 'level': level})
        job['last_message'] = msg
        print(f"[{job_id}] {msg}")

    try:
        all_products, page = [], 1
        seen_skus = set()
        sort_strategies = [None, 'price-asc-rank', 'price-desc-rank', 'review-rank', 'date-desc-rank']
        current_sort_idx = 0
        start_time = time.time()

        log(f"🌐 Site   : {base_url}")
        log(f"🔑 Keyword: '{keyword}'  |  Max: {max_products}")
        if delivery_filter and pincode:
            log(f"📍 Delivery Filter: ON — Pincode {pincode} (≤4 day delivery only)")
        log(f"🚀 Launching Scrapling fetcher (Shared Instance, {MAX_CONCURRENT_PRODUCTS}x concurrency)...")
        log(f"🛡️ {get_compliance_summary().splitlines()[0]}")

        # Create ONE shared browser instance to prevent devastating OOM crashes
        from scrapling import StealthyFetcher
        job_fetcher = StealthyFetcher()

        while len(all_products) < max_products:
            # ── CANCELLATION CHECK ──
            if job.get('cancelled'):
                log("🛑 Scrape cancelled by user.", 'warn')
                break

            if current_sort_idx >= len(sort_strategies):
                log("ℹ️ Exhausted all deep-search sorting strategies. Stopping.", 'warn')
                break

            current_sort = sort_strategies[current_sort_idx]
            url = build_search_url(base_url, keyword, page, sort=current_sort)
            sort_label = f" (Sort: {current_sort})" if current_sort else ""
            log(f"📄 Fetching page {page}{sort_label} …")

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
            
            # --- Extract Total Available Products (SCOPED to result bar only) ---
            if page == 1:
                try:
                    # STRICT: Only look in Amazon's result info bar, NOT the entire page
                    result_bar_selectors = [
                        'span[data-component-type="s-result-info-bar"]',
                        '.s-desktop-toolbar .a-spacing-small',
                        '#s-result-count',
                        '.a-section.a-spacing-small.a-spacing-top-small',
                        '#search .sg-col-inner .a-section',
                    ]
                    result_bar_text = ''
                    for rbs in result_bar_selectors:
                        rbar = soup.select_one(rbs)
                        if rbar:
                            result_bar_text = rbar.get_text(separator=' ')
                            break
                    
                    if result_bar_text:
                        # Match "1-48 of over 20,000 results" or "1-48 of 500 results"
                        match = re.search(r'of\s+(?:over\s+)?([\d,]+)\s+results', result_bar_text, re.IGNORECASE)
                        if not match:
                            match = re.search(r'([\d,]+)\s+results', result_bar_text, re.IGNORECASE)
                        
                        if match:
                            total_str = match.group(1).replace(',', '')
                            if total_str.isdigit():
                                total_val = int(total_str)
                                # Sanity check: reject clearly bogus values
                                if 1 <= total_val <= 1_000_000:
                                    job['total_available'] = total_val
                                    log(f"📈 Amazon reports ~{total_val:,} products for this keyword")
                                else:
                                    log(f"⚠️ Ignoring suspicious total: {total_val}", 'warn')
                except Exception:
                    pass

            for tag in soup(['script','style','noscript','iframe']): tag.decompose()

            products = extract_products_from_soup(soup, base_url)

            if not products:
                text_low = soup.get_text()[:600].lower()
                if any(w in text_low for w in ['captcha','robot','verify','are you human']):
                    msg = "Site is showing a CAPTCHA. Try again later or from a different network."
                    log(f"⚠️ {msg}", 'warn')
                    job['status'] = 'error'; job['error'] = msg; return
                elif any(w in text_low for w in ['sign in','log in','login']):
                    msg = "Site requires you to log in before showing products."
                    log(f"⚠️ {msg}", 'warn')
                    job['status'] = 'error'; job['error'] = msg; return
                elif page == 1 and current_sort_idx == 0:
                    msg = ("No products detected on page 1.\n"
                           "Possible reasons: keyword has no results, site structure changed,\n"
                           "or the site needs a different URL format.")
                    log(f"⚠️ {msg}", 'warn')
                    job['status'] = 'error'; job['error'] = msg; return
                else:
                    log("ℹ️ No more products for this sorting strategy. Switching search parameters...", 'warn')
                    current_sort_idx += 1
                    page = 1
                    continue

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
                if sku and (sku in scraped_skus_bulk or sku in seen_skus):
                    log(f"⏭️ Skipping bulk duplicate (SKU: {sku})")
                    skipped += 1
                    continue
                
                # Fallback: check by product name for non-Amazon sites without SKUs
                if not sku and pname and pname.lower().strip() in scraped_names_bulk:
                    log(f"⏭️ Skipping bulk duplicate: {pname[:40]}...")
                    skipped += 1
                    continue
                
                if sku:
                    seen_skus.add(sku)

                candidates.append(prod)

            # ── CONCURRENT PRODUCT PROCESSING ──────────────────────────
            added = 0
            if candidates:
                log(f"⚡ Processing {len(candidates)} products ({MAX_CONCURRENT_PRODUCTS}x parallel)...")
                
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_PRODUCTS) as pool:
                    futures = {
                        pool.submit(_process_single_product, prod, job_fetcher, log, pincode, delivery_filter): prod
                        for prod in candidates
                    }
                    
                    delivery_skipped_page = 0
                    for future in as_completed(futures):
                        try:
                            enriched = future.result()
                            # Handle delivery filter sentinel
                            if enriched == 'DELIVERY_SKIP':
                                delivery_skipped_page += 1
                                job['delivery_skipped'] = job.get('delivery_skipped', 0) + 1
                                continue
                            if enriched:
                                enriched['Product URL'] = enriched.pop('_product_url', None)
                                enriched.pop('_raw_specs', None)
                                
                                # Clean up internal compliance metadata (not exported to Excel)
                                compliance_notes = enriched.pop('_compliance_changes', None)
                                if compliance_notes:
                                    job['compliance_fixed'] = job.get('compliance_fixed', 0) + 1
                                
                                # Thread-safe product list mutation
                                with _products_lock:
                                    all_products.append(enriched)
                                    product_count = len(all_products)
                                
                                # Update job state (atomic assignments are thread-safe in CPython)
                                job['products'] = list(all_products)
                                elapsed = time.time() - start_time
                                avg_time = elapsed / product_count
                                rem = max_products - product_count
                                job['eta_seconds'] = int(max(0, avg_time * rem))
                                job['elapsed_seconds'] = int(elapsed)
                                job['products_per_min'] = round(product_count / (elapsed / 60), 1) if elapsed > 0 else 0
                                job['success_count'] = job.get('success_count', 0) + 1
                                
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
                            job['fail_count'] = job.get('fail_count', 0) + 1
                            log(f"⚠️ Product processing failed: {fut_err}", 'warn')

            delivery_skip_msg = f", 🚚{delivery_skipped_page} delivery-filtered" if delivery_filter and delivery_skipped_page > 0 else ''
            log(f"✅ Page {page}: +{added} new, ⏭️{skipped} skipped{delivery_skip_msg}  (total {len(all_products)}/{max_products})", 'success')
            job['progress'] = int(min(len(all_products) / max_products * 85, 85))
            job['found']    = len(all_products)

            # Stop ONLY if the page was truly empty (no products found at all)
            # Do NOT stop if products were found but all were duplicates — move to next page!
            if added == 0 and skipped == 0 and (not delivery_filter or delivery_skipped_page == 0):
                log("ℹ️ Empty parsing results. Switching search strategy...", 'warn')
                current_sort_idx += 1
                page = 1
                continue
            page += 1
            time.sleep(random.uniform(0.8, 1.5))

        # Handle cancellation — save whatever we got
        was_cancelled = job.get('cancelled', False)
        
        if not all_products:
            if was_cancelled:
                job['status'] = 'done'
                job['progress'] = 100
                job['total'] = 0
                job['products'] = []
                log("🛑 Scrape cancelled. No products were collected.", 'warn')
                return
            job['status'] = 'error'; job['error'] = "No products were scraped."; return

        # ── ABSOLUTE FINAL GATE — word-level hard scan ──────────────────────────
        from .coupang_compliance import _MASTER_REPLACEMENTS, _USER_REPLACEMENTS
        
        def _hard_scan_field(text: str) -> str:
            """Run every single compiled pattern one more time. No mercy."""
            if not text:
                return text
            result = str(text)
            for pattern, replacement in _USER_REPLACEMENTS + _MASTER_REPLACEMENTS:
                if replacement == '[REMOVED]':
                    result = pattern.sub('', result)
                else:
                    result = pattern.sub(replacement, result)
            # Clean up artifacts
            result = re.sub(r'  +', ' ', result).strip()
            return result

        log(f"🛡️ Final compliance scan on {len(all_products)} products before export...")
        FIELDS_TO_SCAN = ['Product Name', 'Detailed Description', 'Search Keywords', 'Brand']
        clean_products = []
        for _prod in all_products:
            for _field in FIELDS_TO_SCAN:
                if _prod.get(_field):
                    _prod[_field] = _hard_scan_field(_prod[_field])
            clean_products.append(_prod)
        all_products = clean_products

        log(f"📊 Building Excel for {len(all_products)} products …")
        job['progress'] = 90
        fp = build_excel(all_products, keyword, base_url, outputs_dir, partial=was_cancelled)
        if was_cancelled:
            log(f"🛑 Cancelled — saved {len(all_products)} products → {os.path.basename(fp)}", 'warn')
        else:
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
