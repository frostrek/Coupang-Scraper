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
MAX_CONCURRENT_PRODUCTS = 5  # Reduced from 15 to 5 due to limits on EC2 t3a.medium (4GB memory) and API rate limits (HTTP & Gemini)

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
    BROWSERS = ["chrome120", "chrome124"]
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
        
        return response.body.decode('utf-8', errors='ignore')
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
        'Option Type 1': '',
        'Option Value 1': '',
        'Option Type 2': '',
        'Option Value 2': '',
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

def fetch_product_details(url, existing_p, fetcher=None, return_html=False, fast_only=False):
    """Visits the Product Detail Page (PDP) to extract deep information.

    Includes dedicated price extraction from PDP for higher accuracy than SERP.

    If return_html=True, returns (product_dict, html_str) so callers can reuse
    the already-fetched HTML for variant extraction without a second HTTP trip.
    If the product is unavailable the html is still returned so the caller can
    distinguish "page fetched but product dead" from "fetch failed entirely".
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
    if not html and not fast_only:
        html = fetch_with_scrapling(url, wait_sec=0, fetcher=fetcher)

    if not html or isinstance(html, str) and html.startswith("ERROR:"):
        return (existing_p, None) if return_html else existing_p
    
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
            return (None, html) if return_html else None  # Product is dead or unavailable, abort scraping
    
    # 2. Page-level broad check for "No featured offers available" widget
    # STRICTER OUT-OF-STOCK CHECK
    for el in soup.select('#buybox, #desktop_buybox, #rightCol, #availability, #deliveryBlockMessage'):
        txt = el.get_text().lower()
        if 'no featured offers available' in txt or 'currently unavailable' in txt or 'out of stock' in txt:
            return (None, html) if return_html else None  # Strict unavailability indicator
        if 'see all buying options' in txt and 'add to cart' not in txt:
            return (None, html) if return_html else None  # No direct buy box available
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
    # PDP PRICE EXTRACTION v3 — Hardened for Amazon India + Global
    #
    #  Sale Price      = MRP / Striked / Original (the higher, crossed-out one)
    #  Discount Base   = Current / Discounted (what buyer actually pays)
    # ─────────────────────────────────────────────────────────────────────
    # Constrain price queries to the main product section
    price_context = (
        soup.select_one('#corePriceDisplay_desktop_feature_div') or
        soup.select_one('#corePrice_desktop') or
        soup.select_one('#corePrice_feature_div') or
        soup.select_one('#centerCol') or
        soup
    )

    def _is_junk_price_ancestor(el):
        """Returns True if this price element is inside a carousel, EMI, coupon, Save block, or is a Unit Price."""
        # ── EXPLICIT UNIT PRICE CLASS MATCHING ──
        el_classes = ' '.join(el.get('class', [])).lower()
        if 'priceperunit' in el_classes or 'ppu' in el_classes:
            return True
            
        parent = el.parent
        parent_classes = ' '.join(parent.get('class', [])).lower() if parent else ''
        if 'priceperunit' in parent_classes or 'ppu' in parent_classes:
            return True

        if el.find_parent(class_=re.compile(r'carousel|sponsored|similar|recommendation|compare|comparison|rhf-border|rhf-results-|percolate-', re.I)):
            return True
        if el.find_parent(id=re.compile(r'rhf|compare|comparison|HLCX|similarFeatures|percolate|recommendations', re.I)):
            return True
        if el.find_parent(id=re.compile(r'emi|sns|coupon|promo|delivery|price-shipping', re.I)):
            return True
        if el.find_parent(class_=re.compile(r'emi|sns|coupon|promo', re.I)):
            return True
        
        # Look higher up for text indicating this is an offer/save amount, not the main price
        # Amazon often renders "Save ₹50 (20%)" in a div alongside the price. If we grab the 50, it breaks the lowest-price logic.
        wrapper = el.find_parent('tr') or el.find_parent('div', class_=re.compile(r'a-section|a-row|savings', re.I)) or el.parent
        wrapper_txt = (wrapper.get_text() or '').lower()
        if any(kw in wrapper_txt for kw in ['save', 'coupon', 'emi ', 'subscribe', 'cashback', 'with exchange']):
            # Exception: Sometimes the wrapper contains the main price AND the "save" text inside it. 
            # If the wrapper text is extremely long, it might be the whole buybox.
            if len(wrapper_txt) < 150: 
                # If the element itself is the strike-through MRP, it's NOT a save amount even if "save" is nearby.
                is_strike = (
                    el.get('data-a-strike') == 'true' or
                    el.find_parent(attrs={'data-a-strike': 'true'}) or
                    el.find_parent('span', class_=re.compile(r'a-text-strike', re.I)) or
                    el.find_parent('del')
                )
                if not is_strike:
                    return True
            
        # ── ROBUST UNIT PRICE REJECTION ──
        # Critical fix: If 'el' is already the .a-price span, use it directly. Otherwise look at parent.
        tgt_class = el.get('class') or []
        parent_price = el if 'a-price' in tgt_class else el.find_parent('span', class_='a-price')
        
        if parent_price:
            next_sib = parent_price.find_next_sibling(string=True)
            if next_sib and '/' in next_sib:
                return True
            prev_sib = parent_price.find_previous_sibling(string=True)
            if prev_sib and '/' in prev_sib:
                return True
            # Check the immediate wrapping span text just in case, heavily scoped length
            parent_span = parent_price.parent
            if parent_span and parent_span.name == 'span':
                span_txt = parent_span.get_text()
                if len(span_txt) < 50 and re.search(r'/\s*\d*\s*(?:gm?|gram|grams|kg|ml|l|oz|lb|unit|piece|count)', span_txt, re.I):
                    return True
                    
        # Extra layer of unit price rejection stringency:
        # Check if the text actually extracted by the regex was immediately followed by a slash somewhere in its container
        # CRITICAL FIX: Only apply this to small, tight containers. If applied to large containers (like the whole div),
        # it rejects the actual main price because the container encompasses the unit price too!
        parent_text = el.parent.get_text()
        if len(parent_text) < 50:
            if re.search(r'/\s*\d*\s*(?:gm?|gram|grams|kg|ml|l|oz|lb|unit|piece|count)', parent_text, re.I):
                return True
            
        return False

    # ─────────────────────────────────────────────────────────────────────
    # PDP PRICE EXTRACTION v5 — Fail-proof Highest/Lowest + Unit Price Fix
    # ─────────────────────────────────────────────────────────────────────
    # Find all prices inside the main buy box / center section
    price_context = (
        soup.select_one('#corePriceDisplay_desktop_feature_div') or
        soup.select_one('#corePrice_desktop') or
        soup.select_one('#corePrice_feature_div') or
        soup.select_one('#centerCol') or
        soup.select_one('#desktop_buybox') or
        soup
    )

    extracted_prices = []
    
    # 1. Grab EVERY price container in the price context (including those without .a-offscreen)
    for el in price_context.select('.a-price, .a-text-price, .basisPrice, .priceBlockStrikePriceString, .a-text-strike'):
        if _is_junk_price_ancestor(el):
            continue
            
        # Amazon often duplicates prices inside containers (e.g. <offscreen>₹248</offscreen><aria-hidden>₹248</aria-hidden>)
        # so el.get_text() might be "₹248.00₹248.00". extract_price elegantly pulls the first valid number.
        txt = el.get_text(separator=' ')
        val = extract_price(txt)
        if val and re.search(r'\d', val):
            # Parse numeric value for sorting
            try:
                num_val = float(re.sub(r'[^\d.]', '', val))
                extracted_prices.append((num_val, val))
            except (ValueError, TypeError):
                pass
                
    # 2. Check for explicit MRP text label as an additional fallback (Amazon India)
    for mrp_label in price_context.find_all(string=re.compile(r'M\.?R\.?P\.?\s*:?', re.I)):
        parent = mrp_label.find_parent()
        if parent:
            # Look broadly at the next span since format can vary
            price_el = parent.find_next('span', class_=re.compile(r'a-offscreen|a-price|a-text-strike|a-color-secondary'))
            if price_el and not _is_junk_price_ancestor(price_el):
                val = extract_price(price_el.get_text())
                if val and re.search(r'\d', val):
                    try:
                        num_val = float(re.sub(r'[^\d.]', '', val))
                        extracted_prices.append((num_val, val))
                    except:
                        pass

    # 3. Deduplicate and order the prices
    unique_prices = []
    seen_nums = set()
    for num, txt in extracted_prices:
        if num not in seen_nums:
            seen_nums.add(num)
            unique_prices.append((num, txt))
            
    # Sort descending (highest price first, lowest price last)
    unique_prices.sort(key=lambda x: x[0], reverse=True)

    # ── UNIT-PRICE GUARD ────────────────────────────────────────────────────
    # Amazon sometimes shows "₹658" alongside "₹11.74 /ml" (unit price).
    # The junk-filter above catches most of these, but not all.  As a last
    # line of defence: drop any price that is < 5 % of the highest price —
    # a real discounted price is never less than 5 % of MRP.
    if unique_prices:
        highest_val = unique_prices[0][0]
        threshold = highest_val * 0.05
        unique_prices = [(n, t) for n, t in unique_prices if n >= threshold]

    # 4. Map the Prices precisely
    if len(unique_prices) >= 2:
        # Highest = Original MRP (Sale Price / crossed-out price)
        # Second-highest = Current actual price (Discount Base Price)
        # We intentionally use [1] not [-1] so that if 3+ prices leak through,
        # we don't accidentally grab a third stray price as the "discounted" one.
        p['Sale Price'] = unique_prices[0][1]
        p['Discount Base Price'] = unique_prices[1][1]
    elif len(unique_prices) == 1:
        # Exactly one price found: product is not on discount
        single_price = unique_prices[0][1]
        p['Sale Price'] = single_price
        p['Discount Base Price'] = single_price
    else:
        # NO PRICES FOUND = Product is unavailable or out of stock. 
        # We must ABORT scraping this product so it doesn't appear in the sheet.
        return None

    # ── Description Extraction — multi-source, de-noised ─────────────────
    raw_desc_parts = []

    # Source 1: Feature bullets (#feature-bullets) — most info-dense
    feature_el = soup.select_one('#feature-bullets')
    if feature_el:
        # Remove the "About this item" heading that Amazon always injects at the top
        for hdr in feature_el.select('.a-declarative, h1, h2, h3, h4, [class*="heading"]'):
            hdr.decompose()
        txt = feature_el.get_text(separator='\n')
        # Strip the literal text "About this item" if it survived as plain text
        txt = re.sub(r'(?im)^\s*about\s+this\s+item\s*$', '', txt)
        txt = clean_text(txt).strip()
        if txt:
            raw_desc_parts.append(txt)

    # Source 2: Product description paragraph (#productDescription)
    desc_el = soup.select_one('#productDescription')
    if not desc_el:
        desc_el = soup.select_one('#aplus_feature_div, #aplus, .aplus-module')
    if desc_el:
        txt = clean_text(desc_el.get_text(separator='\n')).strip()
        if txt and txt not in ' '.join(raw_desc_parts):
            raw_desc_parts.append(txt)

    raw_desc = '\n\n'.join(raw_desc_parts)[:2000]

    if raw_desc:
        sanitized_desc, _desc_changes = compliance_sanitize_text(raw_desc)
        p['Detailed Description'] = sanitized_desc

    # ── Variant / Size Extraction from active swatches ──
    variant_size = None
    size_block = soup.select_one('#variation_size_name .selection, #variation_color_name .selection')
    if size_block:
        variant_size = size_block.get_text(strip=True)
    if not variant_size:
        swatch = soup.select_one('li.swatchSelect[data-dp-url] .twisterTextSpan, button.swatchAvailable.selected .twisterTextSpan')
        if swatch:
            variant_size = swatch.get_text(strip=True)
            
    if variant_size:
        v_low = variant_size.lower()
        if any(u in v_low for u in ['ml', 'liter', 'litre', ' l ', 'oz', 'fl oz']):
            p['Volume'] = variant_size
        elif any(u in v_low for u in [' g', 'kg', 'gram', 'ounce', 'pound', ' lb']):
            p['Weight'] = variant_size

    # ── PHASE 2: Deep Extraction from PDP Tables (Product Information / Technical Details) (Brand, Manufacturer, ASIN)
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
                    if key and val and key not in spec_data:
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
                if key and val and key.lower() != val.lower() and key not in spec_data:
                    spec_data[key] = val
            else:
                text = clean_text(li.get_text(separator=' '))
                if ':' in text:
                    parts = text.split(':', 1)
                    if len(parts) == 2:
                        key = parts[0].strip().lower()
                        val = parts[1].strip()
                        if key and val and key.lower() != val.lower() and key not in spec_data:
                            spec_data[key] = val
                    
        # Case C: Generic rows (divs)
        for row in container.select('.a-row'):
            text = clean_text(row.get_text())
            if ':' in text:
                parts = text.split(':', 1)
                key = parts[0].strip().lower()
                val = parts[1].strip()
                if key and val and key.lower() != val.lower() and key not in spec_data:
                    spec_data[key] = val

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
                    # Only map fallback if it's currently empty
                    if not p.get(field):
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
            'play-button', 'playbtn', 'play_icon',
            '/vse-vms-', 'vse', 'PT0_', 'play-',
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
            '/sponsored/', '/ad-', 'sims-fbt', 'dp-ads', '/aplus/', 'ads-center'
        ]
        for pat in FOREIGN_PATTERNS:
            if pat in url_lower:
                return False
        return True

    # Use the shared function — no more duplicate code
    _is_product_gallery_image = _is_product_gallery_ancestor_safe
    
    # Strategy A: Extract from Amazon's inline JSON image dictionary (most reliable, hi-res guaranteed)
    # HARDENED: Uses safe bracket-balancing to avoid regex truncating inner arrays inside the JSON block
    def _extract_json_array(text, start_idx):
        if text[start_idx] != '[': return None
        depth, in_string, escape = 0, False, False
        for i in range(start_idx, len(text)):
            c = text[i]
            if escape: escape = False; continue
            if c == '\\': escape = True; continue
            if c == '"' or c == "'": in_string = not in_string; continue
            if not in_string:
                if c == '[': depth += 1
                elif c == ']':
                    depth -= 1
                    if depth == 0: return text[start_idx:i+1]
        return None

    try:
        img_data = None
        # Pattern 1: Standard colorImages format
        start_match = re.search(r'[\'"]colorImages[\'"]\s*:\s*\{\s*[\'"]initial[\'"]\s*:\s*\[', html)
        if start_match:
            array_str = _extract_json_array(html, start_match.end() - 1)
            if array_str:
                try:
                    img_data = json.loads(array_str)
                except Exception:
                    pass
        
        # Pattern 2: imageGalleryData format
        if not img_data:
            start_match = re.search(r'[\'"]imageGalleryData[\'"]\s*:\s*\[', html)
            if start_match:
                array_str = _extract_json_array(html, start_match.end() - 1)
                if array_str:
                    try:
                        img_data = json.loads(array_str)
                    except Exception:
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
        
        unit_regex = r'\b(\d+(?:\.\d+)?)\s*[-\s]?\s*(kg|kilogram|kilograms|kgs|gm|gram|grams|g|ml|millilitre|milliliter|millilitres|milliliters|mls|fl\.?\s*oz\.?|fluid\s*ounce|cc|l|litre|liter|liters|litres|ltr|ltrs|oz|ounce|ounces|lb|lbs|pound|pounds)\b'
        
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
    
    return (p, html) if return_html else p


# ─────────────────────────────────────────────────────────────────────────────
# VARIANT EXTRACTION  (all shades / sizes on a single PDP — NO extra fetches)
# ─────────────────────────────────────────────────────────────────────────────
# Amazon's internal dimension key → friendly display label
_VARIANT_DIM_LABELS = {
    'color_name': 'Color',   'colour_name': 'Color',
    'color': 'Color',        'colour': 'Color',
    'shade': 'Shade',        'shade_name': 'Shade',
    'size_name': 'Size',     'size': 'Size',
    'flavor_name': 'Flavor', 'flavour_name': 'Flavor',
    'scent_name': 'Scent',
    'style_name': 'Style',   'pattern_name': 'Pattern',
    'material_type': 'Material',
    'edition': 'Edition',    'package_type': 'Package Type',
    'count': 'Count',        'item_package_quantity': 'Package Quantity',
}


def _extract_variant_data(html: str, base_asin: str = '') -> list:
    """Parse ALL variant ASIN/label pairs from the raw PDP HTML.

    Works entirely on the raw string — no extra HTTP requests.

    Returns a list of dicts:
        [{"asin": "B0XXX", "option_type_1": "Color", "option_value_1": "Pink",
                           "option_type_2": "Size",  "option_value_2": "30 ml"}, ...]

    Returns [] if no variant data is found.
    """
    if not html:
        return []

    variants = []

    # ─── Strategy 1: Mine asin_variation_values from raw HTML ─────────────
    # Amazon embeds this JSON in a <script> block.  Rather than trying to
    # parse the entire (nested) object — which always fails — we pull out
    # individual ASIN entries with a targeted per-entry regex:
    #   "B0XXXXXXXX": {"color_name": "Pink", "size_name": "30 ml"}
    # The inner object is always a flat key:string map, so [^{}]+ is safe.
    try:
        # Only scan the neighbourhood of asin_variation_values
        ctx = re.search(r'asin_variation_values', html)
        if ctx:
            window = html[max(0, ctx.start() - 200): ctx.start() + 500_000]

            # --- Dimension names array (optional but improves label quality) ---
            dims = []
            dims_m = re.search(r'"dimensions"\s*:\s*\[([^\]]+)\]', window)
            if dims_m:
                dims = re.findall(r'"([^"]+)"', dims_m.group(1))

            # --- Per-ASIN entries ----------------------------------------
            # Match: "B0XXXXXXXX": { ... } where inner content has no braces
            asin_entry_re = re.compile(
                r'"([A-Z0-9]{10})"\s*:\s*\{([^{}]+)\}', re.S
            )
            seen = set()

            for m in asin_entry_re.finditer(window):
                asin = m.group(1)
                if asin in seen:
                    continue
                seen.add(asin)

                # Parse inner k→v pairs  (all values are strings)
                props = dict(re.findall(r'"(\w+)"\s*:\s*"([^"]*)"', m.group(2)))
                if not props:
                    continue

                ot1 = ov1 = ot2 = ov2 = ''

                # Use the dimensions array if available; otherwise fall back
                # to the first recognised beauty/colour dimension key.
                if dims:
                    for idx, dk in enumerate(dims[:2]):
                        friendly = _VARIANT_DIM_LABELS.get(
                            dk.lower(), dk.replace('_', ' ').title()
                        )
                        val = props.get(dk, '')
                        if idx == 0:
                            ot1, ov1 = friendly, val
                        else:
                            ot2, ov2 = friendly, val
                else:
                    for dk in ('color_name', 'colour_name', 'shade_name',
                               'size_name', 'flavor_name', 'style_name'):
                        if dk in props:
                            ot1 = _VARIANT_DIM_LABELS.get(dk, dk.title())
                            ov1 = props[dk]
                            break

                if ov1:  # Only include if we extracted at least a label
                    variants.append({
                        'asin': asin,
                        'option_type_1': ot1, 'option_value_1': ov1,
                        'option_type_2': ot2, 'option_value_2': ov2,
                    })

            if variants:
                return variants
    except Exception:
        pass

    # ─── Strategy 2: New Amazon JSON Structure (sortedDimValuesForAllDims) ────────
    # Amazon has migrated many PDPs to a new React/Redux based frontend state.
    # Structure: "sortedDimValuesForAllDims": { "color_name": [...], "size_name": [...] }
    # Each key is a dimension type (color, size, flavor, etc.) — fully dynamic.
    try:
        if 'sortedDimValuesForAllDims' in html:
            pos = html.find('sortedDimValuesForAllDims')
            window = html[pos:pos+500_000]
            
            dims_re = re.compile(r'"([^"]+)"\s*:\s*\[(.*?)\]\}', re.S)
            
            # Collect per-ASIN data across ALL dimensions first
            # asin_data = { asin: [(dim_name, label), ...] }
            asin_data = {}
            dim_order = []  # preserve order dimensions appear in JSON

            for dim_match in dims_re.finditer(window):
                dim_key = dim_match.group(1)
                # Skip non-dimension keys that might match the regex
                if not any(kw in dim_key.lower() for kw in (
                    'color', 'colour', 'shade', 'size', 'flavor', 'flavour',
                    'scent', 'style', 'pattern', 'material', 'edition',
                    'package', 'count', 'quantity', 'name'
                )):
                    continue
                dim_name = _VARIANT_DIM_LABELS.get(dim_key.lower(), dim_key.replace('_', ' ').title())
                if dim_name not in dim_order:
                    dim_order.append(dim_name)

                items_str = dim_match.group(2)
                items = re.split(r'\},\{', items_str)
                
                for item in items:
                    asin_m = re.search(r'"defaultAsin"\s*:\s*"([A-Z0-9]{10})"', item)
                    label_m = re.search(r'"dimensionValueDisplayText"\s*:\s*"([^"]+)"', item)
                    
                    if asin_m and label_m:
                        asin = asin_m.group(1)
                        label = label_m.group(1)
                        if asin not in asin_data:
                            asin_data[asin] = {}
                        asin_data[asin][dim_name] = label

            # Now build the variant list with up to 2 option dimensions
            if asin_data:
                for asin, dims_dict in asin_data.items():
                    # Map the first two dimensions found for this ASIN
                    dim_items = [(d, dims_dict[d]) for d in dim_order if d in dims_dict]
                    ot1 = dim_items[0][0] if len(dim_items) > 0 else ''
                    ov1 = dim_items[0][1] if len(dim_items) > 0 else ''
                    ot2 = dim_items[1][0] if len(dim_items) > 1 else ''
                    ov2 = dim_items[1][1] if len(dim_items) > 1 else ''

                    if ov1:
                        variants.append({
                            'asin': asin,
                            'option_type_1': ot1, 'option_value_1': ov1,
                            'option_type_2': ot2, 'option_value_2': ov2,
                        })
            
            if variants:
                return variants
    except Exception:
        pass

    # ─── Strategy 3: DOM swatch img alt texts (no JS required) ────────────
    # Amazon renders each color swatch as an <li data-dp-url="...">
    # with a child <img alt="Shade Name">.  These ARE in the static HTML.
    try:
        soup_tmp = BeautifulSoup(html, 'lxml')

        # Detect dimension type from the variation heading label
        dim_type = 'Color'
        for head_sel in (
            '#variation_color_name .a-form-label',
            '#variation_colour_name .a-form-label',
            '#variation_shade_name .a-form-label',
            '#variation_size_name .a-form-label',
        ):
            el = soup_tmp.select_one(head_sel)
            if el:
                raw = el.get_text(strip=True).rstrip(':').strip().lower()
                dim_type = _VARIANT_DIM_LABELS.get(raw, raw.title())
                break

        seen_asins = set()

        for li in soup_tmp.select('li[data-dp-url], li.swatchAvailable'):
            dp_url = li.get('data-dp-url', '')
            asin_m = re.search(r'/dp/([A-Z0-9]{10})', dp_url)
            asin = asin_m.group(1) if asin_m else ''

            if asin and asin in seen_asins:
                continue
            if asin:
                seen_asins.add(asin)

            # Label = img alt → span text → title attr
            label = ''
            img = li.select_one('img')
            if img:
                label = img.get('alt', '').strip()
            if not label:
                sp = li.select_one('.twisterTextSpan, .a-button-text')
                if sp:
                    label = sp.get_text(strip=True)
            if not label:
                label = li.get('title', '') or li.get('aria-label', '')
            label = label.strip()

            if label:
                variants.append({
                    'asin': asin,
                    'option_type_1': dim_type, 'option_value_1': label,
                    'option_type_2': '',        'option_value_2': '',
                })

        if variants:
            return variants
    except Exception:
        pass

    return []




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
def _process_single_product(prod, job_fetcher, log_fn, pincode='', delivery_filter=False, job_ref=None):
    """Process a single product: Fast Fetch -> Gemini AI -> Output.

    Returns a LIST of product dicts:
      - element 0  : the base product (fully Gemini-enriched)
      - elements 1+: sibling variant rows (cloned, option columns filled in)
    Returns [] if the base product is unavailable / cancelled.
    """
    if job_ref and job_ref.get('cancelled'):
        return []

    pname = prod.get('Product Name', '')
    product_url = prod.get('_product_url')

    # ── PHASE 1: PDP Enrichment ───────────────────────────────────────────────
    # fetch_product_details now returns (product, html) so we can reuse the
    # validated PDP HTML for variant extraction — zero extra HTTP requests.
    base_html = None
    try:
        if job_ref and job_ref.get('cancelled'): return []
        if product_url:
            log_fn(f"Deep scraping: {pname[:40]}...")

            result = fetch_product_details(product_url, prod, fetcher=job_fetcher, return_html=True)
            if isinstance(result, tuple):
                prod, base_html = result
            else:
                prod = result  # backward-compat safety

            if not prod:
                log_fn(f"Skipped {pname[:30]}... (Out of stock / Unavailable)", 'warn')
                return []

            # ── DELIVERY FILTER CHECK ──
            if delivery_filter and pincode:
                # Re-use base_html if it exists, otherwise do a pincode-aware fetch
                delivery_html = base_html or fetch_pdp_fast(product_url, pincode=pincode)
                if delivery_html:
                    is_deliverable = _check_delivery(delivery_html, pincode)
                    if not is_deliverable:
                        log_fn(f"Skipped {pname[:35]}... (Not deliverable to {pincode})", 'warn')
                        return 'DELIVERY_SKIP'
    except Exception as pdp_err:
        log_fn(f"PDP enrichment failed for {pname[:30]}: {pdp_err}. Continuing with basic data.", 'warn')

    # ── PHASE 2: Compliance + Gemini LLM ─────────────────────────────────────
    try:
        if job_ref and job_ref.get('cancelled'): return []
        prod, compliance_changes = compliance_sanitize_product(prod)
        if compliance_changes:
            log_fn(f"Compliance fix ({', '.join(compliance_changes.keys())}): {pname[:30]}...")

        if job_ref and job_ref.get('cancelled'): return []

        log_fn(f"Perfecting with Gemini: {pname[:30]}...")
        prod = sanitize_product_data(prod)

        prod, _post_changes = compliance_sanitize_product(prod)
        if _post_changes:
            log_fn(f"Post-Gemini fix ({', '.join(_post_changes.keys())}): {pname[:30]}...")
    except Exception as llm_err:
        log_fn(f"LLM processing failed for {pname[:30]}: {llm_err}. Using scraped data.", 'warn')
        try:
            prod, _ = compliance_sanitize_product(prod)
        except Exception:
            pass

    # ── PHASE 3: Variant Row Generation (CLONE — no extra HTTP fetches) ───────
    # Extract all variant label/ASIN pairs from the HTML we already downloaded.
    result_rows = [prod]

    if base_html and isinstance(base_html, str):
        base_asin = prod.get('SKU', '')
        variant_data = _extract_variant_data(base_html, base_asin)

        if variant_data:
            log_fn(f"Found {len(variant_data)} variant(s) — adding as rows (no extra fetch)...")

        # Give the base product its Option Type/Value
        base_var = next((v for v in variant_data if v['asin'] == base_asin), None)
        if base_var:
            prod['Option Type 1']  = base_var.get('option_type_1', '')
            prod['Option Value 1'] = base_var.get('option_value_1', '')
            prod['Option Type 2']  = base_var.get('option_type_2', '')
            prod['Option Value 2'] = base_var.get('option_value_2', '')

        for var_info in variant_data:
            if job_ref and job_ref.get('cancelled'):
                break

            var_asin = var_info.get('asin', '')
            if not var_asin or var_asin == base_asin:
                continue

            # Clone base product and only overwrite the variant-specific fields
            var_prod = prod.copy()
            var_prod['Option Type 1']  = var_info.get('option_type_1', '')
            var_prod['Option Value 1'] = var_info.get('option_value_1', '')
            var_prod['Option Type 2']  = var_info.get('option_type_2', '')
            var_prod['Option Value 2'] = var_info.get('option_value_2', '')

            # Give the variant its own ASIN/SKU so each row is uniquely identified
            var_prod['SKU'] = var_asin
            var_prod['Model Number'] = var_asin + '-1'
            var_prod['Barcode'] = ''
            var_prod['_product_url'] = f"https://www.amazon.in/dp/{var_asin}"

            log_fn(f"  + Fetching variant details: '{var_info.get('option_value_1', var_asin)}'...")
            var_fetched = fetch_product_details(var_prod['_product_url'], var_prod, fetcher=None, fast_only=True)
            
            if var_fetched:
                result_rows.append(var_fetched)
            else:
                log_fn(f"  - Variant '{var_info.get('option_value_1', var_asin)}' skipped (Out of stock or fetch failed)")
            
            time.sleep(1.0)  # Polite delay to prevent Amazon PerimeterX bot detection

    return result_rows


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND JOB
# ─────────────────────────────────────────────────────────────────────────────
def scrape_job(job_id, jobs, base_url, keyword, max_products, outputs_dir, pincode='', delivery_filter=False, search_mode='category'):
    job = jobs[job_id]
    job['status'] = 'running'
    job['delivery_skipped'] = 0  # Track delivery filter skips
    job['compliance_fixed'] = 0  # Track compliance keyword replacements
    job['elapsed_seconds'] = 0
    job['products_per_min'] = 0
    job['success_count'] = 0     # Products successfully enriched
    job['fail_count'] = 0        # Products that failed enrichment
    job['db_saved_count'] = 0    # Products persisted to DB in real-time
    job['already_scraped_count'] = 0  # Products skipped as DB duplicates
    job['brand_filtered_count'] = 0   # Products removed by brand filter
    job['total_candidates_found'] = 0 # Raw products found before filtering
    job['pages_scraped'] = 0     # Actual search result pages fetched
    job['finish_reason'] = ''    # Why the scrape ended (for UI messaging)
    
    # Thread-safe lock for product list mutations
    _products_lock = threading.Lock()

    # ── SIGNAL HANDLING — graceful shutdown on SIGTERM/SIGINT ──
    import signal
    def _graceful_shutdown(signum, frame):
        job['cancelled'] = True
        print(f"[{job_id}] Received signal {signum}, triggering graceful shutdown...")
    try:
        signal.signal(signal.SIGTERM, _graceful_shutdown)
    except (OSError, ValueError):
        pass  # Can't set signal handler in non-main thread — that's OK

    def log(msg, level='info'):
        job['log'].append({'msg': msg, 'level': level})
        job['last_message'] = msg
        print(f"[{job_id}] {msg}")

    try:
        all_products = []
        base_products_collected = 0
        page = 1
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
        try:
            from scrapling import StealthyFetcher
            job_fetcher = StealthyFetcher()
        except ImportError as imp_err:
            job['status'] = 'error'
            job['error'] = f'Scrapling library not installed: {imp_err}'
            log(f'❌ {job["error"]}', 'error')
            return
        except Exception as fetch_init_err:
            job['status'] = 'error'
            job['error'] = f'Failed to initialize browser: {fetch_init_err}'
            log(f'❌ {job["error"]}', 'error')
            return

        while base_products_collected < max_products:
            # Final explicit Cancellation Check to break main loop completely
            if job.get('cancelled'):
                break

            if current_sort_idx >= len(sort_strategies):
                log("ℹ️ Exhausted all deep-search sorting strategies. Stopping.", 'warn')
                break

            current_sort = sort_strategies[current_sort_idx]
            url = build_search_url(base_url, keyword, page, sort=current_sort)
            sort_label = f" (Sort: {current_sort})" if current_sort else ""
            log(f"📄 Fetching page {page}{sort_label} …")

            if job.get('cancelled'): break

            # ── PAGE FETCH WITH RETRY (up to 3 attempts with backoff) ──
            html = None
            page_fetch_attempts = 3
            for _attempt in range(page_fetch_attempts):
                if job.get('cancelled'): break
                html = fetch_with_scrapling(url, wait_sec=5, fetcher=job_fetcher)
                if html and not (isinstance(html, str) and html.startswith("ERROR:")):
                    break  # Success
                if _attempt < page_fetch_attempts - 1:
                    wait_time = (2 ** _attempt) + random.uniform(0.5, 1.5)
                    log(f"⚠️ Page fetch attempt {_attempt + 1} failed, retrying in {wait_time:.1f}s...", 'warn')
                    time.sleep(wait_time)
                    html = None  # Reset for next attempt

            # Check for failure after all retries
            if not html or (isinstance(html, str) and html.startswith("ERROR:")):
                specific_err = html if html else "Unknown error occurred"
                msg = (f"Scrape Failed after {page_fetch_attempts} retries.\nDetailed Error: {specific_err}\n"
                       "This usually means the site is blocking access from this IP or Playwright crashed.")
                log(f"❌ {msg}", 'error')
                if len(all_products) > 0:
                    log(f"⚠️ Rescuing {len(all_products)} products before server crash...", 'warn')
                    break  # Trigger save
                else:
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
                    if len(all_products) > 0: break
                    else: job['status'] = 'error'; job['error'] = msg; return
                elif any(w in text_low for w in ['sign in','log in','login']):
                    msg = "Site requires you to log in before showing products."
                    log(f"⚠️ {msg}", 'warn')
                    if len(all_products) > 0: break
                    else: job['status'] = 'error'; job['error'] = msg; return
                elif page == 1 and current_sort_idx == 0:
                    msg = "No products detected on page 1 matching your structure or keywords."
                    log(f"⚠️ {msg}", 'warn')
                    break  # Gracefully exit to trigger the No Products state
                else:
                    log(f"ℹ️ Reached end of available products. Only {len(all_products)} found in total. Stopping search.", 'warn')
                    break

            # ── Filter duplicates BEFORE expensive PDP/LLM processing ──
            candidates = []
            skipped = 0
            brand_skipped_page = 0
            db_skipped_page = 0
            
            # Track raw product count before any filtering
            job['total_candidates_found'] = job.get('total_candidates_found', 0) + len(products)
            
            # ── STRICT BRAND FILTER (Name-Start-Only) ──
            # ONLY accept products where the product name BEGINS with the keyword.
            # Do NOT accept brand-field-only matches — those let random products through
            # because the brand field often falls back to the first word of the name.
            if search_mode == 'brand':
                kw_lower = keyword.lower().strip()
                kw_words = kw_lower.split()
                valid_prods = []
                for prod in products:
                    name_lower = str(prod.get('Product Name', '')).lower().strip()
                    
                    # Product name must START with the brand keyword
                    # Uses startswith directly to handle names that have punctuation attached
                    # e.g., kw="The Brand", name="The Brand-Something"
                    match_name = name_lower.startswith(kw_lower)
                    
                    if match_name:
                        valid_prods.append(prod)
                    else:
                        brand_skipped_page += 1
                        skipped += 1
                job['brand_filtered_count'] = job.get('brand_filtered_count', 0) + brand_skipped_page
                if brand_skipped_page > 0:
                    log(f"🏷️ Brand filter: kept {len(valid_prods)}, rejected {brand_skipped_page} (name must start with '{keyword}')")
                products = valid_prods
            
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
                if base_products_collected + len(candidates) >= max_products:
                    break
                
                sku = prod.get('SKU')
                pname = prod.get('Product Name', '')

                # ── Duplicate Prevention (SKU-based, then product-name fallback) ──
                if sku and (sku in scraped_skus_bulk or sku in seen_skus):
                    log(f"⏭️ Skipping already-scraped (SKU: {sku})")
                    db_skipped_page += 1
                    skipped += 1
                    continue
                
                # Fallback: check by product name for non-Amazon sites without SKUs
                if not sku and pname and pname.lower().strip() in scraped_names_bulk:
                    log(f"⏭️ Skipping already-scraped: {pname[:40]}...")
                    db_skipped_page += 1
                    skipped += 1
                    continue
                
                if sku:
                    seen_skus.add(sku)

                candidates.append(prod)
            
            job['already_scraped_count'] = job.get('already_scraped_count', 0) + db_skipped_page

            # ── CONCURRENT PRODUCT PROCESSING ──────────────────────────
            added = 0
            if candidates:
                log(f"⚡ Processing {len(candidates)} products ({MAX_CONCURRENT_PRODUCTS}x parallel)...")
                
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_PRODUCTS) as pool:
                    futures = {
                        pool.submit(_process_single_product, prod, job_fetcher, log, pincode, delivery_filter, job): prod
                        for prod in candidates
                    }
                    
                    delivery_skipped_page = 0
                    for future in as_completed(futures):
                        try:
                            enriched_result = future.result()
                            # Handle delivery filter sentinel
                            if enriched_result == 'DELIVERY_SKIP':
                                delivery_skipped_page += 1
                                job['delivery_skipped'] = job.get('delivery_skipped', 0) + 1
                                continue
                            # Normalise: _process_single_product returns a list, but
                            # guard against old-style None / dict returns just in case.
                            if enriched_result is None:
                                continue
                            if isinstance(enriched_result, dict):
                                enriched_result = [enriched_result]
                            
                            # Increment base product counter only once per base product processed
                            if enriched_result and len(enriched_result) > 0:
                                with _products_lock:
                                    base_products_collected += 1
                                    added += 1

                            for enriched in enriched_result:
                                if not enriched:
                                    continue
                                # Fix: default to '' instead of None for Product URL
                                enriched['Product URL'] = enriched.pop('_product_url', '') or ''
                                enriched.pop('_raw_specs', None)

                                # Clean up internal compliance metadata (not exported to Excel)
                                compliance_notes = enriched.pop('_compliance_changes', None)
                                if compliance_notes:
                                    job['compliance_fixed'] = job.get('compliance_fixed', 0) + 1

                                # Thread-safe product list mutation & job state update
                                with _products_lock:
                                    all_products.append(enriched)
                                    product_count = len(all_products)
                                    job['products'] = list(all_products)

                                # Update job stats
                                elapsed = time.time() - start_time
                                avg_time = elapsed / max(1, base_products_collected)
                                rem = max_products - base_products_collected
                                job['eta_seconds'] = int(max(0, avg_time * rem))
                                job['elapsed_seconds'] = int(elapsed)
                                job['products_per_min'] = round(product_count / (elapsed / 60), 1) if elapsed > 0 else 0
                                job['success_count'] = job.get('success_count', 0) + 1
                        except Exception as fut_err:
                            job['fail_count'] = job.get('fail_count', 0) + 1
                            log(f"Product processing failed: {fut_err}", 'warn')


            delivery_skip_msg = f", 🚚{delivery_skipped_page} delivery-filtered" if delivery_filter and delivery_skipped_page > 0 else ''
            db_skip_msg = f", 💽{db_skipped_page} already-scraped" if db_skipped_page > 0 else ''
            brand_skip_msg = f", 🏷️{brand_skipped_page} brand-mismatch" if brand_skipped_page > 0 else ''
            log(f"✅ Page {page}: +{added} new base products (total {len(all_products)} rows), ⏭️{skipped} skipped{db_skip_msg}{brand_skip_msg}{delivery_skip_msg}", 'success')
            job['progress'] = int(min(base_products_collected / max_products * 85, 85))
            job['found']    = len(all_products)
            job['pages_scraped'] = page

            # Stop ONLY if the page was truly empty (no products found at all)
            # Do NOT stop if products were found but all were duplicates — move to next page!
            if added == 0 and skipped == 0 and (not delivery_filter or delivery_skipped_page == 0):
                log(f"ℹ️ Reached end of available products. Only {len(all_products)} found in total. Stopping search.", 'warn')
                break
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
                job['finish_reason'] = 'cancelled_empty'
                log("🛑 Scrape cancelled. No products were collected.", 'warn')
                return
            
            # ── DETERMINE FINISH REASON for smart UI feedback ──
            total_found = job.get('total_candidates_found', 0)
            brand_filtered = job.get('brand_filtered_count', 0)
            already_scraped = job.get('already_scraped_count', 0)
            
            if total_found == 0:
                job['finish_reason'] = 'no_products_on_site'
                job['status'] = 'done'; job['progress'] = 100; job['total'] = 0; job['products'] = []
                log("📭 No products found on Amazon for this keyword.", 'warn')
            elif search_mode == 'brand' and brand_filtered > 0 and already_scraped == 0:
                job['finish_reason'] = 'brand_mismatch'
                job['status'] = 'done'; job['progress'] = 100; job['total'] = 0; job['products'] = []
                log(f"🏷️ Found {total_found} products, but none matched brand '{keyword}'.", 'warn')
            elif already_scraped > 0 and brand_filtered == 0:
                job['finish_reason'] = 'all_already_scraped'
                job['status'] = 'done'; job['progress'] = 100; job['total'] = 0; job['products'] = []
                log(f"💽 All {already_scraped} matching products were already scraped. Nothing new.", 'warn')
            elif already_scraped > 0 and brand_filtered > 0:
                job['finish_reason'] = 'mixed_already_scraped_and_brand'
                job['status'] = 'done'; job['progress'] = 100; job['total'] = 0; job['products'] = []
                log(f"💽🏷️ {already_scraped} already scraped + {brand_filtered} brand mismatch. No new products.", 'warn')
            else:
                job['finish_reason'] = 'no_products_on_site'
                job['status'] = 'error'; job['error'] = "No products were scraped."
            return

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

        job['finish_reason'] = 'success'
        job.update({'status':'done','progress':100,'filepath':fp,'total':len(all_products),'products':all_products})

    except Exception as e:
        import traceback
        job['status'] = 'error'; job['error'] = str(e)
        log(f"💥 {e}", 'error')
        print(traceback.format_exc())
        
        # ── EMERGENCY RESCUE BLOCK ──
        # If a fatal error happens, secure the products collected so far.
        if 'all_products' in locals() and all_products:
            log(f"⚠️ Interrupted! Attempting to rescue {len(all_products)} collected products...", 'warn')
            try:
                # [MODIFIED] DB save removed — user triggers via Download button
                # Build Excel for emergency download
                fp = build_excel(all_products, keyword, base_url, outputs_dir, partial=True)
                job.update({'filepath': fp, 'total': len(all_products), 'products': all_products})
                log(f"✅ Emergency save successful: {os.path.basename(fp)} saved.", 'success')
            except Exception as rescue_err:
                log(f"❌ Emergency save failed: {rescue_err}", 'error')
    finally:
        # Prevent Memory Leaks! Explicitly close browser
        if 'job_fetcher' in locals() and job_fetcher:
            try:
                log("🧹 Cleaning up browser instance...")
                if hasattr(job_fetcher, 'stop'):
                    job_fetcher.stop()
            except Exception as clean_err:
                print(f"[Cleanup] Failed to stop fetcher: {clean_err}")
