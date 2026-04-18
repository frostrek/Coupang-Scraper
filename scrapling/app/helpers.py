import re
from urllib.parse import urlparse, quote_plus

def clean_text(t):
    return re.sub(r'\s+', ' ', (t or '').strip())

def extract_price(t):
    """Extract a clean price string from raw text. Handles ₹, $, €, £, ¥ and comma-formatted numbers."""
    if not t:
        return ''
    
    # Strip HTML entities and extra whitespace
    t = t.replace('\xa0', ' ').strip()
    
    # Strip per-unit pricing patterns like "(₹999 / 100g)" but NOT arbitrary slashes (URLs etc.)
    t = re.sub(r'[₹$€£¥]?\s*[\d,]+\.?\d*\s*/\s*\d*\s*(?:gm?|gram|grams|kg|ml|l|oz|lb|unit|piece|count|tablet|capsule|sachet|strip|pack)\b', '', t, flags=re.I)
    
    def _format_decimals(price_str):
        if not price_str: return price_str
        m = re.search(r'([\d,]+)(?:\.(\d+))?', price_str)
        if m:
            whole = m.group(1)
            frac = m.group(2)
            if not frac:
                frac = "00"
            elif len(frac) == 1:
                frac += "0"
            elif len(frac) > 2:
                frac = frac[:2]
            return price_str[:m.start()] + f"{whole}.{frac}" + price_str[m.end():]
        return price_str

    # Find a price pattern: optional currency symbol, then digits with optional commas and decimal
    m = re.search(r'([₹$€£¥])\s*([\d,]+\.?\d*)', t)
    if m:
        return _format_decimals((m.group(1) + m.group(2)).strip())
    
    # Fallback: just digits with commas/decimals (no currency symbol)
    m = re.search(r'([\d,]+\.?\d+)', t)
    if m:
        val = m.group(1)
        # Reject tiny numbers that are clearly not prices (like ratings "4.5")
        try:
            if float(val.replace(',', '')) < 1:
                return ''
        except ValueError:
            pass
        return _format_decimals(val.strip())
    
    return ''

def get_domain(url):
    return urlparse(url).netloc.lower()

def build_search_url(base_url, keyword, page=1, sort=None):
    d  = get_domain(base_url)
    s  = urlparse(base_url).scheme
    kw = quote_plus(keyword)
    if 'amazon'   in d: 
        url = f"{s}://{d}/s?k={kw}&page={page}"
        if sort:
            url += f"&s={sort}"
        return url
    if 'flipkart' in d: return f"{s}://{d}/search?q={kw}&page={page}"
    if 'nykaa'    in d: return f"{s}://{d}/search/result/?q={kw}&page={page}"
    if 'meesho'   in d: return f"{s}://{d}/search?q={kw}&page={page}"
    if 'snapdeal' in d: return f"{s}://{d}/search?keyword={kw}&page={page}"
    if 'ebay'     in d: return f"{s}://{d}/sch/i.html?_nkw={kw}&_pgn={page}"
    if 'walmart'  in d: return f"{s}://{d}/search?q={kw}&page={page}"
    if 'myntra'   in d: return f"{s}://{d}/{keyword.replace(' ','-')}?p={page}"
    if 'ajio'     in d: return f"{s}://{d}/s/{keyword.replace(' ','-')}?rows=45&start={(page-1)*45}"
    return f"{base_url.rstrip('/')}/search?q={kw}&page={page}"
