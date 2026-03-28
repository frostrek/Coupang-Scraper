import re
from urllib.parse import urlparse, quote_plus

def clean_text(t):
    return re.sub(r'\s+', ' ', (t or '').strip())

def extract_price(t):
    m = re.search(r'[\$₹€£¥]?\s*[\d,]+\.?\d*', t or '')
    return m.group().strip() if m else clean_text(t)

def get_domain(url):
    return urlparse(url).netloc.lower()

def build_search_url(base_url, keyword, page=1):
    d  = get_domain(base_url)
    s  = urlparse(base_url).scheme
    kw = quote_plus(keyword)
    if 'amazon'   in d: return f"{s}://{d}/s?k={kw}&page={page}"
    if 'flipkart' in d: return f"{s}://{d}/search?q={kw}&page={page}"
    if 'nykaa'    in d: return f"{s}://{d}/search/result/?q={kw}&page={page}"
    if 'meesho'   in d: return f"{s}://{d}/search?q={kw}&page={page}"
    if 'snapdeal' in d: return f"{s}://{d}/search?keyword={kw}&page={page}"
    if 'ebay'     in d: return f"{s}://{d}/sch/i.html?_nkw={kw}&_pgn={page}"
    if 'walmart'  in d: return f"{s}://{d}/search?q={kw}&page={page}"
    if 'myntra'   in d: return f"{s}://{d}/{keyword.replace(' ','-')}?p={page}"
    if 'ajio'     in d: return f"{s}://{d}/s/{keyword.replace(' ','-')}?rows=45&start={(page-1)*45}"
    return f"{base_url.rstrip('/')}/search?q={kw}&page={page}"
