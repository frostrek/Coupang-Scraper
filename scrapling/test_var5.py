import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling, _extract_variant_data

# First do a search to find a real ASIN
search_url = 'https://www.amazon.in/s?k=maybelline+lipstick'
search_html = fetch_with_scrapling(search_url, wait_sec=2)

if search_html and not (isinstance(search_html, str) and search_html.startswith("ERROR")):
    match = re.search(r'data-asin="([A-Z0-9]{10})"', search_html)
    if match:
        asin = match.group(1)
        print(f"Found ASIN: {asin}")
        url = f'https://www.amazon.in/dp/{asin}'
        html = fetch_with_scrapling(url, wait_sec=2)
        if html and not (isinstance(html, str) and html.startswith("ERROR")):
            print(f"HTML fetched, length: {len(html)}")
            variants = _extract_variant_data(html, asin)
            print(f"Extracted variants: {len(variants)}")
            for v in variants:
                print(v)
        else:
            print("PDP fetch failed")
    else:
        print("No ASIN found in search")
else:
    print("Search failed")
