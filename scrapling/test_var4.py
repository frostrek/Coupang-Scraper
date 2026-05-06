import sys, re
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_with_scrapling, _extract_variant_data

url = 'https://www.amazon.in/dp/B0068VQLN0' # Another popular lipstick ASIN
html = fetch_with_scrapling(url, wait_sec=2)

if html and not (isinstance(html, str) and html.startswith("ERROR")):
    print(f"HTML fetched, length: {len(html)}")
    variants = _extract_variant_data(html, 'B0068VQLN0')
    print(f"Extracted variants: {len(variants)}")
    for v in variants:
        print(v)
else:
    print(f"Fetch failed: {html}")
