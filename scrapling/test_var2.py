import sys
sys.stdout.reconfigure(encoding='utf-8')
from app.scraper import fetch_pdp_fast, _extract_variant_data

url = 'https://www.amazon.in/dp/B074V7DDRY'
html = fetch_pdp_fast(url)

if html:
    print(f"HTML fetched, length: {len(html)}")
    variants = _extract_variant_data(html, 'B074V7DDRY')
    print(f"Extracted variants: {len(variants)}")
    for v in variants:
        print(v)
else:
    print("Fetch failed")
